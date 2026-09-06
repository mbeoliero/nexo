# Platform integration

## User ids

| Prefix | Who | Example |
| --- | --- | --- |
| `u___` | platform user, int64 id | `u___123` |
| `ag__` | platform agent | `ag__7` |
| `nx__` | native account (UUIDv7) | `nx__01a06a92-...` |

Conversation ids: single `si_<a>:<b>` (ids sorted), group `sg_<group_id>`.

## Client tokens (platform JWT)

HS256 with one of `auth.external_jwt.secrets`. Claims:

```json
{"user_id": 123, "role": "user", "exp": 1790000000}
```

`user_id` maps to `u___123` (`ag__` when role is `agent`). The token carries no platform: HTTP sends
`X-Platform-Id` (default `auth.default_platform_id`), WS sends `platform_id` in the query. Platform ids
follow open-im: 1 iOS 2 Android 3 Windows 4 macOS 5 Web 6 MiniWeb 7 Linux 8 AndroidPad 9 iPad 10 Admin.

Before a platform user can send or receive, the backend must create the profile:

```
POST /api/v1/internal/user/upsert   {"id":"u___123","nickname":"...","avatar":"...","extra":""}
```

User and group `extra` values are limited to **65,535 UTF-8 bytes**, not characters. Empty strings are
allowed; larger values return `10001` before any write, without truncation. The same limit applies to
HTTP, internal and embedded calls. For partial profile updates, omitted/`null` extra leaves it unchanged;
`"extra":""` clears it.

## Internal channel (backend → nexo), HMAC

Headers: `X-Service-Name`, `X-Timestamp` (unix seconds, ±`max_skew_seconds`), `X-Nonce` (≥16 random
bytes, unique per request), `X-User-Id` (only on as-user routes), `X-Platform-Id` (optional), `X-Signature`.

```
sig = hex(HMAC-SHA256(secret,
        service + "\n" + ts + "\n" + nonce + "\n" + METHOD + "\n"
        + rawPath + "\n" + rawQuery + "\n" + userId + "\n" + platformId + "\n"
        + hex(sha256(body))))
```

`rawPath` / `rawQuery` are the actual HTTP request-target split at its first `?`; keep the mount prefix,
percent escapes and query ordering, without the `?` in `rawQuery`. Sign the exact body bytes sent on the
wire. Empty header values sign as empty strings, even when a platform default is applied after verification.
This signature is incompatible with the legacy scheme that omitted nonce, user/platform headers and query.
Nonces are rejected when replayed within 2×`max_skew_seconds`.

With `internal_auth.require_tls`, use a directly negotiated TLS connection or a TLS-terminating proxy
whose socket peer address is covered by `server.trusted_proxies` and which sets `X-Forwarded-Proto: https`.
An untrusted client cannot satisfy the TLS requirement by supplying this header; an empty trusted-proxy
list permits direct TLS only. Proxies must overwrite forwarded headers rather than pass client values through.

Go reference: `auth.Sign(secret, auth.InternalRequest{...})` in `internal/auth/internal.go`. Go callers should use
`github.com/mbeoliero/nexo/sdk` (`sdk.New(baseUrl, sdk.WithInternalAuth(service, secret))`, `Internal*` methods with
`sdk.AsUser(id)`); it signs the full path, mount prefix included.

Routes: `GET /internal/health`, `POST /internal/user/upsert`, `GET /internal/user/info`,
`GET /internal/user/online_status`; as-user: `POST /internal/message/send`, `GET /internal/conversation/list`,
`POST /internal/group/{create,join,kick}`. All under `/api/v1`.

`GET /api/v1/internal/health` succeeds with `{"code":0,"message":"","data":{"status":"ok"}}`.
Callers previously reading top-level `status` must read `data.status`; deploy this server response before
using the stricter SDK. The LB `/healthz` response remains top-level `status`/`node_id`, without an envelope.

Platform-sent messages default to `sender_read=false`: the sender's own devices receive the push and see
it as unread. Custom payloads use `content_type=100`.

## HTTP envelope and error codes

`{"code":0,"message":"","data":{...}}`. Codes are five digits `K MM NN`: `1xxxx` business (handle on the
client), `2xxxx` system (retry later). The middle pair groups the codes: `00` generic, `01` auth, `02` user,
`03` group, `04` message, `05` conversation, `06` connection. Success and ordinary business errors use
HTTP 200; authentication refusals use 401, permission refusals 403, and rate limiting (`10005`) 429.
System errors use 500 by default, `20101` uses 503, and the timeout code uses 504. Unknown server errors
map to `500/20001`. The WS handshake also uses 400 for bad parameters, 429 for connection quotas and
503 for draining nodes.

Authentication dependency failures retain distinct mappings:

| Failure | HTTP / code |
| --- | --- |
| Native TokenStore read during Bearer verification or WS handshake | 503 / 20101 |
| Login token write, or Logout deletion after Bearer succeeds | 500 / 20001 |
| Internal nonce Cache.SetNX | 500 / 20002 |

Logout atomically deletes the platform slot only when it still matches the request's token ID. A request
that passed Bearer before a newer same-platform login can finish successfully without revoking that newer
token. If the old token was already replaced before Bearer verification, the request is still rejected
with 401. There is no read-then-delete window. Embedded callers must supply the verified Identity.TokenId;
a missing token ID is an invalid parameter. The SDK's local concurrent Login/Logout ordering below is
unchanged; serialize those calls when ordering matters.

Logout runs Bearer first: a read failure stops it with `503/20101` before deletion. Dependency failures
are fail-closed but do not prove the credential invalid; do not clear a token solely because of them.
External-token verification does not consult TokenStore. An established native WS tolerates two
consecutive unavailable rechecks, then sends `2002` with `reason=token_expired` and closes on the third;
a successful check resets that count. There is no HTTP error envelope on an established WS.
Internal signature/service/time/replay refusals remain `401/10002`. Wrong login credentials use
`200/10105`, distinct from Bearer credential refusals.

Treat an unrecognised code by its first digit: `1xxxx` is the client's problem (show it, do not retry the
same request), `2xxxx` is ours (retry with backoff). New codes are added within existing groups, so do not
match on the full list.

## Go SDK concurrency and response validation

Share a `*sdk.Client` across requests and token updates: `SetToken`, `Token`, Login and Logout synchronize
local token access. Each request takes one snapshot; changing the token does not rewrite an in-flight
request. Successful Login stores the returned token, successful Logout clears it, and failed calls preserve
it. Concurrent auth operations are not generation-ordered; serialize them if their order matters. Apply
constructor options only in `New`, do not copy a used Client, and provide a concurrency-safe custom transport.

Success requires HTTP 2xx and an explicit non-null integer `code=0`. Data-returning methods also require
non-null `data` that decodes into the DTO; individual business fields are not revalidated. No-data methods
allow missing/null data. Invalid envelopes and responses exceeding 8 MiB return errors rather than empty
successes; malformed response bodies are not echoed in error text. `sdk.Error.Code=0` on a non-nil error
means an HTTP/protocol failure without an envelope error code, not success. Handle `err` before `CodeOf`.

## WebSocket

`GET /ws?token=<jwt>&platform_id=<1..10>[&encoding=json&compression=none]` (or `Authorization: Bearer`).
Text frames, JSON, `data` is a nested object. Empty response `message` is omitted.
The message's `content` is itself a string containing JSON, for example
`"content":"{\"text\":\"hello\"}"`; this differs from the outer frame's nested `data`.

```jsonc
{"req_id":1003,"op_id":"uuid","msg_incr":"c-17","data":{...}}                        // request
{"req_id":1003,"op_id":"uuid","msg_incr":"c-17","code":0,"data":{...}}               // response
{"req_id":2001,"op_id":"uuid","data":{...}}                                          // server push
```

| req_id | Direction | data |
| --- | --- | --- |
| 1001 GetMaxSeqs | C→S | `{cursor?, limit≤200}` → `{items:[{conversation_id,max_seq,min_seq,read_seq}], next_cursor, has_more}` |
| 1002 PullMsgBySeqRange | C→S | `{conversation_id, begin_seq, end_seq, limit≤100}` → `{messages[], has_more}` |
| 1003 SendMsg | C→S | `{client_msg_id, session_type(1 single/2 group), recv_id \| group_id, content_type, content, sender_read=true}` → `{server_msg_id, conversation_id, seq, send_time}` |
| 1004 MarkRead | C→S | `{conversation_id, read_seq}` → `{read_seq}` |
| 2001 PushMsg | S→C | full message |
| 2002 KickOnline | S→C | `{reason: new_login \| token_expired \| over_limit}`; do not reconnect |
| 2003 ConvRead | S→C | `{conversation_id, read_seq}` |
| 2004 Resync | S→C | `{reason}`; run 1001 then 1002 for gaps |

A rejected handshake answers with the envelope and an HTTP status that says whether retrying helps:

| Status | code | Meaning | Client |
| --- | --- | --- | --- |
| 400 | 10001 | bad `platform_id`, encoding or compression | fix the request |
| 401 | 10101/10102/10103 | token invalid, expired or missing | refresh the token, then reconnect |
| 429 | 10601 | this user, token or IP is at its connection limit | back off, then retry |
| 503 | 20101 | native authentication dependency unavailable | keep the token; retry with backoff |
| 503 | 10604 | the node is draining (rolling restart) | reconnect at once; the LB picks another node |

The per-connection frame rate limit applies before JSON decoding, including malformed frames. A frame
rejected by that limit gets `code=10005`, `req_id=0`, and no echoed `op_id` or `msg_incr`; three consecutive
limit violations close the connection. Inflight-request limits still echo the decoded request identifiers.

Defaults are a server ping every 30s and a 75s read timeout (`ws.ping_interval` / `ws.pong_wait`).
Pongs and subsequent message reads renew the deadline; 75s is not a maximum connection lifetime.
Same-token reconnects do not kick each other. If an external-token connection misses a kick but keeps
exchanging messages/pongs, it can remain connected until token expiry or another close condition;
there is no fixed 75s recovery guarantee. A replaced native token is also detected by periodic TokenStore
checks, but an unchanged token is not revoked by reconnecting.

Graceful server-initiated closes use code 1001 (reconnect through
the LB, then resync); forced closes, including an exhausted shutdown deadline, can close the socket
without a close frame. A draining connection admits no new requests. Queue flushing has a 10s overall
budget; an earlier node shutdown deadline wins. In-flight requests may commit without an ACK: reuse the
same message ID when retrying after reconnecting is permitted (a kicked client must re-authenticate).
The `content` string must contain valid JSON within `limits.max_content_bytes` (default 8192 UTF-8
bytes). For every recognized content type, the sender accepts any JSON value, including arrays, scalars
and null; it does not enforce an object or validate type-specific fields. The recommended typed wire
objects are `{"text":"..."}`, `{"image":"<url>"}`, `{"video":"<url>"}`, `{"audio":"<url>"}` and
`{"file":"<url>","name":"..."}`. Storing valid JSON does not guarantee that `msgbody.Parse` will accept
its shape or produce a nonempty preview.

`client_msg_id` must be 1–64 bytes and must not end in Unicode whitespace (Go `unicode.IsSpace`,
including spaces, tabs, newlines and ideographic spaces). HTTP, internal and WS sends return `10001`
for violations, including retries. IDs are rejected, never trimmed; leading and internal whitespace
remain unchanged.

For a newly stored message, the server commits the transaction, synchronously calls Publish with a
five-second context budget independent of request cancellation, starts the asynchronous offline-push
task, and returns the ACK. Publication failure does not roll back the message. The context budget is
cooperative, not a forced interruption of a custom publisher that ignores cancellation. ACK does not
wait for recipient delivery or offline-push completion; either may occur before or after the ACK.
A missing ACK does not imply a failed commit: retry with the same `client_msg_id`.

Sending a new message never decreases its conversation's message timestamps or existing user-conversation
sort keys, even under clock rollback; equal millisecond timestamps are valid. Use `seq`, not time, for strict ordering.
Retries retain the original ACK and do not refresh timestamps. MarkRead clamps its target to a single
membership/conversation snapshot, so quitting while new messages arrive cannot advance its response,
stored cursor or broadcast beyond the frozen visible range.

Client sync rule: keep `local_max` per conversation. On connect or 2004 run 1001; for each conversation pull
`[max(local_max+1, min_seq), max_seq]`. On 2001 with `seq == local_max+1` apply, `seq > local_max+1` pull
the gap first, `seq <= local_max` drop.

## Offline push webhook

`POST offline_push.webhook_url` with `{event_id, user_ids, notification, preview}`; `event_id` is
`conversation_id:seq`, dedupe on it. `notification` carries the facts (`conversation_id`, `seq`,
`session_type`, `sender_id`, `group_id`, `content_type`, `content`, `send_time`). Headers `X-Nexo-Timestamp` and
`X-Nexo-Signature = hex(HMAC-SHA256(webhook_secret, ts + "\n" + hex(sha256(body))))`. One attempt, 3s
timeout. Redirects are never followed, including same-host HTTPS redirects; any 3xx is a failed attempt,
not retried. Configure the final HTTPS receiver URL directly so neither the signed payload nor its headers
are forwarded to another endpoint. `preview` is a default text (the message text or `[图片]` etc.); render your own from `notification.content` if you prefer.
