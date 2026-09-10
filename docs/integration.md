# Platform integration

Current HTTP/WS contracts and client integration. Architecture and transaction invariants live in
[design.md](design.md); unimplemented synchronization proposals are separate in [sync-design.md](sync-design.md).

## HTTP API

### Public routes

Prefix `/api/v1`. Except for register/login, all routes require `Authorization: Bearer <token>` and may
carry `X-Platform-Id`. Native register/login/logout are available only when `auth.providers` includes `native`.

| Method | Path | Input / result |
| --- | --- | --- |
| POST | /auth/register | username, password, nickname → native user id |
| POST | /auth/login | username, password, platform_id → token |
| POST | /auth/logout | revoke only the request token |
| GET | /user/me | current profile |
| PUT | /user/me | nickname, avatar, extra |
| GET | /user/info?user_ids=a,b | profiles |
| GET | /user/online_status?user_ids=a,b | `{items:[{user_id, online, platform_ids}]}` |
| POST | /group/create | name, member_ids |
| POST | /group/join | group_id; no approval workflow |
| POST | /group/quit | group_id |
| POST | /group/kick | administrator / owner only |
| GET | /group/info?group_id= | members only |
| GET | /group/members?group_id= | members only |
| POST | /message/send | same input / ACK as WS 1003 |
| GET | /message/pull | same range / result as WS 1002 |
| GET | /message/max_seqs | same cursor / result as WS 1001 |
| GET | /conversation/list | cursor, limit≤100, with_last_message |
| POST | /conversation/read | same input / result as WS 1004 |
| PUT | /conversation/opt | recv_msg_opt, is_pinned |

Conversation lists return `{conversations:[{…, unread, read_seq, max_seq, last_message?}], next_cursor, has_more}`.
The cursor is unpadded base64url of `<updated_at in Unix milliseconds>:<conversation_id>`, ordered by
`updated_at DESC, conversation_id DESC`; GetMaxSeqs uses the same cursor shape. `last_message` respects
the caller's visible range and is absent when that range is empty. `is_pinned` is stored but does not affect
server ordering; clients may move pinned entries within the pages they have loaded. Query mechanics are
in [design §8.8](design.md#88-会话列表服务端排序--服务端返回-last_message).

LB health is a separate `GET /healthz` (or `<prefix>/healthz` when mounted). It requires no Bearer and
returns top-level `status` / `node_id`, not a business envelope: HTTP 200 with `status:"ok"` when the
database probe succeeds, or 503 with `status:"unavailable"` when it fails.

### Internal routes

Prefix `/api/v1/internal`. Every route uses [HMAC](#internal-channel-backend--nexo-hmac).
As-user routes additionally require `X-User-Id`; they share handlers with the public routes.

| Method | Path | As user | Input / result |
| --- | --- | --- | --- |
| GET | /health | no | `{"code":0,"message":"","data":{"status":"ok"}}` |
| POST | /user/upsert | no | `{id, nickname, avatar, extra}`; platform `u___` / `ag__` ids, idempotent |
| GET | /user/info?user_ids= | no | profiles |
| GET | /user/online_status?user_ids= | no | online platforms |
| POST | /message/send | yes | sender = `X-User-Id`; custom messages use `content_type=100` |
| GET | /conversation/list | yes | caller's conversations |
| POST | /group/create, /group/join, /group/quit, /group/kick | yes | same business inputs as public routes; the Go SDK exposes matching Internal* methods |

For `/internal/health`, callers previously reading top-level `status` must read `data.status`.
Deploy the server's envelope response before upgrading to the SDK that strictly validates it.

### Profile fields

User and group `extra` values are limited to **65,535 UTF-8 bytes**, not characters. Empty strings are
allowed; larger values return `10001` before any write, without truncation. The same limit applies to
HTTP, internal and embedded calls. For partial profile updates, omitted/`null` extra leaves it unchanged;
`"extra":""` clears it.

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

## Native tokens

When `native` is enabled, registration creates a native account and login issues its token. Login signs
HS256 with `auth.native.secret`; claims are `{sub: user_id, pid: platform_id, jti: token_id, exp}`. The signed `pid`
is authoritative; a request cannot choose a different platform for that token. WS still requires a valid
`platform_id` query parameter. Each user/platform slot holds one current token, so a later login on the
same platform invalidates the previous token. Logout revokes only its request token; concurrency and
dependency-failure behavior are defined [below](#http-envelope-and-error-codes).

## Internal channel (backend → nexo), HMAC

Headers: `X-Service-Name` (must be in `allowed_services`), `X-Timestamp` (unix seconds,
±`max_skew_seconds`), `X-Nonce` (≥16 random bytes encoded as hex/base64, unique per request),
`X-User-Id` (required on as-user routes, otherwise empty), `X-Platform-Id` (optional, default 5), `X-Signature`.

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
match on the full list. A code's class and module never change: server logs/metrics classify system errors
with `^2\d{4}$`, or a module such as message with `^\d04\d{2}$`; system failures are logged at error level
and expose only a generic client message.

## Go SDK

Use [`sdk/`](../sdk/) for the public and internal routes above; the independent LB `/healthz` is not
wrapped. The [compiled example](../sdk/example_test.go) shows login, requests and internal HMAC signing.
`WithPlatformId` sets the client's default platform; per-request platform/user options apply only to
methods declaring `...RequestOption`. `WithHttpClient` accepts a custom `*http.Client`.

### Concurrency and response validation

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
Only `encoding=json` and `compression=none` are accepted. Text frames, JSON, `data` is a nested object,
not base64. Responses echo `op_id` and `msg_incr`; server pushes generate `op_id` and omit `msg_incr`.
Empty response `message` is omitted; `code` retains the full error code.
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
| 1006 SetOnlineSubscriptions | C→S | `{revision, user_ids}` → `{revision, snapshot_interval_ms}`; replace this connection's online-status subscription set |
| 2001 PushMsg | S→C | full message |
| 2002 KickOnline | S→C | `{reason: new_login \| token_expired \| over_limit}`; do not reconnect |
| 2003 ConvRead | S→C | `{conversation_id, read_seq}` |
| 2004 Resync | S→C | `{reason}`; run 1001 then 1002 for gaps |
| 2005 OnlineChanged | S→C | `{revision, items:[{user_id, online, platform_ids}], stale?}`; complete online-status snapshot, see below |

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

An idempotent Send may publish the original stored message again. The fast path shares the ordinary
message-send quota: if no quota remains, it skips republication but still returns the original ACK.
In a group it also rechecks that the sender is still a member of a group that is not dismissed,
before any quota is spent; a sender who is no longer allowed to post gets the original ACK with no
republication and no 403. That recheck is not taken under the conversation row lock, so a removal
racing a retry can still let one republication through. A concurrent duplicate detected inside the
transaction has already spent its quota and does not spend it twice. The event uses the original
stored content and the current request's sender connection; other sender devices and recipients can
therefore receive a duplicate 2001. Deduplicate by `(conversation_id, seq)`, including repeated
ACKs. HTTP, internal and embedded sends use the same service behavior. Republication does not update
timestamps, allocate a new sequence, or invoke offline push again.
It is an attempt to repair a missing live push, not a delivery receipt or a guaranteed recovery path.

Until an ACK arrives or the user cancels, retry unacknowledged sends with the original `client_msg_id`
and bounded backoff. After reconnecting, limit immediate resends and retain backoff for the rest so old
requests do not exhaust the shared quota for new messages. Known Bus failures do not turn a committed
message's ACK into an error and do not schedule a server-side retry. Keep periodic reconciliation even
after receiving an ACK: it proves storage, not delivery or successful processing by another client.

Sending a new message never decreases its conversation's message timestamps or existing user-conversation
sort keys, even under clock rollback; equal millisecond timestamps are valid. Use `seq`, not time, for strict ordering.
Retries retain the original ACK and do not refresh timestamps. MarkRead clamps its target to a single
membership/conversation snapshot, so quitting while new messages arrive cannot advance its response,
stored cursor or broadcast beyond the frozen visible range.

### Client synchronization

Persist one `local_max` per conversation, initially 0: it is a continuous synchronization baseline, not a
local message database. On connect, foreground recovery, 2004 or app-push wakeup, page through 1001 and
pull `[max(local_max+1, min_seq), max_seq]` in 1002 pages. On rejoining a group, raise the baseline to at
least `min_seq-1`; if the resulting begin is above the server maximum, set `local_max=max_seq` and skip
the empty range. Otherwise advance to the target only after completing the pull. This also handles a
server reset that lowers the maximum. On 2001 with `seq == local_max+1` apply, `seq > local_max+1` pull
the gap first, and `seq <= local_max` drop.

GetMaxSeqs uses a moving `updated_at` cursor: a conversation updated during pagination may move before
the cursor, so finishing one round is not a consistent-snapshot completeness proof. Keep the client's
periodic reconciliation fallback for silent last-message loss. Online-status subscriptions do not replace
message reconciliation; server-driven message synchronization and its acceptance gates remain a
[draft](sync-design.md), with unsupported reserved frame numbers. A baseline does not
mean message bodies survived an app restart: reload visible history when the local view has no bodies.

### Online status subscriptions

1006 replaces the complete subscription set for the current WS connection. Authentication, user-ID
validation and access scope match `/user/online_status`; this does not introduce friend-based authorization.
Subscribe only to users needed by the visible conversation list and active chat. Combine overlapping UI
references into one set and remove a user only after its last reference is released; group chats do not
subscribe to all members automatically. An empty set cancels all subscriptions, confirms immediately,
does not query OnlineStore, and produces no 2005. Subscriptions are not persisted across connections.
Wire types and subscription limits live in
[`internal/gateway/presence.go`](../internal/gateway/presence.go).

`revision` is a positive, increasing integer within one connection. A lower revision returns
`10001 InvalidParam` without changing the set; the same revision and set is an idempotent retry, and
the same revision with a different set returns `10001`. The response confirms that the set was accepted,
not that its initial state has been read. OnlineStore read failures do not reject an accepted set.
An invalid ID or excessive per-connection count returns `10001`; exhausting the node's total subscription
references returns `10005 TooManyRequests`. These failures retain the previously accepted set without
truncation. Nodes without the online-query dependencies, and older servers that do not implement 1006,
return `10603 InvalidProtocol`; use the HTTP query path on those connections.

Two rejections mean this connection will never accept a set, so retrying a revision on it is wrong.
`10604 NodeDraining` says the node has begun graceful shutdown, the same code its handshake answers with
HTTP 503. Stop submitting on this connection; once it closes, reconnect through the LB, which picks
another node, and submit the desired set there as a new connection generation. `10602 ConnClosed` says
this connection is already draining or closed, so the reply may be among the last frames it delivers and
may not arrive at all; treat it exactly like a disconnect. Neither code retains anything: no set was
accepted, and the reconnected connection submits its full desired set.

Serialize 1006 submissions and retain the latest desired set while a request is pending. After a `10005`,
keep the last confirmed set and retry the latest desired set with backoff. A timeout with an unknown
outcome on the same connection must retry the same revision and set before submitting a replacement;
`10604` and `10602` are known outcomes and are never retried on that connection.
After reconnecting, start a new connection generation and submit the current desired set; discard replies
and pushes from the old generation. The server cannot recover a subscription it never accepted, so a
rejected first subscription requires another 1006.

Every non-stale 2005 replaces the state for its entire revision; missing users become unknown. There are
no delta frames or `full` flag. Match the frame to the applicable submitted revision, and ignore obsolete
revisions. A 2005 can precede its 1006 response, so an already submitted revision is valid before the
confirmation arrives. Before the first valid snapshot, show unknown for all subscribed users. A
`stale:true` frame carries no usable online state: mark the entire set unknown. A failed read never
refreshes freshness with previously cached values.

`snapshot_interval_ms` reports the server's periodic reread cadence. Start an independent freshness
deadline when the first nonempty subscription is confirmed; each valid snapshot refreshes it. After
three times that interval without a valid snapshot, mark the whole set unknown. A new revision does not
reset or extend the previous deadline; cancelling to an empty set stops it. `stale` and disconnect also
make the state unknown. Ping/Pong and 1006 retry confirmations do not prove fresh online state. An
accepted subscription continues to be refreshed by the server: missing or stale snapshots alone do not
trigger repeated 1006 requests or HTTP polling. This differs from retrying an unaccepted subscription.

Presence pushes are best effort. Exceeding the outbound byte budget drops 2005 without sending 2004;
the next snapshot repairs the loss. The node's snapshot rate limit drops nothing: refreshes above it
wait in due order, so a presence storm delays snapshots instead of losing them, and only the
client's own freshness deadline turns a long delay into unknown state. A full send queue closes the
connection as usual. A user remains online while OnlineStore has any valid connection for that user;
closing one device is not necessarily an offline transition. Crashed nodes and failed removals
become visible only after the remaining presence TTL expires and a subsequent read succeeds. There
is no hard offline-detection deadline during dependency failure. Online subscriptions affect neither
message delivery nor message reconciliation timers.

Deploy the server before clients use 1006. This version emits `presence_changed` when running and has no
separate enable switch: coordinate upgrading all nodes sharing a Bus before starting the compatible fleet,
or replace the fleet together. A rolling mix with old nodes does not satisfy that deployment condition.
The `10603` fallback still applies if a client connects to an older server or after rollback.

## Offline push webhook

`POST offline_push.webhook_url` with `{event_id, user_ids, notification, preview}`; `event_id` is
`conversation_id:seq`, dedupe on it. `notification` carries the facts (`conversation_id`, `seq`,
`session_type`, `sender_id`, `group_id`, `content_type`, `content`, `send_time`). Headers `X-Nexo-Timestamp` and
`X-Nexo-Signature = hex(HMAC-SHA256(webhook_secret, ts + "\n" + hex(sha256(body))))`. One attempt, 3s
timeout. Redirects are never followed, including same-host HTTPS redirects; any 3xx is a failed attempt,
not retried. Configure the final HTTPS receiver URL directly so neither the signed payload nor its headers
are forwarded to another endpoint. `preview` is a default text (the message text or `[图片]` etc.); render your own from `notification.content` if you prefer.
