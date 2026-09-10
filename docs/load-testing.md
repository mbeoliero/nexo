# Cross-node single-chat load and correctness

Start with the small check (requires Go 1.27, Docker, curl and openssl):

```sh
make test-load LOAD_ARGS='-users 100 -active 100 -duration 3s -ramp 2s -settle 3s'
```

This creates **ten independent Nexo containers**, a private PostgreSQL and Redis, and runs the Go
client on the host. Ports are randomly assigned and bound to loopback. It does not use or stop the
three-node development Compose stack. Cleanup removes only resources owned by this invocation.

## Workloads

| Command | Online | Sending users | Target rate | Planned messages |
| --- | ---: | ---: | ---: | ---: |
| `make test-load` | 10,000 | 1,000 | 1,000/s | 300,000 |
| `make test-load LOAD_ARGS='-active 10000'` | 10,000 | 10,000 | 10,000/s | 3,000,000 |

Both use one message per second per active user for five minutes, after a two-minute connection
ramp. Native registration/login (bcrypt) happen **before** the ramp and can take several minutes.
Both participants in a pair may send concurrently; inactive users remain connected and keep reading
WS control frames. In the 1,000-active profile, the remaining 9,000 users keep real connections open.

Users and active senders are distributed evenly across nodes. Pairs are explicitly assigned to different
node addresses; placement cycles through all directed node offsets rather than hoping a load balancer
routes two clients to different servers. The default
profiles cover all 90 directed links. Smaller runs report only the
links actually exercised; distinct supplied URLs must really identify distinct instances.

## What is checked

1. **Before sending:** create fresh identities through the public API; verify registration/login IDs
   agree. Every WS must complete a successful application-level `1001` round trip before steady load.
   HTTP 101 alone is not counted as ready.
2. **Every send:** unique `client_msg_id` and deterministic text; match `1003` ACK by `op_id`, require
   explicit `code=0`, and record conversation, server ID, seq and send time. The sender socket is
   pinned to its assigned instance. A push can arrive before the ACK.
3. **Every push:** require the designated recipient's socket, exact sender/receiver/conversation,
   session/content type, body, server ID, seq and send time. Unknown, duplicate, malformed and
   misrouted messages fail the run. Idle users are also monitored for unexpected delivery.
4. **After sending:** wait `-settle`, freeze realtime statistics, then paginate the **entire** history
   as **both participants**, using each one's own token and node. Check max seq, visibility, contiguous
   seq from 1, exact request content, ACK/push identity agreement and no duplicate/extra messages.
   Idle users must still have no conversations. There is no sampling. Thus 3 million sends require
   verification of 6 million history entries, in addition to realtime traffic.

Persistence is checked through the public read path, not direct SQL: an acknowledged message missing
or corrupted in a user's history fails. This is not a crash/durability or idempotent-retry benchmark;
existing focused transaction/idempotency tests remain necessary.

### Capacity outside this workload

This generator exercises single-chat delivery. Its connected-user count establishes neither hot-group
throughput nor online-subscription capacity; it does not send subscription frames.

Group acceptance needs separate runs varying member count, send rate per group and node count.
Record conversation-row lock waits, transaction latency, user-conversation writes and receive P99:
each group send currently updates active members inside the conversation transaction.

Online-subscription acceptance needs separate runs varying subscribed connections, users per
subscription, overlap between sets and competing message traffic. Measure the fraction of clients
receiving fresh snapshots within the [protocol freshness window](integration.md#online-status-subscriptions),
plus stale transitions, snapshot drops and OnlineStore latency. The node snapshot rate in
[`presence.go`](../internal/gateway/presence.go) is a separate bound from the configured WS connection
limit: even without read failures or other contention, sustaining one snapshot per subscribed
connection every refresh interval requires subscribed connections ≤ frame rate × refresh interval.
Exceeding it delays refreshes rather than dropping them, so measure snapshot lateness and stale
transitions there; the drops to record are the byte-budget ones.
That bound is necessary, not a measured capacity guarantee. Keep hardware, limits and backend choices
with results; the current single-chat report cannot supply this evidence.

### Missed push versus lost data

Normal load acceptance is strict: every planned send must be written to its socket, acknowledged,
received live and verified in both histories. Errors, unexpected disconnects or an overloaded generator
that cannot send its planned requests produce a nonzero exit code.

`-allow-missed-push` permits a realtime gap **only if final pull verifies every message**. This matches
the at-most-once Bus contract, but is **not** permission to ignore wrong content, duplicate delivery,
failed ACKs, identity mismatches or disconnects. It does not inject a Bus outage or reconnect clients.

The settle window is a cutoff: later frames do not improve the realtime result. Final pull time does
not count toward send throughput or realtime receive latency.

## Flags and reports

```sh
# Preserve report.json after cleanup (and container logs on failure).
NEXO_LOAD_OUTPUT=/tmp/nexo-load-results make test-load

# Smaller standalone regression, also used in CI.
NEXO_LOAD_NODES=3 make test-load LOAD_ARGS='-users 12 -active 12 -duration 3s -ramp 1s -settle 3s'

# Against ALREADY disposable servers; origins must be direct nodes, not LB addresses.
go run ./deploy/load -disposable -nodes http://127.0.0.1:18081,http://127.0.0.1:18082 \
  -users 20 -active 10 -rate 1 -duration 10s -ramp 2s

go run ./deploy/load -help
```

Use `-help` for the complete flag list and current defaults. The main controls are:

| Option | Meaning |
| --- | --- |
| `-users`, `-active` | Even total population; 1 ≤ active ≤ users |
| `-rate`, `-duration` | Per-user messages/s; planned count per sender is floor(rate × seconds) |
| `-ramp`, `-settle` | Connection ramp and post-send ACK/push drain window |
| `-workers`, `-timeout` | Setup/pull concurrency and per-HTTP/handshake/write timeout |
| `-allow-missed-push` | Permit realtime gaps only when full persisted verification passes |

`NEXO_LOAD_NODES` controls disposable node count (default 10). `NEXO_LOAD_IMAGE` optionally reuses an
existing server image without building or deleting it. The script forces its owned endpoints after
caller flags, so forwarded `-nodes` cannot redirect its automatically confirmed run elsewhere.

Stdout is a JSON report; phase progress goes to stderr. On success without `NEXO_LOAD_OUTPUT`, the
report is printed but temporary files are removed. Failure artifacts are retained and their location
is printed. Error examples are capped at 20; `errors` and `omitted_errors` expose the full count.

Senders are staggered across the first 90% of each per-user period, leaving 10% scheduling
headroom (about 100ms at one message/second). Per-user frequency, planned totals, the fixed send
window and strict acceptance remain unchanged: late sends fail rather than being retried or
sent beyond the window. Reports expose `send_phase_fraction: 0.9`. This is a different arrival
profile from older full-period staggering; do not treat their latency results as identical-load
comparisons. A `send window expired` error includes the user/node, zero-based message index,
scheduled offset, lateness, cutoff overrun and remaining unsent messages for that user.

Important fields:

- `planned`, `sent`, `acked`, `received`, `verified_both_views`: all should agree in a strict run.
- `missing_push`, `recovered_by_pull`: separate live gaps from successful history recovery.
- `unverified_messages`: missing **or failed** history verification; not automatically proof of
  physical database loss. Check `error_samples` for read errors, corruption or sequence faults.
- `ack_latency`, `receive_latency`: P95/P99 milliseconds from the client's initial send attempt;
  consult the corresponding message count when interpreting percentiles.
- `links`: per-directed-node planned/sent/ACK/received/verified counts. Top-level identity mismatch,
  duplicate ID and disconnect counters must be zero.

## Resource boundaries

A ten-container run on one laptop is **not ten machines**. Server CPU, DB throughput, Docker memory,
host file descriptors and the generator itself can be bottlenecks. Keep the generator's file descriptor
limit above the connection count (for example 32,768). Exact per-message verification retains records
for every planned message, plus IDs and latency samples; the high profile needs hundreds of MB to
more than 1 GB on the client, besides server and database memory. Final verification adds read load.

The disposable wrapper raises only the per-IP WS cap to 20,000; the normal value of 50 deliberately
cannot accommodate a single-host generator. It does not remove message/frame rate limits: rates above
the deployed limits should fail, not silently claim more capacity. Run heavier profiles on appropriate
hardware and inspect container CPU/memory with `docker stats` from a separate terminal.

Automated checks: `make test` covers verifier rejection cases; `python3 scripts/test-load.py` covers
resource ownership and failure cleanup without Docker. CI runs a small real three-node workload,
not the full 10,000-user profiles.
