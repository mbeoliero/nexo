# Nexo

[![ci](https://github.com/mbeoliero/nexo/actions/workflows/ci.yml/badge.svg)](https://github.com/mbeoliero/nexo/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mbeoliero/nexo.svg)](https://pkg.go.dev/github.com/mbeoliero/nexo)

Single-binary IM server in Go. Run it as one process, as N stateless nodes behind a load balancer, or
embed it as a library in a Hertz service you already have.

- **Stateless nodes.** Presence and cross-node fan-out go through a bus, so any node can serve any
  connection and losing one costs only its own sockets.
- **Pluggable infrastructure.** PostgreSQL or MySQL; Redis, Postgres or in-process for bus, cache and
  presence. Selected by config — no build tags, one binary.
- **Two auth models, composable.** Verify your platform's HS256 JWT, or let nexo own the accounts
  (native username/password). Run either, or both in a chain.
- **Gapless ordering per conversation.** Every message gets a monotonic `seq`; a client that missed
  frames pulls the range it lacks instead of losing messages.
- **Backend channel.** HMAC-signed internal routes let your own services send as a user, manage groups
  and read presence without holding a user token.
- **Embeddable.** `server.New` + `Mount` puts the routes on the host's Hertz engine and hands back the
  services for in-process calls.

## Quick start

Install Go 1.27, `make`, `curl` and `jq`, and prepare a PostgreSQL database that nexo can connect to.
The example config uses PostgreSQL/sqlc; for MySQL, set `db.driver=mysql` and `db.access=gorm` as well as
the DSN.

```sh
cp config/config.example.yaml config/config.yaml   # edit db.dsn
export NEXO_AUTH_NATIVE_SECRET=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
make migrate                                       # goose, SQL embedded in the binary
make run                                           # serve on :8080
```

`make run` stays in the foreground. In a second terminal, check readiness:

```sh
curl -fsS http://localhost:8080/healthz
```

Expect HTTP 200 with `{"status":"ok","node_id":"..."}`. Keep the server running for the examples below.

### First messages

```sh
BASE=http://localhost:8080/api/v1
curl -fsS "$BASE/auth/register" -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pw-at-least-8","nickname":"Alice"}'
BOB_ID=$(curl -fsS "$BASE/auth/register" -H 'Content-Type: application/json' \
  -d '{"username":"bob","password":"pw-at-least-8","nickname":"Bob"}' | jq -er .data.user_id)

TOKEN=$(curl -fsS "$BASE/auth/login" -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"pw-at-least-8","platform_id":5}' | jq -er .data.token)

jq -n --arg recv_id "$BOB_ID" \
  '{client_msg_id:"c-1",session_type:1,recv_id:$recv_id,content_type:1,content:({text:"hi"}|tojson)}' | \
  curl -fsS "$BASE/message/send" -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' --data-binary @-

curl -fsS "$BASE/conversation/list" -H "Authorization: Bearer $TOKEN"
```

The send response has `code: 0` and a `data` object containing the assigned message id and `seq`.
To receive subsequent messages live, log Bob in and connect to
`ws://localhost:8080/ws?token=<his token>&platform_id=5`. Frame formats,
`req_id` table and the client resync rule are in [`docs/integration.md`](docs/integration.md).

### A cluster on your laptop

Install Docker with Compose. Create `deploy/.env` from [`deploy/.env.example`](deploy/.env.example) and
set `NEXO_AUTH_NATIVE_SECRET` there before starting the stack.

```sh
make compose-up            # nexo:dev + nginx :18080 + 3 nodes :18081-18083 + PG + Redis
go run ./deploy/smoke      # cross-node push, kick, online_status, node failover
make compose-down          # stop the stack and remove its data volumes
```

`NEXO_COMPOSE_CONFIG=config.pg-only.yaml make compose-up` runs the same stack with no Redis at all
(`bus=postgres`, `cache=pg`, `online_store=db`); `deploy/docker-compose.mysql.yml` is the MySQL variant.

### Cross-node load and message correctness

Use the [load-testing guide](docs/load-testing.md) for the small disposable check first, then the
10,000-user profiles. It covers ACK/push/history verification, resource limits and report interpretation.

## How it fits together

```
        clients (WS + HTTP)                 your backend
                 │                                │  HMAC-signed
          ┌──────┴──────┐                         │  /api/v1/internal/*
          │  LB / nginx │                         │
          └──────┬──────┘                         │
     ┌───────────┼───────────┐                    │
  ┌──┴──┐     ┌──┴──┐     ┌──┴──┐ ◄───────────────┘
  │node1│     │node2│     │node3│      stateless: any node serves any client
  └──┬──┘     └──┬──┘     └──┬──┘
     └───────────┼───────────┘
          bus (push / kick / conv events)  ·  store (messages, seq)  ·  presence
```

A node owns only the sockets connected to it. Everything shared — the `seq` allocation, who is online,
and the events that wake up another node's sockets — lives in the store and the bus.

## Infrastructure options

| Concern | Options | Config |
| --- | --- | --- |
| Database | PostgreSQL (sqlc or GORM), MySQL (GORM) | `db.driver`, `db.access` |
| Event bus | redis, postgres (`LISTEN/NOTIFY`), local | `bus.driver` |
| Cache / native tokens | redis, pg table, local | `cache.driver` |
| Online presence | db table, redis | `online_store.driver` |
| Offline push | noop, webhook, or `server.WithOfflinePusher` | `offline_push.driver` |
| Auth | platform HS256 JWT, native username/password | `auth.providers` |

`local` bus and cache are single-node only; a cluster needs redis or postgres for both.

## Configuration

The example config documents every key. Deployment profiles contain overrides only; omitted keys use the built-in defaults.

Select a file with `-config path` or `NEXO_CONFIG`.
Every key in [`config/config.example.yaml`](config/config.example.yaml) is read by the code. Any of them
can be overridden with `NEXO_` + the path joined by `_`:

```sh
NEXO_DB_DSN=postgres://... NEXO_BUS_DRIVER=redis NEXO_NODE_ID=node-a nexo serve
```

Secrets belong in the environment, never in the file: `auth.native.secret` must be at least 32 bytes and
the server refuses to start on a short or published-placeholder value. Behind a proxy, list its CIDRs in
`server.trusted_proxies` — the connection and rate limits key on the client IP, and an untrusted
`X-Forwarded-For` would make them meaningless.

## Embedding

Use `server.New` → `Mount` → `Start` on the host's Hertz engine. Follow the
[embedding guide](docs/embedding.md) for connection ownership and coordinated shutdown;
[`server/example_test.go`](server/example_test.go) is the complete, compile-checked example.

## Documentation

| Need | Read |
| --- | --- |
| Understand architecture and invariants | [`docs/design.md`](docs/design.md) |
| Integrate a client or backend | [`docs/integration.md`](docs/integration.md), including the Go [`sdk/`](sdk/) |
| Embed in a Hertz host | [`docs/embedding.md`](docs/embedding.md) |
| Run load acceptance | [`docs/load-testing.md`](docs/load-testing.md) |
| Evaluate future synchronization | [`docs/sync-design.md`](docs/sync-design.md) — **unimplemented draft**; [`docs/sync-research.md`](docs/sync-research.md) — source evidence and historical experiments |

## Development

Build/test commands, disposable-database safety and acceptance checks are in
[`CONTRIBUTING.md`](CONTRIBUTING.md). Long-term coding and documentation rules live in [`AGENTS.md`](AGENTS.md).

## License

[Apache License 2.0](LICENSE). Contributions are accepted under the same terms; see
[`CONTRIBUTING.md`](CONTRIBUTING.md).
