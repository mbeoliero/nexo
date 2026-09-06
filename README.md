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

```sh
cp config/config.example.yaml config/config.yaml   # edit db.dsn
export NEXO_AUTH_NATIVE_SECRET=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
make migrate                                       # goose, SQL embedded in the binary
make run                                           # serve on :8080
curl localhost:8080/healthz
```

### First messages

```sh
BASE=localhost:8080/api/v1
curl -s $BASE/auth/register -d '{"username":"alice","password":"pw-at-least-8","nickname":"Alice"}'
curl -s $BASE/auth/register -d '{"username":"bob","password":"pw-at-least-8","nickname":"Bob"}'

TOKEN=$(curl -s $BASE/auth/login \
  -d '{"username":"alice","password":"pw-at-least-8","platform_id":5}' | jq -r .data.token)

# Bob's user_id came back from his register call; send him a message.
curl -s $BASE/message/send -H "Authorization: Bearer $TOKEN" \
  -d '{"client_msg_id":"c-1","session_type":1,"recv_id":"nx__...","content_type":1,"content":"{\"text\":\"hi\"}"}'

curl -s "$BASE/conversation/list" -H "Authorization: Bearer $TOKEN"
```

Bob receives it live over `ws://localhost:8080/ws?token=<his token>&platform_id=5`. Frame formats,
`req_id` table and the client resync rule are in [`docs/integration.md`](docs/integration.md).

### A cluster on your laptop

```sh
make compose-up            # nexo:dev + nginx :18080 + 3 nodes :18081-18083 + PG + Redis
go run ./deploy/smoke      # cross-node push, kick, online_status, node failover
make compose-down
```

`NEXO_COMPOSE_CONFIG=config.pg-only.yaml make compose-up` runs the same stack with no Redis at all
(`bus=postgres`, `cache=pg`, `online_store=db`); `deploy/docker-compose.mysql.yml` is the MySQL variant.
Set `NEXO_AUTH_NATIVE_SECRET` in `deploy/.env` first — see `deploy/.env.example`.

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

```go
cfg := nexo.DefaultConfig()
cfg.Db.Access = "gorm"             // DefaultConfig uses sqlc; WithGormDb requires gorm
cfg.Db.Driver = "postgres"         // must match the host db: "postgres" or "mysql"
cfg.Auth.Providers = []string{"external_jwt"}
cfg.Auth.ExternalJwt.Secrets = []string{platformJwtSecret}

s, err := nexo.New(ctx, cfg, nexo.WithGormDb(db), nexo.WithRoutePrefix("/im"))
if err != nil {
    return err
}
s.Mount(h.Engine)                 // h is your own Hertz server
s.Start(ctx)

ack, err := s.Message().Send(ctx, nexo.SendInput{ /* ... */ }) // in-process, still fans out over the bus
if err != nil {
    return err
}
_ = ack // use the acknowledgement
```

The host's `db` must use `gorm.Config{TranslateError: true}`; its Hertz engine must use the standard
transport for WebSocket Hijack. Check `New` before calling `Mount`; initialization failure returns a nil server.
Connection injection, shutdown ordering and the offline-push hook: [`docs/embedding.md`](docs/embedding.md).

## Documentation

| | |
| --- | --- |
| [`docs/design.md`](docs/design.md) | Architecture and the reasoning behind it |
| [`docs/integration.md`](docs/integration.md) | Wire protocol: ids, tokens, HMAC signing, WS frames, error codes, push webhook |
| [`docs/embedding.md`](docs/embedding.md) | Running nexo inside a Hertz host |
| [`sdk/`](sdk/) | Go client for the public and internal HTTP APIs |

## Development

```sh
make build         # bin/nexo
make test          # unit tests; DB / Redis suites skip without NEXO_TEST_* set
make test-all      # throwaway PG, MySQL and Redis containers, migrate, run everything, tear down
make lint          # gofmt + go vet + staticcheck
```

Test-DSN safety rules, `make sqlc`, the deployment probes, layering, and what generated code must
not be edited by hand are in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE). Contributions are accepted under the same terms; see
[`CONTRIBUTING.md`](CONTRIBUTING.md).
