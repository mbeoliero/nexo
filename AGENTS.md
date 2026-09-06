# Nexo IM

Go **1.27** single-binary IM server. One module, one binary, multi-node behind an LB.
Subcommands: `serve` (long-running node) and `migrate` (one-shot).
Also embeddable: `server/` (`New` / `Mount` / `Start` / `Shutdown` + type aliases over `internal`) for a Hertz host; `sdk/` is the net/http client. Design §15.

Architecture: `docs/design.md` (v3.2).
The previous IM code base may be consulted for individual pieces (identity format, HMAC scheme, error codes) but its patterns are not a reference; this design and this file win.
If code and the design doc disagree, ask. Design changes go into the doc first.

## Commands

```text
make test          go test -p=1 ./... (DB/Redis suites skip without NEXO_TEST_PG_DSN / NEXO_TEST_MYSQL_DSN / NEXO_TEST_REDIS_ADDR)
make test-all      throwaway PG + MySQL + Redis containers, migrate, run everything, tear down (needs Docker)
make run           serve with config/config.example.yaml on :8080
make migrate       nexo migrate (goose, embedded SQL; needs db.dsn)
make sqlc          sqlc generate, run out-of-module at SQLC_VERSION (pgstore + PG cache)
make lint          gofmt -l + go vet + staticcheck
make tidy
make image         docker build nexo:dev (GOPROXY from `go env`)
make compose-up    3 nodes + nginx + pg (+ redis overlay unless NEXO_COMPOSE_CONFIG=config.pg-only.yaml) on :18080 (nodes :18081-18083); `go run ./deploy/smoke` runs the acceptance
make compose-down
```

Config: `-config path` or `NEXO_CONFIG`; env vars override (`NEXO_DB_DSN`, `NEXO_AUTH_NATIVE_SECRET`).
Every config key must be read by code; `config/config.example.yaml` mirrors `internal/config`, and every `deploy/config*.yaml` mirrors the example (drift test in `internal/config`). Do not commit `config/config.yaml` or secrets.

After changing `internal/store/pgstore/queries/*.sql` or `migrations/postgres/*.sql`, run `make sqlc`. Never edit generated code under `pgstore/gen/`. sqlc is pinned by `SQLC_VERSION` in the Makefile and run with `go run ...@version`, deliberately not a `tool` directive: it would drag sqlc's compiler into the dependency graph of everything importing `server/` or `sdk/`. `sqlc.yaml` sets `initialisms: []` so generated fields are `Id` / `UserId`.

## Architecture

1. One Go module. No internal RPC, no per-domain `go.mod`.
2. Layers: `api` (handlers + routes) and `gateway` call `service` only; `middleware` (Trace, AccessLog, Bearer, InternalAuth) depends on `webx` + `auth`; `webx` is the HTTP helper (envelope `{code,message,data}`, identity and errcode accessors) and imports no business package. `service` calls `store`, `bus`, `onlinestore`, `offlinepush`, `tokenstore`. `auth` calls `tokenstore`; `api` middleware, the `gateway` handshake/recheck, and `service/user` (native issue/revoke) call `auth`. `tokenstore` depends on `cache` only. `app` wires everything (`Build` / `Start` / `Shutdown`); no handlers, DTOs, domain rules, signals, or Hertz construction in `app`. `api.Register` mounts routes on a caller-supplied engine. Signals, `log.WithHertz()`, and the standalone Hertz live only in `cmd/nexo` and `server.ListenAndServe`. `server` is the public facade: type aliases over `internal` DTOs (`server/types.go`) plus lifecycle, no business logic; nothing moves out of `internal`. A new service DTO gets an alias in the same change. `sdk` imports no other package of this module. `msgbody` (root) holds content_type constants, typed content parsing, and the default push preview; it depends on stdlib only so webhook receivers and hosts can import it, and it is the single definition `service/message` and `offlinepush` use.
3. Data access is the `store.Store` interface with two implementations: `gormstore` (MySQL + PG, GORM generics API) and `pgstore` (sqlc + pgx, PG only). Transactions: `Store.WithTx(ctx, func(Store) error)`; the boundary is defined in `service`. No consumer depends on all of `store.Store`: each declares the methods it calls (`service/group.Tx`, `service/message.Tx`, `service/conversation.Store`, `service/conv.Lister`) or reuses the matching `store` sub-interface (`service/user` takes `store.UserStore`, `onlinestore/db` takes `store.OnlineConnStore`). In `group` and `message` the `WithTx` callback gets the package's own `Tx`, bridged by that package's single `Adapt(store.Store)`; a new store method reaches a service only by being added to that service's interface. Schema source of truth is the SQL migration files; no AutoMigrate. A table change touches PG SQL, MySQL SQL, and the GORM model.
4. Everything "optional Redis" goes through one interface with a non-Redis implementation: `Bus` (redis | postgres | local), `Cache` (redis | pg | local), `OnlineStore` (db | redis). Do not read Redis directly from business code.
5. Business packages under `service` must not import each other; compose in `app`. A response type two services share lives in `service/dto` (stdlib + `store` only), and a helper only `service` uses lives under `service` too (`service/conv` = conversation ids and cursors). A leaf package stays at `internal/` top level only when it has consumers in more than one layer (`identity`, `ratelimit`); never group leaves by kind (`utils`, `common`, `pkg`, `shared`). Schema belongs to `store`: `store/migrate` applies `migrations/` and takes `(driver, dsn)` like the `gormstore` / `pgstore` constructors.
6. External clients (webhook pusher, platform HMAC) live in the package that uses them. Define an interface only when a consumer or a test needs a fake; the consumer owns it.

HTTP: Hertz, standard transporter (WS needs Hijack). WS: `hertz-contrib/websocket`, same port. JSON: `encoding/json/v2` for new code; Hertz's `c.JSON` / `BindAndValidate` stay as is. Log: `github.com/mbeoliero/kit/log` with context. Errors: `errcode`. Codes are five digits `K MM NN`: K=1 business (client handles, no alert) / 2 system (our fault or a dependency; log error, alert), MM module, NN sequence, so logs and metrics classify by regex (`^2\d{4}$`). Wrap with `%w`; non-errcode errors surface as `20001`, never as `code=0`.

Time: DB columns are `timestamptz` (PG) / `datetime(3)` (MySQL) with `DEFAULT now()`; Go side is `time.Time` set explicitly by the service (one `now` per transaction, truncated to milliseconds so cursors round-trip; GORM auto timestamps are disabled per model); the API and cursors carry unix milliseconds.

Ids: user ids are `u___{int}` / `ag__{int}` / `nx__{uuid}` (`internal/identity`); conversation ids use `:` as separator because user ids contain `_`. server_msg_id / conn_id / token_id are UUIDv7 via stdlib `uuid`. No snowflake.

## Go

- `gofmt`. Tabs. Imports: stdlib, third party, this module.
- Before writing Go, run the `use-modern-go` skill's `list` for the file and follow it.
- Initialisms: `Id` / `Sql` / `Http` / `Url` / `Db` / `Dsn` / `Ws` / `Ttl` / `Jwt`, not all-caps. Leave third-party types alone.
- Comments only when the code cannot say it (a non-obvious rule, a workaround, a wire format). No doc comments that restate the name; no package comments unless they carry a rule. No ad-hoc SQL in non-test code (`pgstore` = sqlc queries, `gormstore` = GORM API + clauses); the one exemption is `bus/postgres` (`LISTEN` / `pg_notify` are session commands sqlc cannot express).
- Generics over `any` helper types. No DI frameworks (`wire`, `fx`, `samber/do`), no `samber/oops`.

Use stdlib / `samber/lo`. Do not hand-roll equivalents; replace old forms when you touch them:

- Errors: wrap `%w`; match `errors.AsType[T]` — not `var e *T; errors.As`.
- Defaults: `cmp.Or` / `cmp.Or(vals...)` — not `if x == "" { x = def }`.
- Two-value pick: `lo.Ternary` / `lo.If` — not `v := a; if cond { v = b }`.
- Plain `if` last: side effects, short-circuit, errors, multi-assign.
- `min` / `max` builtins. `for i := range n`. `new(value)` for pointers to literals.
- Slices/maps: `slices.Contains` / `Index` / `Chunk` / `Compact` / `Collect` / `Clone` / `Sorted`, `maps.Clone` / `Collect` / `Keys` / `Values`. Sorted keys: `slices.Sorted(maps.Keys(m))`.
- `strings.Cut` / `CutPrefix` / `CutSuffix` / `CutLast` over Index + slicing.
- `lo` only when stdlib has no one-liner: `Map` / `Filter` / `FilterMap` / `Uniq` / `SliceToMap` / `GroupBy` / `Find` / `Some`; `FromPtr` / `FromPtrOr` / `EmptyableToPtr`. Not `lo.Contains` / `Keys` / `Chunk` / `ForEach` / `ToPtr`.
- Memoize: `sync.OnceValue` / `OnceValues` / `OnceFunc`. Goroutines under a WaitGroup: `wg.Go`.
- Struct literals with embedded fields: set promoted fields directly (Go 1.27).
- Concurrency state: typed atomics (`atomic.Int64`, `atomic.Bool`).

## Tests

- `_test.go` beside the code. Stdlib `testing`. Use `t.Context()`.
- `t.Parallel()` only when the test shares no mutable process or external state (env, cwd, DB, ports, global logger).
- Skip DB tests when `NEXO_TEST_PG_DSN` / `NEXO_TEST_MYSQL_DSN` is empty. Store tests run the same suite against `gormstore` and `pgstore`.
- `NEXO_TEST_*` must point only to dedicated disposable instances. Whole-table reset and embedded migration tests require `NEXO_TEST_DISPOSABLE=1`; `scripts/test-all.sh` sets it for its unique containers and loopback-assigned ports. Direct real-dependency `go test` must use `-p=1`; separate concurrent runs need separate instances. CI runs `scripts/test-all.sh -race`, checks both sqlc outputs for drift, and pins staticcheck to `v0.8.1`.
- Bug fix: reproduce first when practical. A high-risk rule (seq allocation, idempotent send, visible range) gets one focused test.

Verify with `make test` (and `make sqlc` if SQL changed).

## Do not

- Start a new phase or change architecture without confirming; the user wants decisions asked first.
- Add Redis, Kafka, NATS or an outbox table on phase 1. `Bus` is at-most-once + `Resync` by design.
- Bypass `Store` from `service` or `api`; no ad-hoc GORM / pgx calls outside `internal/store`.
- Return `code=0` for unknown errors; go through `errcode.From`.
- Encode WS frame `data` as base64; it is a nested JSON object.
- Log tokens, passwords, `Authorization`, or HMAC secrets. Redact login/register bodies (`log.redact_paths`).
- Pre-create empty packages; add a package only when the current phase needs it.
- Commit secrets or production DSNs.
