# Nexo IM

Go **1.27** IM server: one module, one binary (`serve` / `migrate`), multi-node behind an LB.
`server/` supports embedding; `sdk/` is the independent HTTP client.

## Before making changes

- Read relevant contracts, code and tests first; ask about code/design conflicts.
- Get explicit approval before changing schema (tables, columns, indexes, constraints), architecture, protocols, transaction boundaries, lock order or contract behavior; also before new phases, dependencies or `internal/` packages. Explain necessity, alternatives and compatibility/migration impact.
- Existing approval covers its stated scope; ask only before expanding it. Routine work within existing boundaries can proceed. New packages need a current responsibility and consumer boundary; no scaffolding.
- After approval, update the owning contract before implementation in the same change. Never rewrite applied migrations; add versioned migrations.

## Commands

[CONTRIBUTING](CONTRIBUTING.md#build-and-test) owns development commands and test safety;
[README](README.md#quick-start) owns usage; [load-testing](docs/load-testing.md) owns load acceptance.

- After changing `sqlc.yaml` or its PG schema/query inputs, run `make sqlc`. Never edit generated output; both generators and pinning rules are in [Code generation](CONTRIBUTING.md#code-generation).
- Every config key must be used. `config/config.example.yaml` covers every `internal/config` key; `deploy/config*.yaml` contains deployment overrides only. Tests check known keys and effective deployment settings after defaults are applied.

## Documentation

- One owner per contract: [design](docs/design.md) for architecture/rationale, [integration](docs/integration.md) for wire/API, [embedding](docs/embedding.md) for host operations. Consult legacy code only for formats; these contracts govern behavior.
- Keep full DDL, API declarations and defaults in source; link instead of copying. Prefer compile-checked examples. Do not relocate redundant prose into new documents.
- Design holds required behavior, boundaries and reasons, not estimates or progress/debugging logs. Keep reproducible evidence, environments and limitations in research/validation docs; session logs in PR/CI artifacts. Mark unimplemented drafts clearly.
- Preserve transaction/visibility boundaries, failure windows, compatibility and negative evidence when shortening; update references and affected checks. Edit AGENTS.md only for lasting rules or workflows.

## Architecture

- No internal RPC or per-domain `go.mod`. `api` and `gateway` call `service`; services do not import each other. `app` composes them and owns wiring/lifecycle only.
- `service` uses `store`, `bus`, `onlinestore`, `offlinepush`, `tokenstore`; native issue/revoke also uses `auth`. Middleware and gateway authenticate through `auth`, which uses `tokenstore`, which uses `cache`. `webx` owns HTTP helpers without business imports.
- `api.Register` mounts on a supplied engine. Signals, `log.WithHertz()` and standalone Hertz construction belong in `cmd/nexo` or `server.ListenAndServe`.
- `server` exposes lifecycle and internal DTO aliases, not business logic; keep implementation internal and add an alias for each new public service DTO. `sdk` imports no other package of this module. `msgbody` owns content types, parsing and default previews, with stdlib dependencies only.
- Services own transactions and consume narrow Store interfaces. `group`/`message` use package-local `Tx` and one `Adapt(store.Store)` each; expose new methods explicitly. Business DB access must go through Store.
- Backends: `gormstore` uses GORM generics for MySQL/PG; `pgstore` uses sqlc/pgx for PG. No ad-hoc production SQL except `bus/postgres` session commands. SQL migrations own schema; no AutoMigrate. Table changes update PG SQL, MySQL SQL and GORM models together.
- Keep Redis optional behind `Bus`, `Cache`, `OnlineStore`, each with a non-Redis implementation; no business Redis access. Bus is at-most-once plus Resync; no Kafka, NATS or outbox.
- Shared service DTOs belong in `service/dto` (stdlib + `store` only); service-only helpers stay under service. Top-level internal leaves need consumers in multiple layers; no `utils`, `common`, `pkg` or `shared` buckets.
- External clients and interfaces belong to their consumers; introduce interfaces only for an actual consumer/test need.

Time: service writes one millisecond-truncated `time.Time` per transaction; disable GORM auto timestamps.
API/cursors use Unix milliseconds; DB types and defaults belong to [design §4](docs/design.md#4-数据模型).
IDs: use `internal/identity` for users, `:` in conversation IDs, and stdlib UUIDv7 for server message/connection/token IDs; no snowflake.

## Go

- Run the `use-modern-go` skill's `list` for the file before writing Go. Use `gofmt`; import stdlib, third party, then this module.
- Initialisms: `Id` / `Sql` / `Http` / `Url` / `Db` / `Dsn` / `Ws` / `Ttl` / `Jwt`; leave third-party names unchanged.
- Use Hertz's standard transporter (WS needs Hijack) with `hertz-contrib/websocket` on the same port. New JSON code uses `encoding/json/v2`; leave Hertz `c.JSON` / `BindAndValidate` as is. WS frame `data` is nested JSON, never base64.
- Use `github.com/mbeoliero/kit/log` with context. Route errors through `errcode.From` (unknown errors become `20001`); log system failures at error level and alert. [Error classes and wire mapping](docs/integration.md#http-envelope-and-error-codes) are stable contracts.
- Comments explain rules, workarounds or formats; no name-restating doc comments or rule-free package comments. Prefer generics over `any` helpers; no DI frameworks or `samber/oops`.

Prefer stdlib, then `samber/lo`; replace hand-written equivalents when touching code:

- Wrap errors with `%w`; match with `errors.AsType[T]`.
- Defaults: `cmp.Or`. Value selection: `lo.Ternary` / `lo.If`; plain `if` for effects, short-circuiting, errors or multi-assignment.
- Use built-in `min`/`max`, `for i := range n`, `new(value)` and direct promoted-field initialization (Go 1.27).
- Use `slices`, `maps` and `strings.Cut*` operations instead of loops or index/slicing equivalents; sorted keys: `slices.Sorted(maps.Keys(m))`. Use `lo` only without a stdlib one-liner; no `lo.ForEach` or `lo.ToPtr`.
- Use `sync.OnceValue` / `OnceValues` / `OnceFunc`, `wg.Go` and typed atomics.

## Tests

- Stdlib `testing`, adjacent `_test.go`, `t.Context()`. Use `t.Parallel()` only without shared mutable process/external state (env, cwd, DB, ports, logger).
- Skip DB tests without `NEXO_TEST_PG_DSN` / `NEXO_TEST_MYSQL_DSN`; run the shared Store suite against both implementations.
- External tests require dedicated disposable instances, `NEXO_TEST_DISPOSABLE=1` for resets/migrations and `-p=1` for direct runs. Concurrent runs need separate instances; follow [CONTRIBUTING](CONTRIBUTING.md#build-and-test).
- Reproduce bugs first when practical; protect high-risk rules (seq, idempotency, visibility) with focused tests. Verify with `make test`.

## Do not

- Log tokens, passwords, `Authorization` or HMAC secrets; redact login/register bodies (`log.redact_paths`).
- Commit `config/config.yaml`, secrets or production DSNs.
