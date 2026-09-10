# Contributing

Thanks for taking the time. Bug reports, wire-protocol clarifications and store/bus drivers are
all welcome — open an issue before a large change so the design is settled first.

## Build and test

Go 1.27, no code generation needed for a normal build.

```sh
make build         # bin/nexo
make test          # unit tests; the DB / Redis suites skip unless NEXO_TEST_PG_DSN,
                   # NEXO_TEST_MYSQL_DSN or NEXO_TEST_REDIS_ADDR is set
make test-all      # throwaway PG, MySQL and Redis containers, migrate, run everything, tear down
make lint          # gofmt + go vet + staticcheck
make sqlc          # regenerate pgstore and PG cache queries using pinned SQLC_VERSION
make tidy          # go mod tidy
make image         # nexo:dev image; GOPROXY comes from go env
```

Run, migration and Compose commands are in the [quick start](README.md#quick-start).

When `redis-server` is on PATH, the presence-recovery test starts and cleans up its own temporary
Redis instance over a Unix socket, even without `NEXO_TEST_REDIS_ADDR`. It requires permission to
bind a local socket; the instance uses no TCP port or persistent storage.

`make test-all` needs Docker; it creates uniquely named disposable instances with dynamically
allocated loopback ports and removes only its own containers. Separate invocations can run
concurrently. `PG_PORT`, `MY_PORT` and `REDIS_PORT` optionally select fixed host ports; leave them
unset when running multiple stacks. To match CI, run `./scripts/test-all.sh -race`.

Never point `NEXO_TEST_*` variables at shared or production instances. Driver suites reset whole
tables and the embedding test applies migrations: they also require `NEXO_TEST_DISPOSABLE=1`.
The container script sets this automatically. For manually provisioned instances, this flag is an
explicit acknowledgement that the destination is disposable, not a check of the destination.
`make test` and the container script serialize packages; direct `go test` with external test
variables must use `-p=1`. Bus channels are shared within each PG database or Redis server, so
separate concurrent runs still need separate instances even though reconnect tests target only
their own connection IDs.

CI runs unit and real-dependency tests with `-race`, pins staticcheck to `v0.8.1`, regenerates both
sqlc outputs and rejects tracked or untracked generated drift. `python3 scripts/test-deploy.py`
runs the command/config checks without Docker; the optional `--docker` deployment probes are
separate from this CI check.

## Code generation

[`sqlc.yaml`](sqlc.yaml) generates two packages from the shared schema in
[`migrations/postgres/`](migrations/postgres/):

| Query input | Generated output |
| --- | --- |
| [`internal/store/pgstore/queries/`](internal/store/pgstore/queries/) | `internal/store/pgstore/gen/` |
| [`internal/cache/pg/queries/`](internal/cache/pg/queries/) | `internal/cache/pg/gen/` |

After changing either query directory, the PostgreSQL migrations or `sqlc.yaml`, run `make sqlc` and
include both outputs' changes. Never edit generated files by hand. CI checks tracked and untracked
files in both output directories for drift.

The Makefile pins `SQLC_VERSION` and invokes `go run ...@version`. Keep sqlc out of `go.mod`'s `tool`
directives so its compiler dependencies do not enter the dependency graph of consumers importing
`server/` or `sdk/`. Both generators set `initialisms: []` to produce `Id` / `UserId` fields.

## Acceptance

These are required checks, not a record of completed runs. Use focused tests beside the implementation,
then the shared Store/Bus suites and real-dependency matrix; report skipped dependencies explicitly.

| Area | Required evidence |
| --- | --- |
| Startup and authentication | MySQL/GORM, PG/GORM and PG/sqlc migrate and serve with `/healthz`; identity formats; platform token mapping; native same-platform replacement and logout with both PG/Redis cache; provider secrets are not interchangeable; HMAC tampering, time windows and service allowlist reject; user upsert is idempotent |
| Groups, messages and conversations | Join/quit/rejoin visibility bounds and initial-member insertion; 50 concurrent sends keep seq continuous; duplicate client ids return the same ACK; `sender_read=false` increases both unread counts; list pagination has no gaps/duplicates in the fixed-data case and last_message stays within visibility |
| Gateway and multiple nodes | Local and cross-node delivery; slow consumers close and are counted; token expiry is rechecked within the documented timing budgets; different-token same-platform login kicks, same-token reconnect does not; node loss reconnects and pulls gaps; Bus recovery emits Resync; online status is global |
| Offline push | A single send invokes the webhook once for eligible offline recipients, not online or muted recipients; invocation is not proof of provider delivery |
| Public facade | Standalone smoke remains valid; a host mounts `/im` and sends in-process to a WS client; SDK round trips cover public/internal routes; exported service DTOs remain nameable from `server` |

Cross-node workload commands, full-history checks and report interpretation belong to the
[load-testing guide](docs/load-testing.md). Compiled examples in [`server`](server/example_test.go) and
[`sdk`](sdk/example_test.go) check API compatibility but are not database or end-to-end acceptance.

## Layering

Follow [AGENTS.md](AGENTS.md#architecture) for dependency boundaries, generated files and configuration
consistency. `server/` and `sdk/` are public: exported signature changes need a reason in the pull request.

## Changes to the design

[`docs/design.md`](docs/design.md) owns architecture and behavior; [`docs/integration.md`](docs/integration.md)
owns the wire contract. If code and design disagree, stop and ask rather than silently changing either.
Confirm architecture changes first, update the owning document before implementation, and keep them in
the same pull request. Unimplemented drafts are not supported contracts. Follow the
[documentation rules](AGENTS.md#documentation) instead of appending another copy.

## Style

Coding and testing conventions are in [`AGENTS.md`](AGENTS.md#go), shared by human contributors and
coding agents. New behavior arrives with a focused test.

## License

nexo is [Apache-2.0](LICENSE). By opening a pull request you agree that your contribution is
licensed under those terms. There is no separate CLA. Keep the header-free style of the existing
files — the top-level `LICENSE` covers the whole tree.
