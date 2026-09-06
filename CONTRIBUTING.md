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
```

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

## Layering

The rule that matters most: dependencies point one way, and business packages do not import each
other.

```text
api → service → store          gateway → service, bus, onlinestore
             ↘ bus, cache      app wires everything; nothing imports app
```

- A package under `internal/service` must not import a sibling. Compose them in `internal/app`.
  A type two services share belongs in `internal/service/dto`.
- `server/` and `sdk/` are the public surface. Changing an exported signature there breaks
  embedders, so it needs a reason in the pull request.
- Generated code (`internal/store/pgstore/gen/`) is never edited by hand. Change the `.sql` file
  and run `make sqlc`.
- Every key in `internal/config` must be read by code and mirrored in
  `config/config.example.yaml` and in every `deploy/config*.yaml`. `internal/config` has a drift
  test that fails otherwise. Never commit `config/config.yaml` or a real secret.

## Changes to the design

`docs/design.md` is the source of truth for behaviour. If code and the doc disagree, the doc wins
and the code is a bug — so a behaviour change goes into the doc in the same pull request.

## Style

Follow what is already in the file you are editing. Tests are table-driven where that reads well;
new behaviour arrives with a test. Comments explain why, not what.

The full ruleset, including the conventions above in more detail, is in
[`AGENTS.md`](AGENTS.md) — it is also the file coding agents read.

## License

nexo is [Apache-2.0](LICENSE). By opening a pull request you agree that your contribution is
licensed under those terms. There is no separate CLA. Keep the header-free style of the existing
files — the top-level `LICENSE` covers the whole tree.
