#!/usr/bin/env bash
# Full test matrix with throwaway containers: PG (sqlc + gorm), MySQL (gorm), Redis (cache, bus, online store).
set -euo pipefail
cd "$(dirname "$0")/.."
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/nexo-test.XXXXXXXXXX")
run_name=${run_dir##*/}
cleanup() {
  for cidfile in "$run_dir/pg.cid" "$run_dir/mysql.cid" "$run_dir/redis.cid"; do
    if [ -s "$cidfile" ]; then
      id=
      IFS= read -r id < "$cidfile" || true
      if [ -n "$id" ]; then docker rm -f "$id" >/dev/null 2>&1 || true; fi
    fi
  done
  rm -rf "$run_dir" || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# wait_for <name> <tries> <command...> — poll until the command succeeds, or fail loudly.
# Falling through a readiness loop leaves the tests to fail with "connection refused" instead.
wait_for() {
  name=$1; tries=$2; shift 2
  for _ in $(seq 1 "$tries"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "$name did not become ready after ${tries}s; check 'docker logs $run_name-$name'" >&2
  return 1
}

# Docker records ownership before start, including when a signal interrupts the shell.
docker run -d --rm --cidfile "$run_dir/pg.cid" --name "$run_name-pg" -e POSTGRES_USER=nexo -e POSTGRES_PASSWORD=nexo -e POSTGRES_DB=nexo -p "127.0.0.1:${PG_PORT:-}:5432" postgres:16-alpine >/dev/null
pg_id=$(<"$run_dir/pg.cid")
docker run -d --rm --cidfile "$run_dir/mysql.cid" --name "$run_name-mysql" -e MYSQL_ROOT_PASSWORD=nexo -e MYSQL_DATABASE=nexo -p "127.0.0.1:${MY_PORT:-}:3306" mysql:8 >/dev/null
mysql_id=$(<"$run_dir/mysql.cid")
docker run -d --rm --cidfile "$run_dir/redis.cid" --name "$run_name-redis" -p "127.0.0.1:${REDIS_PORT:-}:6379" redis:7-alpine >/dev/null
redis_id=$(<"$run_dir/redis.cid")
PG_PORT=$(docker port "$pg_id" 5432/tcp); PG_PORT=${PG_PORT##*:}
MY_PORT=$(docker port "$mysql_id" 3306/tcp); MY_PORT=${MY_PORT##*:}
REDIS_PORT=$(docker port "$redis_id" 6379/tcp); REDIS_PORT=${REDIS_PORT##*:}
export NEXO_TEST_DISPOSABLE=1
export NEXO_TEST_PG_DSN="postgres://nexo:nexo@127.0.0.1:$PG_PORT/nexo?sslmode=disable"
export NEXO_TEST_MYSQL_DSN="root:nexo@tcp(127.0.0.1:$MY_PORT)/nexo?parseTime=true&loc=UTC"
export NEXO_TEST_REDIS_ADDR="127.0.0.1:$REDIS_PORT"
wait_for pg 60 docker exec "$pg_id" pg_isready -U nexo
# --protocol=TCP is load-bearing: the mysql:8 entrypoint runs a temporary server with networking
# disabled while it initialises the data directory, and a socket ping answers there. Probing over TCP
# waits for the real server, so migrate does not hit "unexpected EOF" on the restart.
wait_for mysql 120 docker exec "$mysql_id" mysqladmin ping -h 127.0.0.1 --protocol=TCP -uroot -pnexo --silent
wait_for redis 60 docker exec "$redis_id" redis-cli ping
NEXO_DB_DRIVER=postgres NEXO_DB_ACCESS=sqlc NEXO_DB_DSN="$NEXO_TEST_PG_DSN" go run ./cmd/nexo migrate -config config/config.example.yaml >/dev/null
NEXO_DB_DRIVER=mysql NEXO_DB_ACCESS=gorm NEXO_DB_DSN="$NEXO_TEST_MYSQL_DSN" go run ./cmd/nexo migrate -config config/config.example.yaml >/dev/null
# ${1+"$@"} not "$@": under `set -u`, bash < 4.4 (macOS ships 3.2) errors on an empty "$@".
# Store and presence packages share tables; serialize packages after user flags so -p cannot override it.
go test -count=1 ${1+"$@"} -p=1 ./...
