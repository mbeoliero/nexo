#!/usr/bin/env bash
# Disposable cross-node load test. Requires Docker, Go, curl, and openssl.
# NEXO_LOAD_NODES defaults to 10 (minimum 2); arguments pass through to deploy/load.
# NEXO_LOAD_IMAGE uses an existing image without building or deleting it.
# NEXO_LOAD_OUTPUT optionally names a local artifact parent directory. Each run gets
# a private subdirectory: report.json always, owned-container logs on failure.
# Without it, successful artifacts are removed; failed artifacts stay in /tmp.
set -euo pipefail
cd "$(dirname "$0")/.."
nodes=${NEXO_LOAD_NODES-10}
case "$nodes" in ''|*[!0-9]*) echo 'NEXO_LOAD_NODES must be an integer >= 2' >&2; exit 2;; esac
if ! [ "$nodes" -ge 2 ] 2>/dev/null; then
  echo 'NEXO_LOAD_NODES must be an integer >= 2' >&2
  exit 2
fi
# Prevent octal interpretation in arithmetic below.
nodes=$((10#$nodes))
umask 077
if [ -n "${NEXO_LOAD_OUTPUT:-}" ]; then
  mkdir -p "$NEXO_LOAD_OUTPUT"
  run_dir=$(mktemp -d "$NEXO_LOAD_OUTPUT/nexo-load.XXXXXXXXXX")
else
  run_dir=$(mktemp -d "${TMPDIR:-/tmp}/nexo-load.XXXXXXXXXX")
fi
run_name=${run_dir##*/}
image=${NEXO_LOAD_IMAGE:-nexo-load:$run_name}
cleanup() {
  status=$?
  trap - EXIT
  for cidfile in "$run_dir"/*.cid; do
    if [ -s "$cidfile" ]; then
      id=
      IFS= read -r id < "$cidfile" || true
      if [ -n "$id" ]; then
        if [ "$status" -ne 0 ]; then
          docker logs "$id" > "${cidfile%.cid}.log" 2>&1 || true
        fi
        docker rm -f -v "$id" >/dev/null 2>&1 || true
      fi
    fi
  done
  if [ -s "$run_dir/network.id" ]; then
    id=
    IFS= read -r id < "$run_dir/network.id" || true
    if [ -n "$id" ]; then docker network rm "$id" >/dev/null 2>&1 || true; fi
  fi
  # Remove only our unique tag, never an image ID potentially shared by other tags.
  if [ -s "$run_dir/image.id" ]; then docker image rm "$image" >/dev/null 2>&1 || true; fi
  if [ "$status" -ne 0 ] || [ -n "${NEXO_LOAD_OUTPUT:-}" ]; then
    echo "Load artifacts: $run_dir" >&2
  else
    rm -rf "$run_dir" || true
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
wait_for() {
  local name=$1 tries=$2 attempt=0
  shift 2
  while [ "$attempt" -lt "$tries" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
    attempt=$((attempt + 1))
  done
  echo "$name did not become ready after ${tries} attempts" >&2
  return 1
}

# Secrets stay out of command arguments, reports, and environment dumps.
NEXO_AUTH_NATIVE_SECRET=$(openssl rand -hex 32)
export NEXO_AUTH_NATIVE_SECRET
[ -n "$NEXO_AUTH_NATIVE_SECRET" ]
export NEXO_DB_DSN='postgres://nexo:nexo@pg:5432/nexo?sslmode=disable'
export NEXO_REDIS_ADDR='redis:6379'
docker network create "$run_name" > "$run_dir/network.id"
network=$(<"$run_dir/network.id")
docker run -d --cidfile "$run_dir/pg.cid" --name "$run_name-pg" --network "$network" --network-alias pg \
  -e POSTGRES_USER=nexo -e POSTGRES_PASSWORD=nexo -e POSTGRES_DB=nexo \
  postgres:16-alpine -c "max_connections=$((nodes * 10 + 50))" >/dev/null
docker run -d --cidfile "$run_dir/redis.cid" --name "$run_name-redis" --network "$network" --network-alias redis \
  redis:7-alpine >/dev/null
wait_for pg 60 docker exec "$(<"$run_dir/pg.cid")" pg_isready -h 127.0.0.1 -U nexo
wait_for redis 60 docker exec "$(<"$run_dir/redis.cid")" redis-cli ping
if [ -z "${NEXO_LOAD_IMAGE:-}" ]; then
  docker build --iidfile "$run_dir/image.id" --build-arg "GOPROXY=$(go env GOPROXY)" -t "$image" . >&2
fi
# Retain stopped containers until cleanup so failure logs remain available.
docker run --cidfile "$run_dir/migrate.cid" --name "$run_name-migrate" --network "$network" \
  -v "$PWD/deploy/config.yaml:/etc/nexo/config.yaml:ro" \
  -e NEXO_NODE_ID="$run_name-migrate" -e NEXO_DB_DSN -e NEXO_REDIS_ADDR -e NEXO_AUTH_NATIVE_SECRET \
  "$image" migrate -config /etc/nexo/config.yaml >&2
urls=
i=1
while [ "$i" -le "$nodes" ]; do
  docker run -d --cidfile "$run_dir/node-$i.cid" --name "$run_name-node-$i" --network "$network" \
    -p '127.0.0.1::8080' -v "$PWD/deploy/config.yaml:/etc/nexo/config.yaml:ro" \
    -e NEXO_NODE_ID="$run_name-node-$i" -e NEXO_DB_DSN -e NEXO_REDIS_ADDR -e NEXO_AUTH_NATIVE_SECRET \
    -e NEXO_LIMITS_WS_CONNS_PER_IP=20000 -e NEXO_LIMITS_AUTH_PER_IP_PER_MIN=0 \
    "$image" >/dev/null
  binding=$(docker port "$(<"$run_dir/node-$i.cid")" 8080/tcp)
  url="http://127.0.0.1:${binding##*:}"
  wait_for "node-$i" 60 curl --fail --silent --max-time 2 "$url/healthz"
  urls="${urls:+$urls,}$url"
  i=$((i + 1))
done
# Bash 3.2 with set -u cannot expand an empty "$@" directly.
# Forced endpoints come last: caller flags cannot redirect this auto-confirmed run to another environment.
go run ./deploy/load ${1+"$@"} -nodes "$urls" -disposable | tee "$run_dir/report.json"
