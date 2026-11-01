#!/usr/bin/env bash
# Runs the Jepsen test: compiles phoenix_app/, brings up the cluster (docker/compose.yml), makes
# the S2 basin, starts the clients' driver, then `lein run test` with any arguments given
# here. Needs a running Docker daemon.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
export REPOSITORY="$(cd "$here/.." && pwd)"
export MISE_INSTALLS="$(cd "$(mise where erlang)/../.." && pwd)"
compose=(docker compose -f "$here/docker/compose.yml")

(cd "$here/phoenix_app" && ELIXIR_ERL_OPTIONS=+fnu mix do deps.get, compile)
"${compose[@]}" up -d --build --wait --remove-orphans
# Answered 409 when the basin is there from an earlier run.
curl -s -o /dev/null -X POST http://127.0.0.1:4251/v1/basins -H 'content-type: application/json' \
  -d '{"basin": "phoenix-durable-channel-jepsen", "config": {"create_stream_on_append": true, "create_stream_on_read": true}}'

nodes=$(for node in n1 n2 n3; do
  printf '%s:4000,' "$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "phoenix-durable-channel-jepsen-$node-1")"
done)
(cd "$here/driver" && exec node driver.mjs 4100 4101 "${nodes%,}" >"$here/driver.log" 2>&1) &
driver=$!
trap 'kill $driver; wait $driver' EXIT
until grep -q '^ready' "$here/driver.log"; do
  kill -0 $driver || { cat "$here/driver.log"; exit 1; }
  sleep 1
done

cd "$here" && lein run test --nodes n1,n2,n3 "$@"
