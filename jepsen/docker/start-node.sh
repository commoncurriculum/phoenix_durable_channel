#!/usr/bin/env bash
# Runs phoenix_app/ on this node as jepsen@<hostname>, with the mounted checkout and mise tools.
# Usage: start-node.sh <port> <store: mongo, s2 or durable-streams> <peer node names, comma separated>
set -euo pipefail
"$(dirname "$0")/forward-databases.sh"
elixir="$MISE_INSTALLS/elixir/1.17.3-otp-25"
export MIX_HOME="$elixir/.mix" MIX_ARCHIVES="$elixir/.mix/archives"
export PATH="$MISE_INSTALLS/erlang/25.3.2.21/bin:$elixir/bin:$PATH"
export PORT="$1" STORE="$2" PEERS="$3"
cd "$REPOSITORY/jepsen/phoenix_app"
exec elixir --sname "jepsen@$(hostname)" --cookie jepsen \
  -S mix run --no-halt --no-compile --no-deps-check
