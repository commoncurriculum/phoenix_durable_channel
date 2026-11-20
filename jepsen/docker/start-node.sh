#!/usr/bin/env bash
set -euo pipefail
"$(dirname "$0")/forward-databases.sh"
elixir="$MISE_INSTALLS/elixir/1.17.3-otp-27"
export MIX_HOME="$elixir/.mix" MIX_ARCHIVES="$elixir/.mix/archives"
export PATH="$MISE_INSTALLS/erlang/27.3.4.9/bin:$elixir/bin:$PATH"
export PORT="$1" STORE="$2" PEERS="$3"
cd "$REPOSITORY/jepsen/phoenix_app"
exec elixir --sname "jepsen@$(hostname)" --cookie jepsen \
  -S mix run --no-halt --no-compile --no-deps-check
