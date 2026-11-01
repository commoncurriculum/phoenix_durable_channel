#!/usr/bin/env bash
set -euo pipefail
for forward in 27017:mongo:27017 4251:s2:4243 4252:durable-streams:4252; do
  IFS=: read -r local host remote <<<"$forward"
  pgrep -f "socat TCP-LISTEN:$local," >/dev/null ||
    (setsid nohup socat "TCP-LISTEN:$local,fork,reuseaddr,bind=127.0.0.1" "TCP:$host:$remote" </dev/null >/dev/null 2>&1 &)
done
