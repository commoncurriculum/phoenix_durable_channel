#!/usr/bin/env bash
set -euo pipefail
port="$1" project="$2" secret="$3"
here="$(cd "$(dirname "$0")" && pwd)"
wrangler="$here/../../node_modules/.bin/wrangler"
configuration=(--persist-to "$(mktemp -d)" --config "$here/wrangler.toml")
"$wrangler" kv key put --local --binding REGISTRY "$project" \
  "{\"signingSecrets\":[\"$secret\"],\"corsOrigins\":[\"*\"]}" "${configuration[@]}" >/dev/null
exec "$wrangler" dev --local --ip "${IP:-127.0.0.1}" --port "$port" "${configuration[@]}"
