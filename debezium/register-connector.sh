#!/usr/bin/env bash
set -euo pipefail

connector_url="${CONNECT_URL:-http://localhost:8083}/connectors/shopslot-outbox/config"
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

until curl --fail --silent --show-error "${connector_url%/connectors/*}/" >/dev/null; do
  sleep 2
done

curl --fail --silent --show-error --request PUT "${connector_url}" \
  --header 'Content-Type: application/json' \
  --data @"${root_dir}/debezium/outbox-router.json"
echo

for _ in $(seq 1 30); do
  status="$(curl --silent "${connector_url%/config}/status" || true)"
  if grep -q '"state":"RUNNING"' <<<"${status}"; then
    exit 0
  fi
  sleep 2
done

echo "Connector did not reach RUNNING state within 60 seconds." >&2
exit 1
