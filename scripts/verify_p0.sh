#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root_dir}"

status="$(curl --fail --silent http://localhost:8083/connectors/shopslot-outbox/status)"
if ! grep -q '"state":"RUNNING"' <<<"${status}"; then
  echo "Debezium connector is not running: ${status}" >&2
  exit 1
fi

event_count="$(docker compose exec -T mysql mysql -ushopslot -pshopslot -N -e 'SELECT COUNT(*) FROM shopslot.outbox_events')"
if [ "${event_count}" -ne 10 ]; then
  echo "Expected 10 outbox rows, found ${event_count}" >&2
  exit 1
fi

messages="$(docker compose exec -T redpanda rpk topic consume booking.events.v1 --num=10 --format '%v\\n')"
message_count="$(grep -o 'booking_created' <<<"${messages}" | wc -l | tr -d ' ')"
if [ "${message_count}" -ne 10 ]; then
  echo "Expected 10 Kafka events, found ${message_count}" >&2
  exit 1
fi

echo "P0 smoke test passed: 10 committed outbox rows and 10 Kafka booking.events.v1 records."
