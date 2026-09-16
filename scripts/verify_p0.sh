#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${root_dir}"

assert_count() {
  local label="$1"
  local actual="$2"
  local expected="$3"

  if [ "${actual}" -ne "${expected}" ]; then
    echo "Expected ${label}=${expected}, found ${actual}" >&2
    exit 1
  fi
}

count_event_type() {
  local event_type="$1"
  local haystack="$2"
  local count

  count="$(grep -o "\"event_type\":\"${event_type}\"" <<<"${haystack}" | wc -l | tr -d ' ' || true)"
  echo "${count:-0}"
}

status="$(curl --fail --silent http://localhost:8083/connectors/shopslot-outbox/status)"
if ! grep -q '"state":"RUNNING"' <<<"${status}"; then
  echo "Debezium connector is not running: ${status}" >&2
  exit 1
fi

database_counts="$(
  docker compose exec -T mysql sh -ec \
    'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -e "$1" "$MYSQL_DATABASE"' \
    sh "
      SELECT
        (SELECT COUNT(*) FROM bookings),
        (SELECT COUNT(*) FROM payments),
        (SELECT COUNT(*) FROM payment_transactions),
        (SELECT COUNT(*) FROM outbox_events),
        (
          SELECT COUNT(*)
          FROM payments p
          WHERE p.payout_amount_krw <> p.paid_amount_krw + p.refund_amount_krw
             OR p.paid_amount_krw <> COALESCE((
                  SELECT SUM(CASE
                    WHEN pt.transaction_type = 'payment' THEN pt.amount_krw
                    WHEN pt.transaction_type = 'refund' THEN -pt.amount_krw
                    ELSE 0
                  END)
                  FROM payment_transactions pt
                  WHERE pt.payment_id = p.id
                ), 0)
             OR p.payout_amount_krw <> COALESCE((
                  SELECT SUM(pt.amount_krw)
                  FROM payment_transactions pt
                  WHERE pt.payment_id = p.id
                    AND pt.transaction_type = 'payment'
                ), 0)
             OR p.refund_amount_krw <> COALESCE((
                  SELECT SUM(pt.amount_krw)
                  FROM payment_transactions pt
                  WHERE pt.payment_id = p.id
                    AND pt.transaction_type = 'refund'
                ), 0)
        )
    "
)"

IFS=$'\t' read -r \
  booking_count payment_count transaction_count outbox_count inconsistent_payment_count \
  <<<"${database_counts}"

assert_count bookings "${booking_count}" 10
assert_count payments "${payment_count}" 7
assert_count payment_transactions "${transaction_count}" 13
assert_count outbox_events "${outbox_count}" 40
assert_count inconsistent_payments "${inconsistent_payment_count}" 0

messages="$(
  docker compose exec -T redpanda \
    rpk topic consume booking.events.v1 --num=40 --format '%v\n'
)"

assert_count kafka_events "$(printf '%s\n' "${messages}" | wc -l | tr -d ' ')" 40
assert_count kafka_booking_created "$(count_event_type booking_created "${messages}")" 10
assert_count kafka_booking_rescheduled "$(count_event_type booking_rescheduled "${messages}")" 1
assert_count kafka_booking_cancelled "$(count_event_type booking_cancelled "${messages}")" 1
assert_count kafka_checked_in "$(count_event_type checked_in "${messages}")" 7
assert_count kafka_no_show_marked "$(count_event_type no_show_marked "${messages}")" 1
assert_count kafka_payment_requested "$(count_event_type payment_requested "${messages}")" 7
assert_count kafka_payment_completed "$(count_event_type payment_completed "${messages}")" 9
assert_count kafka_payment_refunded "$(count_event_type payment_refunded "${messages}")" 4

echo "P0 flow verified: MySQL -> outbox -> Debezium -> Kafka (40 events)."
