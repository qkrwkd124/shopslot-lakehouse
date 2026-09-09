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
        (SELECT COUNT(*) FROM shops),
        (SELECT COUNT(*) FROM customers),
        (SELECT COUNT(*) FROM services),
        (SELECT COUNT(*) FROM bookings),
        (SELECT COUNT(*) FROM payments),
        (SELECT COUNT(*) FROM payment_transactions),
        (SELECT COUNT(*) FROM outbox_events),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'booking_created'),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'booking_rescheduled'),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'booking_cancelled'),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'checked_in'),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'no_show_marked'),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'payment_completed'),
        (SELECT COUNT(*) FROM outbox_events WHERE event_type = 'payment_refunded'),
        (SELECT COUNT(*) FROM bookings WHERE status = 'scheduled'),
        (SELECT COUNT(*) FROM bookings WHERE status = 'checked_in'),
        (SELECT COUNT(*) FROM bookings WHERE status = 'cancelled'),
        (SELECT COUNT(*) FROM bookings WHERE status = 'no_show'),
        (SELECT COUNT(*) FROM payments WHERE payment_status = 'paid'),
        (SELECT COUNT(*) FROM payments WHERE payment_status = 'partially_refunded'),
        (SELECT COUNT(*) FROM payments WHERE payment_status = 'refunded'),
        (
          SELECT COUNT(*)
          FROM payments p
          WHERE p.refunded_amount_krw > p.paid_amount_krw
             OR p.paid_amount_krw <> COALESCE((
                  SELECT SUM(pt.amount_krw)
                  FROM payment_transactions pt
                  WHERE pt.payment_id = p.id
                    AND pt.transaction_type = 'payment'
                ), 0)
             OR p.refunded_amount_krw <> COALESCE((
                  SELECT SUM(pt.amount_krw)
                  FROM payment_transactions pt
                  WHERE pt.payment_id = p.id
                    AND pt.transaction_type = 'refund'
                ), 0)
        ),
        (
          SELECT COUNT(*)
          FROM outbox_events
          WHERE event_type IN ('payment_completed', 'payment_refunded')
            AND (
              JSON_EXTRACT(payload, '$.payment_id') IS NULL
              OR JSON_EXTRACT(payload, '$.payment_transaction_id') IS NULL
              OR JSON_EXTRACT(payload, '$.amount_krw') IS NULL
            )
        ),
        (
          SELECT COUNT(DISTINCT table_name)
          FROM information_schema.key_column_usage
          WHERE table_schema = DATABASE()
            AND constraint_name = 'PRIMARY'
            AND column_name = 'id'
            AND table_name IN ('shops', 'customers', 'services', 'staffs', 'bookings', 'payments', 'payment_transactions')
        ),
        (
          SELECT COUNT(*)
          FROM information_schema.columns
          WHERE table_schema = DATABASE()
            AND (
              (table_name = 'shops' AND column_name = 'shop_id')
              OR (table_name = 'customers' AND column_name = 'customer_id')
              OR (table_name = 'services' AND column_name = 'service_id')
              OR (table_name = 'bookings' AND column_name = 'booking_id')
              OR (table_name = 'payments' AND column_name = 'payment_id')
              OR (table_name = 'payment_transactions' AND column_name = 'payment_transaction_id')
            )
        ),
        (
          SELECT COUNT(*)
          FROM information_schema.key_column_usage
          WHERE table_schema = DATABASE()
            AND constraint_name = 'PRIMARY'
            AND (
              (table_name = 'outbox_events' AND column_name = 'event_id')
            )
        ),
        (
          SELECT COUNT(*)
          FROM information_schema.columns
          WHERE table_schema = DATABASE()
            AND data_type = 'bigint'
            AND column_type LIKE '%unsigned%'
            AND (
              (column_name = 'id' AND table_name IN ('shops', 'customers', 'services', 'staffs', 'bookings', 'payments', 'payment_transactions'))
              OR (table_name = 'services' AND column_name = 'shop_id')
              OR (table_name = 'staffs' AND column_name = 'shop_id')
              OR (table_name = 'bookings' AND column_name IN ('shop_id', 'customer_id', 'service_id', 'staff_id'))
              OR (table_name = 'payments' AND column_name = 'booking_id')
              OR (table_name = 'payment_transactions' AND column_name = 'payment_id')
            )
        ),
        (
          SELECT COUNT(*)
          FROM information_schema.columns
          WHERE table_schema = DATABASE()
            AND extra LIKE '%auto_increment%'
            AND column_name = 'id'
            AND table_name IN ('shops', 'customers', 'services', 'staffs', 'bookings', 'payments', 'payment_transactions')
        ),
        (
          SELECT COUNT(*)
          FROM bookings b
          LEFT JOIN staffs s ON b.staff_id = s.id
          WHERE s.id IS NULL OR b.shop_id <> s.shop_id
        )
    "
)"

IFS=$'\t' read -r \
  shop_count customer_count service_count booking_count payment_count transaction_count outbox_count \
  booking_created_count booking_rescheduled_count booking_cancelled_count checked_in_count \
  no_show_count payment_completed_count payment_refunded_count scheduled_count checked_in_status_count \
  cancelled_status_count no_show_status_count paid_status_count partially_refunded_status_count \
  refunded_status_count inconsistent_payment_count invalid_payment_payload_count entity_id_pk_count \
  legacy_entity_pk_count log_semantic_pk_count bigint_id_column_count auto_increment_id_count \
  invalid_booking_staff_count \
  <<<"${database_counts}"

assert_count shops "${shop_count}" 3
assert_count customers "${customer_count}" 10
assert_count services "${service_count}" 3
assert_count bookings "${booking_count}" 10
assert_count payments "${payment_count}" 7
assert_count payment_transactions "${transaction_count}" 9
assert_count outbox_events "${outbox_count}" 29

assert_count booking_created "${booking_created_count}" 10
assert_count booking_rescheduled "${booking_rescheduled_count}" 1
assert_count booking_cancelled "${booking_cancelled_count}" 1
assert_count checked_in "${checked_in_count}" 7
assert_count no_show_marked "${no_show_count}" 1
assert_count payment_completed "${payment_completed_count}" 7
assert_count payment_refunded "${payment_refunded_count}" 2

assert_count scheduled_bookings "${scheduled_count}" 1
assert_count checked_in_bookings "${checked_in_status_count}" 7
assert_count cancelled_bookings "${cancelled_status_count}" 1
assert_count no_show_bookings "${no_show_status_count}" 1
assert_count paid_payments "${paid_status_count}" 5
assert_count partially_refunded_payments "${partially_refunded_status_count}" 1
assert_count refunded_payments "${refunded_status_count}" 1
assert_count inconsistent_payments "${inconsistent_payment_count}" 0
assert_count invalid_payment_payloads "${invalid_payment_payload_count}" 0
assert_count entity_id_primary_keys "${entity_id_pk_count}" 7
assert_count legacy_entity_primary_keys "${legacy_entity_pk_count}" 0
assert_count log_semantic_primary_keys "${log_semantic_pk_count}" 1
assert_count bigint_entity_and_fk_columns "${bigint_id_column_count}" 15
assert_count auto_increment_entity_ids "${auto_increment_id_count}" 7
assert_count invalid_booking_staffs "${invalid_booking_staff_count}" 0

messages="$(
  docker compose exec -T redpanda \
    rpk topic consume booking.events.v1 --num=29 --format '%v\n'
)"

assert_count kafka_booking_created "$(count_event_type booking_created "${messages}")" 10
assert_count kafka_booking_rescheduled "$(count_event_type booking_rescheduled "${messages}")" 1
assert_count kafka_booking_cancelled "$(count_event_type booking_cancelled "${messages}")" 1
assert_count kafka_checked_in "$(count_event_type checked_in "${messages}")" 7
assert_count kafka_no_show_marked "$(count_event_type no_show_marked "${messages}")" 1
assert_count kafka_payment_completed "$(count_event_type payment_completed "${messages}")" 7
assert_count kafka_payment_refunded "$(count_event_type payment_refunded "${messages}")" 2

echo "P0 smoke test passed: 10 bookings, 7 payments, 9 transactions, and 29 Kafka events."
