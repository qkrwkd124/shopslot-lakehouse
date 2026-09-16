-- 결제 1건 = 1행. payment_events_clean에 기록된 이벤트 직후 요약 중 최신 상태를 선택한다.
WITH payment_events AS (
    SELECT *
    FROM silver_payment_events_clean
),
-- event_time이 같으면 이후 컬럼으로 결과를 결정론적으로 고른다.
-- Kafka partition 사이 offset은 업무 전역 순서가 아니므로 동률의 업무 순서는 보장하지 않는다.
latest_state AS (
    SELECT *, row_number() OVER (
        PARTITION BY payment_id
        ORDER BY event_time DESC, bronze_ingested_at DESC,
                 kafka_topic, kafka_partition, kafka_offset
    ) AS state_rank
    FROM payment_events
),
-- 최초 결제 요청 시각을 보존하고 요청 이벤트가 누락된 결제를 식별한다.
creation AS (
    SELECT *, row_number() OVER (
        PARTITION BY payment_id
        ORDER BY event_time, bronze_ingested_at,
                 kafka_topic, kafka_partition, kafka_offset
    ) AS creation_rank
    FROM payment_events
    WHERE event_type = 'payment_requested'
)
SELECT
    s.payment_id,
    s.booking_id,
    s.shop_id,
    s.request_amount_krw,
    s.payout_amount_krw,
    s.refund_amount_krw,
    s.paid_amount_krw,
    s.unpaid_amount_krw,
    s.payment_status,
    s.needs_repayment,
    s.payment_transaction_id AS latest_transaction_id,
    s.transaction_type AS latest_transaction_type,
    s.amount_krw AS latest_transaction_amount_krw,
    c.payment_id IS NULL AS is_orphan,
    s.event_id AS state_event_id,
    s.event_type AS state_event_type,
    s.event_time AS state_event_time,
    c.event_time AS requested_event_time
FROM latest_state s
LEFT JOIN creation c
    ON c.payment_id = s.payment_id AND c.creation_rank = 1
WHERE s.state_rank = 1
