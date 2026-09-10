-- 3. 유효한 이벤트 중 event_id별로 가장 먼저 적재된 레코드 하나를 남긴다.
-- Kafka offset은 partition 사이에서 비교 가능한 전역 순서가 아니다.
-- topic/partition/offset은 적재 시각이 같을 때 대표 행을 결정하는 tie-breaker다.
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY event_id
        ORDER BY bronze_ingested_at, kafka_topic, kafka_partition, kafka_offset
    ) AS duplicate_rank
    FROM silver_event_candidates
    WHERE validation_error IS NULL
)
SELECT
    event_id, event_type, schema_version, event_time, ingest_time,
    booking_id, shop_id, customer_id, service_id, staff_id,
    start_at, old_start_at, cancelled_at, booked_price_krw, status,
    payment_status, payment_id, payment_transaction_id, amount_krw,
    payload, kafka_topic, kafka_partition, kafka_offset,
    kafka_timestamp, bronze_ingested_at
FROM ranked
WHERE duplicate_rank = 1
