-- Kafka 재전송·재처리로 같은 event_id가 여러 번 관측되면 최초 적재 한 건을 남긴다.
-- Kafka offset은 partition 사이에서 비교 가능한 전역 순서가 아니다.
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY event_id
        ORDER BY bronze_ingested_at, kafka_topic, kafka_partition, kafka_offset
    ) AS duplicate_rank
    FROM silver_event_candidates
    WHERE validation_error IS NULL
)
SELECT
    event_id,
    event_type,
    schema_version,
    event_time,
    ingest_time,
    booking_id,
    shop_id,
    payload,
    kafka_key,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    kafka_headers,
    bronze_ingested_at
FROM ranked
WHERE duplicate_rank = 1
