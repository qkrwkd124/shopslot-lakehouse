-- Bronze 원문에서 현재 토픽의 공통 event envelope만 타입화한다.
-- 예약·결제 전용 필드는 payload에 남겨 downstream 도메인 모델이 해석한다.
WITH parsed AS (
    SELECT *, from_json(payload, 'STRUCT<
        event_id: STRING, event_type: STRING, schema_version: STRING,
        event_time: STRING, ingest_time: STRING,
        booking_id: STRING, shop_id: STRING,
        _corrupt_record: STRING
    >', map('mode', 'PERMISSIVE', 'columnNameOfCorruptRecord', '_corrupt_record')) AS event
    FROM silver_bronze_input
), typed AS (
    SELECT
        event.event_id AS event_id,
        event.event_type AS event_type,
        try_cast(event.schema_version AS INT) AS schema_version,
        try_cast(event.event_time AS TIMESTAMP) AS event_time,
        try_cast(event.ingest_time AS TIMESTAMP) AS ingest_time,
        try_cast(event.booking_id AS BIGINT) AS booking_id,
        try_cast(event.shop_id AS BIGINT) AS shop_id,
        payload,
        kafka_key,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        kafka_headers,
        bronze_ingested_at,
        event IS NULL OR event._corrupt_record IS NOT NULL AS malformed_json
    FROM parsed
)
SELECT *, CASE
    WHEN malformed_json THEN 'malformed_json'
    WHEN event_id IS NULL OR trim(event_id) = '' THEN 'missing_event_id'
    WHEN event_type IS NULL OR trim(event_type) = '' THEN 'missing_event_type'
    WHEN schema_version IS NULL OR schema_version <> 1 THEN 'unsupported_schema_version'
    WHEN booking_id IS NULL OR booking_id <= 0 OR shop_id IS NULL OR shop_id <= 0
        THEN 'invalid_required_id'
    WHEN event_time IS NULL OR ingest_time IS NULL THEN 'invalid_required_time'
    ELSE NULL
END AS validation_error
FROM typed
