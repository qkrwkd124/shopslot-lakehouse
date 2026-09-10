-- 1. JSON 원문을 펼친다. 문자열로 먼저 읽고 try_cast로 변환 실패를 NULL로 만든다.
-- silver_bronze_input은 실행기가 고정한 Bronze snapshot의 임시 view다.
WITH parsed AS (
    SELECT *, from_json(payload, 'STRUCT<
        event_id: STRING, event_type: STRING, schema_version: STRING,
        event_time: STRING, ingest_time: STRING, booking_id: STRING, shop_id: STRING,
        customer_id: STRING, service_id: STRING, staff_id: STRING,
        start_at: STRING, old_start_at: STRING, cancelled_at: STRING,
        booked_price_krw: STRING, status: STRING, payment_status: STRING,
        payment_id: STRING, payment_transaction_id: STRING, amount_krw: STRING,
        _corrupt_record: STRING
    >', map('mode', 'PERMISSIVE', 'columnNameOfCorruptRecord', '_corrupt_record')) AS event
    FROM silver_bronze_input
), typed AS (
    SELECT
        event.event_id AS event_id, event.event_type AS event_type,
        try_cast(event.schema_version AS INT) AS schema_version,
        try_cast(event.event_time AS TIMESTAMP) AS event_time,
        try_cast(event.ingest_time AS TIMESTAMP) AS ingest_time,
        try_cast(event.booking_id AS BIGINT) AS booking_id,
        try_cast(event.shop_id AS BIGINT) AS shop_id,
        try_cast(event.customer_id AS BIGINT) AS customer_id,
        try_cast(event.service_id AS BIGINT) AS service_id,
        try_cast(event.staff_id AS BIGINT) AS staff_id,
        try_cast(event.start_at AS TIMESTAMP) AS start_at,
        try_cast(event.old_start_at AS TIMESTAMP) AS old_start_at,
        try_cast(event.cancelled_at AS TIMESTAMP) AS cancelled_at,
        try_cast(event.booked_price_krw AS BIGINT) AS booked_price_krw,
        event.status AS status, event.payment_status AS payment_status,
        try_cast(event.payment_id AS BIGINT) AS payment_id,
        try_cast(event.payment_transaction_id AS BIGINT) AS payment_transaction_id,
        try_cast(event.amount_krw AS BIGINT) AS amount_krw,
        payload, kafka_topic, kafka_partition, kafka_offset,
        kafka_timestamp, bronze_ingested_at,
        event IS NULL OR event._corrupt_record IS NOT NULL AS malformed_json,
        (event.customer_id IS NOT NULL AND try_cast(event.customer_id AS BIGINT) IS NULL)
        OR (event.service_id IS NOT NULL AND try_cast(event.service_id AS BIGINT) IS NULL)
        OR (event.staff_id IS NOT NULL AND try_cast(event.staff_id AS BIGINT) IS NULL)
        OR (event.start_at IS NOT NULL AND try_cast(event.start_at AS TIMESTAMP) IS NULL)
        OR (event.old_start_at IS NOT NULL AND try_cast(event.old_start_at AS TIMESTAMP) IS NULL)
        OR (event.cancelled_at IS NOT NULL AND try_cast(event.cancelled_at AS TIMESTAMP) IS NULL)
        OR (event.booked_price_krw IS NOT NULL AND try_cast(event.booked_price_krw AS BIGINT) IS NULL)
        OR (event.payment_id IS NOT NULL AND try_cast(event.payment_id AS BIGINT) IS NULL)
        OR (event.payment_transaction_id IS NOT NULL AND try_cast(event.payment_transaction_id AS BIGINT) IS NULL)
        OR (event.amount_krw IS NOT NULL AND try_cast(event.amount_krw AS BIGINT) IS NULL)
        AS invalid_optional_type
    FROM parsed
)
-- 2. 최초 위반 사유 한 개만 분류한다. 완전한 업무 DQ는 다음 테이블의 과제다.
SELECT *, CASE
    WHEN malformed_json THEN 'malformed_json'
    WHEN event_id IS NULL OR trim(event_id) = '' THEN 'missing_event_id'
    WHEN schema_version IS NULL OR schema_version <> 1 THEN 'unsupported_schema_version'
    WHEN event_type IS NULL OR event_type NOT IN (
        'booking_created', 'booking_rescheduled', 'booking_cancelled',
        'checked_in', 'no_show_marked', 'payment_completed', 'payment_refunded'
    ) THEN 'unknown_event_type'
    WHEN booking_id IS NULL OR booking_id <= 0 OR shop_id IS NULL OR shop_id <= 0
        THEN 'invalid_required_id'
    WHEN event_time IS NULL OR ingest_time IS NULL THEN 'invalid_required_time'
    WHEN invalid_optional_type THEN 'invalid_optional_type'
    WHEN event_type IN ('payment_completed', 'payment_refunded') AND (
        payment_id IS NULL OR payment_id <= 0
        OR payment_transaction_id IS NULL OR payment_transaction_id <= 0
        OR amount_krw IS NULL OR amount_krw <= 0
    ) THEN 'invalid_payment_fields'
    ELSE NULL
END AS validation_error
FROM typed
