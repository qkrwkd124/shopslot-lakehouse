-- events_clean에서 수납·환불 이벤트만 선택하고 거래 전용 payload를 타입화한다.
WITH payment_input AS (
    SELECT *
    FROM silver_events_input
    WHERE event_type IN ('payment_completed', 'payment_refunded')
), parsed AS (
    SELECT *, from_json(payload, 'STRUCT<
        payment_id: STRING, payment_transaction_id: STRING,
        request_amount_krw: STRING, amount_krw: STRING,
        payment_status: STRING, refund_type: STRING
    >') AS payment
    FROM payment_input
), typed AS (
    SELECT
        event_id,
        event_type,
        schema_version,
        event_time,
        ingest_time,
        booking_id,
        shop_id,
        try_cast(payment.payment_id AS BIGINT) AS payment_id,
        try_cast(payment.payment_transaction_id AS BIGINT) AS payment_transaction_id,
        try_cast(payment.request_amount_krw AS BIGINT) AS request_amount_krw,
        CASE
            WHEN event_type = 'payment_completed' THEN 'payment'
            WHEN event_type = 'payment_refunded' THEN 'refund'
        END AS transaction_type,
        try_cast(payment.amount_krw AS BIGINT) AS amount_krw,
        payment.payment_status AS payment_status,
        payment.refund_type AS refund_type,
        payload,
        kafka_key,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        kafka_headers,
        bronze_ingested_at,
        (payment.payment_id IS NOT NULL
            AND try_cast(payment.payment_id AS BIGINT) IS NULL)
        OR (payment.payment_transaction_id IS NOT NULL
            AND try_cast(payment.payment_transaction_id AS BIGINT) IS NULL)
        OR (payment.request_amount_krw IS NOT NULL
            AND try_cast(payment.request_amount_krw AS BIGINT) IS NULL)
        OR (payment.amount_krw IS NOT NULL
            AND try_cast(payment.amount_krw AS BIGINT) IS NULL)
        AS invalid_payment_type
    FROM parsed
)
SELECT *, CASE
    WHEN invalid_payment_type THEN 'invalid_payment_type'
    WHEN payment_id IS NULL OR payment_id <= 0 THEN 'invalid_payment_id'
    WHEN payment_transaction_id IS NULL OR payment_transaction_id <= 0
        THEN 'invalid_payment_transaction_id'
    WHEN request_amount_krw IS NULL OR request_amount_krw <= 0
        THEN 'invalid_payment_request_amount'
    WHEN amount_krw IS NULL OR amount_krw <= 0 THEN 'invalid_payment_amount'
    WHEN amount_krw > request_amount_krw THEN 'payment_amount_exceeds_request'
    WHEN event_type = 'payment_completed' AND (
        payment_status IS NULL
        OR payment_status NOT IN ('paid', 'partially_refunded')
        OR refund_type IS NOT NULL
    ) THEN 'invalid_payment_completed_fields'
    WHEN event_type = 'payment_refunded' AND (
        payment_status IS NULL OR refund_type IS NULL
        OR NOT (
            (payment_status = 'partially_refunded' AND refund_type = 'partial')
            OR (payment_status = 'refunded' AND refund_type = 'full')
        )
    ) THEN 'invalid_payment_refunded_fields'
    ELSE NULL
END AS validation_error
FROM typed
