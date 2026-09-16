-- 결제 요청·수납·환불 이벤트를 선택하고 payload를 결제 도메인 타입으로 변환한다.
WITH payment_input AS (
    SELECT *
    FROM silver_events_input
    WHERE event_type IN (
        'payment_requested',
        'payment_completed',
        'payment_refunded'
    )
), parsed AS (
    SELECT *, from_json(payload, 'STRUCT<
        payment_id: STRING, payment_transaction_id: STRING,
        request_amount_krw: STRING, amount_krw: STRING,
        payout_amount_krw: STRING, refund_amount_krw: STRING,
        paid_amount_krw: STRING, unpaid_amount_krw: STRING,
        payment_status: STRING, needs_repayment: BOOLEAN,
        refund_type: STRING
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
        try_cast(payment.payout_amount_krw AS BIGINT) AS payout_amount_krw,
        try_cast(payment.refund_amount_krw AS BIGINT) AS refund_amount_krw,
        try_cast(payment.paid_amount_krw AS BIGINT) AS paid_amount_krw,
        try_cast(payment.unpaid_amount_krw AS BIGINT) AS unpaid_amount_krw,
        payment.payment_status AS payment_status,
        payment.needs_repayment AS needs_repayment,
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
        OR (payment.payout_amount_krw IS NOT NULL
            AND try_cast(payment.payout_amount_krw AS BIGINT) IS NULL)
        OR (payment.refund_amount_krw IS NOT NULL
            AND try_cast(payment.refund_amount_krw AS BIGINT) IS NULL)
        OR (payment.paid_amount_krw IS NOT NULL
            AND try_cast(payment.paid_amount_krw AS BIGINT) IS NULL)
        OR (payment.unpaid_amount_krw IS NOT NULL
            AND try_cast(payment.unpaid_amount_krw AS BIGINT) IS NULL)
        AS invalid_payment_type
    FROM parsed
), validated AS (
    SELECT *, CASE
        WHEN invalid_payment_type THEN 'invalid_payment_type'
        WHEN payment_id IS NULL OR payment_id <= 0 THEN 'invalid_payment_id'
        WHEN request_amount_krw IS NULL OR request_amount_krw <= 0
            THEN 'invalid_payment_request_amount'
        WHEN payout_amount_krw IS NULL OR payout_amount_krw < 0
            OR refund_amount_krw IS NULL OR refund_amount_krw < 0
            OR paid_amount_krw IS NULL OR paid_amount_krw < 0
            OR unpaid_amount_krw IS NULL OR unpaid_amount_krw < 0
            OR needs_repayment IS NULL
            THEN 'invalid_payment_summary'
        WHEN payout_amount_krw <> paid_amount_krw + refund_amount_krw
            OR paid_amount_krw > request_amount_krw
            THEN 'inconsistent_payment_amounts'
        WHEN event_type = 'payment_requested' AND (
            payment_transaction_id IS NOT NULL OR transaction_type IS NOT NULL
            OR amount_krw IS NOT NULL OR refund_type IS NOT NULL
            OR payout_amount_krw <> 0 OR refund_amount_krw <> 0
            OR paid_amount_krw <> 0 OR unpaid_amount_krw <> request_amount_krw
            OR payment_status <> 'unpaid' OR needs_repayment
        ) THEN 'invalid_payment_requested_fields'
        WHEN event_type IN ('payment_completed', 'payment_refunded') AND (
            payment_transaction_id IS NULL OR payment_transaction_id <= 0
            OR amount_krw IS NULL OR amount_krw <= 0
            OR amount_krw > request_amount_krw
        ) THEN 'invalid_payment_transaction_fields'
        WHEN event_type = 'payment_completed' AND refund_type IS NOT NULL
            THEN 'invalid_payment_completed_fields'
        WHEN event_type = 'payment_refunded' AND (
            refund_type IS NULL
            OR (paid_amount_krw = 0 AND refund_type <> 'full')
            OR (paid_amount_krw > 0 AND refund_type <> 'partial')
        ) THEN 'invalid_payment_refunded_fields'
        WHEN NOT (
            (payment_status = 'unpaid'
                AND paid_amount_krw < request_amount_krw
                AND unpaid_amount_krw = request_amount_krw - paid_amount_krw)
            OR (payment_status = 'paid'
                AND paid_amount_krw = request_amount_krw
                AND unpaid_amount_krw = 0 AND NOT needs_repayment)
            OR (payment_status = 'partially_refunded'
                AND paid_amount_krw > 0 AND paid_amount_krw < request_amount_krw
                AND refund_amount_krw > 0
                AND unpaid_amount_krw = 0 AND NOT needs_repayment)
            OR (payment_status = 'refunded'
                AND paid_amount_krw = 0 AND refund_amount_krw > 0
                AND unpaid_amount_krw = 0 AND NOT needs_repayment)
        ) THEN 'inconsistent_payment_status'
        WHEN needs_repayment AND payment_status <> 'unpaid'
            THEN 'invalid_repayment_status'
        ELSE NULL
    END AS validation_error
    FROM typed
)
SELECT * FROM validated
