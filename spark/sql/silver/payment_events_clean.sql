-- 한 행은 검증을 통과한 결제 도메인 이벤트 하나(event_id)다.
-- 거래가 없는 payment_requested에서는 거래 관련 컬럼이 NULL이다.
SELECT
    event_id,
    event_type,
    schema_version,
    event_time,
    ingest_time,
    payment_id,
    payment_transaction_id,
    booking_id,
    shop_id,
    request_amount_krw,
    transaction_type,
    amount_krw,
    payout_amount_krw,
    refund_amount_krw,
    paid_amount_krw,
    unpaid_amount_krw,
    payment_status,
    needs_repayment,
    refund_type,
    payload,
    kafka_key,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    kafka_headers,
    bronze_ingested_at
FROM silver_payment_event_candidates
WHERE validation_error IS NULL
