-- 한 행은 결제 원장의 개별 수납·환불 거래(payment_transaction_id)다.
-- 공통 event_id 중복 제거는 events_clean에서 끝났고, 실행기가 거래 ID 유일성을 검사한다.
SELECT
    payment_transaction_id,
    payment_id,
    booking_id,
    shop_id,
    request_amount_krw,
    transaction_type,
    amount_krw,
    payment_status,
    refund_type,
    event_id,
    event_type,
    schema_version,
    event_time AS occurred_at,
    ingest_time,
    payload,
    kafka_key,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    kafka_headers,
    bronze_ingested_at
FROM silver_payment_transaction_candidates
WHERE validation_error IS NULL
