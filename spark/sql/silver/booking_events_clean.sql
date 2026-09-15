-- 공통 이벤트 계약과 event_id 중복 제거는 events_clean에서 끝났다.
-- 이 테이블은 예약 도메인 계약을 통과한 typed lifecycle 이벤트만 보존한다.
SELECT
    event_id,
    event_type,
    schema_version,
    event_time,
    ingest_time,
    booking_id,
    shop_id,
    customer_id,
    service_id,
    staff_id,
    start_at,
    old_start_at,
    cancelled_at,
    booked_price_krw,
    status,
    payload,
    kafka_key,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    kafka_headers,
    bronze_ingested_at
FROM silver_booking_event_candidates
WHERE validation_error IS NULL
