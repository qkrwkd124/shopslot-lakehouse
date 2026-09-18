{{
  config(
    materialized='table',
    tblproperties={
      'format-version': '2',
      'write.format.default': 'parquet'
    }
  )
}}

-- ephemeral 중간 모델이 컴파일 시 CTE로 삽입된다. 이 모델은 정상 행만 저장한다.
select
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
from {{ ref('int_booking_events_validated') }}
where validation_error is null
