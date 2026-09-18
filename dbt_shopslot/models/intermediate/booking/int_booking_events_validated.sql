{{ config(materialized='ephemeral') }}

-- 예약 이벤트를 타입화하고 검증 결과를 붙이는 내부 중간 모델이다.
-- ephemeral 모델이므로 Iceberg 테이블로 저장되지 않고 ref()한 모델의 CTE로 삽입된다.
with booking_input as (
    select *
    from {{ source('existing_silver', 'events_clean') }}
    where event_type in (
        'booking_created',
        'booking_rescheduled',
        'booking_cancelled',
        'checked_in',
        'no_show_marked'
    )
), parsed as (
    select
        *,
        from_json(payload, 'STRUCT<
            customer_id: STRING, service_id: STRING, staff_id: STRING,
            start_at: STRING, old_start_at: STRING, cancelled_at: STRING,
            booked_price_krw: STRING, status: STRING
        >') as booking
    from booking_input
), typed as (
    select
        event_id,
        event_type,
        schema_version,
        event_time,
        ingest_time,
        booking_id,
        shop_id,
        try_cast(booking.customer_id as bigint) as customer_id,
        try_cast(booking.service_id as bigint) as service_id,
        try_cast(booking.staff_id as bigint) as staff_id,
        try_cast(booking.start_at as timestamp) as start_at,
        try_cast(booking.old_start_at as timestamp) as old_start_at,
        try_cast(booking.cancelled_at as timestamp) as cancelled_at,
        try_cast(booking.booked_price_krw as bigint) as booked_price_krw,
        booking.status as status,
        payload,
        kafka_key,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        kafka_headers,
        bronze_ingested_at,
        (booking.customer_id is not null and try_cast(booking.customer_id as bigint) is null)
        or (booking.service_id is not null and try_cast(booking.service_id as bigint) is null)
        or (booking.staff_id is not null and try_cast(booking.staff_id as bigint) is null)
        or (booking.start_at is not null and try_cast(booking.start_at as timestamp) is null)
        or (booking.old_start_at is not null and try_cast(booking.old_start_at as timestamp) is null)
        or (booking.cancelled_at is not null and try_cast(booking.cancelled_at as timestamp) is null)
        or (booking.booked_price_krw is not null and try_cast(booking.booked_price_krw as bigint) is null)
        as invalid_booking_type
    from parsed
), validated as (
    select
        *,
        case
            when invalid_booking_type then 'invalid_booking_type'
            when event_type = 'booking_created' and (
                customer_id is null or customer_id <= 0
                or service_id is null or service_id <= 0
                or staff_id is null or staff_id <= 0
                or start_at is null
                or booked_price_krw is null or booked_price_krw < 0
                or status is null or status <> 'scheduled'
            ) then 'invalid_booking_created_fields'
            when event_type = 'booking_rescheduled' and (
                old_start_at is null or start_at is null
                or status is null or status <> 'rescheduled'
            ) then 'invalid_booking_rescheduled_fields'
            when event_type = 'booking_cancelled' and (
                cancelled_at is null or status is null or status <> 'cancelled'
            ) then 'invalid_booking_cancelled_fields'
            when event_type = 'checked_in' and (status is null or status <> 'checked_in')
                then 'invalid_checked_in_fields'
            when event_type = 'no_show_marked' and (status is null or status <> 'no_show')
                then 'invalid_no_show_fields'
            else null
        end as validation_error
    from typed
)
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
    bronze_ingested_at,
    validation_error
from validated
