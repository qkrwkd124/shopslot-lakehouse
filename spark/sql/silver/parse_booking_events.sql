-- events_clean에서 예약 lifecycle 이벤트만 선택하고 예약 전용 payload를 타입화한다.
WITH booking_input AS (
    SELECT *
    FROM silver_events_input
    WHERE event_type IN (
        'booking_created',
        'booking_rescheduled',
        'booking_cancelled',
        'checked_in',
        'no_show_marked'
    )
), parsed AS (
    SELECT *, from_json(payload, 'STRUCT<
        customer_id: STRING, service_id: STRING, staff_id: STRING,
        start_at: STRING, old_start_at: STRING, cancelled_at: STRING,
        booked_price_krw: STRING, status: STRING
    >') AS booking
    FROM booking_input
), typed AS (
    SELECT
        event_id,
        event_type,
        schema_version,
        event_time,
        ingest_time,
        booking_id,
        shop_id,
        try_cast(booking.customer_id AS BIGINT) AS customer_id,
        try_cast(booking.service_id AS BIGINT) AS service_id,
        try_cast(booking.staff_id AS BIGINT) AS staff_id,
        try_cast(booking.start_at AS TIMESTAMP) AS start_at,
        try_cast(booking.old_start_at AS TIMESTAMP) AS old_start_at,
        try_cast(booking.cancelled_at AS TIMESTAMP) AS cancelled_at,
        try_cast(booking.booked_price_krw AS BIGINT) AS booked_price_krw,
        booking.status AS status,
        payload,
        kafka_key,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        kafka_headers,
        bronze_ingested_at,
        (booking.customer_id IS NOT NULL AND try_cast(booking.customer_id AS BIGINT) IS NULL)
        OR (booking.service_id IS NOT NULL AND try_cast(booking.service_id AS BIGINT) IS NULL)
        OR (booking.staff_id IS NOT NULL AND try_cast(booking.staff_id AS BIGINT) IS NULL)
        OR (booking.start_at IS NOT NULL AND try_cast(booking.start_at AS TIMESTAMP) IS NULL)
        OR (booking.old_start_at IS NOT NULL AND try_cast(booking.old_start_at AS TIMESTAMP) IS NULL)
        OR (booking.cancelled_at IS NOT NULL AND try_cast(booking.cancelled_at AS TIMESTAMP) IS NULL)
        OR (booking.booked_price_krw IS NOT NULL AND try_cast(booking.booked_price_krw AS BIGINT) IS NULL)
        AS invalid_booking_type
    FROM parsed
)
SELECT *, CASE
    WHEN invalid_booking_type THEN 'invalid_booking_type'
    WHEN event_type = 'booking_created' AND (
        customer_id IS NULL OR customer_id <= 0
        OR service_id IS NULL OR service_id <= 0
        OR staff_id IS NULL OR staff_id <= 0
        OR start_at IS NULL
        OR booked_price_krw IS NULL OR booked_price_krw < 0
        OR status IS NULL OR status <> 'scheduled'
    ) THEN 'invalid_booking_created_fields'
    WHEN event_type = 'booking_rescheduled' AND (
        old_start_at IS NULL OR start_at IS NULL
        OR status IS NULL OR status <> 'rescheduled'
    ) THEN 'invalid_booking_rescheduled_fields'
    WHEN event_type = 'booking_cancelled' AND (
        cancelled_at IS NULL OR status IS NULL OR status <> 'cancelled'
    ) THEN 'invalid_booking_cancelled_fields'
    WHEN event_type = 'checked_in' AND (status IS NULL OR status <> 'checked_in')
        THEN 'invalid_checked_in_fields'
    WHEN event_type = 'no_show_marked' AND (status IS NULL OR status <> 'no_show')
        THEN 'invalid_no_show_fields'
    ELSE NULL
END AS validation_error
FROM typed
