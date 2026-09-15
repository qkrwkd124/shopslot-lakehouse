-- 예약 1건 = 1행. 이벤트 스트림에 흩어진 delta를 합쳐 MySQL bookings 한 행을 재구성한다.
-- 결제 이벤트는 예약 상태를 바꾸지 않으므로 제외한다. 수납·환불은 payments_current의 책임이다.
WITH booking_events AS (
    SELECT *
    FROM silver_booking_events_clean
    WHERE event_type IN (
        'booking_created', 'booking_rescheduled', 'booking_cancelled',
        'checked_in', 'no_show_marked'
    )
),
-- 예약 상태를 바꾸는 이벤트 중 가장 나중 것. cancelled_at도 이 행에 딸려온다.
-- event_time 동률은 업무 순서로 판단할 수 없다. 뒤의 컬럼들은 결정론 확보용 tie-breaker이며
-- 서로 다른 partition의 offset을 전역 업무 순서로 해석하지 않는다.
latest_status AS (
    SELECT *, row_number() OVER (
        PARTITION BY booking_id
        ORDER BY event_time DESC, bronze_ingested_at DESC,
                 kafka_topic, kafka_partition, kafka_offset
    ) AS status_rank
    FROM booking_events
),
-- customer/service/staff/예약가격은 booking_created에만 실린다.
-- 같은 예약의 생성 이벤트가 재전송되면 최초 생성분을 대표로 삼는다.
creation AS (
    SELECT *, row_number() OVER (
        PARTITION BY booking_id
        ORDER BY event_time, bronze_ingested_at,
                 kafka_topic, kafka_partition, kafka_offset
    ) AS creation_rank
    FROM booking_events
    WHERE event_type = 'booking_created'
),
-- start_at은 booking_created와 booking_rescheduled에만 있다.
-- 취소·체크인·노쇼 이벤트에는 없으므로 상태와 별도로 최신 값을 구한다.
latest_start AS (
    SELECT *, row_number() OVER (
        PARTITION BY booking_id
        ORDER BY event_time DESC, bronze_ingested_at DESC,
                 kafka_topic, kafka_partition, kafka_offset
    ) AS start_rank
    FROM booking_events
    WHERE start_at IS NOT NULL
)
SELECT
    s.booking_id,
    s.shop_id,
    s.status,
    t.start_at,
    s.cancelled_at,
    c.customer_id,
    c.service_id,
    c.staff_id,
    c.booked_price_krw,
    -- 생성 이벤트를 관측하지 못한 예약. 행을 버리지 않고 표기만 남긴다.
    c.booking_id IS NULL AS is_orphan,
    s.event_id AS status_event_id,
    s.event_type AS status_event_type,
    s.event_time AS status_event_time,
    c.event_time AS created_event_time
FROM latest_status s
LEFT JOIN creation c ON c.booking_id = s.booking_id AND c.creation_rank = 1
LEFT JOIN latest_start t ON t.booking_id = s.booking_id AND t.start_rank = 1
WHERE s.status_rank = 1
