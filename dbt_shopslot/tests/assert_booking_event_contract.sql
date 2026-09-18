-- 모델 필터가 예약 이벤트별 필수 필드 계약을 계속 보장하는지 검사한다.
select *
from {{ ref('booking_events_clean') }}
where booking_id <= 0
   or shop_id <= 0
   or (
       event_type = 'booking_created'
       and (
           customer_id is null or customer_id <= 0
           or service_id is null or service_id <= 0
           or staff_id is null or staff_id <= 0
           or start_at is null
           or booked_price_krw is null or booked_price_krw < 0
           or status is null or status <> 'scheduled'
       )
   )
   or (
       event_type = 'booking_rescheduled'
       and (
           old_start_at is null or start_at is null
           or status is null or status <> 'rescheduled'
       )
   )
   or (
       event_type = 'booking_cancelled'
       and (
           cancelled_at is null
           or status is null or status <> 'cancelled'
       )
   )
   or (
       event_type = 'checked_in'
       and (status is null or status <> 'checked_in')
   )
   or (
       event_type = 'no_show_marked'
       and (status is null or status <> 'no_show')
   )
