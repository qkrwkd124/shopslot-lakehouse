{{
  config(
    materialized='table',
    tblproperties={
      'format-version': '2',
      'write.format.default': 'parquet'
    }
  )
}}
with booking_counts as (
    select
    shop_id,
    cast(event_time as date) as report_date,
    count(case when event_type = 'booking_created' then 1 end) as created_count,
    count(case when event_type = 'booking_cancelled' then 1 end) as cancelled_count
    from {{ ref('booking_events_clean') }}
    where event_type in ('booking_created', 'booking_cancelled')
    group by shop_id, cast(event_time as date)
),
report_dates as (
    select
    shop_id,
    shop_name,
    explode(
        sequence(opened_on, least(closed_on,date_sub(current_date(), 1)))
    ) as report_date
    from {{ref('dim_shops') }}
    where opened_on < current_date()
)
select 
	rd.shop_id,
	rd.shop_name,
	rd.report_date,
	coalesce(bc.created_count,0) as created_count,
	coalesce(bc.cancelled_count,0) as cancelled_count
from report_dates rd
left join booking_counts bc on rd.shop_id = bc.shop_id and rd.report_date = bc.report_date
;
