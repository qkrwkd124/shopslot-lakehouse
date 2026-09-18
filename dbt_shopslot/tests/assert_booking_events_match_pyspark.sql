-- dbt 이관 결과가 기존 PySpark full-refresh 기준 결과와 완전히 같은지 양방향 비교한다.
with dbt_result as (
    select * from {{ ref('booking_events_clean') }}
), pyspark_result as (
    select * from {{ source('existing_silver', 'booking_events_clean') }}
), dbt_minus_pyspark as (
    select * from dbt_result
    except all
    select * from pyspark_result
), pyspark_minus_dbt as (
    select * from pyspark_result
    except all
    select * from dbt_result
)
select 'dbt_minus_pyspark' as difference, * from dbt_minus_pyspark
union all
select 'pyspark_minus_dbt' as difference, * from pyspark_minus_dbt
