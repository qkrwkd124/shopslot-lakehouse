{{
  config(
    materialized='table',
    tblproperties={
      'format-version': '2',
      'write.format.default': 'parquet'
    }
  )
}}

select
    id as shop_id,
    shop_name,
    region,
    is_active,
    created_at,
    updated_at,
    opened_on,
    closed_on,
    bronze_ingested_at
from {{ source('bronze', 'shops') }}