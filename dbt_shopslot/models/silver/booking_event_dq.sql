{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dq_key',
    merge_exclude_columns=['first_detected_at'],
    file_format='iceberg',
    tblproperties={
      'format-version': '2',
      'write.format.default': 'parquet'
    }
  )
}}

-- 예약 오류만 관리한다. 다른 도메인의 DQ 모델과 저장 테이블을 공유하지 않는다.
{{ event_dq_rows(ref('int_booking_events_validated'), 'booking_events_clean') }}
