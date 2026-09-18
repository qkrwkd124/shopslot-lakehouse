# Models

- `sources.yml`: 기존 `lakehouse.silver`의 입력과 PySpark 기준 결과를 source로 등록한다.
- `intermediate/booking/int_booking_events_validated.sql`: 예약 lifecycle을 타입화하고 `validation_error`를 계산하는 ephemeral 중간 모델이다. 실제 테이블을 만들지 않고 이를 참조하는 SQL의 CTE로 컴파일된다.
- `intermediate/booking/_int_booking_events_validated.yml`: 중간 모델과 검증 결과 컬럼을 문서화한다.
- `silver/booking_events_clean.sql`: 중간 모델에서 정상 행만 선택해 저장하는 첫 full-refresh dbt 모델이다.
- `silver/_booking_events_clean.yml`: 모델 설명과 event_id·event_type·업무 ID 기본 테스트를 정의한다.

dbt 결과는 기존 PySpark 테이블과 분리된 `lakehouse.silver_dbt`에 생성한다. 첫 모델의 결과 일치를 확인한 뒤 증분 materialization을 별도 단계에서 적용한다.
