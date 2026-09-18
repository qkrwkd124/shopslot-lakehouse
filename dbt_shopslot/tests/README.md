# Tests

- `assert_booking_event_contract.sql`: 예약 이벤트 종류별 필수 필드와 상태 계약을 검사한다.
- `assert_booking_events_match_pyspark.sql`: dbt와 기존 PySpark 결과를 `EXCEPT ALL`로 양방향 비교한다.

테스트 SQL은 위반 행을 반환하도록 작성하며 결과가 0행일 때 통과한다.
