# Silver 첫 모델 — booking_events_clean

2026-09-10. 네 개의 목표 테이블 중 `lakehouse.silver.booking_events_clean` 하나만 구현했다. 이 단계는 dbt 이전의 Spark SQL 학습용 배치다. dbt 모델/테스트로 옮기는 최종 방향은 유지하지만, 이번에는 실행 환경을 늘리지 않는다.

## 실행과 저장

```bash
make iceberg-up            # spark가 실행 중이면 생략 가능
make silver-events         # Bronze 전체 snapshot을 읽어 이 Silver 테이블만 재작성 후 종료
make iceberg-sql
```

`silver-events`는 재실행하면 기존 파생 테이블을 교체한다. Bronze·Kafka·MySQL 원본은 수정하지 않는다. `verify-silver-events`는 메모리 테스트를 먼저 실행한 뒤 동일한 재작성을 수행하므로 읽기 전용 검증 명령이 아니다. 동시에 여러 Silver writer를 실행하지 않는다.

새 컨테이너, streaming query, checkpoint는 없다. 기존 `spark`에서 실행하고, 같은 `lakehouse` catalog의 `silver` namespace에 Iceberg 테이블을 만든다. 결과 Parquet과 metadata는 MinIO에 저장된다. 소규모 전체 재계산 예제라 partition은 아직 추가하지 않았다.

## 파일을 읽는 순서

1. `spark/sql/silver/parse_booking_events.sql`: JSON 파싱 → 타입 변환 → 검증 사유 분류.
2. `spark/sql/silver/booking_events_clean.sql`: 유효한 행 → event_id별 순번 → 대표 행 선택.
3. `spark/jobs/silver_booking_events.py`: SQL을 실행하고 결과를 Iceberg에 저장하는 얇은 실행기.
4. `spark/jobs/test_silver_booking_events.py`: 원본 저장소를 건드리지 않는 작은 입력 예제.

`silver_bronze_input`, `silver_event_candidates`, `silver_clean_result`는 해당 SparkSession 안에서만 존재하는 임시 view다. 별도의 영구 Silver 테이블 세 개를 추가한 것이 아니다.

## 한 행의 의미와 컬럼

한 행은 **유효한 논리 이벤트 한 개(event_id)**다. booking_id가 같아도 생성·변경·체크인 등 서로 다른 이벤트는 모두 남는다. 예약별 최신 상태 한 행은 다음 `bookings_current`의 책임이다.

| 컬럼 묶음 | 용도 |
| --- | --- |
| event_id, event_type, schema_version | 이벤트 식별과 계약 |
| event_time, ingest_time | payload의 업무 시각·ingest 시각을 timestamp로 변환 |
| booking_id, shop_id, customer_id, service_id, staff_id | BIGINT 식별자. 이벤트에 없는 선택 필드는 NULL |
| start_at, old_start_at, cancelled_at | 해당 이벤트에 포함된 예약 관련 시각 |
| booked_price_krw, status | 예약 가격과 상태. 최신 상태를 보충하지 않음 |
| payment_id, payment_transaction_id, amount_krw, payment_status | 결제·환불 정보 |
| payload, kafka_topic/partition/offset, kafka_timestamp, bronze_ingested_at | 원문과 추적 위치 |

`ingest_time`은 payload 값을 변환한 것이며 실제 Kafka 도착 시각을 새로 측정하지 않는다. 현재 P0 generator는 event_time과 같은 값을 넣는다. Kafka timestamp·Bronze 적재 시각과 혼동하지 않는다. 시간 표시 설정은 기존 Asia/Seoul을 사용한다.

## 핵심 SQL 개념

- `WITH`: 중간 SELECT에 이름을 붙여 파싱·변환·정제 단계를 읽기 쉽게 나눈다.
- `from_json`: 지정한 필드 구조로 JSON을 펼친다. PERMISSIVE와 corrupt-record 필드로 잘못된 JSON을 분류한다.
- `try_cast`: 숫자/시간 변환이 불가능하면 NULL로 만들어 검증할 수 있게 한다.
- `CASE`: 위반 조건에 따라 최초 오류 사유 하나를 지정한다.
- `row_number() over (partition by event_id order by ...)`: 같은 논리 이벤트끼리 묶어서 대표 행을 고른다.

여기서 window 함수의 `PARTITION BY event_id`는 논리적인 그룹 구분이다. Kafka partition이나 Iceberg 저장 partition을 만드는 명령이 아니다.

## 이번 검증 규칙과 한계

지원 범위는 schema v1과 현재 7가지 event_type이다. 필수 event_id(비어 있지 않은 문자열), 양수 booking_id/shop_id, 변환 가능한 event_time/ingest_time이 필요하다. 결제·환불 이벤트는 양수 payment_id/payment_transaction_id/amount_krw도 검사한다. 선택 필드가 제공됐는데 타입 변환에 실패하면 제외한다.

유효한 행 중 같은 event_id는 가장 이른 bronze_ingested_at을 우선하고, 동률이면 Kafka topic/partition/offset으로 대표 행을 결정한다. partition 사이 offset을 업무 전역 순서로 해석하지 않는다. 같은 event_id의 유효한 원문 문자열이 다르면 임의 선택 대신 쓰기 전에 실패시킨다. 공백만 다른 JSON도 충돌로 간주하는 보수적인 초기 정책이다.

제외 건수와 사유는 로그에 출력하고 원문은 Bronze에 보존한다. `event_dq` 영구 테이블은 아직 만들지 않았다. UUID 형식, FK 존재, 이벤트별 상태 전이, 선택 ID의 양수 여부 등 모든 업무 계약을 검증한 것은 아니다. 알 수 없는 추가 필드는 컬럼으로 펼치지 않고 payload에 남는다. v2 지원·JSON 의미 기반 충돌 판단·격리 테이블은 후속 과제다.

## 재실행 방식

실행 시작에 Bronze의 main snapshot ID를 고정해, 실행 중 새 이벤트가 추가돼도 읽는 구간이 바뀌지 않게 한다. 새 이벤트는 다음 실행에서 반영한다. 전체 결과를 계산하고 `CREATE OR REPLACE TABLE ... AS SELECT`로 파생 테이블만 교체한다. SparkCatalog의 원자적 테이블 교체를 사용하며 Bronze는 유지한다.

동일 Bronze snapshot에 같은 규칙을 적용하면 행 결과는 같다. 재실행이 아무 쓰기도 하지 않는다는 뜻은 아니며 Iceberg snapshot/파일은 새로 생길 수 있다. 빈 입력/유효한 이벤트 0건/원문 충돌이면 기존 Silver를 교체하지 않고 실패한다. 다중 writer 조정과 대량 증분 처리·snapshot maintenance는 아직 범위 밖이다.

## 직접 조회하기

```sql
SELECT event_id, booking_id, event_type, event_time
FROM lakehouse.silver.booking_events_clean
ORDER BY booking_id, event_time;

SELECT event_type, count(*)
FROM lakehouse.silver.booking_events_clean GROUP BY event_type;

-- 결과가 없어야 한다: 논리 이벤트 중복
SELECT event_id, count(*) AS cnt
FROM lakehouse.silver.booking_events_clean GROUP BY event_id HAVING count(*) > 1;

SELECT file_path, record_count FROM lakehouse.silver.booking_events_clean.files;
```

## 네가 이어서 만들 부분

다음 후보는 `bookings_current`다. 먼저 예약 하나에 여러 이벤트가 있을 때 무엇을 최신 상태로 선택할지, payment 이벤트가 예약 상태를 덮으면 안 되는 이유, 이후 이벤트에 없는 customer_id/staff_id를 어디에서 가져올지 정리한다. 이 문서에는 다음 테이블의 완성 SQL은 넣지 않는다.

공식 참고: [Spark SQL 함수](https://spark.apache.org/docs/3.5.6/api/sql/index.html), [Iceberg RTAS](https://iceberg.apache.org/docs/latest/spark-ddl/#replace-table--as-select).
