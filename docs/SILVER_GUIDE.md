# Silver 변환 명세 — booking_events_clean

2026-09-10. 네 개의 목표 테이블 중 `lakehouse.silver.booking_events_clean` 하나만 구현했다. 현재 구현은 Spark SQL full-refresh 배치다. dbt 이관 및 증분 처리는 후속 계획이며 현재 배치에 포함되지 않는다.

## 실행과 저장

```bash
make iceberg-up            # spark가 실행 중이면 생략 가능
make silver-events         # Bronze 전체 snapshot을 읽어 이 Silver 테이블만 재작성 후 종료
make iceberg-sql
```

`silver-events`는 재실행하면 기존 파생 테이블을 교체한다. Bronze·Kafka·MySQL 원본은 수정하지 않는다. `verify-silver-events`는 메모리 테스트를 먼저 실행한 뒤 동일한 재작성을 수행하므로 읽기 전용 검증 명령이 아니다. 동시에 여러 Silver writer를 실행하지 않는다.

새 컨테이너, streaming query, checkpoint는 없다. 기존 `spark`에서 실행하고, 같은 `lakehouse` catalog의 `silver` namespace에 Iceberg 테이블을 만든다. 결과 Parquet과 metadata는 MinIO에 저장된다. 소규모 전체 재계산 예제라 partition은 아직 추가하지 않았다.

## 구현 파일

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

## 검증 규칙과 한계

지원 범위는 schema v1과 현재 7가지 event_type이다. 필수 event_id(비어 있지 않은 문자열), 양수 booking_id/shop_id, 변환 가능한 event_time/ingest_time이 필요하다. 결제·환불 이벤트는 양수 payment_id/payment_transaction_id/amount_krw도 검사한다. 선택 필드가 제공됐는데 타입 변환에 실패하면 제외한다.

유효한 행 중 같은 event_id는 가장 이른 bronze_ingested_at을 우선하고, 동률이면 Kafka topic/partition/offset으로 대표 행을 결정한다. partition 사이 offset을 업무 전역 순서로 해석하지 않는다. 같은 event_id의 유효한 원문 문자열이 다르면 임의 선택 대신 쓰기 전에 실패시킨다. 공백만 다른 JSON도 충돌로 간주하는 보수적인 초기 정책이다.

제외 건수와 사유는 로그에 출력하고 원문은 Bronze에 보존한다. `event_dq` 영구 테이블은 아직 만들지 않았다. UUID 형식, FK 존재, 이벤트별 상태 전이, 선택 ID의 양수 여부 등 모든 업무 계약을 검증한 것은 아니다. 알 수 없는 추가 필드는 컬럼으로 펼치지 않고 payload에 남는다. v2 지원·JSON 의미 기반 충돌 판단·격리 테이블은 후속 과제다.

## 재실행 방식

실행 시작에 Bronze의 main snapshot ID를 고정해, 실행 중 새 이벤트가 추가돼도 읽는 구간이 바뀌지 않게 한다. 새 이벤트는 다음 실행에서 반영한다. 전체 결과를 계산하고 `CREATE OR REPLACE TABLE ... AS SELECT`로 파생 테이블만 교체한다. SparkCatalog의 원자적 테이블 교체를 사용하며 Bronze는 유지한다.

동일 Bronze snapshot에 같은 규칙을 적용하면 행 결과는 같다. 재실행이 아무 쓰기도 하지 않는다는 뜻은 아니며 Iceberg snapshot/파일은 새로 생길 수 있다. 빈 입력/유효한 이벤트 0건/원문 충돌이면 기존 Silver를 교체하지 않고 실패한다. 다중 writer 조정과 대량 증분 처리·snapshot maintenance는 아직 범위 밖이다.

## 조회 예제

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

## 확장 계획

### Full refresh → 증분 처리 비교 계획

2026-09-14 기준 후속 개발 계획이다. 증분 처리와 아래 성능·장애 실험은 아직 구현·검증하지 않았다.

현재 `booking_events_clean`은 실행 시작에 고정한 Bronze snapshot의 전체 데이터를 읽고 정제 결과로 테이블을 교체한다. snapshot은 직전 배치의 추가분이 아니라 해당 commit 이후 테이블 전체 상태를 나타낸다. 입력이 100건 → 300건 → 500건으로 늘면 매번 전체 100/300/500건을 처리한다. 모두 유효한 고유 이벤트라면 결과는 각각 100/300/500건이지 누적 900건이 아니다. 결과 중복 적재를 피하는 것과 기존 입력의 중복 계산을 피하는 것은 다르다.

1. **기준 결과 확보:** `bookings_current`까지 full refresh SQL로 작성한다. 이벤트별 정보와 현재 상태 규칙을 먼저 확정하고 소규모 입력으로 검증한다.
2. **증분 처리 설계:** 마지막 성공 입력 snapshot과 이번 종료 snapshot 사이의 추가 데이터를 읽는 방식을 설계한다. 현재 Bronze append-only 조건에서 출발하며 snapshot 만료·이력 단절 시 처리 정책도 정한다. snapshot ID는 숫자 크기로 순서를 비교하지 않는다.
3. **모델별 반영:** clean은 기존 `event_id`와 비교해 중복을 막고 동일 ID의 payload 충돌을 다룬다. current는 영향을 받은 `booking_id`의 상태를 재구성하고 `MERGE` 등으로 반영한다. 생성 정보 보존과 늦은 이벤트를 고려해야 하므로 단순 append로 전환하지 않는다.
4. **재실행 안전성:** 결과 쓰기 성공 뒤 진행 위치를 기록한다. 두 기록은 자동으로 하나의 트랜잭션이 되지 않으므로, 쓰기 성공 후 위치 기록 전에 실패해도 재처리가 중복을 만들지 않도록 설계한다. 실패 주입은 운영 데이터와 분리된 검증 환경에서 수행한다.
5. **정합성 비교:** 동일한 입력 종료 snapshot·변환 규칙으로 계산한 full refresh와 증분 결과를 별도 대상에서 비교한다. 건수뿐 아니라 양방향 행 차이, 키 중복, 예약별 상태·속성 일치를 검사한다. 동일 ID 재전송, 지연 도착, 기존 예약 변경, 동일 입력 재실행을 포함한다.
6. **규모별 비용 비교:** 작은 데이터에서 시작해 규모를 단계적으로 늘린다. 각 규모에서 같은 신규 이벤트 집합을 두 방식에 적용하고, Spark 자원·캐시 조건을 맞춰 소요 시간, 읽기/쓰기 bytes·records, 생성 파일 수를 기록한다. 증분 처리도 대상 테이블 조회·파일 재작성 비용이 있으므로 신규 입력만큼만 비용이 든다고 가정하지 않는다.

측정 결과는 입력 snapshot, 전체/신규 건수, 자원·캐시 조건, 처리 방식, 소요 시간, I/O, 결과 일치 여부와 함께 기록한다. 측정 전부터 증분 방식이 몇 배 빠르다고 주장하지 않는다. full refresh 기준 버전은 증분 구현의 정확성을 검증하는 비교 대상으로 유지한다.

### bookings_current 설계 범위

후속 모델 `bookings_current`는 예약별 현재 상태를 표현한다. 예약 상태와 결제 상태를 구분하고 생성 이벤트의 customer_id/staff_id 등 기본 정보를 보존해야 한다. 이벤트 시각 동률과 지연 도착 처리 규칙을 확정한 뒤 구현·검증한다. 현재 배포된 정제 배치의 기능으로 간주하지 않는다.

공식 참고: [Spark SQL 함수](https://spark.apache.org/docs/3.5.6/api/sql/index.html), [Iceberg RTAS](https://iceberg.apache.org/docs/latest/spark-ddl/#replace-table--as-select).
