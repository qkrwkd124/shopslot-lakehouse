# Silver 변환 명세

2026-09-16. 공통 논리 이벤트 경계 `lakehouse.silver.events_clean`, 예약 전용 `booking_events_clean`, 결제 전용 `payment_events_clean`, 예약 상태 `bookings_current`를 구현했다. 현재 구현은 Spark SQL full-refresh 배치다. 결제 current, dbt 이관 및 증분 처리는 후속 계획이다.

## 실행과 저장

```bash
make iceberg-up            # spark가 실행 중이면 생략 가능
make silver-events-clean   # Bronze 전체 snapshot에서 공통 논리 이벤트를 재작성
make silver-bookings-events # events_clean에서 예약 이벤트 clean을 재작성
make silver-bookings-current # clean 전체 snapshot을 읽어 예약별 현재 상태를 재작성 후 종료
make silver-payment-events # events_clean에서 결제 이벤트 clean을 재작성
make iceberg-sql
```

각 명령은 재실행하면 자기 파생 테이블을 교체한다. Bronze·Kafka·MySQL 원본은 수정하지 않는다. 동시에 같은 Silver 테이블에 여러 writer를 실행하지 않는다.

새 컨테이너, streaming query, checkpoint는 없다. 기존 `spark`에서 실행하고, 같은 `lakehouse` catalog의 `silver` namespace에 Iceberg 테이블을 만든다. 결과 Parquet과 metadata는 MinIO에 저장된다. 소규모 전체 재계산 예제라 partition은 아직 추가하지 않았다.

## 구현 파일

1. `spark/sql/silver/parse_events.sql`: 공통 envelope 파싱·타입 변환·검증 사유 분류.
2. `spark/sql/silver/events_clean.sql`: 유효한 행의 event_id 중복 제거.
3. `spark/jobs/silver_events_clean.py`: Bronze snapshot을 고정하고 `events_clean`을 Iceberg에 저장.
4. `spark/sql/silver/parse_booking_events.sql`: 예약 이벤트 5종 필터링, payload 타입화와 도메인 검증.
5. `spark/sql/silver/booking_events_clean.sql`: 도메인 계약을 통과한 예약 이벤트 컬럼 선택.
6. `spark/jobs/silver_bookings_events.py`: `events_clean` snapshot을 고정하고 예약 clean을 저장.
7. `spark/sql/silver/bookings_current.sql`: 예약 이벤트에 흩어진 속성과 최신 상태를 예약별 한 행으로 재구성.
8. `spark/jobs/silver_bookings_current.py`: 입력 snapshot 고정, 중복 검사와 Iceberg 저장을 담당하는 실행기.
9. `spark/sql/silver/parse_payment_events.sql`: 결제 이벤트 3종 필터링, payload 타입화와 결제 요약 계약 검증.
10. `spark/sql/silver/payment_events_clean.sql`: 유효한 결제 요청·수납·환불 이벤트 컬럼 선택.
11. `spark/jobs/silver_payment_events.py`: 입력 snapshot, 요청 금액 일관성과 거래 ID 유일성을 검사하고 결제 clean을 저장.

`silver_bronze_input`, `silver_events_input`, `silver_event_candidates`, `silver_booking_event_candidates`, `silver_payment_event_candidates`와 각 `*_result`는 해당 SparkSession 안에서만 존재하는 임시 view다.

## events_clean 공통 경계

한 행은 중복 제거된 유효한 논리 이벤트 하나(`event_id`)다. Bronze의 물리 Kafka 레코드와 도메인별 타입 모델 사이에서 공통 계약만 책임진다. `event_id`, `event_type`, `schema_version`, 공통 시각과 ID를 타입화하고 payload 및 Kafka key/topic/partition/offset/timestamp/headers, Bronze 적재 시각을 보존한다.

예약의 customer/service/staff/status와 결제의 payment/transaction/amount/refund 필드는 펼치지 않는다. 따라서 향후 `refund_type` 같은 결제 필드가 바뀌어도 `events_clean` 스키마와 예약 모델은 영향을 받지 않는다. 공통 계약을 통과하지 못한 원문은 Bronze에 남으며, 영구 격리는 후속 `event_dq`의 책임이다.

현재 이벤트 계약은 단일 `booking.events.v1` 토픽을 전제로 `booking_id`와 `shop_id`를 공통 필수 ID로 사용한다. 향후 예약·결제 토픽을 나누면 토픽별 staging 경계와 generic aggregate 식별자를 다시 설계한다.

## booking_events_clean 한 행의 의미와 컬럼

한 행은 **도메인 계약을 통과한 예약 lifecycle 이벤트 한 개(event_id)**다. booking_id가 같아도 생성·변경·체크인 등 서로 다른 이벤트는 모두 남는다. 결제 이벤트는 들어오지 않으며 예약별 최신 상태 한 행은 `bookings_current`의 책임이다.

| 컬럼 묶음 | 용도 |
| --- | --- |
| event_id, event_type, schema_version | 이벤트 식별과 계약 |
| event_time, ingest_time | payload의 업무 시각·ingest 시각을 timestamp로 변환 |
| booking_id, shop_id, customer_id, service_id, staff_id | BIGINT 식별자. 생성 이후 delta 이벤트에 없는 속성은 NULL |
| start_at, old_start_at, cancelled_at | 해당 이벤트에 포함된 예약 관련 시각 |
| booked_price_krw, status | 예약 가격과 상태. 최신 상태를 보충하지 않음 |
| payload, kafka_key/headers, topic/partition/offset, kafka_timestamp, bronze_ingested_at | 원문과 추적 위치 |

`ingest_time`은 payload 값을 변환한 것이며 실제 Kafka 도착 시각을 새로 측정하지 않는다. 현재 P0 generator는 event_time과 같은 값을 넣는다. Kafka timestamp·Bronze 적재 시각과 혼동하지 않는다. 시간 표시 설정은 기존 Asia/Seoul을 사용한다.

## bookings_current 재구성

한 행은 예약 한 건(`booking_id`)이다. 결제 이벤트를 제외한 예약 이벤트 5종에서 최신 상태를 고르고, `booking_created`의 고객·서비스·직원·예약 가격과 `start_at`이 있는 최신 이벤트의 예약 시각을 결합한다. 상태 이벤트만 있고 생성 이벤트가 없으면 행을 버리지 않고 기본 정보는 NULL, `is_orphan=true`로 남긴다.

`event_time` 동률에서는 `bronze_ingested_at`, Kafka topic/partition/offset을 결정론적 tie-breaker로 사용한다. 서로 다른 Kafka partition의 offset을 업무 전역 순서로 해석하지 않는다. 실행기는 `booking_events_clean`의 main snapshot을 고정해 동시 교체가 실행 중 입력을 바꾸지 않게 한다. 저장 전 빈 결과, `booking_id` 유일성, 입력의 고유 booking_id가 결과에 모두 포함됐는지를 검사한 뒤 `CREATE OR REPLACE TABLE`로 결과를 저장한다.

현재 모델은 upstream 예약 clean의 이벤트 종류와 필드 검증을 신뢰하고 같은 검증을 반복하지 않는다. 저장 후 전체 행을 `exceptAll`로 다시 비교하지도 않는다. Iceberg 쓰기 자체의 성공 여부는 Spark 명령의 예외로 판단하고, current 단계에서는 새 grain과 상태 재구성에서 생길 수 있는 누락·join fan-out만 검사한다. orphan은 배치를 실패시키지 않고 `is_orphan=true`와 실행 로그 건수로 남기며, 영구 이력과 알림은 후속 `event_dq`에서 담당한다.

## payment_events_clean 한 행의 의미와 컬럼

한 행은 결제 도메인 이벤트 하나(`event_id`)다. 결제 요청 `payment_requested`부터 수납 `payment_completed`, 환불 `payment_refunded`까지 같은 생명주기를 보존한다. 거래가 없는 요청 이벤트는 `payment_transaction_id`, `transaction_type`, `amount_krw`, `refund_type`이 NULL이다. 수납과 환불은 각각 `transaction_type=payment`, `transaction_type=refund`로 정규화한다.

모든 이벤트는 양수 payment ID와 요청 금액을 요구한다. 거래 이벤트는 추가로 양수 transaction ID와 거래 금액을 요구하며, 서로 다른 event_id가 같은 거래 ID를 사용하면 기존 테이블을 교체하지 않고 실패한다. 같은 payment_id의 요청 금액도 이벤트 사이에서 달라질 수 없다.

각 이벤트에는 이벤트 발생 직후의 결제 요약도 함께 기록된다. `payout_amount_krw`와 `refund_amount_krw`는 누적 수납·환불이고 `paid_amount_krw`는 둘의 차이인 순수납액이다. `unpaid_amount_krw`는 실제로 다시 받을 금액이며 일반 환불에서는 0, `needs_repayment=true`인 환불에서는 청구액과 순수납액의 차이다. parser는 이 금액 관계와 상태 조합을 검증하지만 값을 다시 계산해 수정하지 않는다.

거래 통계는 `payment_transaction_id IS NOT NULL` 조건으로 같은 테이블에서 조회할 수 있다. 별도 `payment_transactions_clean` 물리 테이블은 현재 데이터와 검증 로직이 중복되므로 새 흐름에서는 사용하지 않는다. 거래 전용 보존 정책·권한·성능 요구가 생기면 view 또는 별도 모델로 분리한다. Silver `payments_current`는 이벤트 순서와 요약 필드로 결제별 최신 상태를 재구성할 예정이다.

## 핵심 SQL 개념

- `WITH`: 중간 SELECT에 이름을 붙여 파싱·변환·정제 단계를 읽기 쉽게 나눈다.
- `from_json`: 지정한 필드 구조로 JSON을 펼친다. PERMISSIVE와 corrupt-record 필드로 잘못된 JSON을 분류한다.
- `try_cast`: 숫자/시간 변환이 불가능하면 NULL로 만들어 검증할 수 있게 한다.
- `CASE`: 위반 조건에 따라 최초 오류 사유 하나를 지정한다.
- `row_number() over (partition by event_id order by ...)`: `events_clean`에서 같은 논리 이벤트끼리 묶어서 대표 행을 고른다.

여기서 window 함수의 `PARTITION BY event_id`는 논리적인 그룹 구분이다. Kafka partition이나 Iceberg 저장 partition을 만드는 명령이 아니다.

## 검증 규칙과 한계

`events_clean`은 schema v1, 비어 있지 않은 event_id/event_type, 양수 booking_id/shop_id, 변환 가능한 event_time/ingest_time을 공통 검증한다. 예약 clean은 예약 이벤트 5종만 선택하고 선택 필드가 제공됐는데 숫자·시각 타입 변환에 실패하면 제외한다. 생성은 customer/service/staff/start_at/price와 `scheduled`, 일정 변경은 old/new start와 `rescheduled`, 취소는 cancelled_at과 `cancelled`, 체크인·노쇼는 각각 대응 상태를 요구한다.

공통 경계에서 같은 event_id는 가장 이른 bronze_ingested_at을 우선하고, 동률이면 Kafka topic/partition/offset으로 대표 행을 결정한다. partition 사이 offset을 업무 전역 순서로 해석하지 않는다. 같은 event_id의 유효한 원문 문자열이 다르면 임의 선택 대신 쓰기 전에 실패시킨다. 예약 clean은 이 결과를 신뢰하므로 중복 제거를 반복하지 않는다.

제외 건수와 사유는 로그에 출력하고 원문은 upstream `events_clean`과 Bronze에 보존한다. `event_dq` 영구 테이블은 아직 만들지 않았다. UUID 형식, FK 존재, 이전 상태를 고려한 상태 전이, 일정 변경 전후 시각 차이 등 모든 업무 계약을 검증한 것은 아니다. 알 수 없는 추가 필드는 컬럼으로 펼치지 않고 payload에 남는다. v2 지원·JSON 의미 기반 충돌 판단·격리 테이블은 후속 과제다.

## 재실행 방식

실행 시작에 Bronze의 main snapshot ID를 고정해, 실행 중 새 이벤트가 추가돼도 읽는 구간이 바뀌지 않게 한다. 새 이벤트는 다음 실행에서 반영한다. 전체 결과를 계산하고 `CREATE OR REPLACE TABLE ... AS SELECT`로 파생 테이블만 교체한다. SparkCatalog의 원자적 테이블 교체를 사용하며 Bronze는 유지한다.

동일 Bronze snapshot에 같은 규칙을 적용하면 행 결과는 같다. 재실행이 아무 쓰기도 하지 않는다는 뜻은 아니며 Iceberg snapshot/파일은 새로 생길 수 있다. 빈 입력/유효한 이벤트 0건/원문 충돌이면 기존 Silver를 교체하지 않고 실패한다. 다중 writer 조정과 대량 증분 처리·snapshot maintenance는 아직 범위 밖이다.

## 조회 예제

```sql
SELECT event_id, booking_id, event_type, event_time
FROM lakehouse.silver.booking_events_clean
ORDER BY booking_id, event_time;

SELECT payment_transaction_id, payment_id, transaction_type, amount_krw
FROM lakehouse.silver.payment_events_clean
WHERE payment_transaction_id IS NOT NULL
ORDER BY payment_id, event_time;

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

현재 `events_clean`은 고정한 Bronze snapshot 전체를, `booking_events_clean`은 고정한 events_clean snapshot 전체를 읽고 각 결과 테이블을 교체한다. snapshot은 직전 배치의 추가분이 아니라 해당 commit 이후 테이블 전체 상태를 나타낸다. 입력이 100건 → 300건 → 500건으로 늘면 매번 전체를 처리한다. 결과를 append하지 않으므로 누적 중복은 없지만 기존 입력을 다시 계산하는 비용은 발생한다.

1. **기준 결과 확보:** `bookings_current`까지 full refresh SQL로 작성한다. 이벤트별 정보와 현재 상태 규칙을 먼저 확정하고 소규모 입력으로 검증한다.
2. **증분 처리 설계:** 마지막 성공 입력 snapshot과 이번 종료 snapshot 사이의 추가 데이터를 읽는 방식을 설계한다. 현재 Bronze append-only 조건에서 출발하며 snapshot 만료·이력 단절 시 처리 정책도 정한다. snapshot ID는 숫자 크기로 순서를 비교하지 않는다.
3. **모델별 반영:** clean은 기존 `event_id`와 비교해 중복을 막고 동일 ID의 payload 충돌을 다룬다. current는 영향을 받은 `booking_id`의 상태를 재구성하고 `MERGE` 등으로 반영한다. 생성 정보 보존과 늦은 이벤트를 고려해야 하므로 단순 append로 전환하지 않는다.
4. **재실행 안전성:** 결과 쓰기 성공 뒤 진행 위치를 기록한다. 두 기록은 자동으로 하나의 트랜잭션이 되지 않으므로, 쓰기 성공 후 위치 기록 전에 실패해도 재처리가 중복을 만들지 않도록 설계한다. 실패 주입은 운영 데이터와 분리된 검증 환경에서 수행한다.
5. **정합성 비교:** 동일한 입력 종료 snapshot·변환 규칙으로 계산한 full refresh와 증분 결과를 별도 대상에서 비교한다. 건수뿐 아니라 양방향 행 차이, 키 중복, 예약별 상태·속성 일치를 검사한다. 동일 ID 재전송, 지연 도착, 기존 예약 변경, 동일 입력 재실행을 포함한다.
6. **규모별 비용 비교:** 작은 데이터에서 시작해 규모를 단계적으로 늘린다. 각 규모에서 같은 신규 이벤트 집합을 두 방식에 적용하고, Spark 자원·캐시 조건을 맞춰 소요 시간, 읽기/쓰기 bytes·records, 생성 파일 수를 기록한다. 증분 처리도 대상 테이블 조회·파일 재작성 비용이 있으므로 신규 입력만큼만 비용이 든다고 가정하지 않는다.

측정 결과는 입력 snapshot, 전체/신규 건수, 자원·캐시 조건, 처리 방식, 소요 시간, I/O, 결과 일치 여부와 함께 기록한다. 측정 전부터 증분 방식이 몇 배 빠르다고 주장하지 않는다. full refresh 기준 버전은 증분 구현의 정확성을 검증하는 비교 대상으로 유지한다.

### 이벤트 스키마 진화와 Schema Registry 실험 계획

현재 plain JSON 이벤트의 `schema_version=1`을 애플리케이션과 Silver가 수동으로 관리한다. 후속 실험에서는 Schema Registry에 JSON Schema 또는 Avro/Protobuf 계약을 등록하고 호환성 정책을 적용한다. 완료된 구현이 아니며 도구 도입 자체보다 운영 중 계약 전환과 과거 이벤트 재처리를 검증하는 것이 목적이다.

1. **v2 계약 정의:** 필드의 추가·삭제·타입 및 의미 변경을 구분하고, 하위 호환 변경인지 새로운 버전이 필요한 변경인지 결정한다.
2. **소비자 선배포:** Silver가 `v1`과 `v2`를 각각 해석한 뒤 하나의 표준 clean 스키마로 정규화하도록 먼저 배포한다. 전체 Silver job을 버전별로 복제하지 않고 버전별 parser를 경계로 분리한다.
3. **생산자 전환:** 애플리케이션의 신규 발행을 `v2`로 전환하고, 전환 기간에는 버전별 입력·제외 건수와 오류를 관찰한다.
4. **호환성 검증:** 호환되는 변경은 등록되는지, 필수 필드 삭제나 비호환 타입 변경은 Registry 정책에 따라 거부되는지 확인한다.
5. **재처리와 격리:** 신규 `v1` 발행이 끝나도 Bronze의 과거 `v1`과 새 `v2`를 함께 full refresh해 같은 표준 결과를 만드는지 검증한다. 알 수 없는 버전과 계약 위반 이벤트는 `event_quarantine`으로 분리한다.
6. **폐기 기준:** Kafka retention만 보고 `v1` parser를 제거하지 않는다. Bronze 재처리 기간, 보존 정책, 변환 완료 여부를 기준으로 지원 종료 조건을 문서화한다.

버전은 애플리케이션 배포마다 올리지 않는다. 기존 소비자가 안전하게 처리할 수 없는 구조 또는 업무 의미 변경에 사용하며, Registry의 구조 호환성 검사와 Silver의 업무 의미 변환을 별도 책임으로 유지한다.

### bookings_current 구현 범위

`bookings_current`는 예약 상태와 결제 상태를 구분하고 생성 이벤트의 customer_id/staff_id 등 기본 정보를 보존한다. 최신 예약 상태, 최초 생성 정보, 최신 start_at을 결합하며 생성 이벤트가 없으면 orphan으로 남긴다. 현재는 full refresh이고 지연 도착·동률의 업무 우선순위와 증분 갱신은 후속 과제다.

공식 참고: [Spark SQL 함수](https://spark.apache.org/docs/3.5.6/api/sql/index.html), [Iceberg RTAS](https://iceberg.apache.org/docs/latest/spark-ddl/#replace-table--as-select).
