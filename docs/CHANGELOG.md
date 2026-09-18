# Change log

## 2026-09-18 — 첫 dbt Silver 비교 모델

- 기존 `lakehouse.silver.events_clean`과 PySpark `booking_events_clean`을 dbt source로 등록하고, 예약 lifecycle 5종을 타입화·검증하는 `lakehouse.silver_dbt.booking_events_clean` full-refresh 모델을 추가했다.
- 예약 이벤트의 선택·payload 파싱·타입 변환·`validation_error` 계산을 `int_booking_events_validated` ephemeral 중간 모델로 분리했다. 물리 테이블은 만들지 않고 최종 clean SQL의 `__dbt__cte__int_booking_events_validated` CTE로 컴파일되며, clean은 정상 행 선택과 저장만 담당한다.
- event_id not-null/unique, event_type 허용값, booking_id/shop_id not-null, 이벤트별 업무 계약과 기존 PySpark 결과의 양방향 `EXCEPT ALL` 비교를 포함한 8개 테스트를 추가했다.
- dbt-spark가 별도 database/catalog 모델 설정을 허용하지 않는 제약에 맞춰 Thrift의 기본 catalog를 `lakehouse`로 변경했다. 단발 `iceberg-init`이 `lakehouse.default` namespace를 먼저 보장하므로 초기 JDBC 연결과 dbt의 2-part relation을 함께 지원한다.
- 검증: 중간 모델 분리 후 dbt가 모델 2개(ephemeral 1개, table 1개)를 인식했고, 실제 table 모델 1개와 테스트 8개가 모두 통과했다. 기존 PySpark와 dbt 결과는 각각 20행이며 컴파일 SQL에서 ephemeral CTE 삽입을 확인했다. `silver_dbt`의 물리 테이블은 `booking_events_clean` 하나뿐이다. dbt 테이블의 commit은 Iceberg overwrite snapshot이다. 기존 `lakehouse.silver`는 수정하지 않았으며 dbt 증분 materialization과 DQ 쓰기는 아직 적용하지 않았다.

## 2026-09-17 — dbt-spark 실행 환경과 Thrift 연결 구성

- FastAPI 의존성과 분리된 `dbt_shopslot` 프로젝트를 만들고 `dbt-spark[PyHive] 1.11.0`과 전이 의존성을 `uv.lock`으로 고정했다.
- dbt를 단발 Compose 도구로 추가해 기존 Spark Thrift Server로 SQL을 전달하도록 구성했다. 기존 `lakehouse.silver`와 비교할 수 있도록 개발 target은 `lakehouse.silver_dbt`를 사용한다.
- 빌드, 버전 확인, 연결 진단 명령을 Make target으로 제공했다. 아직 source·SQL 모델·테스트는 추가하지 않았고 Iceberg 테이블도 생성하지 않았다.
- 검증: Compose 설정과 잠금 파일을 확인하고 dbt 이미지를 빌드했다. dbt Core 1.12.5, spark adapter 1.11.0을 확인했으며 `dbt debug`의 설정·필수 의존성·`spark-thrift:10000` 연결 검사가 모두 통과했다.

## 2026-09-16 — 공통 event_dq 저장 추가

- 세 clean Job의 `validation_error` 행을 하나의 `lakehouse.silver.event_dq`에 저장하는 공통 writer를 추가했다.
- 정상 이벤트는 event_id, event_id가 없는 잘못된 envelope는 Kafka topic/partition/offset으로 `dq_key`를 만들고, Iceberg `MERGE`로 재실행 중복을 방지한다.
- 최초·마지막 발견 시각, 검증 단계·사유, 원문과 Kafka 위치, 입력 snapshot을 보존한다. current orphan과 집계형 품질 지표는 아직 포함하지 않는다.
- DQ 입력 준비와 테이블 초기 생성은 공통 Python writer에 두고, upsert 규칙은 `merge_event_dq.sql`로 분리했다.
- 정상 P0와 분리해 `make add-dq-fixture`로 음수 가격 예약과 요청액보다 큰 결제 거래 이벤트를 각각 한 건 추가하는 재현 시나리오를 제공한다.
- 검증: 오류가 없는 P0 입력에서 `event_dq` 0건을 확인한 뒤 fixture를 Outbox→Kafka→Bronze로 전달했다. 결제 후보 21건 중 1건이 `invalid_payment_transaction_fields`로 제외되고 정상 20건이 유지됐다. 결제 clean을 재실행해 DQ 행은 1건으로 유지되고 `first_detected_at`은 보존되며 `last_detected_at`만 갱신되는 것도 확인했다.

## 2026-09-16 — payments_current 상태 재구성 추가

- `payment_events_clean`의 최신 이벤트 사후 요약을 선택해 결제 한 건당 한 행인 `lakehouse.silver.payments_current`를 생성하는 full-refresh 배치를 추가했다.
- 최초 요청 시각, 최신 거래와 상태의 근거 이벤트를 보존하고 요청 이벤트가 없는 결제는 `is_orphan=true`로 남긴다.
- 입력 snapshot을 고정하고 payment_id 유일성과 입력 결제의 누락 여부를 저장 전에 검사한다.
- 검증: 결제 이벤트 20건을 결제 7건으로 재구성했고 orphan 0건, 상태 `paid 3 / partially_refunded 1 / refunded 1 / unpaid 2`를 확인했다.

## 2026-09-16 — 결제 생명주기 Silver clean 추가

- `payment_requested`, `payment_completed`, `payment_refunded`를 결제별 전체 생명주기로 보존하는 `payment_events_clean` full-refresh 배치를 추가했다.
- 요청 이벤트에서는 거래 컬럼을 NULL로 두고, 수납·환불 이벤트에는 거래 ID·종류·금액과 이벤트 직후의 누적 수납·환불·순수납·미수 상태를 함께 저장한다.
- 결제 요약 금액과 상태의 내부 일관성, payment별 요청 금액 일치와 거래 ID 유일성을 검사한다. 거래만 필요하면 `payment_transaction_id IS NOT NULL`로 선택할 수 있어 별도 거래 clean을 새 흐름에서는 사용하지 않는다.
- Python 문법, Compose 설정, Make 실행 명령과 diff 형식을 확인했다. `events_clean` 40건에서 결제 이벤트 20건을 선택해 제외 0건으로 저장했고, 요청 7건·수납 9건·환불 4건이 새 계약과 일치했다.

## 2026-09-16 — 미수와 환불 의도를 분리한 결제 요약 모델

- `payments`를 누적 수납액, 누적 환불액, 순수납액, 실제 미수액과 `needs_repayment`를 가진 거래 원장의 현재 요약으로 변경했다. `payout = paid + refund`와 상태별 미수 규칙을 ORM 및 초기 Alembic revision에 반영했다.
- 결제 요청을 먼저 만들고 `payment_requested`를 발행한 뒤, 각 수납·환불 거래가 결제 요약 갱신과 거래 이벤트를 같은 트랜잭션에서 기록하도록 generator를 재구성했다. 일반 환불은 미수를 만들지 않고 재수납 필요 환불만 차액을 미수로 유지한다.
- P0는 결제 7건, 거래 13건, 전체 이벤트 40건이다. 최초 부분수납, 일반 부분·전액 환불, 재수납이 필요한 환불과 완전·부분 재수납을 포함한다.
- `verify_p0.sh`에서 PK 타입·상태별 고정 결과 등 중복 검사를 제거했다. MySQL 거래 합계와 결제 요약의 일치, Connector 상태, Outbox→Kafka 전달만 확인하는 smoke test로 축소했다.
- Python/Bash 문법과 diff 형식을 확인했다. 로컬 Python에는 PyMySQL이 없어 generator 함수 import 실행은 생략했고, 기존 볼륨을 지우는 새 계약의 end-to-end 검증은 수행하지 않았다. Silver 결제 parser 동기화가 후속 작업이다.

## 2026-09-16 — 이벤트 스키마 진화 후속 실험 계획

- Schema Registry를 이용한 이벤트 계약 등록과 호환성 정책 검증을 Silver 후속 계획에 추가했다.
- 생산자보다 `v1`/`v2` 동시 해석 소비자를 먼저 배포하고, 버전별 parser를 하나의 표준 clean 스키마로 정규화하는 전환 순서를 명시했다.
- 신규 `v1` 발행 중단 뒤에도 Bronze 과거 데이터 재처리를 위해 parser를 유지하며, 미지원 버전 격리와 폐기 기준까지 검증 범위로 남겼다. 아직 구현·검증한 기능은 아니다.

## 2026-09-15 — 결제 거래 Silver clean 추가

- 공통 `events_clean`에서 `payment_completed`와 `payment_refunded`만 선택해 거래별 한 행으로 만드는 `payment_transactions_clean` full-refresh 배치를 추가했다.
- 결제 ID·거래 ID·금액을 BIGINT로 변환하고 양수 여부를 검사한다. 수납의 `paid`, 부분 환불의 `partially_refunded + partial`, 전액 환불의 `refunded + full` 조합을 계약으로 검증한다.
- 서로 다른 유효 이벤트가 같은 `payment_transaction_id`를 사용하면 거래 한 행의 기준을 위반하므로 실패하며 기존 테이블을 보존한다. `make silver-payment-transactions` 실행 명령을 추가했다.
- 검증: events_clean 30건에서 결제 이벤트 9건을 선택해 제외 0건으로 저장했다. 수납 7건, 부분 환불 1건, 전액 환불 1건을 확인했다.

## 2026-09-15 — bookings_current를 예약 clean 경계에 맞게 정리

- `booking_events_clean`이 예약 lifecycle 이벤트만 보장하므로 current SQL의 이벤트 종류 재필터링을 제거하고, 생성 속성·최신 일정·최신 상태를 결합하는 책임만 남겼다.
- 저장 후 전체 결과를 양방향 `exceptAll`로 재조회하던 검증을 제거했다. 대신 저장 전에 `booking_id` 유일성과 입력의 고유 예약 수 대비 출력 예약 수를 검사해 current grain의 중복·누락을 짧게 확인한다.
- orphan은 데이터 유실을 피하기 위해 행과 실행 요약에 계속 남긴다. 영구 품질 이력과 알림은 후속 `event_dq` 책임으로 분리한다.

## 2026-09-15 — booking_events_clean 예약 도메인 분리

- `booking_events_clean` 입력을 Bronze에서 공통 `events_clean`로 전환하고 예약 lifecycle 5종만 남겼다. 결제·환불 컬럼과 공통 event_id 중복 제거 책임을 제거했다.
- 예약 payload의 customer/service/staff/start 시각/가격/status만 타입화하고 이벤트 종류별 필수 필드와 상태값을 검증한다. `NULL <> 값`이 참이 되지 않는 SQL 3값 논리를 고려해 상태 NULL을 명시적으로 거부한다.
- 공통 파싱·중복·payload 충돌 fixture와 `verify-silver-events`를 제거했다. 이 규칙은 `events_clean` 책임이며 후속 dbt 테스트에서 공통 계약과 예약 계약을 분리한다.
- 검증: events_clean 30건에서 예약 이벤트 21건을 선택해 제외 0건으로 저장했다. 이어서 bookings_current를 재생성해 예약 11건, orphan 0건, 상태 `checked_in 7 / cancelled 1 / no_show 1 / scheduled 2`를 확인했다.

## 2026-09-15 — 공통 events_clean 경계 추가

- `lakehouse.bronze.booking_events`의 고정 snapshot을 읽어 공통 envelope만 타입화하고 `event_id` 중복을 제거하는 `lakehouse.silver.events_clean` full-refresh 배치를 추가했다.
- 예약·결제 전용 필드는 펼치지 않고 payload에 보존하며, Kafka key/topic/partition/offset/timestamp/headers와 Bronze 적재 시각을 lineage로 유지한다.
- 동일 `event_id`의 payload가 충돌하면 기존 테이블을 교체하지 않고 실패한다. 저장 후 전체 행 재비교는 생략하고 저장 전 빈 결과와 충돌을 검사한다.
- `make silver-events-clean`을 추가했다. 기존 wide `booking_events_clean`과 `bookings_current`의 입력 전환은 다음 작업으로 남겨 단계적 리팩터링임을 문서에 명시했다.
- 검증: Bronze snapshot `502242981853940174`의 30건을 읽어 제외 0건, 중복 제거 0건, `events_clean` 30건을 저장했다. 저장 결과의 event_id 30개가 모두 고유하고 Iceberg main snapshot이 생성된 것을 확인했다.

## 2026-09-15 — bookings_current 상태 재구성 추가

- `booking_events_clean`의 예약 이벤트 5종을 결합해 예약 한 건당 한 행인 `lakehouse.silver.bookings_current`를 생성하는 full-refresh Silver 배치를 추가했다.
- 최신 예약 상태, 최초 `booking_created`의 기본 정보, 최신 `start_at`을 각각 선택한 뒤 결합한다. 결제 이벤트는 제외하고 생성 이벤트가 없는 예약은 삭제하지 않고 `is_orphan`으로 표시한다.
- 실행 시작 시 입력 `booking_events_clean` snapshot을 고정하며, 빈 결과와 `booking_id` 중복을 검사한 뒤 Iceberg 테이블을 원자적으로 교체한다.
- `make silver-current` 실행 명령을 추가했다.

## 2026-09-14 — 예약·결제 상태 기계 분리 명시

- 설계서의 상태 기계 그림이 예약 전이도에 결제 이벤트를 이어 붙여 `checked_in → payment_completed → payment_refunded`로 그려져 있었다. 예약 상태가 수납완료로 전이되는 것처럼 읽히지만 실제 구현은 그렇지 않다. 예약과 결제의 상태 기계를 분리해 다시 그렸다.
- 코드 확인 결과 `complete_payments`와 `refund_payments`는 `bookings` 테이블을 UPDATE하지 않는다. `bookings.status`가 가질 수 있는 값은 `scheduled`, `rescheduled`, `checked_in`, `cancelled`, `no_show` 다섯 개뿐이다. 데이터는 정상이며 그림만 틀렸다.
- `no_show_marked`가 `rescheduled` 아래에만 달려 있었으나 실제로는 `scheduled`에서 바로 전이된다(P0 예약 8번). 상태값과 event_type 이름이 섞여 있던 것도 상태값으로 통일했다.
- `SEED=42` 고정 난수 서술을 실제 구현(난수 없이 `uuid5` 기반 결정론적 생성)으로 정정하고, Zipf 매장 분포는 P3 계획임을 구분했다.
- `status = 'rescheduled'`를 알려진 문제로 기록했다. 일정 변경은 사건이지 현재 조건이 아니므로 `scheduled`만 세는 집계에서 누락되고 Gold funnel 분모가 틀어진다. P0에서는 해당 예약이 체크인으로 끝나 드러나지 않는다. 개선 방향만 적었고 적용하지 않았다.
- 문서 변경만 수행했다. 실행 코드와 데이터는 바꾸지 않았다.

## 2026-09-14 — 이벤트 계약 문서 정정과 refund_type 편입

- 설계서의 이벤트 계약 절이 실제 payload와 달랐다. `payment_completed` 예시가 `customer_id`, `service_id`, `start_at`, `booked_price_krw`, `status`를 담고 있었으나 구현에는 없는 필드다. event_type별 필드 표와 실제 payload 두 건으로 교체했다.
- generator가 `payment_refunded`에 기록하던 `refund_type`(`full`/`partial`)이 계약 문서에 없었고 Silver의 `from_json` 구조체에도 없어 조용히 버려지고 있었다. 환불 이벤트에는 `booked_price_krw`가 없어 `amount_krw`만으로 부분·전액을 판별할 수 없으므로 계약에 편입하고 `booking_events_clean` 컬럼으로 펼친다.
- `payment_status`, `staff_id`도 계약 문서에 누락돼 있어 함께 반영했다. 예약 정보와 결제 정보가 한 이벤트에 함께 오지 않는다는 점을 명시했다. `bookings_current`가 최신 상태 이벤트만으로 만들어질 수 없는 이유가 여기서 나온다.
- `refund_type`은 값 검증 없이 문자열로 전달한다. 기존 검증 규칙과 제외 사유는 바꾸지 않았다.
- 검증: 아직 실행하지 않았다. Silver 컬럼이 하나 늘었으므로 `make silver-events` 재실행이 필요하며, 기존 29건과 event_type별 건수가 유지되는지 확인해야 한다.

## 2026-09-14 — 공개 기술 문서 정리

- 공개 문서를 실행 가이드, 변환 명세, 검증 결과, 기술적 한계 및 변경 기록으로 정리했다.
- Spark·Silver 가이드의 경로를 정리하고 README의 문서 링크를 갱신했다. 증분 전환·정합성·성능 비교 계획은 미구현 항목으로 유지한다.
- 실행 코드와 데이터는 변경하지 않았다.

## 2026-09-14 — 단발 예약·outbox 추가 스크립트

- `generator/add_booking.py`와 `make add-booking`을 추가했다. 기존의 유효한 참조 데이터로 신규 예약 1건과 UUID 이벤트 1건을 같은 MySQL 트랜잭션으로 기록한다.
- 고정 P0 생성과 분리했으며 기존 데이터·이벤트 계약은 유지한다. 반복 실행은 매번 신규 예약이며 요청의 멱등성과 예약 슬롯 충돌 판단은 범위 밖이다.
- README에 수동 실행과 snapshot 전후 비교, 고정 P0 검증 기대값에 미치는 영향을 기록했다. 문법·Make dry-run·diff만 확인하며 데이터 생성이나 서비스 기동은 실행하지 않는다.

## 2026-09-14 — Silver 증분 전환과 비교 실험 계획

- 전체 설계서와 `SILVER_GUIDE.md`에 full refresh 기준 결과 확보 → 증분 반영 → 정합성·규모별 성능 비교 순서를 명시했다.
- 전체 snapshot 읽기와 추가분 읽기, 결과 중복과 중복 계산을 구분하고 진행 위치·재실행·지연 이벤트 검증 항목을 정리했다.
- 향후 계획이며 구현·측정 완료가 아니다. 이번 변경은 문서에 한정하고 기존 실행 코드와 작업 중 파일은 수정하지 않았다.

## 2026-09-14 — Spark Thrift JDBC 접속

- 검증: Compose 설정 검사·빌드 성공. Beeline으로 JDBC 접속 후 `SELECT 1`, Bronze 29건, Silver 29건 조회 성공(종료 코드 0). DBeaver GUI 자체는 아직 연결하지 않았다. 초기 namespace 오류와 Thrift 전용 기본 catalog 수정은 TROUBLESHOOTING에 기록했다.
- Compose에 `spark-thrift`를 추가하고 공용 실행기에 foreground Thrift 모드를 추가했다. 기존 이미지와 Iceberg 설정을 재사용하며 기본 기동에 포함한다.
- 로컬 전용 JDBC `10000`, Spark UI `4042`, 독립 Derby 경로와 TCP healthcheck를 구성했다. 기존 CLI·Bronze 프로세스와 데이터 볼륨은 변경하지 않는다.
- `thrift-up/stop/logs` 명령, DBeaver Hive JDBC 접속법과 무인증 로컬 구성의 제한을 README에 기록했다.

## 2026-09-10 — 로컬 관찰용 접속 포트

- Bronze Spark UI `4041:4040`, Catalog PostgreSQL `5432:5432` 매핑을 기록하고 README 접속 정보를 맞췄다.
- 해당 포트는 loopback 한정이 아니므로 로컬 개발 환경의 접근 통제에 주의한다.

## 2026-09-10 — 첫 Silver SQL 배치 모델

- `lakehouse.silver.booking_events_clean` 하나를 만드는 배치를 추가했다. 새 컨테이너/dbt/checkpoint는 추가하지 않고 기존 Spark에서 SQL을 실행한다.
- JSON 파싱·타입 변환·기본 schema v1 검증과 event_id 중복 제거를 두 SQL 파일로 분리했다. 유효한 동일 event_id의 payload 충돌은 쓰기 전에 실패시킨다. 제외 사유를 집계하고 원문은 Bronze에 유지한다.
- Bronze snapshot 고정 후 파생 Silver만 원자적으로 전체 교체한다. 빈 입력/유효 결과 0건이면 기존 결과를 보존한다. 대량 증분 처리·영구 DQ·current-state 테이블은 후속 개발 범위다.
- `make silver-events`, `make verify-silver-events`를 추가했다. 후자는 테스트 후 테이블도 재작성한다. 기본 make 목표를 up으로 명시해 새 target 추가로 인한 의도치 않은 Silver 실행을 막았다.
- 작은 메모리 fixture에서 JSON·타입 오류, 필수값 누락, v2 미지원, 결제 필드 누락, 중복 대표행 선택, payload 충돌 거부를 검증했다. 최초 적재는 Bronze 29건 → Silver 29건, 제외 0건이며 계산 결과와 저장 결과를 양방향 비교했다.
- Silver 기술 문서와 README·설계서에 구현 상태와 제약을 반영했다.
- 동일 Bronze snapshot으로 `make silver-events`를 재실행해 Silver 29건 유지와 계산/저장 결과 일치를 다시 확인했다. 새 snapshot으로 교체되지만 행이 append되어 두 배로 늘어나지 않았다.

## 2026-09-10 — Spark 기술 문서

- `docs/SPARK_GUIDE.md`에 PySpark 역할, local[2]/다중 노드, Bronze DDL·source·writer 옵션, trigger와 실행 시작/대기, MinIO 파일 쓰기와 Iceberg commit 시점을 정리했다.
- README에 기술 문서 링크를 추가했다. 공식 문서와 현재 구현을 대조했으며 실행 코드·아키텍처·데이터 변경은 없다. 따라서 설계 변경이나 신규 장애 기록은 추가하지 않았다.

## 2026-09-09 — Kafka → Iceberg Bronze 스트리밍

- Spark Kafka connector와 전이 의존성을 고정 버전으로 이미지에 추가했다.
- `lakehouse.bronze.booking_events`에 원문 문자열·바이트, key/headers, topic/partition/offset, Kafka timestamp와 적재 시각을 append한다. 적재일 partition을 사용하고 JSON 필터링·업무 중복 제거는 하지 않는다.
- 영속 checkpoint 볼륨, 동일 checkpoint writer 잠금, checkpoint만 남고 테이블이 없는 경우의 시작 거부를 추가했다. Spark 실행기는 종료 signal을 자식 프로세스에 전달한다.
- `bronze-once`, `bronze-up`, `bronze-stop`, `bronze-logs`, `verify-bronze` Make target을 추가했다. 상시 작업은 별도 `streaming` profile 서비스에서 1분 trigger로 실행한다.
- 검증: 최초 29건 적재, 동일 checkpoint 재실행은 offset 29부터 신규 입력 0건. MySQL outbox ID 29개 일치, Kafka/Bronze 각 29건의 원문·metadata 양방향 일치, 중복 Kafka 위치 0건. 별도 Bronze 서비스의 스트리밍 시작도 확인했다.
- 검증 입력의 stdin 대기 문제를 임시 파일 전달로 수정했다. 상세 내용은 `TROUBLESHOOTING.md`에 기록했다.
- 설계서와 README에 구현 상태를 반영했다. 5분 live 생성과 처리 중 강제 종료/재개 실험은 다음 단계이며 P1 완료로 표기하지 않는다.

## 2026-09-09 — Iceberg Catalog 선택 근거 기록

- PostgreSQL JDBC Catalog 선택 이유와 별도 인스턴스 운영 비용을 설계 문서에 기록했다.
- PostgreSQL의 성능 우위를 검증한 선택이 아님을 명시하고, 현재 연결 검증과 미검증 항목을 구분했다.

## 2026-09-09 — Spark·Iceberg·MinIO 연결 실습

- Spark 3.5.6 Java 17 이미지에 Iceberg 1.11.0 runtime/AWS bundle과 PostgreSQL JDBC 42.7.7을 빌드 시 설치한다.
- 별도 PostgreSQL 17.6 JDBC catalog와 영속 볼륨을 추가하고, `S3FileIO`를 MinIO endpoint/path-style로 연결했다.
- 공용 Spark 설정과 환경변수 기반 자격 증명 실행기를 추가했다. SQL 콘솔과 데모 작업이 동일한 catalog를 사용한다.
- `make iceberg-up`, `make iceberg-demo`, `make iceberg-sql`을 추가했다. 데모 데이터는 별도 `demo` namespace에 저장한다.
- 미사용 Spark master 8080 포트를 제거하고 실행 중 application UI 4040을 문서화했다.
- Kafka Bronze 적재·checkpoint 검증은 다음 단계로 남긴다.
- 검증: `make iceberg-demo`로 2행 쓰기/조회, append snapshot 1개, MinIO Parquet 경로 2개를 확인했다. 별도 `spark-sql` 프로세스에서도 같은 두 행이 조회됐다. Compose 설정 검사와 diff 공백 검사도 통과했다.

ShopSlot의 구현·계약·운영 방식에 영향을 주는 변경을 날짜순으로 기록한다. 커밋 메시지의 대체물이 아니라, 왜 변경했는지와 검증 결과를 빠르게 파악하기 위한 문서다.

## 2026-09-08 — 직원 엔터티와 예약 FK

### Added

- 매장 소속 `staffs` 엔터티와 ORM 관계를 추가했다.
- 최종 초기 migration에 `staffs`와 `bookings.staff_id`의 `BIGINT UNSIGNED` FK를 추가했다.
- generator가 매장별 직원 2명을 seed하고 숫자형 `staff_id`를 이벤트 payload에 기록하도록 변경했다.

### Verified

- 기존 예약 10건을 보존한 채 `0004`를 적용했다.
- 직원 FK orphan과 예약-직원 매장 불일치가 모두 0임을 확인했다.

## 2026-09-08 — BIGINT 엔터티 식별자와 공통 BaseModel

### Changed

- 엔터티 5개의 PK를 `BIGINT UNSIGNED AUTO_INCREMENT`로, 이를 참조하는 FK 6개를 `BIGINT UNSIGNED`로 변경했다.
- `BaseModel` 추상 모델에 `id`, `created_at`, `updated_at`을 모으고 엔터티 모델이 이를 상속하도록 정리했다.
- P0 generator의 엔터티·payload 식별자를 정수형으로 맞췄다. UUID 기반 `event_id`만 논리 이벤트 식별자로 유지했다.
- 배포 전 migration을 최종 스키마를 생성하는 초기 revision `20260908_0001`로 squash했다.

### Verified

- 기존 P0 데이터를 삭제하지 않고 `0002 → 0003` migration을 적용했다.
- 엔터티 PK 5개가 unsigned bigint auto-increment이고 관련 FK 6개가 unsigned bigint이며, FK orphan이 없음을 확인했다.

## 2026-09-08 — Redpanda Console과 엔터티 PK 규칙

### Added

- Compose 기본 스택에 Redpanda Console `v3.11.0`을 추가하고 호스트 `8084` 포트로 공개했다.
- 초기 revision의 PK/FK 정의를 최종 명명 규칙에 맞췄다.
- P0 검증에 엔터티·로그 테이블의 PK 메타데이터 검사를 추가했다.

### Changed

- `shops`, `customers`, `services`, `bookings`, `payments`의 PK 컬럼을 `id`로 통일하고 모든 참조 FK를 새 컬럼으로 연결했다.
- FK 컬럼과 이벤트 payload는 문맥이 필요한 `shop_id`, `booking_id`, `payment_id` 이름을 유지했다.
- `payment_transactions`도 공통 BIGINT `id` PK를 사용하고, `outbox_events`만 UUID `event_id` PK를 유지한다. 향후 순수 연관 테이블은 별도 `id` 없이 FK 조합 복합 PK를 사용한다.
- generator SQL과 결제 reconciliation 검증을 새 엔터티 PK 규칙에 맞췄다.

### Verified

- 기존 데이터가 있는 DB에서 `0001 → 0002 → 0001 → 0002` upgrade/downgrade를 수행했고 예약 10건, 결제 7건, 거래 9건, 이벤트 29건이 유지됨을 확인했다.
- 별도 임시 빈 DB에 `upgrade head`와 P0 generator를 실행해 당시 PK 구조와 동일한 데이터 건수를 확인한 후 임시 DB만 삭제했다.
- 기본 DB의 `make verify-p0`가 통과했다.
- `http://localhost:8084`에서 Console UI가 응답하고 `booking.events.v1` 토픽을 조회함을 확인했다.

## 2026-09-08 — 결정론적 예약·결제·환불 lifecycle 데이터

### Added

- P0 generator에 일정 변경, 취소, 체크인, 노쇼, 결제 완료, 부분 환불과 전액 환불 시나리오를 추가했다.
- 결제 summary와 append-only 거래 합계, 이벤트별 payload 필수 필드, MySQL 최종 상태와 Kafka 이벤트 유형별 건수를 검증한다.

### Changed

- 고정 P0 데이터셋을 예약 생성 이벤트 10건에서 예약 10건, 결제 7건, 결제 거래 9건, lifecycle 이벤트 29건으로 확장했다.
- Debezium JSON converter의 `schemas.enable=false`를 worker와 connector에 명시해 Kafka value를 schema wrapper 없는 plain JSON으로 고정했다.
- Kafka 검증은 payload 전체 문자열이 아니라 정확한 `event_type` 필드를 집계한다.

### Verified

- `make smoke` 전체 실행이 성공했다.
- MySQL 예약 최종 상태가 checked-in 7건, cancelled 1건, no-show 1건, scheduled 1건임을 확인했다.
- 결제 최종 상태가 paid 5건, partially-refunded 1건, refunded 1건이고 거래 합계와 summary가 일치함을 확인했다.
- `booking.events.v1`에서 7종의 plain JSON 이벤트 총 29건을 확인했다.

## 2026-09-08 — uv 기반 재현 가능한 Python 이미지 빌드

### Added

- `uv.lock`을 생성해 Python 패키지의 정확한 버전과 배포 파일 해시를 고정했다.
- 로컬 가상환경과 불필요한 실행 데이터를 이미지에서 제외하는 `.dockerignore`를 추가했다.

### Changed

- 공용 Dockerfile의 `pip install .`을 고정된 uv 바이너리와 `uv sync --locked --no-dev` 기반 2단계 의존성 설치로 교체했다.
- 애플리케이션 소스보다 의존성 파일을 먼저 복사해 Docker 레이어 캐시를 재사용하도록 구성했다.

### Verified

- `uv lock --check`로 `pyproject.toml`과 `uv.lock`의 일치를 확인했다.
- Compose의 `migrate`와 선택적 `api` 이미지를 새 Dockerfile로 빌드했다.

## 2026-09-08 — ORM/Alembic 기반 P0 원본 스키마

### Added

- `app/db/models/`에 ShopSlot P0 7개 원본 테이블의 SQLAlchemy 모델을 추가했다.
- `alembic/versions/20260908_0001_create_p0_source_model.py`에 최초 적용 가능한 스키마 revision을 추가했다.
- `migrate` Compose 서비스와 `make migrate` 명령을 추가했다.
- 선택적인 FastAPI 경계(`app/api/`, `make api`)와 `/health` endpoint를 추가했다.
- generator가 매장·고객·서비스를 seed한 뒤 새 booking FK 구조에 맞춰 outbox 이벤트를 기록하도록 수정했다.

### Changed

- 테이블 DDL의 단일 원본을 `mysql/init/001_schema.sql`에서 ORM 모델과 Alembic revision으로 옮겼다.
- `mysql/init/001_bootstrap.sql`은 Debezium 복제 계정 생성만 담당한다.
- README와 설계서에 migration ownership과 디렉터리 구조를 반영했다.

### Verified

- 격리된 MySQL 8.4에 revision `20260908_0001`을 적용했다.
- 3 shops, 10 customers, 3 services, 10 bookings, 10 outbox events 생성을 확인했다.
- 격리된 FastAPI 컨테이너의 `GET /health`가 `200 {"status":"ok"}`을 반환함을 확인했다.
