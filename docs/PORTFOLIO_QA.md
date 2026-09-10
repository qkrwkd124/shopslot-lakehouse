# ShopSlot 포트폴리오 인터뷰 Q&A

이 문서는 답을 외우기 위한 대본이 아니라, 구현 근거를 바탕으로 설계 의도를 짧게 설명하는 연습장이다. 구현이 진행될 때마다 답변과 근거 파일을 함께 갱신한다.

상태 표기:

- **현재**: 저장소에서 구현하거나 검증한 내용
- **계획**: 설계는 했지만 아직 구현·측정하지 않은 내용
- **한계**: 현재 구조에서 의도적으로 남겨둔 문제 또는 개선점

## 1. 이 프로젝트를 한 문장으로 설명하면?

**30초 답변**

다점포 예약 서비스의 운영 DB 변경을 transactional outbox와 Debezium으로 Kafka에 전달하고, Spark와 Iceberg로 재처리 가능한 Lakehouse를 만드는 프로젝트다. 단순 적재가 아니라 중복, 지연 이벤트, 파티션 skew, 장애 복구와 원본-마트 정합성을 실험으로 검증하는 것이 핵심이다.

**현재**

- MySQL 운영 모델, Alembic migration, booking과 outbox의 동일 트랜잭션 기록
- Debezium Outbox Event Router와 Redpanda 전달
- 예약 10건, 결제 7건, 거래 9건과 lifecycle 이벤트 29건에 대한 P0 smoke 검증
- Kafka → Iceberg Bronze append와 영속 checkpoint 기반 정상 재실행

**계획**

- 지속 이벤트 생성과 처리 중 장애·재시작 검증
- dbt Silver/Gold와 DQ, watermark·skew·복구 실험

## 2. 일반 테이블 CDC와 Outbox 중 무엇을 선택한 것인가?

**30초 답변**

둘 중 하나를 선택한 것이 아니라 역할이 다르다. Outbox는 애플리케이션이 비즈니스 이벤트를 DB 트랜잭션 안에 명시적으로 남기는 패턴이고, CDC는 그 outbox 행을 binlog에서 읽어 Kafka로 운반하는 수단이다. 이 프로젝트의 흐름은 `운영 상태 + outbox INSERT → MySQL binlog → Debezium → Kafka`다.

**왜 운영 테이블을 바로 CDC하지 않았나?**

- 운영 테이블 변경은 행의 변화이지 항상 비즈니스 이벤트의 의미와 일치하지 않는다.
- 여러 테이블 변경을 소비자가 다시 해석하면 원본 스키마와 강하게 결합된다.
- Outbox에는 `event_type`, `event_id`, `schema_version`, payload를 명시해 계약을 분리할 수 있다.
- 예약 변경과 이벤트 생성이 같은 DB 트랜잭션이므로 둘 중 하나만 성공하는 dual-write 문제를 피한다.

## 3. Outbox를 사용하면 이벤트 유실이 완전히 없어지는가?

**30초 답변**

아니다. Outbox가 해결하는 것은 운영 데이터 변경과 이벤트 생성 사이의 원자성이다. Debezium부터 Kafka와 downstream까지는 재시도 때문에 중복이 가능한 at-least-once 흐름이며, 장시간 장애로 MySQL binlog가 먼저 만료되거나 offset을 잃으면 누락 위험도 있다. 그래서 `event_id` 중복 제거, binlog 보존 기간, connector offset 보존, source-outbox-Bronze reconciliation이 필요하다.

**현재 한계**

- MySQL binlog 보존은 7일이다.
- connector의 `snapshot.mode`가 `no_data`이므로 connector 최초 시작 전에 생성된 기존 outbox 행은 자동 backfill되지 않는다.
- P0 smoke는 connector 등록 후 이벤트를 생성해 이 조건을 통제한다.
- 장기 장애와 offset 유실 복구 절차는 아직 구현하지 않았다.

## 4. 현재 코드에서 booking과 outbox의 원자성은 어떻게 보장하는가?

**30초 답변**

P0 generator는 autocommit을 끈 하나의 PyMySQL connection에서 하나의 업무 변경과 대응 outbox INSERT를 실행한 뒤 한 번만 commit한다. 예약 생성·상태 변경뿐 아니라 payment summary와 append-only transaction 생성, 환불 반영도 각각 대응 이벤트와 함께 commit한다. 중간 예외가 발생하면 rollback하므로 한 업무 트랜잭션의 원본 상태와 이벤트가 함께 성공하거나 함께 실패한다. ORM 경로의 `record_outbox_event()`도 commit하지 않고 호출자가 가진 SQLAlchemy Session에 이벤트만 추가하도록 설계했다.

**현재 한계**

- generator는 아직 ORM과 `record_outbox_event()`를 사용하지 않고 SQL을 직접 작성한다.
- 따라서 ORM 모델과 generator SQL 사이에 계약 중복이 있다.
- 다음 도메인 시나리오 구현 시 공통 application service 또는 repository 경계로 합칠지 검토한다.

## 5. Exactly-once가 아니라면 중복은 어디서 처리할 것인가?

**30초 답변**

각 논리 이벤트에 전역적으로 고유한 `event_id`를 부여한다. Bronze에는 Kafka 원문과 offset을 append-only로 보존하고, Silver에서 `event_id`를 기준으로 중복 제거한다. 이렇게 원본 보존과 소비용 정제를 분리하면 중복 제거 규칙이 바뀌어도 Bronze에서 다시 계산할 수 있다.

**계획한 검증**

- 동일 `event_id` 재전송
- 같은 의미지만 다른 `event_id`를 가진 이벤트 비교
- Spark 재시작과 Kafka 재읽기 후 Silver 결과 불변성 확인
- MySQL outbox, Kafka, Bronze, Silver의 단계별 건수 및 ID reconciliation

## 6. Kafka key를 왜 `shop_id`로 정했는가?

**30초 답변**

같은 매장의 이벤트 순서를 한 partition에서 관찰하고 매장 단위 집계를 쉽게 만들기 위해 `shop_id`를 사용했다. 대신 상위 매장에 트래픽이 집중되면 hot partition이 생긴다. 이 프로젝트에서는 그것을 숨기지 않고 `booking_id` key 또는 salting과 처리량·partition 분포를 비교하는 실험 대상으로 삼는다.

**트레이드오프**

- `shop_id`: 매장 단위 순서에 유리하지만 skew 가능
- `booking_id`: 예약 단위 순서와 분산도에 유리하지만 매장 단위 전체 순서는 없음
- salting: 분산도는 좋아지지만 downstream 재집계와 순서 처리가 복잡해짐

## 7. Bronze를 왜 append-only로 설계했는가?

**30초 답변**

Bronze의 목적은 최신 상태를 보여주는 것이 아니라 재현 가능한 원본 증거를 보존하는 것이다. payload뿐 아니라 Kafka topic, partition, offset, ingestion time을 함께 저장하면 중복 제거·스키마 처리·업무 규칙이 바뀌어도 원본에서 Silver를 다시 만들 수 있다. 최신 예약 상태와 품질 규칙은 Silver의 책임으로 둔다.

## 8. Watermark 30분과 2시간은 무엇을 비교하려는가?

**30초 답변**

watermark가 짧으면 Spark가 보관하는 상태와 결과 지연은 줄지만 늦게 도착한 정상 이벤트를 버릴 가능성이 커진다. 길면 더 많은 지연 이벤트를 반영할 수 있지만 상태 저장 비용과 결과 확정 시간이 증가한다. 동일한 지연 분포를 입력해 반영률, state 크기, 처리 지연을 측정한 뒤 수치로 선택할 계획이다.

**계획**

- 원문 보존용 Bronze Spark job은 구현했지만 watermark는 적용하지 않으며 비교 측정은 아직 하지 않았다.
- 따라서 현재 면접 답변에서는 특정 watermark가 더 좋다고 단정하지 않는다.

## 9. Alembic을 사용한 이유는 무엇인가?

**30초 답변**

`create_all()`은 현재 모델 상태를 새 DB에 만드는 데는 편하지만 운영 중인 DB가 어떤 순서로 변경됐는지 관리하기 어렵다. Alembic revision을 사용하면 스키마 변경을 코드 리뷰하고, 환경마다 같은 순서로 적용하며, 가능한 변경은 downgrade 경로도 명시할 수 있다. 이 프로젝트에서는 ORM이 모델의 의미를, Alembic이 적용 가능한 변경 이력을 담당한다.

**현재 근거**

- 아직 배포 전인 변경 이력은 `20260908_0001` 하나로 squash했다. 새 DB에서 최종 P0 스키마를 바로 만들며 엔터티·결제 거래의 BIGINT PK/FK와 직원 관계를 포함한다.
- 데이터가 있는 DB에서 `0002` upgrade와 `0001` downgrade를 왕복하고 데이터 건수와 계약을 다시 검증했다.
- 빈 임시 DB에도 head까지 적용해 신규 설치 경로를 별도로 확인했다.

## 10. FastAPI와 DB를 왜 아직 동기로 구성했는가?

**30초 답변**

현재 FastAPI는 선택적인 시나리오 실행 경계이고 실제 고동시성 API 부하가 없다. P0의 목적은 outbox 트랜잭션과 CDC 계약 검증이므로 동기 SQLAlchemy와 PyMySQL로 복잡도를 낮췄다. 향후 API가 많은 동시 요청과 외부 I/O를 처리해 측정상 병목이 확인되면 async engine과 async MySQL driver를 도입한다.

**주의할 점**

- FastAPI를 쓴다는 사실만으로 DB 드라이버를 비동기로 바꿀 필요는 없다.
- 비동기로 바꿀 때는 endpoint만 `async`로 만드는 것이 아니라 Session 수명, transaction, connection pool과 테스트까지 함께 바뀐다.

## 11. API가 기본 Compose 실행에서 빠지는 이유는 무엇인가?

**30초 답변**

CDC 파이프라인 검증에 HTTP API는 필수가 아니어서 `api` profile로 분리했다. 기본 `docker compose up -d`는 핵심 데이터 스택만 실행하고, `make api` 또는 `docker compose up -d api`로 필요할 때만 API를 추가한다. 이를 통해 필수 데이터 경로와 데모용 인터페이스의 경계를 드러낸다.

## 12. 장애 후 데이터 정합성을 어떻게 확인할 것인가?

**30초 답변**

서비스가 다시 떴다는 사실만으로 복구됐다고 판단하지 않는다. 기간과 이벤트 종류별로 MySQL outbox의 `event_id` 집합, Kafka offset 범위, Bronze 수신 집합, Silver 중복 제거 결과를 비교한다. Gold는 예약·결제 업무 지표를 운영 DB 집계와 대조하고 차이는 DQ 테이블에 격리한다.

**계획한 장애 시나리오**

- Debezium 중단 후 재시작
- Spark checkpoint 기반 재시작
- 동일 Kafka 구간 재처리
- binlog 보존 기간을 넘긴 복구 불가 상황과 별도 backfill 절차

## 13. 현재 설계에서 가장 먼저 개선할 부분은 무엇인가?

**30초 답변**

첫째는 generator의 직접 SQL과 ORM 모델 사이의 계약 중복이다. 둘째는 `snapshot.mode=no_data` 상태에서 connector보다 먼저 생긴 outbox 이벤트를 복구하는 bootstrap 절차가 없다는 점이다. 셋째는 outbox 보존·정리 정책과 운영 규모의 end-to-end reconciliation이다. 현재는 고정 P0 범위의 MySQL-Kafka-Bronze 비교만 구현했으며, 지속 유입·retention과 Silver/Gold까지의 정합성 검증은 남아 있다.

## 14. AI를 사용했다면 본인이 한 일은 무엇인가?

**30초 답변**

AI를 코드 작성과 반복 작업의 가속기로 사용했지만, 문제 정의와 범위 조정, 기술 선택의 질문, 변경 검토와 실행 검증은 직접 소유했다. 예를 들어 raw CDC와 outbox의 관계, Alembic 도입, API profile, 동기·비동기 선택, 기존 MySQL 볼륨의 인증 문제를 검토하면서 설계를 수정했다. 면접에서는 생성된 코드의 양보다 각 선택의 이유, 실패 조건과 검증 결과를 코드 근거로 설명하겠다.

**피해야 할 답변**

- “AI가 다 만들어서 잘 모른다.”
- “최신 기술이라 사용했다.”
- 아직 측정하지 않은 결과를 이미 검증한 것처럼 말하기

## 15. `payments`와 `payment_transactions`를 왜 분리했는가?

**30초 답변**

`payments`는 예약별 청구액과 현재 결제 상태를 빠르게 조회하기 위한 summary이고, `payment_transactions`는 실제 수납·환불의 append-only 원장이다. 부분 환불이 여러 번 일어나도 거래 이력을 보존하면서 `payments.refunded_amount_krw`와 상태를 갱신할 수 있다. P0에서는 결제 7건 중 20,000원 부분 환불과 55,000원 전액 환불을 각각 한 건 생성하고, summary 금액이 transaction 합계와 일치하는지 smoke test에서 검증한다.

**트레이드오프**

- 조회는 summary가 빠르지만 transaction과 불일치할 가능성이 생긴다.
- 그래서 결제·거래·outbox를 같은 트랜잭션으로 변경하고 reconciliation query를 둔다.
- transaction 행은 수정하지 않고 정정이 필요하면 보정 거래를 추가하는 방향으로 확장한다.

## 16. PK를 왜 모두 `id`로 통일하지 않았는가?

**30초 답변**

업무 엔터티와 결제 거래는 ORM과 repository에서 같은 규칙으로 다루기 위해 공통 `BaseModel`의 `BIGINT UNSIGNED AUTO_INCREMENT` PK `id`를 사용한다. 반면 FK는 조인 문맥이 드러나도록 `shop_id`, `booking_id`를 사용한다. outbox의 UUID `event_id`는 downstream 중복 제거를 위한 논리 이벤트 식별자로 유지한다. 이벤트의 `payment_transaction_id`는 DB의 숫자형 `payment_transactions.id` 값을 전달하는 계약 필드다. 순수 연관 테이블은 행 자체의 별도 정체성이 없으면 FK 조합을 복합 PK로 사용한다.

**트레이드오프**

- 엔터티 접근 규칙은 단순해지지만 여러 테이블을 조인한 SQL에서는 `id`가 모호하므로 테이블 alias와 컬럼 수식이 필요하다.
- DB의 `bookings.id`와 이벤트의 `booking_id`는 역할이 다르다. 이벤트 필드는 외부 계약이므로 내부 PK 이름 변경만으로 같이 바꾸지 않는다.
- 로그 테이블에도 PK는 반드시 있으며, 이름만 범용 `id`가 아닌 논리 이벤트·거래 식별자를 사용한다.

## Iceberg 연결에서 MinIO와 PostgreSQL은 왜 둘 다 필요한가?

MinIO는 Parquet 데이터와 Iceberg metadata 파일을 저장하고, PostgreSQL JDBC catalog는 테이블 이름과 현재 metadata 파일 위치를 관리한다. Spark 작업이 바뀌어도 같은 catalog에 연결하면 테이블을 다시 찾을 수 있다. 데이터 파일만 보존하고 catalog를 잃으면 그대로 조회할 수 없으므로 둘의 보존을 함께 관리한다. 현재는 로컬 `local[2]`에서 Bronze 29건 적재와 정상 재실행까지 확인했으며 다중 노드 성능·고가용성은 검증하지 않았다.

## Catalog DB에 왜 기존 MySQL 대신 PostgreSQL을 추가했는가?

**30초 답변**

Iceberg 테이블 이름과 현재 metadata 위치를 Spark 작업 간에 유지하려고 JDBC Catalog를 사용했다. 운영 MySQL과 catalog를 별도 프로세스·볼륨으로 분리하기 위해 PostgreSQL을 추가했다. 실제 분석 파일은 MinIO에 저장한다. PostgreSQL이 필수이거나 성능상 우월해서 선택한 것은 아니며, 로컬 구성을 단순화하려면 기존 MySQL에 별도 catalog DB를 두어도 충분하다.

**트레이드오프와 근거**

- 장점: 운영 데이터와 catalog의 접속 정보·프로세스·볼륨을 분리하고 역할을 관찰하기 쉽다. 별도 MySQL 인스턴스로도 같은 목적을 달성할 수 있다.
- 비용: DB 컨테이너, JDBC 드라이버, 자격 증명과 백업 대상이 늘어난다.
- 검증 범위: 샘플 2행 저장·조회와 새 Spark SQL 프로세스에서 재조회. MySQL 대비 성능, 동시 쓰기 충돌, 장애 복구는 검증하지 않았다.
- 주의: 같은 Docker 호스트에 있어 물리적 장애 격리는 아니며, `make reset`은 catalog와 MinIO 볼륨도 삭제한다.
- 재검토 조건: 자원 절약이 우선이면 MySQL 재사용, 여러 엔진·사용자와 중앙 인증이 필요하면 REST/관리형 catalog 비교.

**후속 질문과 답변**

**Q. 기존 MySQL에 별도 DB를 만들면 안 되는가?**

가능하다. 로컬 구성을 최소화하려면 기존 MySQL에 `iceberg_catalog` DB를 두어도 충분하다. 이번에는 원본 DB와 catalog의 프로세스·볼륨을 분리하는 데 우선순위를 뒀다. 별도 MySQL 인스턴스로도 같은 목적을 달성할 수 있다.

**Q. 왜 REST Catalog를 사용하지 않았는가?**

현재는 Spark 연결 실습이 우선이어서 JDBC Catalog로 시작했다. REST Catalog는 공통 API를 제공하고 구현에 따라 인증·권한 관리를 중앙화할 수 있지만, 서비스와 backing store 등 구성 요소가 늘어난다. 여러 엔진과 사용자의 접근 관리가 필요해지면 재검토한다.

**Q. Catalog는 무엇을 저장하며 왜 트랜잭션이 필요한가?**

테이블 이름과 현재 metadata 파일 위치 등을 저장한다. Spark가 새 데이터를 쓰면 Iceberg가 metadata 파일을 만들고 catalog의 참조를 갱신한다. JDBC Catalog는 트랜잭션을 지원하는 DB를 이용해 이 갱신을 안전하게 처리한다. Parquet 본문과 metadata 파일 전체는 MinIO에 저장한다.

**Q. 별도 PostgreSQL을 두면 장애 격리와 복구도 해결되는가?**

현재는 같은 Docker 호스트에서 실행하므로 물리적 장애 격리나 고가용성을 보장하지 않는다. Catalog와 MinIO의 데이터가 일관되게 복구되도록 둘의 백업을 함께 관리해야 한다. 해당 복구 절차와 동시 writer 충돌은 아직 검증하지 않았다.

**Q. 어떤 상황에서 선택을 바꿀 것인가?**

로컬 자원과 운영 단순화가 우선이면 기존 MySQL 재사용을 검토한다. 여러 엔진의 호환성이나 중앙 인증이 필요하면 REST 또는 관리형 catalog를 비교한다. 전환할 때는 기존 테이블 등록과 metadata 참조 이관도 검토한다.

**구현 근거**

- `docker-compose.yml`: 별도 `iceberg-catalog-db`와 영속 볼륨
- `spark/conf/spark-defaults.conf`: JDBC Catalog와 MinIO S3FileIO 연결
- `spark/jobs/iceberg_demo.py`: 샘플 쓰기·조회와 snapshot 확인
- 공식 문서: [JDBC Catalog](https://iceberg.apache.org/docs/latest/jdbc/), [Spark Catalog 설정](https://iceberg.apache.org/docs/latest/spark-configuration/)

## Bronze checkpoint가 있으면 모든 중복이 사라지는가?

**30초 답변**

아니다. checkpoint는 Spark query의 Kafka offset과 batch 진행 상태를 보존하고 Iceberg streaming sink와 함께 재개에 사용된다. 같은 checkpoint로 재실행하면 처음 설정한 `startingOffsets=earliest`가 아니라 저장된 진행 위치부터 읽는다. 하지만 같은 `event_id`가 다른 Kafka offset으로 재발행되면 별개의 레코드이므로 Bronze에는 둘 다 남긴다. 업무 중복 제거는 Silver에서 처리한다.

**근거와 한계**

- 첫 실행은 29건을 적재했고 정상 종료 후 재실행은 offset 29에서 신규 입력 0건이었다. query ID는 유지되고 run ID는 변경됐다.
- Kafka consumer group commit이 아니라 Spark checkpoint로 진행 상태를 관리한다.
- `spark-checkpoints`는 동일 Docker 호스트의 영속 볼륨이다. 호스트 장애나 다중 노드에서는 공유·내구성 있는 checkpoint 저장소와 일관된 복구 절차가 필요하다.
- 파일 잠금은 같은 볼륨의 로컬 writer 중복 실행만 방지한다. 별도 checkpoint를 쓰는 다른 writer까지 막는 분산 잠금이 아니다.
- checkpoint 삭제, 토픽 재생성, 테이블 교체 후에는 자동으로 안전한 재개가 보장되지 않는다. 현재 검증을 전체 파이프라인의 exactly-once 증명으로 주장하지 않는다.
- 처리 중 강제 종료·commit 경계 장애와 5분 live 유입 실험은 후속 검증이다.

## Bronze에서 왜 JSON 파싱과 watermark를 하지 않는가?

원문을 보존해야 이후 규칙을 바꿔 재처리할 수 있기 때문이다. 잘못된 JSON, null value, 지연 이벤트와 논리 중복도 보존한다. 조회용 `payload` 외에 원본 `kafka_value` 바이트와 Kafka headers·위치도 저장한다. event-time 대신 적재 시각으로 일 단위 partition을 나누고, 소규모 로컬 실습에서는 작은 파일 증가를 줄이기 위해 1분 trigger를 선택했다. 최적 지연·처리량을 측정한 값은 아니며 compaction과 snapshot 정리는 후속 운영 과제다.

공식 근거: [Iceberg Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/), [Spark Kafka integration](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html).

## Silver 첫 모델은 왜 스트리밍 대신 전체 재계산인가?

현재 29건 규모에서는 SQL의 파싱·검증·중복 제거와 재실행 결과를 먼저 이해하는 것이 목적이다. Bronze snapshot을 고정해서 읽고 booking_events_clean만 원자적으로 교체한다. 같은 입력·규칙이면 행 결과가 같지만 매번 파일·snapshot이 생길 수 있어 무비용 재실행은 아니다. 대량 데이터에서는 비용이 커지므로 dbt 모델과 증분 처리로 확장할 계획이다. 이 학습 배치를 dbt P2 완료나 운영용 증분 파이프라인으로 주장하지 않는다.

잘못된 행은 사유별 건수만 출력하고 Bronze에 보존한다. 영구 event_dq는 다음 과제다. 동일 event_id의 서로 다른 유효 payload는 보수적으로 실패시키며, 의미상 같은 JSON의 표현 차이도 현재 충돌로 취급한다.

## 연습 방법

각 질문마다 다음 순서로 연습한다.

1. 문서를 보지 않고 30초 안에 결론부터 답한다.
2. 파이프라인을 종이에 직접 그린다.
3. 면접관이 “그러면 실패하면?”, “왜 다른 선택은 안 했나?”라고 물었다고 가정해 한 단계 더 답한다.
4. 답변의 근거가 되는 코드·설정·테스트를 저장소에서 바로 찾는다.
5. 실제 구현 또는 측정 결과가 달라지면 이 문서를 즉시 고친다.

첫 연습에서는 2번, 3번, 4번 질문을 우선한다. 이 세 답변이 transactional outbox 설계의 핵심이다.
