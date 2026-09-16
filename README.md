# ShopSlot Lakehouse

가상의 다점포 예약 플랫폼을 위한 CDC 기반 Lakehouse 포트폴리오다. 실제 고객·회사 데이터는 사용하지 않는다.

P0에서는 MySQL의 예약 원본 상태와 transactional outbox를 같은 트랜잭션으로 기록하고, Debezium Outbox Event Router가 이를 Redpanda의 `booking.events.v1` 토픽으로 발행하는 것을 검증한다. 원본 스키마는 SQLAlchemy 모델과 Alembic migration으로 관리한다.

```text
generator/API -> MySQL (operational state + outbox_events, one transaction)
             -> Debezium -> Redpanda topic: booking.events.v1

Redpanda -> Spark Structured Streaming -> Iceberg Bronze
                                     -> MinIO files, PostgreSQL JDBC catalog
```

## Prerequisites

- Docker Desktop with Compose v2
- `curl` (macOS 기본 제공)
- `uv` (Python 의존성을 변경하고 `uv.lock`을 갱신할 때만 필요)

## P0 quick start

```bash
cp .env.example .env
make smoke
```

`make smoke`는 로컬 볼륨을 초기화하고 Compose stack과 Debezium connector를 기동한 뒤, 예약 10건의 lifecycle 데이터를 생성한다. `verify_p0.sh`는 MySQL 결제 요약과 거래 합계의 일치, Outbox에서 Kafka까지의 전달을 확인하는 전체 흐름 smoke test다. 스키마 구조와 Silver 도메인 검증을 반복하지 않는다.

고정 P0 데이터셋은 예약 10건, 결제 7건, 결제 거래 13건과 총 40개의 outbox/Kafka 이벤트로 구성한다. 이벤트 분포는 `booking_created` 10건, `booking_rescheduled` 1건, `booking_cancelled` 1건, `checked_in` 7건, `no_show_marked` 1건, `payment_requested` 7건, `payment_completed` 9건, `payment_refunded` 4건이다. 최초 부분수납, 일반 부분·전액 환불, 재수납이 필요한 부분·전액 환불, 완전·부분 재수납을 구분한다.

`payments`는 거래 원장을 대신하지 않는 현재 요약이다. `payout_amount_krw`와 `refund_amount_krw`는 누적 수납·환불, `paid_amount_krw = payout_amount_krw - refund_amount_krw`는 순수납액이다. 일반 환불은 `unpaid_amount_krw=0`인 부분·전액 환불로 끝나며, `needs_repayment=true`인 환불만 청구액과 순수납액의 차이를 실제 미수로 유지한다. 각 거래와 결제 요약 갱신, outbox 기록은 하나의 MySQL 트랜잭션에서 수행한다.

새 MySQL 볼륨에서는 Compose의 `migrate` 서비스가 `alembic upgrade head`를 실행해 P0 원본 테이블을 생성한다. `mysql/init/001_bootstrap.sql`은 Debezium 복제 계정만 만든다. 예전 SQL 초기화로 만든 로컬 볼륨은 `make reset` 후 다시 시작해야 한다.

엔터티와 결제 거래 테이블(`shops`, `customers`, `services`, `staffs`, `bookings`, `payments`, `payment_transactions`)은 공통 `BaseModel`의 `BIGINT UNSIGNED AUTO_INCREMENT` PK `id`와 생성·수정 시각을 사용한다. 직원은 매장에 소속되고 `bookings.staff_id`로 예약과 연결한다. FK는 같은 `BIGINT UNSIGNED` 타입으로 맞추고 `shop_id`, `booking_id`처럼 대상 이름을 유지한다. outbox는 논리 이벤트 식별자인 UUID `event_id`를 PK로 사용하며, 향후 순수 연관 테이블은 별도 `id` 없이 FK 조합을 복합 PK로 사용한다.

For iterative work:

```bash
make up
make migrate       # 이미 기동한 환경에서 migration만 실행
make connector
make generate-p0
make verify-p0
```

Python 의존성 범위는 `pyproject.toml`, 설치할 정확한 버전과 해시는 `uv.lock`에서 관리한다. 의존성을 변경한 뒤에는 `uv lock`을 실행하고 두 파일을 함께 커밋한다. Docker 이미지 빌드는 `uv sync --locked --no-dev`를 사용하므로 두 파일이 불일치하면 실패한다.

The generator uses a fixed UUID namespace and `Asia/Seoul` timestamps inside the event payload. Each booking transition, payment transaction, or refund and its corresponding `outbox_events` row commit together; a failure rolls the whole business transaction back. Kafka record timestamps remain ingestion-time so a fixed-seed historical timestamp cannot be rejected by a broker timestamp policy.

## Local endpoints

추가 실험은 다음 절의 예약 추가 명령을 사용한다.

### 예약 1건 추가와 snapshot 비교

P0가 생성된 환경에서 `make add-booking`을 실행한다. 기존 데이터를 지우지 않고 AUTO_INCREMENT로 발급한 신규 예약과 UUID를 가진 `booking_created` outbox 이벤트를 같은 트랜잭션으로 추가한다. 매 실행이 별도의 예약 생성이며 재시도 요청의 중복 방지는 없다. 통신 단절 등으로 commit 결과가 불명확하면 재실행 전에 MySQL/outbox를 확인한다.

ID 순으로 첫 번째 활성 매장·동일 매장의 활성 서비스/직원과 기존 고객을 선택하고 서비스 가격을 예약 가격으로 복사한다. 이벤트 시각은 현재 Asia/Seoul, 예약 시각은 다음 날 같은 시각이다. 예약 슬롯 충돌 판단이나 결제는 하지 않는 로컬 검증용 스크립트다. 기초 데이터가 없으면 쓰기 전에 종료한다. P0의 공통 payload/outbox/transaction 함수만 재사용하며 P0 생성 작업 자체는 호출하지 않는다.

```bash
make add-booking
```

출력된 `booking_id`, `event_id`를 기록한다. Debezium이 정상 동작하면 Kafka로 비동기 전달된다. 상시 Bronze가 실행 중이면 다음 처리를 기다린다. 중지 상태라면 Console에서 Kafka 도착을 확인한 뒤 `make bronze-once`를 실행한다. 동일 checkpoint의 writer를 동시에 실행하지 않는다.

DBeaver에서 추가 전후의 main snapshot ID를 기록하고 다음을 비교한다.

```sql
SELECT snapshot_id FROM lakehouse.bronze.booking_events.refs WHERE name = 'main';
SELECT committed_at, snapshot_id, operation
FROM lakehouse.bronze.booking_events.snapshots ORDER BY committed_at DESC;
SELECT count(*) FROM lakehouse.bronze.booking_events;
-- 출력된 실제 event_id로 바꿔 조회한다
SELECT * FROM lakehouse.bronze.booking_events
WHERE get_json_object(payload, '$.event_id') = '<event_id>';
```

신규 이벤트 1건만 한 번 정상 전달·적용됐다면 Bronze는 40→41건이 된다. 새 snapshot은 MySQL commit 시점이 아니라 Bronze commit 후 만들어진다. `make silver-events-clean`과 해당 도메인 clean을 수동 실행하면 Silver도 재계산된다. 기존 데이터를 유지하려면 `make smoke` / `make reset`을 사용하지 않는다. 고정 40건을 전제로 하는 `verify-p0`는 추가 후 기대값이 맞지 않으므로 추가 실험과 고정 P0 검증을 구분한다.

### 접속 주소

| Service | Endpoint |
| --- | --- |
| MySQL | `localhost:3306` |
| Redpanda Kafka API | `localhost:19092` |
| Redpanda Console | `http://localhost:8084` |
| Debezium Connect REST | `http://localhost:8083` |
| MinIO API / Console | `http://localhost:9000` / `http://localhost:9001` |
| Spark application UI (실행 중에만) | `http://localhost:4040` |
| Bronze application UI (실행 중에만) | `http://localhost:4041` |
| Iceberg Catalog PostgreSQL | `localhost:5432` |

## Iceberg connection quick start

```bash
make iceberg-up     # Spark 이미지 빌드, MinIO 버킷과 JDBC catalog DB 준비
make iceberg-demo   # demo 테이블에 2행 추가, 조회·snapshot·파일 경로 출력
make iceberg-sql    # 같은 catalog에 연결된 대화형 Spark SQL
```

SQL 콘솔에서 다음을 실행한다. 종료는 `quit;`이다.

```sql
SHOW NAMESPACES IN lakehouse;
SHOW TABLES IN lakehouse.demo;
SELECT * FROM lakehouse.demo.connection_check;
SELECT committed_at, snapshot_id, operation
FROM lakehouse.demo.connection_check.snapshots;
SELECT file_path, record_count FROM lakehouse.demo.connection_check.files;
```

`make iceberg-demo`는 실행마다 고유한 `run_id`로 2행을 추가한다. 이 데이터는 연결 연습용이며 Bronze 업무 이벤트가 아니다. MinIO Console `http://localhost:9001`의 `warehouse` 버킷에서 `iceberg/demo/connection_check/` 아래 Parquet 데이터와 Iceberg 메타데이터를 직접 볼 수 있다. 로그인 정보는 `.env`의 `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`를 사용한다.

역할은 Spark(연산), Iceberg(테이블·snapshot), MinIO(파일), PostgreSQL(테이블 이름과 현재 metadata 위치를 저장하는 JDBC catalog)로 나뉜다. PostgreSQL에 예약 데이터나 Parquet 본문을 저장하지 않는다. Catalog DB 스키마는 Iceberg가 초기화하며 운영 MySQL용 Alembic과 분리한다.

실행 조합은 Spark 3.5.6 / Scala 2.12 / Java 17 / Iceberg 1.11.0 / PostgreSQL JDBC 42.7.7 / PostgreSQL 17.6이다. Java JAR는 `spark/Dockerfile` 빌드 시 버전을 고정해 설치하며 `uv.lock`과 별개다. Spark는 `local[2]`로 동작하며 별도의 master/worker 클러스터는 없다.

Catalog 접속 비밀번호는 `ICEBERG_CATALOG_PASSWORD`(로컬 기본값 `iceberg-local`)이고, MinIO 자격 증명은 AWS SDK 환경변수로 전달한다. Catalog PostgreSQL은 호스트 5432 포트로 공개하며 DB와 사용자는 `iceberg`다. 현재 Compose의 포트 매핑은 localhost로 제한하지 않으므로 외부 접근 가능성은 호스트 방화벽/네트워크 설정에 따라 달라진다. 이 자격 증명·포트 구성은 로컬 개발용이며 공용 네트워크 노출을 피한다. 데이터 파일은 `minio-data`, catalog는 `iceberg-catalog-data` 볼륨에 유지된다. 둘을 함께 보존해야 테이블을 계속 조회할 수 있다. `make down`은 보존하고, `make reset`은 두 볼륨을 포함해 기존 로컬 데이터 전체를 삭제한다.

## Kafka → Iceberg Bronze

P0 데이터가 생성된 환경에서 실행한다. 기존 데이터·볼륨을 지울 필요는 없다.

```bash
make iceberg-up      # Kafka connector JAR를 포함한 Spark 이미지 갱신
make bronze-stop     # 상시 writer가 실행 중이면 먼저 중지
make bronze-once     # 시작 시점까지 쌓인 이벤트를 적재하고 종료
make verify-bronze   # producer가 멈춘 P0 환경에서 MySQL·Kafka·Bronze 대조
make bronze-up       # 별도 bronze 서비스로 계속 수신 (1분 trigger)
make bronze-logs     # 실행 로그 확인
make bronze-stop     # 상시 적재만 중지; 데이터와 checkpoint 보존
```

`bronze`는 `streaming` profile의 독립 서비스다. `spark`는 SQL·데모·단발 작업용으로 유지한다. 두 서비스가 같은 checkpoint 볼륨을 사용하므로 동시에 writer를 실행하면 파일 잠금으로 거부한다. `bronze-up` 이후 새 메시지는 다음 trigger에서 반영된다. 실행 중 Bronze UI는 `http://localhost:4041`에서 확인한다.

테이블은 `lakehouse.bronze.booking_events`다. MinIO의 `warehouse/iceberg/bronze/booking_events/`에서 파일을 볼 수 있다.

| 컬럼 | 역할 |
| --- | --- |
| `payload`, `kafka_value` | 읽기 편한 UTF-8 문자열과 원본 value 바이트 |
| `kafka_key`, `kafka_headers` | 원본 key와 headers |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | Kafka 레코드 위치 |
| `kafka_timestamp`, `kafka_timestamp_type` | Kafka timestamp와 종류 |
| `bronze_ingested_at` | Spark 적재 시각; 일 단위 Iceberg partition 기준 |

JSON 파싱 실패·null value·같은 `event_id`의 재전송도 버리지 않고 append한다. 정제·업무 중복 제거는 후속 Silver의 책임이다. 작은 파일 생성을 줄이기 위해 기본 trigger를 1분으로 두고, partitioned streaming 쓰기에 Iceberg fanout을 사용한다.

```sql
-- make iceberg-sql로 접속
SELECT get_json_object(payload, '$.event_type') AS event_type, count(*)
FROM lakehouse.bronze.booking_events GROUP BY 1;
SELECT committed_at, snapshot_id, operation
FROM lakehouse.bronze.booking_events.snapshots;
```

checkpoint는 `spark-checkpoints` 볼륨의 `/opt/spark/checkpoints/booking-events-v1`에 저장한다. 처음에는 Kafka에 남은 가장 이른 offset부터 읽고, 이후에는 checkpoint의 진행 위치부터 재개한다. Kafka retention으로 필요한 offset이 사라지면 `failOnDataLoss=true`로 실패시킨다. 같은 호스트의 컨테이너 재생성은 지원하지만 호스트 손실·다중 노드 복구는 아직 범위 밖이다.

`minio-data`, `iceberg-catalog-data`, `spark-checkpoints`를 함께 보존해야 한다. checkpoint만 지우면 재읽기에 따른 중복이 생길 수 있고, 테이블만 없어지면 스크립트가 시작을 거부한다. 토픽 재생성·테이블 교체·볼륨 부분 복구도 자동 복구 대상이 아니다. `make reset`/`make smoke`는 이 데이터까지 초기화하므로 주의한다.

검증기는 P0용 전체 비교다. Kafka의 현재 보존 레코드와 Bronze의 원문·위치를 양방향 대조하고, 중복 위치와 MySQL outbox `event_id` 집합을 검사한다. 지속 유입 중이거나 Kafka retention으로 과거 레코드가 삭제된 환경에서는 비교 범위를 맞춰야 하며, 운영 규모의 기간별 reconciliation은 후속 작업이다.

## Silver 이벤트 정제 (full-refresh 배치)

```bash
make silver-events-clean   # Bronze에서 공통 events_clean 전체 재계산·교체 후 종료
make silver-events         # events_clean에서 예약 전용 booking_events_clean 재계산·교체
make silver-current        # booking_events_clean에서 bookings_current 재계산·교체 후 종료
make silver-payment-transactions # events_clean에서 결제 거래 clean 재계산·교체
```

`lakehouse.silver.events_clean`은 Bronze의 고정 snapshot에서 공통 envelope를 타입화하고 `event_id` 중복을 제거한 논리 이벤트 경계다. 예약·결제 전용 필드는 펼치지 않고 payload에 보존한다. `booking_events_clean`은 예약 lifecycle 5종을, `payment_transactions_clean`은 수납·환불 거래를 각각 타입화하고 도메인 계약을 검증한다. `bookings_current`는 정제된 예약 이벤트를 예약별 현재 상태로 재구성하며 결제 current는 후속 작업이다. 새 컨테이너나 streaming checkpoint는 추가하지 않으며 현재 실행 엔진은 Spark이고 dbt는 아직 도입하지 않았다. 상세 규칙·한계·조회 SQL은 [Silver 변환 명세](docs/SILVER_GUIDE.md)를 참고한다.

## DBeaver에서 Spark SQL 실행

Thrift 연결의 초기 catalog는 `spark_catalog`다. Hive JDBC가 접속 시 여는 `default` namespace를 지원하기 위한 설정이며, Iceberg는 SQL에서 `lakehouse`를 명시해 사용한다. 기존 CLI/배치의 기본 catalog 설정은 변경하지 않는다.

`make thrift-up`으로 JDBC 접속용 `spark-thrift` 서비스를 실행한다. profile 없이 기본 Compose 기동에도 포함된다. 기존 Spark 이미지·Iceberg 설정을 재사용하지만 독립 JVM(`local[2]`, driver 1GB)이므로 추가 메모리가 필요하다. `make thrift-stop`으로 중지하고 `make thrift-logs`로 로그를 확인한다.

DBeaver의 새 연결에서 **Apache Hive** 드라이버를 선택한다.

- Host: `localhost`, Port: `10000`
- JDBC URL: `jdbc:hive2://localhost:10000/` (초기 database 지정 없이 연결)
- Username: `spark`, Password: 비워 둠. 로컬 개발용 무인증(`NONE`) 설정이며 사용자명은 보안 인증 수단이 아니다.
- Spark UI: `http://localhost:4042`. JDBC/UI 모두 호스트 `127.0.0.1`에만 공개한다. Docker 내부 네트워크는 신뢰된 로컬 서비스만 사용하며 외부 공개 시 인증·TLS·접근 통제를 별도 설계한다.

```sql
SHOW NAMESPACES IN lakehouse;
SHOW TABLES IN lakehouse.silver;
SELECT booking_id, event_type, event_time
FROM lakehouse.silver.booking_events_clean LIMIT 100;
```

서버가 Iceberg catalog의 metadata 위치를 조회하고 MinIO의 파일을 읽는다. DBeaver는 SQL 입력/결과 표시 도구이며 PostgreSQL catalog에 직접 연결하는 것과 다르다. Hive JDBC의 탐색기/자동완성에 테이블이 안 보이면 위와 같이 전체 이름을 지정해 조회한다. SQL 문법은 Spark SQL이며 Python 배치에서 만든 임시 view는 이 연결에 공유되지 않는다.

Thrift의 기본 Hive metastore는 컨테이너 전용 `/tmp/shopslot-thrift-metastore`에 분리한다. CLI의 `/opt/spark/work-dir/metastore_db`와 공유하지 않아 두 서버의 Derby 잠금 충돌을 피한다. 이 임시 metastore에 영구 업무 테이블을 만들지 말고 `lakehouse.bronze.*`/`lakehouse.silver.*`처럼 Iceberg catalog를 명시한다. 실제 Iceberg catalog·데이터는 기존 PostgreSQL/MinIO 볼륨을 사용한다. TCP healthcheck는 포트 개방만 검사하므로 catalog/데이터 조회 성공까지 보장하지 않는다.

## Operations

```bash
make ps       # service health/state
make logs     # follow all logs
make down     # stop containers; retain volumes
make reset    # remove containers and local volumes
```

`make reset` is intentionally destructive for local Docker volumes only. It does not remove repository files.

## P0 contract

### Schema ownership

`app/db/models/`가 운영 원본 모델을 정의하고, `alembic/versions/`가 적용 가능한 스키마 이력을 보관한다. 모델 변경 시에는 migration을 새로 만들고 검토한 뒤 적용한다. `mysql/init/`에 테이블 DDL을 추가하지 않는다.

FastAPI는 CDC의 필수 구성요소가 아니다. 필요할 때만 `make api`로 얇은 scenario API를 실행하며, API와 generator 모두 같은 원본 스키마·outbox 계약을 사용한다.

`outbox_events` is an append-only integration-event log. It carries:

- `event_id`: logical-event identity used for replay/deduplication
- `aggregate_id`: booking identity
- `partition_key`: currently `shop_id`, to support the later skew experiment
- `event_time` and JSON `payload`: business event contract

The P1 Spark job persists the event payload, original bytes, and Kafka ingestion metadata to append-only Iceberg Bronze. Historical backfill remains a separate, planned Parquet + Spark batch path.

## Repository layout

```text
debezium/       connector registration and outbox router configuration
app/            optional FastAPI boundary, ORM models, outbox service
alembic/        versioned operational-schema migrations
generator/      deterministic transactional event producer
docker-compose.yml
pyproject.toml   Python 프로젝트와 의존성 범위
uv.lock          재현 가능한 Python 의존성 잠금
mysql/init/     MySQL and Debezium account bootstrap only
spark/          Iceberg/Kafka runtime, catalog configuration, SQL launcher, demo, Bronze jobs
scripts/        repeatable verification commands
```

## 기술 문서

- [문서 안내](docs/README.md): 공개 문서 범위와 구성 요소의 선택 이유

- [Spark Bronze 기술 가이드](docs/SPARK_GUIDE.md): local 모드, Bronze 코드·옵션, trigger·checkpoint와 MinIO 저장 시점
- [Change log](docs/CHANGELOG.md): 구현·계약·운영 방식의 변경과 검증 결과
- [Troubleshooting](docs/TROUBLESHOOTING.md): 재현 조건, 원인, 해결, 검증을 포함한 문제 해결 기록

스키마·이벤트 계약·아키텍처 경계가 변경되면 관련 기술 문서와 변경 기록을 함께 갱신한다.
