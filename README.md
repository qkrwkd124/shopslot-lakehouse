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

`make smoke`는 로컬 볼륨을 초기화하고 Compose stack과 Debezium connector를 기동한 뒤, 예약 10건의 lifecycle 데이터를 생성해 MySQL과 Kafka의 상태·이벤트 계약을 검증한다.

고정 P0 데이터셋은 예약 10건, 결제 7건, 결제 거래 9건과 총 29개의 outbox/Kafka 이벤트로 구성한다. 이벤트 분포는 `booking_created` 10건, `booking_rescheduled` 1건, `booking_cancelled` 1건, `checked_in` 7건, `no_show_marked` 1건, `payment_completed` 7건, `payment_refunded` 2건이다. 환불은 20,000원 부분 환불과 55,000원 전액 환불을 각각 한 건 포함한다.

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

| Service | Endpoint |
| --- | --- |
| MySQL | `localhost:3306` |
| Redpanda Kafka API | `localhost:19092` |
| Redpanda Console | `http://localhost:8084` |
| Debezium Connect REST | `http://localhost:8083` |
| MinIO API / Console | `http://localhost:9000` / `http://localhost:9001` |
| Spark application UI (실행 중에만) | `http://localhost:4040` |

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

Catalog 접속 비밀번호는 `ICEBERG_CATALOG_PASSWORD`(로컬 기본값 `iceberg-local`)이고, MinIO 자격 증명은 AWS SDK 환경변수로 전달한다. Catalog PostgreSQL은 호스트 포트를 공개하지 않는다. 데이터 파일은 `minio-data`, catalog는 `iceberg-catalog-data` 볼륨에 유지된다. 둘을 함께 보존해야 테이블을 계속 조회할 수 있다. `make down`은 보존하고, `make reset`은 두 볼륨을 포함해 기존 로컬 데이터 전체를 삭제한다. 이 자격 증명·단일 노드 구성은 로컬 학습용이다.

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

`bronze`는 `streaming` profile의 독립 서비스다. `spark`는 SQL·데모·단발 작업용으로 유지한다. 두 서비스가 같은 checkpoint 볼륨을 사용하므로 동시에 writer를 실행하면 파일 잠금으로 거부한다. `bronze-up` 이후 새 메시지는 다음 trigger에서 반영된다. Bronze 서비스는 호스트 UI 포트를 노출하지 않는다.

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

## Change records

- [Spark 학습 노트](docs/SPARK_STUDY.md): local 모드, Bronze 코드·옵션, trigger·checkpoint와 MinIO 저장 시점
- [Change log](docs/CHANGELOG.md): 구현·계약·운영 방식의 변경과 검증 결과
- [Troubleshooting](docs/TROUBLESHOOTING.md): 재현 조건, 원인, 해결, 검증을 포함한 문제 해결 기록
- [Portfolio interview Q&A](docs/PORTFOLIO_QA.md): 현재 구현 근거, 설계 트레이드오프, 남은 한계를 설명하는 면접 연습 문서

스키마·이벤트 계약·아키텍처 경계가 변경되면 같은 변경에서 이 문서들과 포트폴리오 설계서를 함께 갱신한다.
