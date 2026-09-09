# ShopSlot Lakehouse

가상의 다점포 예약 플랫폼을 위한 CDC 기반 Lakehouse 포트폴리오다. 실제 고객·회사 데이터는 사용하지 않는다.

P0에서는 MySQL의 예약 원본 상태와 transactional outbox를 같은 트랜잭션으로 기록하고, Debezium Outbox Event Router가 이를 Redpanda의 `booking.events.v1` 토픽으로 발행하는 것을 검증한다. 원본 스키마는 SQLAlchemy 모델과 Alembic migration으로 관리한다.

```text
generator/API -> MySQL (operational state + outbox_events, one transaction)
             -> Debezium -> Redpanda topic: booking.events.v1

MinIO + Spark are provisioned now; Iceberg Bronze ingestion starts in P1.
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
| Spark master UI | `http://localhost:8080` |

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

The P1 Spark job will persist both the event payload and Kafka ingestion metadata to append-only Iceberg Bronze. Historical backfill remains a separate Parquet + Spark batch path.

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
spark/          reserved for P1 Bronze streaming configuration/jobs
scripts/        repeatable verification commands
```

## Change records

- [Change log](docs/CHANGELOG.md): 구현·계약·운영 방식의 변경과 검증 결과
- [Troubleshooting](docs/TROUBLESHOOTING.md): 재현 조건, 원인, 해결, 검증을 포함한 문제 해결 기록
- [Portfolio interview Q&A](docs/PORTFOLIO_QA.md): 현재 구현 근거, 설계 트레이드오프, 남은 한계를 설명하는 면접 연습 문서

스키마·이벤트 계약·아키텍처 경계가 변경되면 같은 변경에서 이 문서들과 포트폴리오 설계서를 함께 갱신한다.
