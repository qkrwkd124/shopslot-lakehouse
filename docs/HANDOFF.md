# ShopSlot Lakehouse — Handoff

## Purpose

ShopSlot은 일반 상품 쇼핑몰이 아니라, 시간 슬롯을 예약하고 현장 서비스 후 결제·환불이 발생하는 가상 다점포 서비스 커머스 플랫폼이다. 이 저장소는 `MySQL + transactional outbox + Debezium + Redpanda + Spark Structured Streaming + Iceberg + dbt` Lakehouse 포트폴리오를 구현한다.

실제 회사 코드, 테이블명, 업무 규칙, 고객 정보 및 PII는 사용하지 않는다.

## Agreed operational model

초기 모델은 의도적으로 단순하다.

```text
shops ─┬─ services
       └─ bookings ─┬─ customers
                     └─ services (예약당 1개)

bookings ── payments ── payment_transactions
```

- `shops`: 다점포 운영 주체. 상위 10개 매장이 이벤트 약 70%를 차지하는 Zipf skew를 만든다.
- `customers`: ID와 세그먼트만 사용한다. 이름·전화·주소 등 PII는 없다.
- `services`: 매장별 서비스 카탈로그. 가격, 소요 시간, 활성 상태를 가진다.
- `bookings`: 예약 하나에 서비스 하나만 연결한다. 예약 당시 가격(`booked_price_krw`)을 보존한다.
- `payments`: 예약당 0~1개. 청구 금액, 현재 수납·환불 상태를 요약한다.
- `payment_transactions`: 실제 수납 또는 환불의 append-only 거래. 부분·전액 환불을 표현한다.
- `outbox_events`: 원본 변경과 같은 DB 트랜잭션에서 기록하는 명시적 비즈니스 이벤트다.

초기 범위에는 `booking_items`, `payment_items`, 상품 판매, 복수 결제수단 분할, 선불권/패스가 없다. 이들은 다중 서비스 예약이나 서비스 단위 환불이 필요해질 때 확장한다.

## Event contract

Kafka topic은 `booking.events.v1`이며, 기본 Kafka key는 `shop_id`다. skew 비교 시에는 `booking_id`를 사용한다.

이벤트 종류:

```text
booking_created
booking_rescheduled
booking_cancelled
checked_in
no_show_marked
payment_completed
payment_refunded
```

모든 이벤트에는 `event_id`, `event_type`, `event_time`, `ingest_time`, `schema_version`, `booking_id`, `shop_id`가 있다. 결제·환불 이벤트만 `payment_id`, `payment_transaction_id`, `amount_krw`를 포함한다. `event_id`는 논리 이벤트의 중복 제거 키이고, `booking_id`는 최신 예약 상태를 재구성하는 키다.

## Target Lakehouse

```text
Historical: generator -> Parquet -> Spark batch -> Iceberg Bronze
Live:       generator -> MySQL + outbox -> Debezium -> Redpanda
            -> Spark Structured Streaming -> Iceberg Bronze
            -> dbt-spark -> Silver / Gold
```

- Bronze: 원문 payload, Kafka metadata, ingestion metadata를 append-only로 보존한다.
- Silver: `booking_events_clean`, `bookings_current`, `payments_current`, `event_dq`를 만든다.
- Gold: `shop_daily_metrics`와 `shop_funnel`을 만든다. 예약·체크인·노쇼, 청구·수납·환불 금액, 예약에서 결제까지의 전환율을 다룬다.
- Airflow는 Silver/Gold/DQ/Iceberg maintenance 배치에만 사용한다. streaming job을 스케줄하지 않는다.

## Data scenarios to prove

- 동일 `event_id` 재전송: 2%
- 동일 `booking_created` 재전송: 1%
- event-time 지연 도착: 1.5%, watermark 30분과 2시간 비교
- `shop_id` 누락: 0.2%, DQ 격리
- schema v2 `source` 필드 추가: 5%, Iceberg schema evolution
- `booking_created` 없는 `checked_in`: 0.3%, orphan 검출

## Current repository state

- Docker Compose의 MySQL, Redpanda, Debezium, MinIO, Spark 기본 구성은 있다.
- P0 원본 모델은 SQLAlchemy ORM과 Alembic 초기 revision(`20260908_0001`)으로 관리한다. Compose의 `migrate` 서비스가 Debezium과 generator보다 먼저 migration을 적용한다.
- Python 의존성 범위는 `pyproject.toml`, 정확한 설치 버전과 해시는 `uv.lock`으로 관리한다. 공용 Dockerfile은 고정된 uv 바이너리와 `uv sync --locked --no-dev`를 사용한다.
- P0 generator는 shops(3), customers(10), services(3)를 seed하고 bookings(10)와 outbox_events(10)를 기록한다. 예약과 outbox 행은 각각 같은 트랜잭션으로 commit한다.
- 격리된 MySQL 8.4에서 Alembic migration 및 generator 기록을 검증했다: 3 shops, 10 customers, 3 services, 10 bookings, 10 outbox events.

## Change and incident record

- 기능·스키마·이벤트 계약·운영 명령 변경은 [CHANGELOG.md](CHANGELOG.md)에 즉시 기록한다.
- 구현·검증 중 발견한 장애와 해결은 [TROUBLESHOOTING.md](TROUBLESHOOTING.md)에 증상·원인·해결·검증 순으로 기록한다.
- 포트폴리오 범위·아키텍처·디렉터리 구조가 바뀌면 저장소 변경과 같은 작업에서 `PORTFOLIO_DESIGN_CDC_LAKEHOUSE.md`도 갱신한다.

## Next task

1. P0 generator에 `payment_completed`와 `payment_refunded` 시나리오를 추가해 payments/payment_transactions와 타입별 outbox 계약을 검증한다.
2. Debezium outbox payload를 이벤트 타입별 계약에 맞춘다.
3. `make smoke`로 새 모델의 MySQL outbox 10건과 Kafka 이벤트 10건을 검증한다.
4. FastAPI scenario API가 필요해지면 `app/api/`에 예약 상태 전이 endpoint를 추가한다. migration은 새 Alembic revision으로만 적용한다.

## Commands

```bash
cp .env.example .env
make smoke
make api    # 선택적인 FastAPI scenario API
make down
```

`make reset`은 ShopSlot Docker 볼륨만 삭제한다.

## Conversation record

The design discussion is available at:

https://chatgpt.com/s/cx_6a9e0d046398819187929c45ebd1a4d0
