# ShopSlot Lakehouse — Handoff

## Purpose

ShopSlot은 일반 상품 쇼핑몰이 아니라, 시간 슬롯을 예약하고 현장 서비스 후 결제·환불이 발생하는 가상 다점포 서비스 커머스 플랫폼이다. 이 저장소는 `MySQL + transactional outbox + Debezium + Redpanda + Spark Structured Streaming + Iceberg + dbt` Lakehouse 포트폴리오를 구현한다.

실제 회사 코드, 테이블명, 업무 규칙, 고객 정보 및 PII는 사용하지 않는다.

## Agreed operational model

초기 모델은 의도적으로 단순하다.

```text
shops ─┬─ services
       ├─ staffs
       └─ bookings ─┬─ customers
                     ├─ services (예약당 1개)
                     └─ staffs (담당 직원 1명)

bookings ── payments ── payment_transactions
```

- `shops`: 다점포 운영 주체. 상위 10개 매장이 이벤트 약 70%를 차지하는 Zipf skew를 만든다.
- `customers`: ID와 세그먼트만 사용한다. 이름·전화·주소 등 PII는 없다.
- `services`: 매장별 서비스 카탈로그. 가격, 소요 시간, 활성 상태를 가진다.
- `staffs`: 매장별 직원. 예약의 담당 직원을 FK로 연결한다.
- `bookings`: 예약 하나에 서비스 하나만 연결한다. 예약 당시 가격(`booked_price_krw`)을 보존한다.
- `payments`: 예약당 0~1개. 청구 금액, 현재 수납·환불 상태를 요약한다.
- `payment_transactions`: 실제 수납 또는 환불의 append-only 거래. 부분·전액 환불을 표현한다.
- `outbox_events`: 원본 변경과 같은 DB 트랜잭션에서 기록하는 명시적 비즈니스 이벤트다.

엔터티와 `payment_transactions`는 공통 `BaseModel`의 `BIGINT UNSIGNED AUTO_INCREMENT` PK `id`와 생성·수정 시각을 사용하고, FK도 같은 타입으로 맞춘다. FK 이름은 `shop_id`, `booking_id`처럼 관계 대상을 남긴다. outbox는 논리 이벤트 식별자인 UUID `event_id`를 PK로 유지한다. 이벤트 payload의 `payment_transaction_id`는 숫자형 거래 PK 값을 전달하는 계약 필드다. 향후 순수 연관 테이블은 별도 `id` 없이 FK 조합을 복합 PK로 사용한다.

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

- 2026-09-09: Spark 3.5.6(Java 17) + Iceberg 1.11.0 + MinIO 연결 구성을 추가했다. `lakehouse` JDBC catalog는 별도 PostgreSQL 17.6에 metadata 위치를 기록하고, 실제 파일은 `s3://warehouse/iceberg`에 저장한다.
- `make iceberg-up`, `make iceberg-demo`, `make iceberg-sql`로 연결 실습을 실행한다. 데모 테이블 `lakehouse.demo.connection_check`는 매 실행 시 2행을 추가한다. Spark는 `local[2]`다.
- Kafka → `lakehouse.bronze.booking_events` append 스트리밍을 구현했다. 원문 문자열·바이트, key, headers, topic/partition/offset, timestamp와 적재 시각을 보존한다. 업무 중복 제거와 watermark는 적용하지 않는다.
- `make bronze-once`는 현재까지 적재 후 종료하고, `make bronze-up`은 `streaming` profile의 별도 서비스에서 1분 trigger로 계속 적재한다. checkpoint는 `spark-checkpoints` 볼륨으로 공유하며 파일 잠금으로 동시 writer를 막는다. `make bronze-stop`으로 중지한다.
- 첫 적재 29건과 동일 checkpoint 재실행의 신규 입력 0건을 확인했다. `make verify-bronze`도 통과했다: Kafka/Bronze 원문·위치 29건과 MySQL outbox ID 29개 일치, 중복 위치 0건. 지속 유입·retention 경계의 운영용 검증은 아니다.
- Catalog DB의 `iceberg-catalog-data`, 파일의 `minio-data`, checkpoint의 `spark-checkpoints` 볼륨은 함께 보존한다. Iceberg 내부 catalog 테이블은 Iceberg가 생성하며 MySQL Alembic 대상이 아니다.
- 연결 검증: 샘플 2행, append snapshot 1개, Parquet 파일 2개를 확인했고 새 Spark SQL 프로세스에서 재조회했다. 파일은 MinIO Console의 `warehouse/iceberg/demo/connection_check/`에서 볼 수 있다.

- Docker Compose의 MySQL, Redpanda, Redpanda Console, Debezium, MinIO, Spark 기본 구성은 있다. Console은 `http://localhost:8084`에서 Kafka 토픽과 메시지를 조회한다.
- 아직 배포 전인 P0 migration은 최종 스키마를 만드는 단일 초기 revision `20260908_0001`로 squash했다. `staffs`, `bookings.staff_id` FK와 `payment_transactions.id`가 모두 포함된다. Compose의 `migrate` 서비스가 Debezium과 generator보다 먼저 migration을 적용한다.
- Python 의존성 범위는 `pyproject.toml`, 정확한 설치 버전과 해시는 `uv.lock`으로 관리한다. 공용 Dockerfile은 고정된 uv 바이너리와 `uv sync --locked --no-dev`를 사용한다.
- P0 generator는 shops(3), customers(10), services(3), bookings(10), payments(7), payment_transactions(9)를 만들고 lifecycle outbox 이벤트 29건을 기록한다. 각 상태 변경·결제·환불과 대응 outbox 행은 같은 트랜잭션으로 commit한다.
- `make smoke`로 MySQL 상태 정합성과 Kafka 이벤트 29건을 검증했다. 예약 최종 상태는 checked-in 7건, cancelled 1건, no-show 1건, scheduled 1건이며 결제에는 부분 환불과 전액 환불이 각각 1건 있다.
- Debezium connector는 outbox JSON payload를 schema wrapper 없는 plain JSON으로 `booking.events.v1`에 발행한다.

## Change and incident record

- 기능·스키마·이벤트 계약·운영 명령 변경은 [CHANGELOG.md](CHANGELOG.md)에 즉시 기록한다.
- 구현·검증 중 발견한 장애와 해결은 [TROUBLESHOOTING.md](TROUBLESHOOTING.md)에 증상·원인·해결·검증 순으로 기록한다.
- 설계 선택, 구현 근거, 현재 한계와 면접 답변은 [PORTFOLIO_QA.md](PORTFOLIO_QA.md)에 구현 진행과 함께 갱신한다.
- 포트폴리오 범위·아키텍처·디렉터리 구조가 바뀌면 저장소 변경과 같은 작업에서 `PORTFOLIO_DESIGN_CDC_LAKEHOUSE.md`도 갱신한다.

## Next task

1. 지속적인 신규 이벤트를 위한 `produce_live.py`를 추가한다. FastAPI scenario API는 필요할 때만 확장한다.
2. 5분 live 유입 중 Bronze 증가와 처리 중 Spark kill/restart 후 정합성을 검증해 P1 완료 조건을 채운다. 현재 확인한 것은 고정 P0 적재와 정상 종료 후 재실행이다.
3. retention·지속 유입을 고려한 기간/offset 범위 기반 reconciliation으로 확장한다.
4. dbt Silver/Gold와 DQ·중복 제거를 구현한다.

## Commands

```bash
cp .env.example .env
make smoke
make api    # 선택적인 FastAPI scenario API
make down
```

기본 `make up`에 Redpanda Console이 포함되며 브라우저에서 `http://localhost:8084`로 접속한다.

`make reset`은 ShopSlot Docker 볼륨만 삭제한다.

## Conversation record

The design discussion is available at:

https://chatgpt.com/s/cx_6a9e0d046398819187929c45ebd1a4d0
