# Change log

## 2026-09-10 — Spark 학습 노트

- `docs/SPARK_STUDY.md`에 PySpark 역할, local[2]/다중 노드, Bronze DDL·source·writer 옵션, trigger와 실행 시작/대기, MinIO 파일 쓰기와 Iceberg commit 시점을 정리했다.
- README에 학습 문서 링크를 추가했다. 공식 문서와 현재 구현을 대조했으며 실행 코드·아키텍처·데이터 변경은 없다. 따라서 설계 변경이나 신규 장애 기록은 추가하지 않았다.

## 2026-09-09 — Kafka → Iceberg Bronze 스트리밍

- Spark Kafka connector와 전이 의존성을 고정 버전으로 이미지에 추가했다.
- `lakehouse.bronze.booking_events`에 원문 문자열·바이트, key/headers, topic/partition/offset, Kafka timestamp와 적재 시각을 append한다. 적재일 partition을 사용하고 JSON 필터링·업무 중복 제거는 하지 않는다.
- 영속 checkpoint 볼륨, 동일 checkpoint writer 잠금, checkpoint만 남고 테이블이 없는 경우의 시작 거부를 추가했다. Spark 실행기는 종료 signal을 자식 프로세스에 전달한다.
- `bronze-once`, `bronze-up`, `bronze-stop`, `bronze-logs`, `verify-bronze` Make target을 추가했다. 상시 작업은 별도 `streaming` profile 서비스에서 1분 trigger로 실행한다.
- 검증: 최초 29건 적재, 동일 checkpoint 재실행은 offset 29부터 신규 입력 0건. MySQL outbox ID 29개 일치, Kafka/Bronze 각 29건의 원문·metadata 양방향 일치, 중복 Kafka 위치 0건. 별도 Bronze 서비스의 스트리밍 시작도 확인했다.
- 검증 입력의 stdin 대기 문제를 임시 파일 전달로 수정했다. 상세 내용은 `TROUBLESHOOTING.md`에 기록했다.
- 설계서·README·HANDOFF·면접 Q&A를 최신화했다. 5분 live 생성과 처리 중 강제 종료/재개 실험은 다음 단계이며 P1 완료로 표기하지 않는다.

## 2026-09-09 — Iceberg Catalog 선택 근거 기록

- 전체 설계서 11.1절에는 PostgreSQL JDBC Catalog 선택 이유와 핵심 비용만 간략히 남겼다. 대안 비교·재검토 조건과 면접 질문·답변은 `docs/PORTFOLIO_QA.md`에 정리했다.
- PostgreSQL의 성능 우위를 검증한 선택이 아님을 명시하고, 현재 연결 검증과 미검증 항목을 구분했다.
- 저장소 내 포트폴리오 Q&A에도 면접 답변과 후속 질문의 근거를 추가했다. 실행 구성 변경은 없다.

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

## 2026-09-08 — 구현 근거 중심 포트폴리오 Q&A

### Added

- `docs/PORTFOLIO_QA.md`에 프로젝트 요약, outbox와 CDC의 역할, 전달 보장, partition key, Bronze, watermark, Alembic, 동기 DB 선택과 장애 복구 질문을 정리했다.
- 답변마다 현재 구현, 향후 계획, 의도적으로 남은 한계를 구분해 아직 검증하지 않은 내용을 성과처럼 설명하지 않도록 했다.

### Changed

- README와 HANDOFF에서 구현 변경과 함께 면접 Q&A도 지속적으로 갱신하도록 문서 운영 규칙을 확장했다.

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
- README, HANDOFF, 포트폴리오 설계서에 migration ownership과 디렉터리 구조를 반영했다.

### Verified

- 격리된 MySQL 8.4에 revision `20260908_0001`을 적용했다.
- 3 shops, 10 customers, 3 services, 10 bookings, 10 outbox events 생성을 확인했다.
- 격리된 FastAPI 컨테이너의 `GET /health`가 `200 {"status":"ok"}`을 반환함을 확인했다.
