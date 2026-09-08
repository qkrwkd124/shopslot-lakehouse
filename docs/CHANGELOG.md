# Change log

ShopSlot의 구현·계약·운영 방식에 영향을 주는 변경을 날짜순으로 기록한다. 커밋 메시지의 대체물이 아니라, 왜 변경했는지와 검증 결과를 빠르게 파악하기 위한 문서다.

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
