# Change log

ShopSlot의 구현·계약·운영 방식에 영향을 주는 변경을 날짜순으로 기록한다. 커밋 메시지의 대체물이 아니라, 왜 변경했는지와 검증 결과를 빠르게 파악하기 위한 문서다.

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
