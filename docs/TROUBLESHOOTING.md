# Troubleshooting

재현 가능한 증상, 근본 원인, 해결 방법, 검증 결과를 기록한다. 같은 문제를 다시 만났을 때 임시 우회 대신 안전한 해결책을 선택하는 기준이다.

## Alembic import 시 `TypeError: Integer() takes no arguments`

### 증상

P0 초기 revision을 실행할 때 `app/db/models/operational.py` import 단계에서 `Integer(unsigned=True)`가 실패한다.

### 원인

SQLAlchemy의 범용 `Integer`와 `SmallInteger`에는 MySQL의 `UNSIGNED` 옵션이 없다. 해당 옵션은 MySQL dialect 타입이 제공한다.

### 해결

ORM 모델에서 `sqlalchemy.dialects.mysql.INTEGER`와 `SMALLINT`를 사용한다. UUID와 마이크로초 datetime도 각각 MySQL `CHAR`, `DATETIME(fsp=6)`으로 명시해 migration DDL과 모델 계약을 일치시켰다.

### 검증

격리된 MySQL 8.4 컨테이너에서 `alembic upgrade head`가 revision `20260908_0001`까지 성공했다.

## 기존 MySQL 볼륨에서 초기 migration 충돌

### 증상

이전 `mysql/init/001_schema.sql`로 이미 테이블이 만들어진 로컬 볼륨에서 최초 Alembic revision을 실행하면 `table already exists`가 발생할 수 있다.

### 원인

기존 볼륨에는 테이블은 있지만 Alembic의 `alembic_version` 이력이 없다. Alembic은 해당 DB를 빈 상태로 보고 초기 DDL을 다시 적용하려 한다.

### 해결

재생성 가능한 로컬 P0 데이터는 `make reset` 후 다시 기동한다. 보존해야 하는 환경에서는 먼저 실제 스키마와 최초 revision이 완전히 일치하는지 비교하고, 검토를 거친 뒤에만 `alembic stamp 20260908_0001` 또는 별도 전환 migration을 사용한다. 확인 없이 stamp하지 않는다.

### 검증

새 볼륨에서 Compose `migrate` 서비스가 다른 서비스보다 먼저 초기 revision을 적용하도록 구성했다.

## `Access denied for user 'shopslot'@'172.x.x.x'`로 migration 인증 실패

### 증상

`migrate` 컨테이너에서 Alembic 실행 시 `pymysql.err.OperationalError: (1045, "Access denied for user 'shopslot' ...")`가 발생한다. 컨테이너 내부에서 현재 `MYSQL_USER`와 `MYSQL_PASSWORD`로 직접 접속해도 실패한다.

### 원인

MySQL 공식 Docker 이미지의 `MYSQL_USER`와 `MYSQL_PASSWORD`는 데이터 디렉터리가 **처음 초기화될 때만** 계정을 만든다. 이후 `.env` 값을 변경하거나 Compose 컨테이너를 다시 만들어도 기존 named volume 안의 MySQL 계정 비밀번호는 바뀌지 않는다. migration 컨테이너는 새 환경변수를 사용하므로 기존 계정과 불일치한다.

### 해결

재생성 가능한 로컬 P0 데이터라면 `make reset` 후 `make up`으로 현재 `.env` 기준 볼륨을 새로 만든다. 데이터를 보존해야 하면 기존에 사용한 MySQL 자격 증명으로 환경을 되돌리거나, 검증된 root 접근 권한으로 `shopslot` 계정 비밀번호를 명시적으로 변경한다. 단순 컨테이너 재시작으로는 해결되지 않는다.

### 검증

현재 Compose 환경의 migration URL은 `shopslot@mysql:3306/shopslot`을 사용하고 있었고, 현재 MySQL 컨테이너 환경변수로 내부 인증이 실패함을 확인했다. 따라서 Docker network 또는 service DNS 문제가 아니라 계정 상태 불일치다.

이번 사례에서는 MySQL 컨테이너가 2026-09-08에 다시 생성됐지만 `shopslot-lakehouse_mysql-data` 볼륨은 2026-09-04에 생성된 것이었다. 재기동 로그에도 `Creating database`, `Creating user`, `init process done`이 없고 기존 데이터 디렉터리로 바로 기동했다. 컨테이너 재생성과 데이터베이스 초기화를 구분해야 한다.
