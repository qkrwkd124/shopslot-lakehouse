# Troubleshooting

재현 가능한 증상, 근본 원인, 해결 방법, 검증 결과를 기록한다. 같은 문제를 다시 만났을 때 임시 우회 대신 안전한 해결책을 선택하는 기준이다.

## 엔터티 PK 변경 후 검증 SQL에서 `Unknown column 'p.payment_id'`

### 증상

엔터티 PK를 `id`로 변경한 뒤 `make verify-p0`의 결제 reconciliation에서 `p.payment_id` 컬럼을 찾을 수 없다는 오류가 발생했다.

### 원인

ORM, migration과 generator는 `payments.id`로 변경했지만 검증 스크립트의 상관 서브쿼리 한 곳이 이전 PK 이름을 참조했다. 스키마 변경 시 쓰기 경로뿐 아니라 운영·검증 쿼리도 함께 바뀌어야 한다.

### 해결

거래 합계 조건을 `pt.payment_id = p.id`로 변경했다. 아울러 `information_schema`를 이용해 엔터티 5개는 `id`, 로그 2개는 의미가 있는 식별자를 PK로 쓰는지 검증하도록 계약 검사를 추가했다.

### 검증

기존 데이터에서 migration downgrade/upgrade 후 `make verify-p0`가 통과했고, 빈 임시 DB에서도 migration과 generator 전체 실행이 성공했다.

## MySQL 재생성 직후 검증에서 로컬 소켓 접속 실패

### 증상

`docker compose up -d mysql && make verify-p0`를 한 번에 실행했을 때 `Can't connect to local MySQL server through socket '/var/run/mysqld/mysqld.sock'` 오류가 발생했다.

### 원인

Compose의 `up -d`는 컨테이너 프로세스 시작까지만 기다린다. 뒤따른 검증이 MySQL healthcheck가 `healthy`가 되기 전에 실행돼 서버 소켓 준비와 경합했다.

### 해결

MySQL을 직접 재생성한 뒤에는 `docker compose ps mysql`에서 `healthy`를 확인하고 검증을 실행한다. 정상 실행 경로에서는 `migrate`와 generator의 `depends_on.condition: service_healthy`를 사용한다.

### 검증

MySQL이 `healthy`가 된 뒤 동일한 `make verify-p0`를 재실행해 성공했다. 데이터 손상이나 인증 문제는 없었다.

## Kafka 이벤트 유형이 두 배로 집계되고 JSON에 schema wrapper가 포함됨

### 증상

P0 검증에서 실제 `checked_in` 이벤트는 7건인데 문자열 집계 결과가 14건으로 나왔다. Kafka value도 의도한 payload 객체가 아니라 `schema`와 `payload`로 감싼 JSON이었다.

### 원인

- 검증기가 `checked_in` 문자열 전체를 세어 한 이벤트 안의 `event_type`과 `status`를 모두 집계했다.
- Compose의 `VALUE_CONVERTER_SCHEMAS_ENABLE` 환경변수는 Debezium worker 설정 파일에 전달되지 않았다. 시작 로그에도 해당 property가 없어서 JsonConverter 기본 schema wrapper가 유지됐다.

### 해결

- Kafka 검증 패턴을 정확한 `\"event_type\":\"checked_in\"` key-value로 제한했다.
- worker에는 `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false`, outbox connector에는 `value.converter.schemas.enable=false`를 명시했다.
- 기존 토픽 레코드 형식은 바뀌지 않으므로 재현 가능한 P0 볼륨을 초기화하고 이벤트를 다시 생성했다.

### 검증

`make smoke`로 29건을 다시 생성한 뒤 7개 이벤트 유형의 기대 건수가 모두 일치했다. Kafka value가 schema wrapper 없는 plain JSON payload임도 직접 확인했다.

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
