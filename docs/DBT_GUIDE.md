# dbt 실행 환경

이 문서는 ShopSlot Lakehouse의 dbt 실행 환경, Spark Thrift 연결과 첫 Silver 비교 모델을 설명한다. 기존 PySpark Silver 테이블은 기준 결과로 읽기만 하고 dbt 결과는 별도 namespace에 저장한다.

## 현재 구성

```text
dbt CLI (단발 컨테이너)
  -> Spark Thrift Server (`spark-thrift:10000`)
  -> Spark SQL
  -> Iceberg JDBC Catalog (PostgreSQL) + 데이터 파일 (MinIO)
```

dbt는 상시 실행 서버가 아니다. 명령을 실행할 때 컨테이너가 만들어지고, 작업이 끝나면 `--rm`으로 제거된다. Spark Thrift Server는 dbt가 Spark SQL을 전달하는 접속 지점이며 실제 테이블 형식과 저장소는 기존 Iceberg/MinIO 구성을 그대로 사용한다.

Python 의존성은 FastAPI 애플리케이션과 분리해 `dbt_shopslot/pyproject.toml`과 `uv.lock`으로 관리한다. 현재 고정한 adapter는 `dbt-spark[PyHive] 1.11.0`이며 잠금 파일이 함께 설치되는 전체 의존성 버전을 재현한다.

## 디렉터리 역할

| 파일/디렉터리 | 역할 |
| --- | --- |
| `dbt_shopslot/Dockerfile` | uv로 잠금된 dbt 환경을 만드는 실행 이미지 |
| `dbt_shopslot/pyproject.toml` | dbt-spark 직접 의존성 선언 |
| `dbt_shopslot/uv.lock` | 전이 의존성을 포함한 재현 가능한 버전 잠금 |
| `dbt_shopslot/profiles.yml` | Spark Thrift 접속과 기본 대상 namespace 설정 |
| `dbt_shopslot/dbt_project.yml` | 프로젝트 경로, Iceberg 파일 형식 등 공통 모델 설정 |
| `dbt_shopslot/models/sources.yml` | 기존 Silver 입력과 PySpark 기준 결과 source |
| `dbt_shopslot/models/intermediate/booking/int_booking_events_validated.sql` | 예약 파싱·타입 변환·검증 오류를 계산하는 ephemeral 중간 모델 |
| `dbt_shopslot/models/silver/booking_events_clean.sql` | 중간 모델의 정상 행을 저장하는 full-refresh 예약 clean 모델 |
| `dbt_shopslot/models/silver/_booking_events_clean.yml` | 모델 설명과 generic data test |
| `dbt_shopslot/tests/` | 예약 계약과 PySpark 결과 비교 singular test |

개발 target의 기본 목적지는 `lakehouse.silver_dbt`다. 기존 PySpark 구현인 `lakehouse.silver`와 분리해 같은 입력에 대한 결과를 비교한 뒤 전환할 수 있게 한다. dbt-spark는 Spark에서 별도 `database` 설정을 허용하지 않으므로 Thrift의 기본 catalog를 `lakehouse`로 두고 profile에는 `schema: silver_dbt`만 지정한다. Compose의 단발 `iceberg-init`이 세션 시작에 필요한 `lakehouse.default` namespace를 먼저 생성한다.

## 실행 순서

```bash
# 의존성 또는 이미지 구성을 바꾼 뒤 실행
make dbt-build

# 설치 버전 확인
make dbt-version

# 프로젝트 설정과 Spark Thrift 연결 확인
make dbt-debug

# booking_events_clean 생성과 전체 테스트 실행
make dbt-booking-events
```

`dbt-debug`는 설정 파일, Git 의존성, adapter 로딩과 Thrift 접속을 확인하지만 변환 테이블을 생성하지 않는다. Spark Thrift가 꺼져 있으면 Compose가 의존 서비스와 함께 기동한다.

## 첫 비교 모델

`source('existing_silver', 'events_clean')`은 기존 `lakehouse.silver.events_clean`을 읽는다. `int_booking_events_validated`는 예약 lifecycle 5종을 선택하고 payload 타입 변환과 `validation_error` 계산을 담당한다. 이 모델의 `materialized='ephemeral'`은 별도 Iceberg relation을 생성하지 않는다. dbt는 이를 참조하는 `booking_events_clean`의 컴파일 SQL에 `__dbt__cte__int_booking_events_validated` CTE로 삽입한다. `booking_events_clean`은 이 CTE에서 오류가 없는 행만 골라 `lakehouse.silver_dbt.booking_events_clean`에 전체 교체 방식으로 저장한다.

```text
events_clean
  -> int_booking_events_validated (ephemeral, 물리 테이블 없음)
      -> booking_events_clean (Iceberg table)
```

중간 모델을 분리한 목적은 이후 공통 `event_dq`가 같은 파싱·검증 규칙을 재사용하게 하는 것이다. 현재는 clean만 참조하므로 SQL을 구조적으로 분리한 단계이며 DQ 쓰기는 아직 dbt로 이관하지 않았다. ephemeral은 참조 모델마다 SQL이 삽입되므로 clean과 DQ가 각각 실행되면 원본을 각각 다시 읽는다. 동일 입력 결과를 물리적으로 한 번 고정해야 하는 단계에서는 우선 이 중간 모델을 table로 바꾼다. view는 현재 Iceberg JDBC Catalog의 view 지원을 활성화한 뒤 별도로 검토한다.

`dbt build --select booking_events_clean`은 ephemeral 중간 모델을 최종 SQL에 포함하고, 저장 모델 생성 후 event_id not-null/unique, event_type 허용값, booking_id/shop_id not-null, 이벤트별 예약 계약과 기존 PySpark 결과 양방향 비교까지 8개 테스트를 실행한다. 중간 모델 분리 후에도 실제 실행 대상은 table 모델 1개이며 테스트 8개가 모두 통과했다. 기존 PySpark와 dbt 결과는 각각 20행으로 일치한다. `SHOW TABLES IN lakehouse.silver_dbt`에는 `booking_events_clean`만 있고 중간 모델은 없다. full refresh 재실행은 행을 누적하지 않고 새 Iceberg overwrite snapshot으로 같은 결과를 교체한다.

## 다음 단계

현재 모델은 `materialized='table'`인 full refresh다. 다음 단계에서는 이 결과를 기준선으로 Iceberg `MERGE` 기반 증분 처리, 동일 구간 재실행 멱등성, 늦게 도착한 이벤트와 실패 복구를 순서대로 검증한다. 공통 `events_clean`과 DQ 쓰기까지 dbt로 이관한 것은 아니다.
