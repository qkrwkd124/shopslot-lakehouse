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

# 예약 도메인 모델 생성과 관련 테스트 실행
make dbt-booking
```

`dbt-debug`는 설정 파일, Git 의존성, adapter 로딩과 Thrift 접속을 확인하지만 변환 테이블을 생성하지 않는다. Spark Thrift가 꺼져 있으면 Compose가 의존 서비스와 함께 기동한다.

## 로그와 실행 결과 보관

Compose의 dbt 서비스는 다음 호스트 폴더를 컨테이너에 연결한다. `--rm`으로 단발 컨테이너가 삭제돼도 파일은 호스트에 남으며, 두 폴더는 기존 `.gitignore` 설정으로 Git에서 제외한다.

| 호스트 경로 | 컨테이너 경로 | 내용 |
| --- | --- | --- |
| `dbt_shopslot/logs/` | `/opt/dbt/logs` | 상세 실행 로그 `dbt.log` |
| `dbt_shopslot/target/` | `/opt/dbt/target` | `run_results.json`, `manifest.json`, 컴파일 SQL 등 |

이 설정은 실행별 이력 보관 기능은 아니다. `target/`의 결과 파일은 후속 명령에서 덮어쓸 수 있고, `dbt clean`은 프로젝트의 `clean-targets`에 따라 연결된 호스트의 `logs/`와 `target/`도 삭제한다. 장기 이력이 필요하면 별도의 실행별 보관 단계를 추가한다.

## 예약 모델과 DQ 실행

`source('existing_silver', 'events_clean')`은 기존 PySpark `lakehouse.silver.events_clean`을 읽는다. 공통 검증과 event_id 중복 제거는 기존 PySpark에서 수행한다.

```text
events_clean
  -> int_booking_events_validated (Iceberg table)
      -> booking_events_clean (정상 행, 전체 교체)
      -> booking_event_dq (오류 행, 증분 MERGE)
```

중간 테이블에는 예약 이벤트의 파싱·타입 변환·오류 판정 결과를 저장한다. 두 하위 모델은 같은 검증 결과를 재사용한다. 같은 파이프라인을 동시에 실행해 중간 테이블을 교체하지 않도록 한다.

`macros/event_dq_rows.sql`은 오류 행 선택과 공통 DQ 컬럼 구성을 재사용하는 SQL 매크로다. 예약 DQ 모델은 이 매크로에 예약 중간 모델과 검증 단계 이름을 전달한다. 향후 결제 DQ는 별도 모델·테이블로 추가해 예약 실행과 분리한다.

`make dbt-booking`은 이미지를 빌드한 뒤 `selectors.yml`의 `booking` 범위를 `dbt build --selector booking`으로 실행한다. 예약 검증 중간 모델과 모든 하위 모델을 포함한다. 기존 `make dbt-booking-events`는 정상·DQ와 상위 모델을 명시적으로 선택한다. 공통 상위 모델은 한 번 생성하고 정상 모델의 기존 8개 테스트와 DQ 키 NULL·중복 및 오류 사유 NULL 테스트를 실행한다. 두 하위 모델 사이에는 선후 의존성이 없으며 실패한 테스트에 의존하는 후속 작업만 건너뛰는 기본 정책을 사용한다.

결과 테이블은 `lakehouse.silver_dbt.booking_events_clean`과 `lakehouse.silver_dbt.booking_event_dq`다. 기존 `lakehouse.silver.event_dq`는 수정하지 않는다. DQ는 `dq_key`가 같으면 갱신하고 없으면 삽입하며 `first_detected_at`은 갱신에서 제외한다. 입력에서 사라진 오류도 DQ 이력에는 남는다. 정상 모델과 중간 모델은 매번 전체 교체한다.

예약 DQ와 상위 모델만 실행하려면 이미지 빌드 후 다음 명령을 사용한다.

```bash
docker compose --profile tools run --rm dbt build --select +booking_event_dq
```

아직 DQ 입력은 매번 전체 중간 테이블을 검사한다. 신규 입력 경계, Spark DQ의 source_snapshot_id 보존, 빈 입력 보호, 해결된 오류 표시 정책은 후속 과제다. 이전 실험의 `silver_dbt.event_dq` 테이블이 존재해도 모델 이름 변경으로 자동 삭제되지는 않는다. 기존 모델의 20행 일치 및 테스트 8개 통과 기록은 이 변경 전 검증 이력이다.

## 다음 단계

예약 clean과 검증 중간 모델은 `materialized='table'`인 full refresh이며 예약 DQ는 incremental MERGE다. 다음 단계에서는 이 결과를 기준선으로 Iceberg `MERGE` 기반 증분 처리, 동일 구간 재실행 멱등성, 늦게 도착한 이벤트와 실패 복구를 순서대로 검증한다. 공통 `events_clean`과 공통·결제 DQ 쓰기는 아직 dbt로 이관하지 않았다.
