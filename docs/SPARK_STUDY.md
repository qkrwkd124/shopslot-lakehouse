# Spark 학습 노트 — ShopSlot Bronze 코드 읽기

기준: 2026-09-10, Spark 3.5.6 / Iceberg 1.11.0. 구현 근거는 `spark/jobs/bronze_stream.py`와 `spark/conf/spark-defaults.conf`다. 학습한 개념과 코드의 연결을 기록하며, 면접 답변은 `PORTFOLIO_QA.md`에서 관리한다.

## 1. 역할부터 구분하기

- Python/PySpark: Spark 작업을 작성하는 언어와 API. Python 반복문으로 메시지를 하나씩 옮기는 구현이 아니다.
- Spark: Kafka를 읽고 변환·쓰기 작업을 실행하는 연산 엔진.
- Iceberg: 파일을 테이블로 관리하는 포맷과 라이브러리. 스키마, partition, snapshot, commit을 관리한다.
- PostgreSQL JDBC Catalog: 테이블 이름과 현재 metadata 위치를 관리하는 저장소. 예약 원문은 여기에 저장하지 않는다.
- MinIO: S3 API로 접근하는 실제 파일 저장소. Parquet 데이터와 Iceberg metadata 파일을 저장한다.
- Bronze: 원본 보존 계층. 이번 구현은 원문 외에 Kafka 위치와 적재 시각을 붙인다. 업무 정제·중복 제거·최신 상태·집계는 후속 Silver/Gold의 책임이다.

Spark는 단순 파일 적재에 필수는 아니다. 이 프로젝트에서는 Iceberg 스트리밍 적재와 checkpoint, 향후 대량 backfill을 같은 엔진으로 학습하려고 사용한다. 29건 처리에 필요한 최소 구성이라고 주장하지 않는다.

## 2. local[2]와 다중 노드

`spark.master local[2]`는 한 머신에서 task 실행용 스레드 2개를 사용한다는 뜻이다. 서버 2대나 executor 2개가 아니다. 기본 task 자원 설정에서 최대 2개 task를 동시에 실행할 수 있지만 CPU 코어를 독점 예약하는 옵션도 아니다.

다중 노드에서는 Driver가 작업을 계획하고 task를 배분한다. Cluster Manager는 자원을 할당하고, Worker Node의 Executor 프로세스들이 데이터를 읽고 계산한다. Worker는 머신, Executor는 프로세스다. Driver와 Master는 같은 개념이 아니다.

Standalone에서는 `local[2]` 대신 `spark://master:7077`에 제출한다. Spark 3.5.6 Standalone의 Python 작업은 cluster deploy mode를 지원하지 않으므로 client mode와 구분해야 한다. 다중 노드 전환에는 네트워크·의존성 배포·checkpoint 저장소와 잠금 방식도 검토해야 한다. 한 노트북의 여러 컨테이너는 여러 물리 머신의 자원·장애 격리와 다르다.

현재 다중 노드 실험은 하지 않았다. 대량 입력·병목 실험과 함께 후속 학습으로 남긴다. [Spark 실행 설정](https://spark.apache.org/docs/3.5.6/submitting-applications.html), [Cluster overview](https://spark.apache.org/docs/3.5.6/cluster-overview.html).

## 3. 코드의 실행 순서

| 코드 | 역할 | 이 단계에서 업무 데이터 적재가 시작되는가? |
| --- | --- | --- |
| `spark.sql("CREATE ...")` | namespace/테이블 준비 | 아니오. DDL은 실행되며 catalog/metadata는 생성될 수 있음 |
| `spark.readStream...load()` | Kafka 입력을 나타내는 streaming DataFrame 구성 | 아니오 |
| `bronze_rows(source)` | 컬럼 선택·변환 정의 | 아니오 |
| `.writeStream...trigger(...)` | 출력과 실행 주기 설정 | 아니오 |
| `writer.toTable(TABLE)` | streaming query 시작, 실행 핸들 반환 | 여기서 시작 |
| `query.awaitTermination()` | query 종료까지 호출한 스레드 대기 | 이미 시작한 query는 별도로 계속 동작 |

입력·변환·출력 정의와 실행 시작을 구분한다. `load()`가 Kafka 데이터를 모두 메모리에 올리는 것도, `awaitTermination()`이 다음 메시지 한 건을 기다리는 것도 아니다.

## 4. CREATE TABLE 쿼리

```sql
CREATE TABLE IF NOT EXISTS lakehouse.bronze.booking_events (
    payload STRING, kafka_value BINARY, kafka_key BINARY,
    kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP, kafka_timestamp_type INT,
    kafka_headers ARRAY<STRUCT<key: STRING, value: BINARY>>,
    bronze_ingested_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(bronze_ingested_at))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');
```

`spark.sql()`은 Spark SQL을 실행한다. Python의 `f"...{TABLE}..."`은 문자열에 TABLE 변수 값을 넣는 문법이다. 현재 TABLE은 코드에 고정된 값이며 사용자 입력을 그대로 SQL에 끼워 넣는 용도가 아니다.

### 이름과 USING

- `lakehouse`: 설정에 정의한 catalog 이름.
- `bronze`: namespace. RDB의 schema/database 구분과 유사한 역할.
- `booking_events`: 테이블 이름.
- `USING iceberg`: 테이블 이름이 아니라 **이 테이블을 관리할 provider/포맷 지정**. 일반 Parquet 파일 테이블이 아닌 Iceberg 테이블로 만든다.
- `IF NOT EXISTS`: 이미 존재하면 생성하지 않는다. 코드의 컬럼 정의를 바꿔도 기존 테이블을 자동 migration하지 않는다.

### 타입과 컬럼

`STRING`은 문자열, `BINARY`는 바이트, `INT/BIGINT`는 정수, `TIMESTAMP`는 시각이다. `kafka_headers`는 key 문자열과 value 바이트를 가진 구조체들의 배열이다. 개념적으로 `[{key: "trace-id", value: <bytes>}, ...]` 형태이며 같은 key가 반복될 수 있어 Map과 다르다.

`payload`는 value를 문자열로 본 값이고 `kafka_value`는 원본 바이트다. `kafka_timestamp`는 Kafka 레코드 시각이며 JSON 안의 업무 `event_time`과 구분한다. `kafka_timestamp_type`은 Kafka timestamp 종류를 보존한다. `bronze_ingested_at`은 Spark에서 추가한 적재 시각이다.

### Partition과 table properties

- `days(bronze_ingested_at)`: 적재 시각을 일 단위로 변환해 partition을 나눈다. 원본 timestamp는 유지하고 partition 변환은 Iceberg가 관리한다. 따로 날짜 컬럼을 추가할 필요는 없다.
- 하루 partition이 Parquet 파일 하나라는 뜻은 아니다. 같은 날에도 여러 task/batch의 파일이 생긴다. Kafka partition이나 Spark 처리 partition과도 다른 개념이다.
- `format-version=2`: Iceberg 테이블 명세 버전이다. Spark/Iceberg 라이브러리 버전, 이벤트 schema_version, Parquet 버전이 아니다. V2는 행 수준 변경을 위한 delete file 표현 등을 지원하지만 이 job은 append만 한다.
- `write.format.default=parquet`: 실제 데이터 파일의 기본 형식을 Parquet로 지정한다. Iceberg metadata까지 모두 Parquet인 것은 아니다.

[Iceberg Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/), [Iceberg configuration](https://iceberg.apache.org/docs/latest/configuration/).

## 5. Kafka source 옵션

`spark.readStream.format("kafka")`는 Kafka connector를 입력으로 사용한다. `.load()`의 결과는 끝없이 추가될 수 있는 입력을 표현하는 streaming DataFrame이다. Kafka key/value는 기본적으로 binary이며 JSON을 자동으로 업무 컬럼으로 펼치지 않는다.

| 옵션 | 현재 값 | 의미 |
| --- | --- | --- |
| `kafka.bootstrap.servers` | `redpanda:9092` | 브로커 정보를 알아내기 위한 최초 접속 주소. 컨테이너 내부 주소 |
| `subscribe` | `booking.events.v1` | 읽을 토픽 |
| `startingOffsets` | `earliest` | checkpoint 없는 새 query는 현재 Kafka에 남은 가장 이른 위치부터 시작 |
| `failOnDataLoss` | `true` | 필요한 offset 소실 등 유실 가능성을 발견하면 실패시킴. 유실 자체를 예방하거나 복구하는 옵션은 아님 |
| `includeHeaders` | `true` | headers 컬럼도 읽음 |
| `maxOffsetsPerTrigger` | `10000` | trigger당 처리 offset 수의 상한. 전체 topic/partition에 걸친 한도이며 partition마다 1만 개가 아님 |

1만 건이 모여야 시작하는 옵션이 아니다. 29건만 있으면 그만큼 처리한다. Kafka retention으로 지워진 데이터는 `earliest`로 복구할 수 없다. 기존 checkpoint를 사용하는 재실행은 저장된 offset에서 재개하며 매번 처음부터 읽지 않는다. 실제 지속 실행 여부는 뒤의 trigger가 결정한다.

[Spark 3.5.6 Kafka integration](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html).

## 6. bronze_rows와 writer 옵션

`bronze_rows(source)`는 `select`로 컬럼 이름을 맞추고 value의 문자열 표현과 `current_timestamp()` 적재 시각을 추가한다. 업무 JSON 필드 추출·필터링·deduplication은 없다.

| 코드/옵션 | 의미 |
| --- | --- |
| `.writeStream` | streaming 출력 설정 시작 |
| `.format("iceberg")` | Iceberg sink 사용. Kafka source의 `format("kafka")`와 대칭 |
| `.queryName(...)` | 실행 중 query를 구분할 이름. 테이블명, Kafka consumer group, checkpoint ID가 아님 |
| `.outputMode("append")` | micro-batch의 새 출력 행을 테이블에 추가. 기존 행 교체나 PK 기준 upsert가 아님 |
| `checkpointLocation` | offset·batch 진행 상태 등의 저장 위치 |
| `fanout-enabled=true` | 한 task에서 Iceberg partition 값별 writer를 유지하여 partition별 사전 정렬 요구를 피함 |

fanout은 Kafka fan-out이나 worker 수 증가 옵션이 아니다. 날짜가 섞인 입력도 partition별 파일로 쓸 수 있지만 task가 끝날 때까지 여러 writer를 유지하므로 partition 값이 많으면 메모리·열린 파일 비용이 증가한다.

checkpoint 경로는 `/opt/spark/checkpoints/booking-events-v1`이며 Docker의 `spark-checkpoints` 볼륨에 보존한다. query 이름만 같다고 재개하는 것은 아니다. 같은 event_id가 다른 Kafka offset에 재발행되면 별도 행으로 보존한다. checkpoint를 지우는 것은 안전한 재시작 방법이 아니다.

[Iceberg Structured Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/).

## 7. trigger, toTable, awaitTermination

Trigger는 micro-batch를 언제 실행하고 언제 종료할지 정하는 정책이다. DB의 INSERT/UPDATE trigger와 다르다.

- `availableNow=True`: 실행 시작 시점에 사용 가능한 미처리 구간을 처리하고 종료한다. 입력 제한에 따라 여러 micro-batch로 나뉠 수 있다. `make bronze-once`가 이 모드이며 deprecated `once=True`와 다른 옵션이다.
- `processingTime="1 minute"`: 실행을 유지하며 1분 간격으로 micro-batch를 실행한다. 한 batch가 1분을 넘기면 종료 후 다음 batch를 가능한 빨리 실행한다. 같은 query의 batch를 무조건 겹쳐 실행하지 않는다. 첫 처리를 반드시 1분 기다리는 설정도 아니다.
- Trigger 간격은 지연 SLA나 event-time 집계 window가 아니다. 1분으로 집계한다는 의미가 없다.

```python
query = writer.toTable(TABLE)  # 실제 스트리밍 시작
query.awaitTermination()      # 종료까지 대기; 읽기·쓰기는 진행 중
```

`awaitTermination()`은 다음 이벤트가 도착하면 반환하는 함수가 아니다. availableNow 완료·중지·실패로 query가 끝날 때까지 호출 스레드를 대기시킨다. 실패 시 예외를 전달한다. 이 코드에서 대기 없이 finally로 진행하면 `spark.stop()`이 실행되어 작업을 종료할 수 있다.

[Spark Structured Streaming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html).

## 8. S3에는 언제, 어떻게 저장되는가?

코드에 `.save("s3://...")`가 없는 이유는 테이블의 저장 위치와 FileIO가 catalog 설정에 있기 때문이다.

```properties
spark.sql.catalog.lakehouse.warehouse s3://warehouse/iceberg
spark.sql.catalog.lakehouse.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint http://minio:9000
```

`warehouse`는 bucket 이름이고 `iceberg`는 그 아래 경로다. 여기서는 AWS의 원격 S3가 아니라 MinIO endpoint로 요청한다. 초기 테이블 위치는 `s3://warehouse/iceberg/bronze/booking_events` 아래다.

실행 순서:

1. `toTable(TABLE)`로 query를 시작한다.
2. 각 micro-batch가 처리할 Kafka offset 구간을 결정한다.
3. task가 데이터를 읽고 Bronze 컬럼을 구성한다.
4. Iceberg writer가 S3FileIO를 통해 MinIO에 Parquet 파일을 기록한다.
5. Iceberg metadata와 새 snapshot을 만들고 catalog의 참조를 commit한다.
6. 정상 완료한 batch의 진행 상태를 checkpoint에 기록하고 다음 처리를 이어간다.

이는 성공 경로의 요약이다. checkpoint에는 batch 실행 전 offset 기록도 있으며 단순히 모든 상태가 마지막에 한 번 저장되는 것은 아니다. 파일 생성과 테이블 commit, Spark batch 완료는 구분해야 한다. commit 전 파일이 저장소에 존재해도 아직 테이블 조회 대상은 아닐 수 있다. 장애 시 미참조 파일이 남을 수도 있다.

S3 쓰기는 `awaitTermination()`이 반환된 뒤가 아니라 기다리는 동안 매 batch에서 일어난다. 테이블 생성만 해도 metadata는 생길 수 있지만 이벤트 Parquet 적재는 streaming 실행 이후다. 1분 trigger가 매분 파일 하나를 만든다는 뜻도 아니다.

## 9. 직접 확인할 순서 — 기존 데이터 유지

```bash
make iceberg-up
make bronze-stop    # 상시 writer가 실행 중일 때 먼저 중지
make bronze-once
make verify-bronze  # producer가 멈춘 P0 범위의 대조
make iceberg-sql
```

```sql
SELECT count(*) FROM lakehouse.bronze.booking_events;
SELECT kafka_topic, kafka_partition, kafka_offset, payload
FROM lakehouse.bronze.booking_events ORDER BY kafka_partition, kafka_offset;
SELECT committed_at, snapshot_id, operation
FROM lakehouse.bronze.booking_events.snapshots;
SELECT file_path, record_count
FROM lakehouse.bronze.booking_events.files;
```

`.snapshots`, `.files`는 Iceberg가 제공하는 metadata table 조회다. 업무 테이블 이름 전체에 임의로 점을 붙인 것이 아니다. MinIO Console의 `warehouse` bucket에서도 파일을 확인할 수 있다. 계속 수신하려면 `make bronze-up`으로 시작한다.

이 문서 작성 중 실행 검증이나 데이터 변경은 하지 않았다. 기존 검증 결과는 최초 29건 적재, 동일 checkpoint 재실행 입력 0건, Kafka/Bronze 29건과 MySQL ID 일치다. 처리 중 장애·다중 노드·대량 부하는 아직 미검증이다. `make smoke`/`make reset`은 볼륨을 초기화하므로 학습용 조회에 사용하지 않는다.

## 10. 다음 분석 때 답해볼 질문

1. `.load()`와 `.toTable()` 중 실제 스트리밍을 시작하는 것은 무엇인가?
2. 29건 처리 후 같은 checkpoint로 다시 실행하면 몇 건을 새로 쓰는가?
3. `maxOffsetsPerTrigger=10000`인데 3건만 들어오면 기다리는가?
4. Kafka partition, Spark partition, Iceberg 날짜 partition은 각각 무엇을 나누는가?
5. 업무 발생 날짜 대신 적재 날짜로 저장한 이유와 조회 시의 비용은 무엇인가?
6. Silver에 필요한 필드·중복 기준·잘못된 데이터 처리 기준은 무엇인가?

학습 순서: 코드와 데이터 확인 → 예상 결과 설명 → 직접 실행/SQL 작성 → 반례 검토. Silver 구현은 요구사항을 먼저 정한 뒤 진행한다.
