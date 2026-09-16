-- dq_key가 이미 존재하면 마지막 발견 정보를 갱신하고, 처음 발견한 오류면 새 행을 만든다.
-- source view는 event_dq.py에서 dq_key당 한 행으로 정리한 뒤 등록한다.
MERGE INTO {target} AS target
USING {source_view} AS source
ON target.dq_key = source.dq_key
WHEN MATCHED THEN UPDATE SET
    target.event_id = source.event_id,
    target.event_type = source.event_type,
    target.validation_stage = source.validation_stage,
    target.validation_error = source.validation_error,
    target.payload = source.payload,
    target.kafka_topic = source.kafka_topic,
    target.kafka_partition = source.kafka_partition,
    target.kafka_offset = source.kafka_offset,
    target.kafka_timestamp = source.kafka_timestamp,
    target.bronze_ingested_at = source.bronze_ingested_at,
    target.source_snapshot_id = source.source_snapshot_id,
    target.last_detected_at = source.detected_at
WHEN NOT MATCHED THEN INSERT (
    dq_key,
    event_id,
    event_type,
    validation_stage,
    validation_error,
    payload,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    bronze_ingested_at,
    source_snapshot_id,
    first_detected_at,
    last_detected_at
)
VALUES (
    source.dq_key,
    source.event_id,
    source.event_type,
    source.validation_stage,
    source.validation_error,
    source.payload,
    source.kafka_topic,
    source.kafka_partition,
    source.kafka_offset,
    source.kafka_timestamp,
    source.bronze_ingested_at,
    source.source_snapshot_id,
    source.detected_at,
    source.detected_at
)
