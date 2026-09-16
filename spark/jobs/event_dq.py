"""Persist rejected Silver events to one idempotent Iceberg DQ table."""

from pathlib import Path

from pyspark.sql import Window, functions as F

TARGET = "lakehouse.silver.event_dq"
MERGE_SOURCE_VIEW = "silver_event_dq_merge_source"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def write_event_dq(
    spark,
    rejected,
    *,
    validation_stage: str,
    source_snapshot_id: int,
) -> None:
    """Upsert one latest DQ row per event, falling back to its Kafka position."""
    detected_at = F.current_timestamp()
    kafka_key = F.concat_ws(
        ":",
        F.lit("kafka"),
        F.coalesce(F.col("kafka_topic"), F.lit("unknown")),
        F.coalesce(F.col("kafka_partition").cast("string"), F.lit("unknown")),
        F.coalesce(F.col("kafka_offset").cast("string"), F.lit("unknown")),
    )
    dq_key = F.when(
        F.col("event_id").isNotNull() & (F.trim(F.col("event_id")) != ""),
        F.concat(F.lit("event:"), F.col("event_id")),
    ).otherwise(kafka_key)

    prepared = rejected.select(
        dq_key.alias("dq_key"),
        "event_id",
        "event_type",
        F.lit(validation_stage).alias("validation_stage"),
        "validation_error",
        "payload",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingested_at",
        F.lit(int(source_snapshot_id)).cast("long").alias("source_snapshot_id"),
        detected_at.alias("detected_at"),
    )

    # 재시도나 중복 적재로 물리 레코드가 여러 개여도 target key 하나당 MERGE source
    # 행은 최대 하나여야 한다. 기본 key는 event_id이고, 쓸 수 있는 event_id가 없는
    # 잘못된 envelope은 Kafka 위치로 대체한다.
    latest_per_key = (
        prepared.withColumn(
            "dq_rank",
            F.row_number().over(
                Window.partitionBy("dq_key").orderBy(
                    F.col("bronze_ingested_at").desc_nulls_last(),
                    F.col("kafka_topic").desc_nulls_last(),
                    F.col("kafka_partition").desc_nulls_last(),
                    F.col("kafka_offset").desc_nulls_last(),
                )
            ),
        )
        .filter("dq_rank = 1")
        .drop("dq_rank")
    )
    latest_per_key.createOrReplaceTempView(MERGE_SOURCE_VIEW)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET} (
            dq_key STRING,
            event_id STRING,
            event_type STRING,
            validation_stage STRING,
            validation_error STRING,
            payload STRING,
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            bronze_ingested_at TIMESTAMP,
            source_snapshot_id BIGINT,
            first_detected_at TIMESTAMP,
            last_detected_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
    """)
    merge_sql = (SQL_DIR / "merge_event_dq.sql").read_text().format(
        target=TARGET,
        source_view=MERGE_SOURCE_VIEW,
    )
    spark.sql(merge_sql)
