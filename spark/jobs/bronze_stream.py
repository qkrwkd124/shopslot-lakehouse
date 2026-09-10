"""Kafka -> append-only Iceberg Bronze; one writer per local checkpoint."""
import argparse
import fcntl
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

TABLE = "lakehouse.bronze.booking_events"
TOPIC = "booking.events.v1"
BROKERS = "redpanda:9092"
CHECKPOINT = Path("/opt/spark/checkpoints/booking-events-v1")


def bronze_rows(source):
    # Keep the exact bytes as well as a convenient UTF-8 view. No JSON filtering.
    return source.select(
        F.col("value").cast("string").alias("payload"),
        F.col("value").alias("kafka_value"),
        F.col("key").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("headers").alias("kafka_headers"),
        F.current_timestamp().alias("bronze_ingested_at"),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--available-now", action="store_true",
                        help="Process offsets available at startup, then exit.")
    args = parser.parse_args()
    CHECKPOINT.mkdir(parents=True, exist_ok=True)
    with (CHECKPOINT / ".writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Bronze writer already running. Run make bronze-stop first.")
        spark = SparkSession.builder.appName("shopslot-bronze").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        try:
            # A retained checkpoint must not silently write into a recreated empty table.
            if (CHECKPOINT / "metadata").exists() and not spark.catalog.tableExists(TABLE):
                raise RuntimeError("Checkpoint exists but Bronze table is missing; restore both together.")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    payload STRING, kafka_value BINARY, kafka_key BINARY,
                    kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
                    kafka_timestamp TIMESTAMP, kafka_timestamp_type INT,
                    kafka_headers ARRAY<STRUCT<key: STRING, value: BINARY>>,
                    bronze_ingested_at TIMESTAMP
                ) USING iceberg
                PARTITIONED BY (days(bronze_ingested_at))
                TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
            """)
            source = (spark.readStream.format("kafka")
                      .option("kafka.bootstrap.servers", BROKERS)
                      .option("subscribe", TOPIC)
                      .option("startingOffsets", "earliest")
                      .option("failOnDataLoss", "true")
                      .option("includeHeaders", "true")
                      .option("maxOffsetsPerTrigger", 10000)
                      .load())
            writer = (bronze_rows(source).writeStream.format("iceberg")
                      .queryName("shopslot-bronze-booking-events")
                      .outputMode("append")
                      .option("checkpointLocation", str(CHECKPOINT))
                      .option("fanout-enabled", "true"))
            writer = (writer.trigger(availableNow=True) if args.available_now
                      else writer.trigger(processingTime="1 minute"))
            query = writer.toTable(TABLE)
            print(f"Bronze started: {TOPIC} -> {TABLE}; checkpoint={CHECKPOINT}", flush=True)
            query.awaitTermination()
            if query.lastProgress:
                print(json.dumps(query.lastProgress), flush=True)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
