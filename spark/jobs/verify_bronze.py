"""Bounded local check: Kafka bytes/positions and optional MySQL event IDs."""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from bronze_stream import BROKERS, TABLE, TOPIC, bronze_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outbox-ids-file", type=Path)
    args = parser.parse_args()
    ids = ([line.strip() for line in args.outbox_ids_file.read_text().splitlines() if line.strip()]
           if args.outbox_ids_file else None)
    if ids is not None and not ids:
        raise SystemExit("MySQL outbox is empty; generate data before reconciliation.")
    spark = SparkSession.builder.appName("shopslot-verify-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Run with producers idle after bronze-once. This scans the retained topic.
        source = (spark.read.format("kafka")
                  .option("kafka.bootstrap.servers", BROKERS).option("subscribe", TOPIC)
                  .option("startingOffsets", "earliest").option("endingOffsets", "latest")
                  .option("includeHeaders", "true").option("failOnDataLoss", "true").load())
        kafka = bronze_rows(source).drop("bronze_ingested_at").cache()
        bronze = spark.table(TABLE).cache()
        actual = bronze.select(kafka.columns)
        kafka_count, bronze_count = kafka.count(), bronze.count()
        if kafka_count == 0:
            raise RuntimeError("Kafka topic has no records; no ingestion was verified.")
        missing = kafka.exceptAll(actual).count()
        extra = actual.exceptAll(kafka).count()
        duplicated = (bronze.groupBy("kafka_topic", "kafka_partition", "kafka_offset")
                      .count().filter("count > 1").count())
        if missing or extra or duplicated:
            raise RuntimeError(f"Reconciliation failed: missing={missing}, extra={extra}, duplicate_positions={duplicated}")
        if ids is not None:
            outbox_ids = spark.createDataFrame([(value,) for value in ids], "event_id STRING").distinct()
            event_ids = bronze.select(F.get_json_object("payload", "$.event_id").alias("event_id")).distinct()
            absent = outbox_ids.join(event_ids, "event_id", "left_anti").count()
            unexpected = event_ids.join(outbox_ids, "event_id", "left_anti").count()
            if absent or unexpected:
                raise RuntimeError(f"Outbox IDs differ: missing={absent}, unexpected={unexpected}")
            print(f"MySQL outbox -> Bronze: {len(ids)} event IDs matched.")
        bronze.select(F.get_json_object("payload", "$.event_type").alias("event_type")) \
            .groupBy("event_type").count().orderBy("event_type").show(truncate=False)
        print(f"Bronze verified: Kafka={kafka_count}, Bronze={bronze_count}; exact records matched, duplicate positions=0.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
