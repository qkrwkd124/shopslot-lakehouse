"""One learning model: batch/full-refresh Bronze -> Silver, no streaming loop."""
import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

SOURCE = "lakehouse.bronze.booking_events"
TARGET = "lakehouse.silver.booking_events_clean"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def candidates(spark, source):
    source.createOrReplaceTempView("silver_bronze_input")
    return spark.sql((SQL_DIR / "parse_booking_events.sql").read_text())


def clean_events(spark, parsed):
    parsed.createOrReplaceTempView("silver_event_candidates")
    # Ambiguous retransmissions must not silently select an arbitrary payload.
    # Conservative: even JSON whitespace differences require investigation.
    conflicts = (parsed.filter("validation_error IS NULL").groupBy("event_id")
                 .agg(F.countDistinct("payload").alias("variants"))
                 .filter("variants > 1"))
    if conflicts.limit(1).count():
        raise RuntimeError("Conflicting payloads for the same event_id; Silver was not replaced.")
    return spark.sql((SQL_DIR / "booking_events_clean.sql").read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="Run small in-memory SQL fixtures first.")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("shopslot-silver-booking-events").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.self_test:
            from test_silver_booking_events import check_fixtures
            check_fixtures(spark)
        # Freeze one input snapshot so a live Bronze append cannot change this run's input.
        snapshot = spark.sql(f"SELECT snapshot_id FROM {SOURCE}.refs WHERE name = 'main'").first()
        if snapshot is None:
            raise RuntimeError("Bronze has no committed snapshot; run bronze-once first.")
        source = spark.read.option("snapshot-id", str(snapshot.snapshot_id)).table(SOURCE)
        parsed = candidates(spark, source).cache()
        try:
            input_count = parsed.count()
            if not input_count:
                raise RuntimeError("Bronze is empty; refusing to replace Silver with an empty result.")
            rejected = parsed.filter("validation_error IS NOT NULL")
            rejected_count = rejected.count()
            rejected.groupBy("validation_error").count().show(truncate=False)
            clean = clean_events(spark, parsed).cache()
            try:
                output_count = clean.count()
                if not output_count:
                    raise RuntimeError("No valid events; existing Silver is preserved.")
                clean.createOrReplaceTempView("silver_clean_result")
                spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
                # SparkCatalog supports atomic RTAS: readers see old or new table state.
                # Full refresh of this derived table only; Bronze is never modified.
                spark.sql(f"""
                    CREATE OR REPLACE TABLE {TARGET} USING iceberg
                    TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                    AS SELECT * FROM silver_clean_result
                """)
                stored = spark.table(TARGET)
                if clean.exceptAll(stored).limit(1).count() or stored.exceptAll(clean).limit(1).count():
                    raise RuntimeError("Stored Silver differs from the computed result.")
                print(f"Silver verified: snapshot={snapshot.snapshot_id}, Bronze={input_count}, "
                      f"rejected={rejected_count}, duplicates_removed={input_count-rejected_count-output_count}, "
                      f"Silver={output_count}", flush=True)
                stored.groupBy("event_type").count().orderBy("event_type").show(truncate=False)
            finally:
                clean.unpersist()
        finally:
            parsed.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
