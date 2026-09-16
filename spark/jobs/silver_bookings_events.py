"""Build typed booking lifecycle events from the common events_clean boundary."""
from pathlib import Path

from pyspark.sql import SparkSession

SOURCE = "lakehouse.silver.events_clean"
TARGET = "lakehouse.silver.booking_events_clean"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def candidates(spark, source):
    source.createOrReplaceTempView("silver_events_input")
    return spark.sql((SQL_DIR / "parse_booking_events.sql").read_text())


def clean_events(spark, parsed):
    parsed.createOrReplaceTempView("silver_booking_event_candidates")
    return spark.sql((SQL_DIR / "booking_events_clean.sql").read_text())


def main():
    spark = SparkSession.builder.appName("shopslot-silver-booking-events").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Freeze the validated common-event boundary for this run.
        snapshot = spark.sql(f"SELECT snapshot_id FROM {SOURCE}.refs WHERE name = 'main'").first()
        if snapshot is None:
            raise RuntimeError("events_clean has no committed snapshot; run make silver-events-clean first.")
        source = spark.read.option("snapshot-id", str(snapshot.snapshot_id)).table(SOURCE)
        source_count = source.count()
        if not source_count:
            raise RuntimeError("events_clean is empty; refusing to replace booking_events_clean.")

        parsed = candidates(spark, source).cache()
        try:
            input_count = parsed.count()
            if not input_count:
                raise RuntimeError("events_clean has no booking events; existing table is preserved.")
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
                # Full refresh of this derived table only; upstream tables are never modified.
                spark.sql(f"""
                    CREATE OR REPLACE TABLE {TARGET} USING iceberg
                    TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                    AS SELECT * FROM silver_clean_result
                """)
                print(f"booking_events_clean written: snapshot={snapshot.snapshot_id}, "
                      f"events_clean={source_count}, booking_events={input_count}, "
                      f"rejected={rejected_count}, stored={output_count}", flush=True)
                clean.groupBy("event_type").count().orderBy("event_type").show(truncate=False)
            finally:
                clean.unpersist()
        finally:
            parsed.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
