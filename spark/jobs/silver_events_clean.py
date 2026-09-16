"""Build the common logical-event boundary from the raw Bronze topic table."""
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from event_dq import write_event_dq

SOURCE = "lakehouse.bronze.booking_events"
TARGET = "lakehouse.silver.events_clean"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def main():
    spark = SparkSession.builder.appName("shopslot-silver-events-clean").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {SOURCE}.refs WHERE name = 'main'"
        ).first()
        if snapshot is None:
            raise RuntimeError("Bronze has no committed snapshot; run bronze-once first.")

        source = spark.read.option("snapshot-id", str(snapshot.snapshot_id)).table(SOURCE)
        source.createOrReplaceTempView("silver_bronze_input")
        candidates = spark.sql((SQL_DIR / "parse_events.sql").read_text()).cache()
        try:
            input_count = candidates.count()
            if not input_count:
                raise RuntimeError("Bronze is empty; refusing to replace events_clean.")

            rejected = candidates.filter("validation_error IS NOT NULL")
            rejected_count = rejected.count()
            rejected.groupBy("validation_error").count().show(truncate=False)

            conflicts = (
                candidates.filter("validation_error IS NULL")
                .groupBy("event_id")
                .agg(F.countDistinct("payload").alias("payload_variants"))
                .filter("payload_variants > 1")
            )
            if conflicts.limit(1).count():
                raise RuntimeError(
                    "Conflicting payloads for the same event_id; events_clean was not replaced."
                )

            candidates.createOrReplaceTempView("silver_event_candidates")
            clean = spark.sql((SQL_DIR / "events_clean.sql").read_text()).cache()
            try:
                output_count = clean.count()
                if not output_count:
                    raise RuntimeError("No valid events; existing events_clean is preserved.")

                clean.createOrReplaceTempView("silver_events_clean_result")
                spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
                spark.sql(f"""
                    CREATE OR REPLACE TABLE {TARGET} USING iceberg
                    TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                    AS SELECT * FROM silver_events_clean_result
                """)
                write_event_dq(
                    spark,
                    rejected,
                    validation_stage="events_clean",
                    source_snapshot_id=snapshot.snapshot_id,
                )

                print(
                    f"events_clean written: snapshot={snapshot.snapshot_id}, "
                    f"Bronze={input_count}, rejected={rejected_count}, "
                    f"duplicates_removed={input_count-rejected_count-output_count}, "
                    f"events={output_count}",
                    flush=True,
                )
                clean.groupBy("event_type").count().orderBy("event_type").show(
                    truncate=False
                )
            finally:
                clean.unpersist()
        finally:
            candidates.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
