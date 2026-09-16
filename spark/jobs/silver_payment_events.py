"""Build typed payment lifecycle events from the common events_clean boundary."""
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from event_dq import write_event_dq

SOURCE = "lakehouse.silver.events_clean"
TARGET = "lakehouse.silver.payment_events_clean"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def candidates(spark, source):
    source.createOrReplaceTempView("silver_events_input")
    return spark.sql((SQL_DIR / "parse_payment_events.sql").read_text())


def clean_events(spark, parsed):
    parsed.createOrReplaceTempView("silver_payment_event_candidates")
    return spark.sql((SQL_DIR / "payment_events_clean.sql").read_text())


def main():
    spark = SparkSession.builder.appName("shopslot-silver-payment-events").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Keep one consistent events_clean version for the whole full-refresh run.
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {SOURCE}.refs WHERE name = 'main'"
        ).first()
        if snapshot is None:
            raise RuntimeError(
                "events_clean has no committed snapshot; run make silver-events-clean first."
            )

        source = spark.read.option(
            "snapshot-id", str(snapshot.snapshot_id)
        ).table(SOURCE)
        source_count = source.count()
        if not source_count:
            raise RuntimeError(
                "events_clean is empty; refusing to replace payment_events_clean."
            )

        parsed = candidates(spark, source).cache()
        try:
            input_count = parsed.count()
            if not input_count:
                raise RuntimeError(
                    "events_clean has no payment events; existing table is preserved."
                )

            rejected = parsed.filter("validation_error IS NOT NULL")
            rejected_count = rejected.count()
            rejected.groupBy("validation_error").count().show(truncate=False)

            valid = parsed.filter("validation_error IS NULL")
            duplicate_transactions = (
                valid.filter("payment_transaction_id IS NOT NULL")
                .groupBy("payment_transaction_id")
                .count()
                .filter("count > 1")
            )
            if duplicate_transactions.limit(1).count():
                raise RuntimeError(
                    "Multiple valid events use the same payment_transaction_id; "
                    "payment_events_clean was not replaced."
                )

            inconsistent_requests = (
                valid.groupBy("payment_id")
                .agg(F.countDistinct("request_amount_krw").alias("request_amounts"))
                .filter("request_amounts > 1")
            )
            if inconsistent_requests.limit(1).count():
                raise RuntimeError(
                    "Events for one payment_id disagree on request_amount_krw; "
                    "payment_events_clean was not replaced."
                )

            clean = clean_events(spark, parsed).cache()
            try:
                output_count = clean.count()
                if not output_count:
                    raise RuntimeError(
                        "No valid payment events; existing table is preserved."
                    )

                clean.createOrReplaceTempView("silver_payment_events_clean_result")
                spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
                spark.sql(f"""
                    CREATE OR REPLACE TABLE {TARGET} USING iceberg
                    TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                    AS SELECT * FROM silver_payment_events_clean_result
                """)
                write_event_dq(
                    spark,
                    rejected,
                    validation_stage="payment_events_clean",
                    source_snapshot_id=snapshot.snapshot_id,
                )

                print(
                    f"payment_events_clean written: snapshot={snapshot.snapshot_id}, "
                    f"events_clean={source_count}, payment_events={input_count}, "
                    f"rejected={rejected_count}, stored={output_count}",
                    flush=True,
                )
                clean.groupBy("event_type", "payment_status").count().orderBy(
                    "event_type", "payment_status"
                ).show(truncate=False)
            finally:
                clean.unpersist()
        finally:
            parsed.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
