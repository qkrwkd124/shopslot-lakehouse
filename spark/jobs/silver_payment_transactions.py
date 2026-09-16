"""Build one typed Silver row per payment transaction event."""
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

SOURCE = "lakehouse.silver.events_clean"
TARGET = "lakehouse.silver.payment_transactions_clean"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def main():
    spark = SparkSession.builder.appName(
        "shopslot-silver-payment-transactions"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
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
                "events_clean is empty; refusing to replace payment_transactions_clean."
            )

        source.createOrReplaceTempView("silver_events_input")
        candidates = spark.sql(
            (SQL_DIR / "parse_payment_transactions.sql").read_text()
        ).cache()
        try:
            input_count = candidates.count()
            if not input_count:
                raise RuntimeError(
                    "events_clean has no payment events; existing table is preserved."
                )

            rejected = candidates.filter("validation_error IS NOT NULL")
            rejected_count = rejected.count()
            rejected.groupBy("validation_error").count().show(truncate=False)

            valid = candidates.filter("validation_error IS NULL")
            duplicate_transactions = (
                valid.groupBy("payment_transaction_id")
                .count()
                .filter("count > 1")
            )
            if duplicate_transactions.limit(1).count():
                raise RuntimeError(
                    "Multiple valid events use the same payment_transaction_id; "
                    "payment_transactions_clean was not replaced."
                )

            inconsistent_requests = (
                valid.groupBy("payment_id")
                .agg(F.countDistinct("request_amount_krw").alias("request_amounts"))
                .filter("request_amounts > 1")
            )
            if inconsistent_requests.limit(1).count():
                raise RuntimeError(
                    "Events for one payment_id disagree on request_amount_krw; "
                    "payment_transactions_clean was not replaced."
                )

            candidates.createOrReplaceTempView("silver_payment_transaction_candidates")
            clean = spark.sql(
                (SQL_DIR / "payment_transactions_clean.sql").read_text()
            ).cache()
            try:
                output_count = clean.count()
                if not output_count:
                    raise RuntimeError(
                        "No valid payment transactions; existing table is preserved."
                    )

                clean.createOrReplaceTempView(
                    "silver_payment_transactions_clean_result"
                )
                spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
                spark.sql(f"""
                    CREATE OR REPLACE TABLE {TARGET} USING iceberg
                    TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                    AS SELECT * FROM silver_payment_transactions_clean_result
                """)

                print(
                    f"payment_transactions_clean written: snapshot={snapshot.snapshot_id}, "
                    f"events_clean={source_count}, payment_events={input_count}, "
                    f"rejected={rejected_count}, transactions={output_count}",
                    flush=True,
                )
                clean.groupBy("transaction_type", "payment_status").count().orderBy(
                    "transaction_type", "payment_status"
                ).show(truncate=False)
            finally:
                clean.unpersist()
        finally:
            candidates.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
