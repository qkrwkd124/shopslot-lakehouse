"""Rebuild one current-state row per payment from typed payment events."""
from pathlib import Path

from pyspark.sql import SparkSession

SOURCE = "lakehouse.silver.payment_events_clean"
TARGET = "lakehouse.silver.payments_current"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def main():
    spark = SparkSession.builder.appName("shopslot-silver-payments-current").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Freeze one input snapshot so a concurrent payment-events refresh cannot
        # change this run's input halfway through the computation.
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {SOURCE}.refs WHERE name = 'main'"
        ).first()
        if snapshot is None:
            raise RuntimeError(
                "payment_events_clean has no committed snapshot; "
                "run make silver-payment-events first."
            )

        source = spark.read.option(
            "snapshot-id", str(snapshot.snapshot_id)
        ).table(SOURCE)
        source.createOrReplaceTempView("silver_payment_events_clean")

        input_count = source.count()
        if not input_count:
            raise RuntimeError(
                "payment_events_clean is empty; refusing to replace payments_current."
            )
        source_payment_count = source.select("payment_id").distinct().count()

        currents = spark.sql((SQL_DIR / "payments_current.sql").read_text()).cache()
        try:
            output_count = currents.count()
            if not output_count:
                raise RuntimeError(
                    "No payment events resolved to a current row; "
                    "existing table is preserved."
                )

            duplicates = currents.groupBy("payment_id").count().filter("count > 1")
            if duplicates.limit(1).count():
                raise RuntimeError(
                    "payments_current produced more than one row for a payment_id."
                )
            if output_count != source_payment_count:
                raise RuntimeError(
                    "payments_current does not cover every payment_id from "
                    "payment_events_clean."
                )

            currents.createOrReplaceTempView("silver_payments_current_result")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
            spark.sql(f"""
                CREATE OR REPLACE TABLE {TARGET} USING iceberg
                TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                AS SELECT * FROM silver_payments_current_result
            """)

            orphans = currents.filter("is_orphan").count()
            print(
                f"payments_current written: snapshot={snapshot.snapshot_id}, "
                f"events={input_count}, payments={output_count}, orphans={orphans}",
                flush=True,
            )
            currents.groupBy("payment_status").count().orderBy(
                "payment_status"
            ).show(truncate=False)
        finally:
            currents.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
