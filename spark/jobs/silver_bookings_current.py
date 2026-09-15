"""One learning model: batch/full-refresh booking_events_clean -> bookings_current."""
from pathlib import Path

from pyspark.sql import SparkSession

SOURCE = "lakehouse.silver.booking_events_clean"
TARGET = "lakehouse.silver.bookings_current"
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "silver"


def main():
    spark = SparkSession.builder.appName("shopslot-silver-bookings-current").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Freeze one input snapshot so a concurrent silver-events replace cannot
        # swap the table underneath this run.
        snapshot = spark.sql(f"SELECT snapshot_id FROM {SOURCE}.refs WHERE name = 'main'").first()
        if snapshot is None:
            raise RuntimeError("booking_events_clean has no committed snapshot; run make silver-events first.")
        source = spark.read.option("snapshot-id", str(snapshot.snapshot_id)).table(SOURCE)
        source.createOrReplaceTempView("silver_booking_events_clean")

        input_count = source.count()
        if not input_count:
            raise RuntimeError("booking_events_clean is empty; refusing to replace bookings_current.")

        currents = spark.sql((SQL_DIR / "bookings_current.sql").read_text()).cache()
        try:
            output_count = currents.count()
            if not output_count:
                raise RuntimeError("No booking events resolved to a current row; existing table is preserved.")
            # A booking must not appear twice; the join must not fan out.
            duplicates = currents.groupBy("booking_id").count().filter("count > 1")
            if duplicates.limit(1).count():
                raise RuntimeError("bookings_current produced more than one row for a booking_id.")
            currents.createOrReplaceTempView("silver_bookings_current_result")

            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
            # SparkCatalog supports atomic RTAS: readers see old or new table state.
            spark.sql(f"""
                CREATE OR REPLACE TABLE {TARGET} USING iceberg
                TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
                AS SELECT * FROM silver_bookings_current_result
            """)
            stored = spark.table(TARGET)
            if currents.exceptAll(stored).limit(1).count() or stored.exceptAll(currents).limit(1).count():
                raise RuntimeError("Stored bookings_current differs from the computed result.")

            orphans = stored.filter("is_orphan").count()
            print(f"bookings_current verified: snapshot={snapshot.snapshot_id}, "
                  f"events={input_count}, bookings={output_count}, orphans={orphans}", flush=True)
            stored.groupBy("status").count().orderBy("status").show(truncate=False)
        finally:
            currents.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
