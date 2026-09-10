"""Append two synthetic rows per run and show Iceberg files and snapshots."""
from uuid import uuid4

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("shopslot-iceberg-connection-demo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.demo")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.demo.connection_check (
            run_id STRING, id BIGINT, message STRING, created_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
    """)
    run_id = str(uuid4())
    spark.sql(f"""
        INSERT INTO lakehouse.demo.connection_check VALUES
        ('{run_id}', 1, 'Hello Iceberg', current_timestamp()),
        ('{run_id}', 2, 'Stored in MinIO', current_timestamp())
    """)
    rows = spark.sql(f"""
        SELECT * FROM lakehouse.demo.connection_check
        WHERE run_id = '{run_id}' ORDER BY id
    """)
    assert rows.count() == 2, "Expected two committed demo rows"
    rows.show(truncate=False)
    spark.sql("""
        SELECT committed_at, snapshot_id, operation
        FROM lakehouse.demo.connection_check.snapshots ORDER BY committed_at
    """).show(truncate=False)
    spark.sql("""
        SELECT file_path, record_count FROM lakehouse.demo.connection_check.files
    """).show(truncate=False)
    print("Iceberg connection verified: 2 rows committed and read from MinIO.")
finally:
    spark.stop()
