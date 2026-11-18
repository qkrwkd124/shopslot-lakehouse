"""Copy the current MySQL shops table into Iceberg Bronze."""

import os

from pyspark.sql import SparkSession, functions as F

TARGET = "lakehouse.bronze.shops"


def main():
    spark = SparkSession.builder.appName("shopslot-bronze-shops").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        database = os.environ["MYSQL_DATABASE"]
        shops = (
            spark.read.format("jdbc")
            .option("url", f"jdbc:mysql://mysql:3306/{database}")
            .option("dbtable", "shops")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("user", os.environ["MYSQL_USER"])
            .option("password", os.environ["MYSQL_PASSWORD"])
            .option("preferTimestampNTZ", "true")
            .load()
        )
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
        (
            shops.withColumn("bronze_ingested_at", F.current_timestamp())
            .writeTo(TARGET)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .createOrReplace()
        )
        print(f"Copied MySQL shops to {TARGET}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
