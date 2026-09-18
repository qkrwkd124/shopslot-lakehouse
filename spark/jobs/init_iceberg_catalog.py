"""Create the namespace required when Lakehouse is the Thrift default catalog."""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("shopslot-iceberg-catalog-init").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.default")
        print("Iceberg catalog namespace ready: lakehouse.default", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
