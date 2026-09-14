"""Launch Spark SQL, submit, or a foreground Thrift server with shared config."""
import os
from pathlib import Path
import subprocess
import signal
import sys
import tempfile


def property_value(value):
    # Java Properties escaping, including whitespace in credentials.
    return (value.replace("\\", "\\\\").replace("\n", "\\n")
            .replace("\r", "\\r").replace("\t", "\\t").replace(" ", "\\ "))


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("sql", "submit", "thrift"):
        raise SystemExit("Usage: run.py {sql|submit|thrift} [Spark arguments]")
    mode = sys.argv[1]
    executable = "spark-sql" if mode == "sql" else "spark-submit"
    arguments = sys.argv[2:]
    if mode == "thrift":
        # Keep the server in the foreground so container signals reach its JVM.
        arguments = [
            "--name", "shopslot-spark-thrift",
            "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
            "--hiveconf", "hive.server2.thrift.bind.host=0.0.0.0",
            "--hiveconf", "hive.server2.thrift.port=10000",
            "--hiveconf", "hive.server2.authentication=NONE",
            "--hiveconf", "hive.server2.enable.doAs=false",
            "--hiveconf", "javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tmp/shopslot-thrift-metastore;create=true",
            *arguments, "spark-internal",
        ]
    config = Path(__file__).with_name("conf").joinpath("spark-defaults.conf").read_text()
    if mode == "thrift":
        # Hive JDBC opens the default namespace before executing user SQL.
        # Keep it in the session catalog; Iceberg uses explicit lakehouse names.
        config += "\nspark.sql.defaultCatalog=spark_catalog\n"
    for suffix, variable in (("user", "ICEBERG_JDBC_USER"), ("password", "ICEBERG_JDBC_PASSWORD")):
        config += f"\nspark.sql.catalog.lakehouse.jdbc.{suffix}={property_value(os.environ[variable])}\n"
    # Credentials only exist in a private temporary file for this process.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".conf") as properties:
        properties.write(config)
        properties.flush()
        child = subprocess.Popen([
            f"/opt/spark/bin/{executable}", "--properties-file", properties.name,
            *arguments,
        ])
        # docker compose stop reaches this wrapper first; forward it to Spark.
        def forward_signal(signum, _frame):
            if child.poll() is None:
                child.send_signal(signum)

        signal.signal(signal.SIGTERM, forward_signal)
        signal.signal(signal.SIGINT, forward_signal)
        returncode = child.wait()
    raise SystemExit(returncode)


if __name__ == "__main__":
    main()
