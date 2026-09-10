"""Launch Spark SQL or submit with the shared catalog configuration."""
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
    if len(sys.argv) < 2 or sys.argv[1] not in ("sql", "submit"):
        raise SystemExit("Usage: run.py {sql|submit} [Spark arguments]")
    executable = {"sql": "spark-sql", "submit": "spark-submit"}[sys.argv[1]]
    config = Path(__file__).with_name("conf").joinpath("spark-defaults.conf").read_text()
    for suffix, variable in (("user", "ICEBERG_JDBC_USER"), ("password", "ICEBERG_JDBC_PASSWORD")):
        config += f"\nspark.sql.catalog.lakehouse.jdbc.{suffix}={property_value(os.environ[variable])}\n"
    # Credentials only exist in a private temporary file for this process.
    with tempfile.NamedTemporaryFile(mode="w", suffix=".conf") as properties:
        properties.write(config)
        properties.flush()
        child = subprocess.Popen([
            f"/opt/spark/bin/{executable}", "--properties-file", properties.name,
            *sys.argv[2:],
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
