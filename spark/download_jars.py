"""Install version-pinned Java dependencies once at image build time."""
import hashlib
from pathlib import Path
from urllib.request import urlopen

ARTIFACTS = (
    ("org/apache/iceberg", "iceberg-spark-runtime-3.5_2.12", "1.11.0"),
    ("org/apache/iceberg", "iceberg-aws-bundle", "1.11.0"),
    ("org/postgresql", "postgresql", "42.7.7"),
    # Compile dependencies from Spark 3.5.6's Kafka connector POM.
    # Spark already provides slf4j, lz4, snappy, jsr305 and spark-tags.
    ("org/apache/spark", "spark-sql-kafka-0-10_2.12", "3.5.6"),
    ("org/apache/spark", "spark-token-provider-kafka-0-10_2.12", "3.5.6"),
    ("org/apache/kafka", "kafka-clients", "3.4.1"),
    ("org/apache/commons", "commons-pool2", "2.11.1"),
)

for group, artifact, version in ARTIFACTS:
    filename = f"{artifact}-{version}.jar"
    url = f"https://repo.maven.apache.org/maven2/{group}/{artifact}/{version}/{filename}"
    with urlopen(url, timeout=120) as response:
        data = response.read()
    with urlopen(url + ".sha1", timeout=30) as response:
        checksum = response.read().decode().strip().split()[0]
    if hashlib.sha1(data).hexdigest() != checksum:
        raise RuntimeError(f"Checksum mismatch: {filename}")
    Path("/opt/spark/jars", filename).write_bytes(data)
    print(f"Installed {filename}", flush=True)
