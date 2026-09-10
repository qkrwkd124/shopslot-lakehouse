FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu
USER root
COPY download_jars.py /tmp/download_jars.py
RUN python3 /tmp/download_jars.py
RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints
USER spark
