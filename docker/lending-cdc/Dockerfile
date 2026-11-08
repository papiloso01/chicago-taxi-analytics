FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV SPARK_LOCAL_IP=127.0.0.1 \
    ICEBERG_RUNTIME_JAR=/opt/iceberg/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[spark,cdc,test]'
COPY scripts/fetch_iceberg_runtime.py /tmp/fetch_iceberg_runtime.py
RUN python /tmp/fetch_iceberg_runtime.py
COPY pipelines ./pipelines
COPY tests ./tests
CMD ["python", "-m", "pytest", "tests/iceberg", "-v"]
