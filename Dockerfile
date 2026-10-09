FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*
RUN mkdir /opt/jdbc && curl -fsSL https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar -o /opt/jdbc/postgresql.jar
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/jdbc/postgresql.jar pyspark-shell"
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[dashboard,transform,spark,lake]'
COPY . .
ENV DBT_PROFILES_DIR=/app/dbt
CMD ["taxi-pipeline"]
