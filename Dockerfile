FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir '.[dashboard,transform]'
COPY . .
ENV DBT_PROFILES_DIR=/app/dbt
CMD ["taxi-pipeline"]
