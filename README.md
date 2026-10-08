# Chicago Taxi Analytics Data Warehouse: bronze → silver → gold (reporting layer)
<img width="1469" height="856" alt="for git2" src="https://github.com/user-attachments/assets/abc76088-9876-4fc9-afb2-307c604cb4df" />

**Review revision:** Python API ingestion → PostgreSQL bronze → PySpark silver warehouse → gold marts → Tableau/Power BI.

This branch replaces the active dbt reporting path with explicit PySpark source schemas, dimension joins and reporting models. Existing dbt files are retained as the earlier demo.

See [reporting definitions and design](docs/reporting-design.md) before interpreting driver, route or demographic metrics.

## Quick start

Docker Desktop with Compose is required. Copy `.env.example` to `.env` and set a local URL-safe password. From the project root:

```bash
docker compose up -d postgres
docker compose build
make sample
docker compose up -d dashboard
```

`make sample` loads synthetic data then runs PySpark to publish silver/gold. Streamlit remains an optional preview at http://localhost:8501. The intended final BI consumer is Tableau or Power BI using the PostgreSQL gold schema. The BI workbook itself is a subsequent step.

## Initial one-year API load

Choose a completed calendar year (example 2025), then transform:

```bash
docker compose run --rm pipeline taxi-pipeline --year 2025
docker compose run --rm pipeline python scripts/run_warehouse.py
```

Daily operation defaults to Chicago-local yesterday; `.env.example` intentionally has no date overrides:

```bash
make run
```

An explicit start/end can repair late-arriving data. End dates are exclusive. A calendar-year backfill followed by daily ingestion needs a separate catch-up window if there is a gap. No year-long API job has been executed as part of this change.

## Layers

| Layer | Tables / purpose |
|---|---|
| Bronze | trips JSONB and pipeline_runs ingestion audit |
| Silver | fct_trips; dim_taxi, dim_payment, dim_company, dim_area, dim_date; quarantine_trips |
| Gold | mart_daily, mart_hourly, mart_payment, mart_taxi_earnings, mart_trip_distances |

Spark stages all warehouse outputs, validates source reconciliation and join keys, then refreshes live silver/gold tables in a single PostgreSQL transaction. Model schemas are defined with PySpark StructType and numeric casts. Hash dimension keys stay stable. Publication is a full rebuild; incremental bronze ingestion does not imply incremental silver/gold processing. Production schema changes require migrations.

## Tests

Install Java 17 and `python -m pip install '.[spark]'`, then:

```bash
python -m unittest discover -s tests -v
```

CircleCI and GitHub Actions provision PostgreSQL 16 and a JDBC driver, then exercise:

- API keyset pagination, empty results, invalid dates/identity and stalled cursors.
- Retryable, permanent and exhausted HTTP failure paths.
- T−1 date boundaries, leap days and completed-year selection.
- PySpark invalid timestamps, malformed/negative values, unknown dimensions and kilometre conversion.
- Real bronze ingestion replay and failed-run auditing.
- Silver trip grain, quarantine reconciliation, dimension uniqueness and fact join integrity.
- Gold count/amount reconciliation across daily, hourly, payment and taxi marts.
- Repeated warehouse execution with consistent outputs.

These cover the implemented paths, not every infrastructure failure: forced mid-publication failure, network interruptions during JDBC writes and year-scale performance are not yet covered. Do not call these large-scale benchmarks.

## Scheduled deployment

`.github/workflows/daily.yml` executes at 06:30 UTC when `ENABLE_DAILY_PIPELINE=true`. Configure GitHub secrets DATABASE_URL, JDBC_URL, DB_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB and optionally SOCRATA_APP_TOKEN. JDBC_URL must refer to the same database as DATABASE_URL and use the deployment's required SSL settings. The server must be reachable from the runner. T−1 requests may return no data because publishing can lag; use deliberate repair windows.

CircleCI runs the sample test pipeline after the repository is connected to a CircleCI project. Test credentials are for an ephemeral CI database; never commit production secrets.

## Sources and limits

[Chicago Taxi Trips](https://catalog.data.gov/dataset/taxi-trips-2024) has medallion-level taxi IDs, not driver identities or passenger gender. Rank taxis by reported gross totals, not driver net income. Reported trip miles are converted to kilometres; this does not reconstruct road geometry. Shortest-trip reporting excludes zero distance while preserving those rows for inspection. Source data retains provider terms; code is MIT licensed.

## Weather and neighbourhood enrichment

See [Chicago Moves enrichment](docs/enrichment.md) for additional sources, safe community/weather joins, map exports and the independent weather catch-up window. Run `make enrich-sample` after the sample warehouse to preview synthetic enrichment data.
