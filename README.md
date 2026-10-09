# Chicago Taxi Analytics

API → bronze → PySpark silver warehouse → gold reporting marts → S3 / Glue / Athena → Tableau.

<img width="1200" height="850" alt="Chicago Taxi Analytics - Synthetic Sample Data" src="https://github.com/user-attachments/assets/19e7f4aa-863b-477e-9244-a11f19f1193f" />

The screenshot is a synthetic Tableau development preview, not verified live business reporting.

## Start locally

Requires Python 3.11 and Java 17. From the repo root (Windows CMD):

```cmd
python -m pip install -e ".[spark,lake]"
set SPARK_LOCAL_IP=127.0.0.1
python pipelines/run_lake.py --sample
python pipelines/export_dashboard.py
explorer data\exports
```

This runs without PostgreSQL or AWS. Source fixtures, including map polygons, are fictional.
The existing PostgreSQL Docker commands remain available during migration:
copy `.env.example` to `.env`, `docker compose build pipeline`, `docker compose up -d postgres`,
then `make sample` / `make enrich-sample` where Make is available. Existing script names are compatibility entry points.

## Repository structure

| Directory | Responsibility |
|---|---|
| `src/chicago_taxi/ingestion/` | API pagination, retries, source/date windows and batch extraction |
| `src/chicago_taxi/transformations/bronze/` | Replay-safe latest-record consolidation |
| `src/chicago_taxi/transformations/silver/` | Cleaning, quarantine and warehouse facts/dimensions |
| `src/chicago_taxi/transformations/gold/` | Reporting grains and geography/weather joins |
| `src/chicago_taxi/schemas/` | Explicit source types and contract-backed output schemas |
| `src/chicago_taxi/quality/` | Schema, grain, nullability, lineage and reconciliation checks |
| `src/chicago_taxi/publishing/` | Native lake and legacy PostgreSQL adapters |
| `pipelines/` | Run and export entry points |
| `metadata/` | Source, table, lineage and metric contracts |
| `tests/` | Functional, integration, ETL validation and E2E suites |
| `infrastructure/` | Opt-in AWS template and local development notes |
| `dashboards/tableau/` | Tableau asset documentation |
| `tools/review/` | Advisory PR reviewer |

## Data and reporting

- **Bronze:** immutable source batches, consolidated raw trip/community/weather tables and run audits.
- **Silver:** `fact_taxi_trip`, taxi/payment/company/area/date dimensions, community polygons,
  hourly weather and rejected trips.
- **Gold:** daily/hourly demand, taxi revenue, trip distances, payment demand, enriched trips,
  community-area demand and weather demand.

See the generated [data dictionary](docs/data_dictionary.md) and [metric contracts](metadata/metrics/reporting.yml).
Athena columns explicitly name USD, km and duration units. PostgreSQL names remain compatible;
the migration map is `src/chicago_taxi/publishing/naming.py`.

Initial ingestion supports a completed year; daily taxi ingestion defaults to Chicago T−1.
Historical weather uses a separate 14-day overlap ending five days before Chicago today.
End dates are exclusive. Silver/gold are full rebuilds, not incremental transformations.

## AWS and Tableau

Review [architecture](docs/architecture/athena.md) and follow [deployment/run instructions](docs/operations/athena.md).
The AWS template creates storage, catalog, workgroup and OIDC role only when manually deployed.
No AWS infrastructure, paid model service or live dashboard is enabled by cloning this repository.

For Tableau, connect to the Athena gold database or use exported CSVs. Local exports retain
existing filenames and field aliases for the current workbook. Modelled weather is city-level;
missing measurements stay unavailable, and repeated DST hours are flagged.

## Testing and PR review

CircleCI has separate functional, integration, ETL validation, E2E and PostgreSQL compatibility jobs.
GitHub Actions mirrors the suites. Both retain test results.

```bash
python -m pip install -e '.[spark,lake,test]'
python -m ruff check src pipelines tools tests
python tools/validate_metadata.py
python -m pytest tests/functional tests/integration tests/etl_validation tests/e2e
```

[Testing and AI setup](docs/operations/testing.md) explains required checks, mocked AWS coverage,
optional API-based reviews and Copilot instructions. AI reviews are advisory; secrets/model/settings
must be configured separately. Real AWS queries, full-year performance and Tableau connector
validation remain deployment acceptance checks.

## Source limits

Taxi IDs identify vehicles, not drivers. No passenger gender exists in the source.
Reported trip charge is not driver profit. Miles × 1.609344 gives reported km, not road-route geometry.
Shortest-trip reporting excludes zero distance while retaining those records.
Area demand separates Pickup/Dropoff; summing both doubles activity counts. Never sum weather
measurements copied onto every trip. Correlations do not establish causation.

Sources: [Chicago taxi trips](https://data.cityofchicago.org/Transportation/Taxi-Trips-2024-/ajtu-isnz),
[community boundaries](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6),
[Open-Meteo historical weather](https://open-meteo.com/en/docs/historical-weather-api).
Code is MIT licensed; source data retains its provider terms.
