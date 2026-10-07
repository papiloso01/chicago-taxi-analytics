# Reporting review

API → bronze PostgreSQL JSONB → PySpark → silver warehouse → gold marts → Tableau or Power BI.

## Reporting questions

| Question | Metric / source | Availability |
|---|---|---|
| Daily trip counts and totals | gold.mart_daily | Implemented |
| Busiest hours | gold.mart_hourly | Implemented |
| Payment mix | gold.mart_payment | Implemented |
| Top earning drivers | Sum reported trip totals by taxi ID for selected dates | Proxy only: source identifies medallions, not drivers; use label “Top taxis by reported trip total” |
| Longest route in km | Maximum reported distance in selected dates, with all tied trip IDs | gold.mart_trip_distances; distance, not reconstructed road geometry |
| Shortest route in km | Minimum positive reported distance in selected dates, with all ties | gold.mart_trip_distances; zero distances remain stored but excluded from shortest-positive metric |
| Which gender takes more trips? | Passenger gender, trip-level | Unsupported: source has no passenger gender; requires a legitimately available enrichment dataset and reliable trip join key |

Distances use 1 mile = 1.609344 km. Totals are gross reported trip amounts, not driver take-home income. A taxi can have multiple drivers. Do not infer gender from identifiers or manufacture demographic labels.

## Source contract

Chicago Taxi Trips (2024 onward), dataset ajtu-isnz. Source metadata: https://data.cityofchicago.org/api/views/ajtu-isnz/columns.json . Taxi ID is stable per medallion. Pickup/dropoff community areas are coarse geography; they are not the exact travelled road route.

## Layer boundaries

Bronze: original JSON payload and ingest metadata; replay-safe trip IDs and pipeline run audit.
Silver: fct_trips (one row per valid trip), dim_taxi, dim_payment, dim_company, dim_area (role-played for pickup/dropoff), dim_date, quarantine_trips with reasons.
Gold: daily/hourly/payment summaries, taxi totals by date, trip distances. Date and dimension joins are explicit in PySpark. Hash keys are deterministic across rebuilds. Unknown members are retained; missing required measurements are quarantined. Missing tips remain null, negative tips rejected.

## Ingestion windows

First load: one completed calendar year selected with `taxi-pipeline --year 2025`. Year boundaries are exclusive at the end. No large API backfill is launched by this PR.
Daily: Chicago-local yesterday 00:00 through today 00:00, default when no dates are supplied. Dates can be explicitly overridden for source repairs. Source can arrive late, so T−1 does not guarantee completeness; operationally use deliberate catch-up/backfill jobs. The initial calendar year and later daily runs can leave a gap; explicitly backfill from that year's end through the first scheduled day if continuity is required.

## BI connection

Connect Tableau or Power BI to PostgreSQL and select the gold schema. In Power BI, use the PostgreSQL connector in Import mode initially; in Tableau use an extract initially. Apply date filters before ranking taxis or resolving distance extremes. Use a read-only database login; grants must be configured on the actual deployment. Refresh after the warehouse job succeeds. This PR prepares datasets, not a .pbix/.twb artifact or hosted BI service.

## Limits

PySpark reads full bronze history for each warehouse refresh; only API ingestion is incremental. All model publication occurs in one database transaction, but separate BI queries can straddle a publication unless the BI refresh uses a coherent snapshot. Tables keep stable names; schema changes need migrations. JDBC credentials must be configured separately for Spark and psycopg. There is no Airflow/NiFi dependency. Existing dbt models and Streamlit remain the legacy demo; the active warehouse path is PySpark/silver/gold.
