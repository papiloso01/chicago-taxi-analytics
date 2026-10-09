# Run and query the lake

## Local preview (Windows CMD)

Install Java 17 and Python 3.11, then from the repository root:

```cmd
python -m pip install -e ".[spark,lake]"
set SPARK_LOCAL_IP=127.0.0.1
python pipelines/run_lake.py --sample
python pipelines/export_dashboard.py
explorer data\exports
```

No PostgreSQL or AWS credentials are required. Fixtures are synthetic, including three fictional
polygons. Do not use them for geographic or business conclusions.

The existing Docker/PostgreSQL commands still work. Native lake execution can also use the Docker
image built by `docker compose build pipeline`, with `--no-deps` and a volume for `/app/data/lake`.
Compose still requires `.env` because it retains the legacy PostgreSQL service configuration.

## AWS setup (one-time, manual)

1. Choose an AWS account and region; the examples use eu-west-2.
2. Create/reuse the GitHub IAM OIDC provider (`token.actions.githubusercontent.com`, audience `sts.amazonaws.com`).
3. Review `infrastructure/aws/athena.yml`. Deploy manually using CloudFormation with CAPABILITY_IAM,
   supplying the existing provider ARN. It creates private encrypted buckets, three Glue databases,
   an Athena workgroup with a 1 GiB per-query scan cutoff, and a main-branch GitHub publisher role.
4. Record stack outputs. Set repository variables `LAKE_BUCKET`, `AWS_ROLE_ARN`, `AWS_REGION`,
   `GLUE_DATABASE_PREFIX=chicago_taxi`, `WAREHOUSE_TARGET=athena`.
5. For local publishing, use an AWS profile/role with equivalent permissions. Do not commit access keys.
6. If Lake Formation governs your catalog, grant the role appropriate database/table/data-location access.
7. Keep `ENABLE_DAILY_PIPELINE` disabled until the initial backfill and query validation pass.

Local CMD example (authenticated AWS profile):

```cmd
set AWS_PROFILE=your-profile
set AWS_REGION=eu-west-2
set LAKE_BUCKET=your-stack-lake-bucket
set GLUE_DATABASE_PREFIX=chicago_taxi
python pipelines/run_lake.py --year 2025 --publish
```

This invokes real APIs and incurs AWS storage/request costs. Verify capacity before a full-year run.
Explicit dates also work: `--start 2025-01-01 --end 2025-02-01 --publish`.
For daily runs, omit dates: taxi requests Chicago yesterday; weather requests the independent
14-day overlap ending five days before today. Weather overrides are `--weather-start` and
`--weather-end`. Explicit taxi backfill dates default to the same weather window.
Do not combine synthetic and live data in the same root/bucket prefix; use a separate development lake.

## Scheduling

After validation, set `ENABLE_DAILY_PIPELINE=true`. `.github/workflows/daily-athena.yml` runs at
06:30 UTC, assumes the OIDC role and publishes a validated snapshot. Manual runs accept a completed
year or explicit dates. The PostgreSQL schedule only runs when `WAREHOUSE_TARGET=postgres`.
Secrets: optional `SOCRATA_APP_TOKEN`; AWS uses OIDC rather than stored keys.

## Athena verification

Select the stack workgroup and `chicago_taxi_gold` database. For example:

```sql
SELECT trip_date, trips, revenue_usd
FROM chicago_taxi_gold.mart_taxi_demand_daily
WHERE trip_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
ORDER BY trip_date;
```

Inspect the successful S3 audit and `control/current.json` before refreshing BI. Check source counts,
quarantine, coverage and revenue reconciliation. Required CI uses fixtures and mocked AWS; it
cannot prove a real Athena query or your account's IAM/region/connector setup.

## Tableau

Use the Amazon Athena connector with the stack region, workgroup, query-results location and
appropriate read/query permissions. A BI reader is separate from the write-only pipeline role.
Use `chicago_taxi_gold.mart_taxi_trip_enriched` for the first dashboard. Athena names are documented
in `docs/data_dictionary.md`; update calculations for explicit `_usd`, `_km`, `_seconds` fields.
Alternatively export local CSVs; `pipelines/export_dashboard.py` retains the three existing filenames
and aliases trip-level fields for the existing workbook. CSVs are snapshots, not scheduled live connections.
