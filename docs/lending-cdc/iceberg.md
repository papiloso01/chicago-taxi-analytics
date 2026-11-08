# PostgreSQL outbox → PySpark → Iceberg

AWS Glue job creation returned account-level AccessDenied in the guided Free plan account.
The local path is independent of that service restriction. No managed Glue run or Athena
publication is claimed. RDS/S3 export is the previously verified live stage.

## Files and responsibility

| File | Responsibility |
| --- | --- |
| `src/lending_cdc/contracts/postgres_v1.json` | Versioned source tables, keys, field types and domain rules |
| `src/lending_cdc/validation.py` | Strict JSON parsing with Decimal and timezone-aware timestamp validation |
| `src/lending_cdc/iceberg.py` | PySpark schemas, deduplication, windows, joins and actual Iceberg SQL MERGE |
| `pipelines/lending/postgres_outbox_to_iceberg.py` | Shared local/Glue entry point |
| `tests/fixtures/lending_cdc/` | Eight pretty-printed synthetic events matching the guided baseline |
| `tests/functional/test_lending_validation.py` | Fast contract checks without Java or AWS |
| `tests/iceberg/` | Real Iceberg v2 integration/reconciliation/replay/failure checks |
| `sql/lending/athena/curated_views.sql` | Future Athena views and reconciliation queries |

## Tables

All objects live in catalog `local`, database `lending_cdc` for the local demo.

| Table | Grain | Purpose |
| --- | --- | --- |
| `quarantine` | SHA256 of raw content plus input file path | Original invalid JSON, source file and rejection reason |
| `event_ledger` | source_system + event_id | Validated immutable event history; duplicate deliveries have no effect |
| `loans_state` | source_system + loan_id | Latest loan, including deletion tombstones |
| `loan_transactions_state` | source_system + transaction_id | Latest transaction, including deletion tombstones |
| `loan_portfolio` | source_system + loan_id | Current loans left joined to aggregated current transactions |

Source keys are BIGINT (signed, not assumed positive); event sequences must be positive.
Amounts are DECIMAL(12,2), never Python float. Required fields and supported domains are
validated, including complete delete before-images. Extra payload fields are quarantined:
introduce and test a new contract rather than silently changing schemas. Parsing uses a
Python UDF to strictly validate JSON once per input record; windows, joins, aggregation
and merges execute with Spark. This is batch processing, not a streaming query.

## Local Windows Docker run — no AWS login required

From PowerShell in `C:\Users\UL16\Projects\chicago-taxi-cdc`, after the PR is merged:

```powershell
git switch main
git pull --ff-only origin main
docker build -f docker/lending-cdc/Dockerfile -t lending-cdc-iceberg .
docker run --rm lending-cdc-iceberg
```

The image supplies Python 3.11, Java 17, Spark 3.5.6 and Iceberg 1.7.1 (matching the
Glue 5.0 bundled Iceberg version). Maven runtime downloads are checked against Maven's
SHA512 digest; no floating/latest JAR. The tests never contact RDS, S3, Glue or Athena.
They use actual local Iceberg tables, not mocks, and fail if the runtime is missing.

Run the eight synthetic files and retain the output in a Docker named volume:

```powershell
docker volume create lending-iceberg-demo
docker run --rm -v lending-iceberg-demo:/warehouse lending-cdc-iceberg python pipelines/lending/postgres_outbox_to_iceberg.py --input tests/fixtures/lending_cdc --warehouse /warehouse --jars /opt/iceberg/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
```

Expected report: input=8, quarantined=0, valid_unique=8, ledger_events=8,
current_loans=2, current_transactions=3, orphan_transactions=0. Repeat the command: ledger stays
at 8 events; current loans=2, current transactions=3; transaction 5999 stays deleted.
`valid_unique` is the distinct valid events in this input, not a count of newly inserted rows.
This distinction matters on replay. Named volume output is a Hadoop filesystem catalog:
it is not registered in AWS and must not be uploaded to S3 as a Glue catalog substitute.
Do not run two containers against the same volume concurrently.

To exercise copies of the real exported events locally, download only the raw event prefix:

```powershell
New-Item -ItemType Directory -Force data/lending/raw
aws s3 cp s3://cdc-lakehouse-raw-ul16/postgresql/outbox/ data/lending/raw/ --recursive --profile cdc-dev
docker run --rm -v "${PWD}/data/lending/raw:/raw:ro" -v lending-iceberg-demo:/warehouse lending-cdc-iceberg python pipelines/lending/postgres_outbox_to_iceberg.py --input /raw --warehouse /warehouse --jars /opt/iceberg/iceberg-spark-runtime-3.5_2.12-1.7.1.jar --database lending_live_copy
```

Use a separate database for live copies so synthetic events never mix with live events.
The download requires s3:ListBucket and s3:GetObject for that prefix on your local identity;
the exporter itself does not need list permissions. No credentials are mounted into Docker.

## Correctness and failure handling

- Scan all raw `*.json` files recursively; one complete envelope per file. No directory
  partition inference and no global high-water mark. A nonexistent input path fails visibly.
- Invalid JSON/contracts go to replay-safe quarantine, while valid events continue. Source
  path/content, not processing time, determines rejection identity. Zero valid events yields
  zero newly accepted events; review the report/quarantine rather than treating it as clean.
- Before state merges, compare incoming and retained ledger hashes. Reused source/event IDs
  with changed content, or different events at the same source/table/key/sequence, fail the run.
  Duplicate equivalent events are collapsed. Conflict runs may already have written quarantine,
  but do not merge the ledger/state/mart. Repair the raw source under a deliberate recovery plan.
- Persist the event ledger, then compute latest state per source/table/key over all history.
  Apply MERGE only when incoming sequence is strictly greater than stored sequence. A lower
  sequence on an unrelated key is still accepted; sequences are not global commit ordering.
- Store deletes as tombstones even if there is no existing row. Current DataFrames/views
  filter tombstones. Never delete tombstones/ledger without an explicit safe replay horizon.
- Each Iceberg table commit is atomic; the whole pipeline is NOT one transaction. If the
  process fails after the ledger commit, rerun the full raw input: state is rebuilt from
  retained history. Reports must be consumed only after a successful full run. Keep job
  concurrency at 1. No distributed multi-writer coordinator is implemented.
- Portfolio sums retain currency at loan grain. No exchange-rate conversion, lending risk
  score or interest/outstanding-balance formula is invented. Orphan reconciliation is a
  post-run check, because separate table commits can temporarily expose unmatched keys.
- Local data is unpartitioned deliberately for this tiny lab. Full ledger scans, Python
  validation UDFs and small raw files are not a high-throughput production solution. Future
  work: bounded manifests, compaction, incremental state planning, auditing/metrics and
  retention. Do not claim schema evolution, streaming, snapshot restore or cloud performance.

## Dedicated CI

Both CircleCI and GitHub Actions have an `iceberg_cdc` job using Java 17, Spark 3.5.6 and
Iceberg 1.7.1. It tests initial load, I/U/D, decimal precision, current joins, duplicate
files, full replay, stale events after delete, newer updates, quarantine replay, historical
conflicts, partial failure recovery and late events for another key. Missing Iceberg causes
failure, not skip. Existing suites and PostgreSQL 18 outbox service tests remain separate.

For a native environment with Python >=3.11 and Java 17:

```text
python -m pip install -e ".[spark,cdc,test]"
python scripts/fetch_iceberg_runtime.py --directory YOUR_JAR_DIRECTORY
```

Set `ICEBERG_RUNTIME_JAR` to the downloaded absolute filename and run
`python -m pytest tests/iceberg -v`. Do not point tests at a production catalog.

## Future Glue deployment — blocked, not yet verified

Use Glue 5.0/Python Spark, 2 G.1X workers, concurrency 1, retries 0 and timeout 10 minutes.
Use role `AWSGlueServiceRole-cdc-lakehouse-dev`. Build executor code with
`python scripts/package_lending_cdc.py` and upload the generated zip plus the pipeline script
to `s3://cdc-lakehouse-curated-ul16/scripts/`. The zip includes the JSON contract; a driver
script alone is insufficient for Spark workers.

Job parameters:

| Key | Value |
| --- | --- |
| `--datalake-formats` | `iceberg` |
| `--extra-py-files` | `s3://cdc-lakehouse-curated-ul16/scripts/lending_cdc.zip` |
| `--mode` | `glue` |
| `--catalog` | `glue_catalog` |
| `--database` | `lending_cdc` |
| `--source` | `lending-postgres-dev` |
| `--input` | `s3://cdc-lakehouse-raw-ul16/postgresql/outbox/` |
| `--warehouse` | `s3://cdc-lakehouse-curated-ul16/warehouse/` |

Session configuration is in `session()`, including Iceberg extensions, GlueCatalog, S3FileIO
and UTC. Glue Data Catalog/CloudWatch and scoped S3 permissions must be effective; Lake
Formation permissions may also be required if that service governs the locations. Leave
bookmarks disabled. When creation is permitted, verify a real managed run and then execute
the Athena SQL using engine v3 and your existing athena-results prefix. Access to the Glue
console, local tests, or creation of a role does not prove cloud job/Athena permissions.

Official references:
- https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html
- https://iceberg.apache.org/docs/1.7.1/spark-writes/
