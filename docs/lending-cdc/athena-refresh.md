# Refresh the lending Iceberg tables with Athena

The initial eight-event batch was verified in Athena SQL in eu-north-1:
eight ledger events after replay, two current loans, three current transactions,
one transaction tombstone, and a portfolio view. This path uses Athena as the
compute engine and the Glue Data Catalog for table metadata. It does not require
a managed Glue ETL job. The PySpark pipeline remains available independently.

## Run on Windows

From the repository root in PowerShell, using the same AWS identity that ran
the Athena SQL and the Athena engine v3 workgroup:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[cdc]"
aws login --profile cdc-dev --region eu-north-1
.\.venv\Scripts\python.exe pipelines/lending/refresh_athena.py
```

The default run reads raw events and the ledger, validates them and prints
expected counts without merging tables. Athena still writes query results to
the workgroup's results location (by default the command requests
`s3://cdc-lakehouse-curated-ul16/athena-results/`). Use `--workgroup` and
`--results` if your workgroup configuration differs. The database and table
locations intentionally match the existing lab in `sql/lending/athena/`.

To merge validated data and reconcile results:

```powershell
.\.venv\Scripts\python.exe pipelines/lending/refresh_athena.py --apply
```

Only on a fresh setup, append `--bootstrap` to create missing databases and
tables before validation. It leaves existing tables in place.

On the verified initial dataset, expect `new_events: 0`, `ledger_events: 8`,
`current_loans: 2`, `current_transactions: 3`, `transaction_tombstones: 1`,
and `orphan_transactions: 0`. The result also checks portfolio counts.
The user verified the individual SQL operations; the combined Python command
still needs an AWS run. Local tests execute the merge SQL in DuckDB and exercise
Athena result pagination and error handling with a fake client.

## Fresh batch acceptance

After an unchanged replay succeeds, pick a **synthetic** transaction and make one
committed PostgreSQL update. For the original seed, transaction 5003 is a
100.00 repayment. In DBeaver, run:

```sql
UPDATE lending.loan_transactions
SET amount = 101.00, updated_at = CURRENT_TIMESTAMP
WHERE transaction_id = 5003 AND transaction_type = 'repayment' AND amount = 100.00
RETURNING transaction_id, amount;
```

Confirm the returned row and commit if auto-commit is off. If no row was
returned, check the source before proceeding. Export the new event with the
existing outbox exporter and confirm one event was uploaded and acknowledged:

```powershell
.\.venv\Scripts\python.exe -m lending_cdc.exporter --config config/lending-cdc.local.json
.\.venv\Scripts\python.exe pipelines/lending/refresh_athena.py
.\.venv\Scripts\python.exe pipelines/lending/refresh_athena.py --apply
```

With no other source changes, expect one new event and nine total ledger events,
with the same current row counts. Compare transaction 5003 and the repayment
total for its loan against PostgreSQL. Apply once more: zero new events and the
same nine ledger rows. Transaction 5999 should remain excluded from reporting.

## How validation and replay work

- The preflight reads the raw table and existing ledger, validates both using the
  same PostgreSQL envelope contract as the PySpark stage, and stops on malformed
  events, incompatible ledger metadata, orphan transactions, or conflicts. Bad
  events remain in raw S3 for investigation; this path does not write quarantine.
- Byte-identical deliveries collapse by source and event ID. Changed content
  under the same ID or reuse of a source sequence by another event stops the
  run. The PostgreSQL source allocates unique sequence values; these are not a
  global arrival checkpoint. Late records are still eligible.
- The ledger MERGE uses only the exact validated event rows. Both state merges
  choose the highest sequence per record and retain delete markers. A run with
  zero new events still merges state, allowing recovery after a partial run.
- The view excludes tombstones and groups repayment and disbursement totals by
  loan. It preserves each loan's currency and performs no FX conversion.
- Use one writer and wait for each run to finish before starting the next.
  The individual tables commit separately. If a query fails after the ledger
  commits, retry after the failed query is terminal and its cause is resolved.
- This lab scans up to 1,000 raw and ledger rows each, caps combined history at
  1,000 unique events and refuses a merge statement over 240 KiB. It does not
  schedule queries or try to load arbitrarily large batches. Raw payloads are
  embedded in the merge query, so use synthetic data: queries and query results
  may contain those payloads. For larger or sensitive workloads, use restricted
  staging storage and a controlled writer instead.
