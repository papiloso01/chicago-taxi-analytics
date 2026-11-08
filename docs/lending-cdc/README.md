# Lending CDC lab: transactional outbox → S3

This is a second, synthetic workload in the existing AWS taxi repository. It does not reproduce any company's internal systems. The source is RDS PostgreSQL 18 in `eu-north-1`; DMS was unavailable on the user's Free plan. This implementation is **trigger-based change capture**, not DMS or log-based CDC. The existing taxi pipeline is separate.

## Current scope

Completed in the guided account setup: two lending source tables, a transactional outbox, row triggers, initial snapshot and manual insert/update/delete/rollback checks. This PR versions those SQL scripts and implements a one-shot local Python exporter. It does not deploy AWS resources, run a schedule or implement Glue/Iceberg/Athena yet. Opening the Glue console has been confirmed; job execution has not.

The raw bucket is `cdc-lakehouse-raw-ul16`. DMS's unused service role cannot be assumed by the local exporter; the exporter uses the console session in AWS profile `cdc-dev`. A scoped IAM policy example is provided below for an appropriately authorised local identity. GitHub OIDC was unavailable on the Free plan and is not required for this manual workflow.

## SQL scripts

Run as the schema owner in your SQL client, against database `transactions`:

| Script | Purpose |
| --- | --- |
| `sql/lending/001_source.sql` | Synthetic source schema and 2 loans / 3 transactions |
| `sql/lending/002_outbox.sql` | Outbox table and I/U/D triggers; stable primary keys required |
| `sql/lending/003_snapshot.sql` | Lock-protected initial snapshot with an idempotency marker |
| `sql/lending/004_exporter_permissions.sql` | Dedicated exporter login with read access and UPDATE only on exported_at |

The guided setup has already applied the equivalent first three scripts. Do not reset the outbox or re-seed changed source data. The initial snapshot marker name must remain stable; changing it deliberately creates another baseline. SQL scripts contain no passwords. Triggers capture row INSERT/UPDATE/DELETE only, not TRUNCATE or DDL; do not use those operations in this lab.

Run script 004, then set a private password in DBeaver:

```sql
ALTER ROLE cdc_exporter WITH PASSWORD 'REPLACE_PRIVATELY_BEFORE_RUNNING';
```

Never commit the edited statement or credentials. The former `dms_reader` account and DMS IAM role are not used by this path; record them for later cleanup.

## Windows setup (PowerShell)

From the checked-out repository, after reviewing/merging the PR:

```powershell
git switch main
git pull origin main
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[cdc]'
Copy-Item config/lending-cdc.example.json config/lending-cdc.local.json
```

Edit the local JSON: set `pg_host` to your actual RDS hostname and `sslrootcert` to your permanent AWS RDS CA bundle path. Forward slashes are accepted in Windows JSON paths. The local config is ignored by Git. No password belongs in that file.

Authenticate and check the batch without changing anything:

```powershell
aws login --profile cdc-dev
.\.venv\Scripts\python.exe -m lending_cdc.exporter --config config/lending-cdc.local.json --dry-run
```

The password prompt is for **cdc_exporter**, not postgres. The connection verifies the RDS hostname and CA (`sslmode=verify-full`). Current Boto3 with CRT is installed by the cdc extra to support the console-login profile. Dry-run reads a bounded set of pending events and does not contact S3 or acknowledge events.

After the guided I/U/D and rollback checks, expect `selected: 8` (5 snapshot events plus 3 committed change events). That count assumes the checks were run once and no other source changes occurred. Then export:

```powershell
.\.venv\Scripts\python.exe -m lending_cdc.exporter --config config/lending-cdc.local.json
```

Expect uploaded=8, acknowledged=8 on the first successful run. A second run selects zero events. Defaults process at most one batch of 100; increase max_batches/batch_size within their validated bounds for larger runs. A success result reports only the bounded work performed, not that the entire table is drained. Config/source identity and prefix must stay stable across retries.

## Delivery and replay guarantees

- Select `exported_at IS NULL` with `FOR UPDATE SKIP LOCKED`. Row locks prevent workers from acknowledging the same event concurrently; locked rows are revisited later.
- Write one object per event under `postgresql/outbox/source=<source>/schema=<schema>/table=<table>/event_id=<uuid>.json`.
- Use S3 `IfNoneMatch=*` to avoid overwrites. On an existing key, compare the actual bytes before accepting a retry. Mismatch stops the batch.
- Acknowledge only after upload. If any upload/ack/commit fails, the batch transaction rolls back its acknowledgements. Already-uploaded objects are retained and verified/reused on retry.
- PostgreSQL constructs JSON text, preserving NUMERIC values without Python float conversion. UUIDs and sequence numbers have stable textual representations; captured_at is UTC.
- Delivery is **at least once**, with idempotent S3 object effects. PostgreSQL and S3 are not one distributed transaction; do not claim global exactly-once delivery.
- No maximum-sequence watermark is used. An earlier allocated sequence may commit after a later one and is still found by the pending-row query. Sequence/captured_at are not global commit time/order and there is no cross-table transaction envelope.
- Raw objects remain replayable independently of source acknowledgement. The later Iceberg stage must retain per-key sequence guards and delete tombstones so late/replayed older records cannot resurrect a deletion. Source namespaces must separate PostgreSQL and any future MySQL source.

One small object per event is intentional for the first lab stage; it is not a high-throughput production layout. Later work should address compaction, retention, source outbox growth, monitoring, audit runs and credentials for scheduled compute. Never purge pending events. Do not reload/reset a source database under the same source identity without a new epoch/versioning design.

## Scoped AWS permissions for the exporter identity

These differ from DMS's service-role permissions. Apply to the identity authorised for your local console session when needed:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::cdc-lakehouse-raw-ul16"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::cdc-lakehouse-raw-ul16/postgresql/outbox/*"
    }
  ]
}
```

The exporter does not list/delete objects or change bucket policy. Keep Block Public Access enabled. The AWS session must be allowed to read existing event objects as well as write them for retry verification.

## Tests

`tests/functional/test_outbox_exporter.py`: offline S3-emulated checks for conditional creation, byte-verified retry, mismatches, denied/conflicting writes, dry-run, region mismatch and upload/ack ordering.

`tests/integration/test_outbox_postgres.py`: actual PostgreSQL transactions with emulated S3. Checks snapshot replay, I/U/D, rollback, interrupted export recovery, late commits, locked rows, decimal precision, bounded batches and the restricted exporter's permissions. These tests refuse to reset any database not named `cdc_test`. They skip outside the dedicated test environment; separate GitHub Actions and CircleCI jobs supply an isolated PostgreSQL 18 service and execute them. Never point test fixtures at the RDS source database.

```text
python -m pip install -e ".[cdc,test]"
python -m pytest tests/functional/test_outbox_exporter.py
```

Unit tests do not prove cloud permissions or live RDS connectivity. Live acceptance is the documented dry-run, first export, raw object checks and empty replay in the user's account.

## Next stages

Glue validation/quarantine, per-key deduplication, Iceberg CDC merges including deletes and replay protection, Athena reconciliation and reporting/model-feature datasets. MySQL will be a separate source adapter after the PostgreSQL path works. IAM, networking, costs and any Free-plan restrictions remain explicit deployment prerequisites.

### Run the nine PostgreSQL tests on your Windows Docker host

A running desktop PostgreSQL container is not reachable from this assistant's cloud workspace. Run the tests on the desktop that owns Docker. Use a separate, disposable database called `cdc_test` in that container; never use the RDS database `transactions` or an existing application database. The fixture resets the lending/cdc schemas inside cdc_test.

After pulling this branch and installing `.[cdc,test]`, supply the local test connection privately in PowerShell (this avoids storing the password in command history):

```powershell
$testConnection = Read-Host 'Local cdc_test connection string' -AsSecureString
$env:CDC_TEST_DSN = [System.Net.NetworkCredential]::new('', $testConnection).Password
try {
    .\.venv\Scripts\python.exe scripts/wait_cdc_database.py
    if ($LASTEXITCODE -ne 0) { throw 'Test database connection failed' }
    .\.venv\Scripts\python.exe -m pytest tests/integration/test_outbox_postgres.py -v
} finally {
    Remove-Item Env:CDC_TEST_DSN -ErrorAction SilentlyContinue
}
```

The private connection string must reference your local published Docker port and `cdc_test`, for example `host=localhost port=5432 dbname=cdc_test user=YOUR_LOCAL_USER password=YOUR_LOCAL_PASSWORD`. Do not paste the actual connection string into chat. PostgreSQL is real in these tests; S3 remains emulated with Moto. Expect nine passes, with no skips when CDC_TEST_DSN is set.

## PySpark and Iceberg stage

See [iceberg.md](iceberg.md) for local Docker execution, versioned contracts, real Iceberg
CDC merge tests, failure/replay semantics, reporting joins and the future Glue deployment.
Glue job creation is currently blocked by account-level AccessDenied; local execution is
separate from managed cloud execution.
