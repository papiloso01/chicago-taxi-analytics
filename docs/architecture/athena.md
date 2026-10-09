# Athena architecture

Sources → immutable raw JSON batches → consolidated bronze Parquet → PySpark silver fact/dimensions and quarantine → gold reporting marts → S3 / Glue catalog → Athena / Tableau.

## Responsibilities

`ingestion/` owns source pagination, retries and Chicago-local end-exclusive date windows.
`transformations/bronze/` owns latest-record consolidation. `transformations/silver/` owns
cleaning and star modelling. `transformations/gold/` owns reporting grains and enrichment.
`schemas/` defines input types; versioned `metadata/tables/` defines every published output type.
`quality/` validates lineage, schema, required fields, keys, joins and reconciliations.
`publishing/` owns orchestration and storage adapters. `pipelines/` provides thin entry points.

## Storage and publication

Each successful run produces a new immutable `snapshots/run_<id>/` containing bronze/silver/gold
Parquet, raw JSON batches, and a manifest. Trip-date datasets use Hive `trip_date=YYYY-MM-DD`
partitions and Snappy Parquet. Glue tables use Athena date partition projection, so no crawler or
`MSCK REPAIR TABLE` is required. Dimensions without dates are unpartitioned. The three catalog
databases are `chicago_taxi_bronze`, `chicago_taxi_silver` and `chicago_taxi_gold`.

Source payloads remain JSON text in bronze. Bronze latest records are consolidated by source key,
UTC ingestion time and batch sequence. All raw batches remain in their original snapshots.
A new daily run downloads the previous bronze snapshot, merges the new source records and
rebuilds silver/gold. Corrected keys replace the prior value; unchanged reruns preserve counts
and amounts. Source deletions are not detected. Taxi T−1 does not guarantee source completeness:
monitor the source lag and use explicit overlapping backfills for late arrivals.

An exclusive S3 conditional-create lock prevents concurrent publishers. A local file lock prevents
concurrent local writers. Locks are not leases: a hard-killed process may leave a stale lock, which
must be investigated before manually removing it. Never force-unlock an active run.

All quality checks and Parquet writes finish before Glue table pointers change. Glue updates are
atomic **per table**, not across the whole warehouse. The completion manifest advances last.
Failures attempt to restore previous Glue locations. There can be mixed table versions during
publication and a rollback can fail. Do not refresh multi-table BI extracts while publishing;
use the successful run barrier and prefer the single enriched trip mart. Failure audits identify
the run for inspection; retain the old snapshots for recovery.

## Boundaries and limits

- No AWS resources are deployed by this PR. The CloudFormation template is opt-in.
- Bronze is incremental by key; silver/gold are full rebuilds. Historical snapshots consume storage.
- Current execution uses local Spark and local disk before S3 upload. A full year has not been
  benchmarked. Use a suitably sized runner or adapt the storage adapter for Glue/EMR before
  claiming production-scale operation. The scheduled GitHub runner is a small-project starter.
- Snapshot cleanup and retention require an explicit policy; do not delete snapshots referenced
  by Glue/current manifests. S3 bucket resources have Retain policies.
- Chicago taxi timestamps are wall-clock text in Athena; UTC offsets must not be invented.
- Unknown geography/weather stays visible; repeated DST weather hours are ambiguous, not Dry.
- Geography is a current boundary snapshot, not an effective-dated historical geography model.
- `taxi_pipeline` and existing scripts remain compatibility imports for PostgreSQL.
  PostgreSQL/dbt/Streamlit are legacy local paths, not the Athena target architecture.

## Sources for implementation

- https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
- https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html
