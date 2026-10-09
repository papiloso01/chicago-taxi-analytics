# Decision 001: Athena reporting platform

Chosen by the project owner: S3 + Glue catalog + Athena, with PySpark transformations and Tableau.
First implementation uses immutable Parquet snapshots and date partition projection rather than
Iceberg mutation/merge semantics. This limits dependencies and supports replay at source-key grain,
but requires full rebuilds and has no transaction across all Glue tables. Revisit Iceberg and
managed Spark after a measured full-year load, rather than claiming scale from synthetic CI.

PostgreSQL compatibility is retained to avoid breaking current local commands while migration
is reviewed. New table contracts and canonical names govern the Athena path.
