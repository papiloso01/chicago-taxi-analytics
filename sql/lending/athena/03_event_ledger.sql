CREATE TABLE IF NOT EXISTS lending_cdc_athena.event_ledger (
    source_system STRING,
    event_id STRING,
    event_sequence BIGINT,
    source_table STRING,
    operation STRING,
    event_json STRING
)
LOCATION 's3://cdc-lakehouse-curated-ul16/warehouse/athena/event_ledger/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
