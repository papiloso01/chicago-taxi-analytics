CREATE TABLE IF NOT EXISTS lending_cdc_athena.loans_state (
    source_system STRING,
    loan_id BIGINT,
    principal_amount DECIMAL(12,2),
    currency STRING,
    status STRING,
    event_sequence BIGINT,
    is_deleted BOOLEAN
)
LOCATION 's3://cdc-lakehouse-curated-ul16/warehouse/athena/loans_state/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
