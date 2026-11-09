CREATE TABLE IF NOT EXISTS lending_cdc_athena.loan_transactions_state (
    source_system STRING,
    transaction_id BIGINT,
    loan_id BIGINT,
    transaction_type STRING,
    amount DECIMAL(12,2),
    event_sequence BIGINT,
    is_deleted BOOLEAN
)
LOCATION 's3://cdc-lakehouse-curated-ul16/warehouse/athena/loan_transactions_state/'
TBLPROPERTIES ('table_type' = 'ICEBERG');
