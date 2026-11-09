MERGE INTO lending_cdc_athena.loan_transactions_state AS t
USING (
    SELECT source_system, transaction_id, loan_id, transaction_type, amount, event_sequence, is_deleted
    FROM (
        SELECT source_system,
            CAST(json_extract_scalar(event_json, '$.record_key.transaction_id') AS BIGINT) AS transaction_id,
            CAST(json_extract_scalar(event_json, '$.record_data.loan_id') AS BIGINT) AS loan_id,
            json_extract_scalar(event_json, '$.record_data.transaction_type') AS transaction_type,
            CAST(json_extract_scalar(event_json, '$.record_data.amount') AS DECIMAL(12,2)) AS amount,
            event_sequence, operation = 'D' AS is_deleted,
            ROW_NUMBER() OVER (
                PARTITION BY source_system, CAST(json_extract_scalar(event_json, '$.record_key.transaction_id') AS BIGINT)
                ORDER BY event_sequence DESC
            ) AS rn
        FROM lending_cdc_athena.event_ledger
        WHERE source_table = 'loan_transactions'
    ) AS ranked
    WHERE rn = 1
) AS s
ON t.source_system = s.source_system AND t.transaction_id = s.transaction_id
WHEN MATCHED AND s.event_sequence > t.event_sequence THEN
    UPDATE SET loan_id = s.loan_id, transaction_type = s.transaction_type,
               amount = s.amount, event_sequence = s.event_sequence, is_deleted = s.is_deleted
WHEN NOT MATCHED THEN
    INSERT (source_system, transaction_id, loan_id, transaction_type, amount, event_sequence, is_deleted)
    VALUES (s.source_system, s.transaction_id, s.loan_id, s.transaction_type, s.amount, s.event_sequence, s.is_deleted);
