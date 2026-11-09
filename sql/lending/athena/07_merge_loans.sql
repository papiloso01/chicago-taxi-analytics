MERGE INTO lending_cdc_athena.loans_state AS t
USING (
    SELECT source_system, loan_id, principal_amount, currency, status, event_sequence, is_deleted
    FROM (
        SELECT source_system,
            CAST(json_extract_scalar(event_json, '$.record_key.loan_id') AS BIGINT) AS loan_id,
            CAST(json_extract_scalar(event_json, '$.record_data.principal_amount') AS DECIMAL(12,2)) AS principal_amount,
            json_extract_scalar(event_json, '$.record_data.currency') AS currency,
            json_extract_scalar(event_json, '$.record_data.status') AS status,
            event_sequence, operation = 'D' AS is_deleted,
            ROW_NUMBER() OVER (
                PARTITION BY source_system, CAST(json_extract_scalar(event_json, '$.record_key.loan_id') AS BIGINT)
                ORDER BY event_sequence DESC
            ) AS rn
        FROM lending_cdc_athena.event_ledger
        WHERE source_table = 'loans'
    ) AS ranked
    WHERE rn = 1
) AS s
ON t.source_system = s.source_system AND t.loan_id = s.loan_id
WHEN MATCHED AND s.event_sequence > t.event_sequence THEN
    UPDATE SET principal_amount = s.principal_amount, currency = s.currency,
               status = s.status, event_sequence = s.event_sequence, is_deleted = s.is_deleted
WHEN NOT MATCHED THEN
    INSERT (source_system, loan_id, principal_amount, currency, status, event_sequence, is_deleted)
    VALUES (s.source_system, s.loan_id, s.principal_amount, s.currency, s.status, s.event_sequence, s.is_deleted);
