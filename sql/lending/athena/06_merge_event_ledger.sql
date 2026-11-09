MERGE INTO lending_cdc_athena.event_ledger AS t
USING (
    VALUES {validated_rows}
) AS s (source_system, event_id, event_sequence, source_table, operation, event_json)
ON t.source_system = s.source_system AND t.event_id = s.event_id
WHEN NOT MATCHED THEN
    INSERT (source_system, event_id, event_sequence, source_table, operation, event_json)
    VALUES (s.source_system, s.event_id, s.event_sequence, s.source_table, s.operation, s.event_json);
