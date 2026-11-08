BEGIN;
SELECT pg_advisory_xact_lock(2026110801);
CREATE TABLE IF NOT EXISTS cdc.bootstrap_runs (
    snapshot_name TEXT PRIMARY KEY,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
LOCK TABLE lending.loans, lending.loan_transactions IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM cdc.bootstrap_runs WHERE snapshot_name = 'lending-initial-v1') THEN
        INSERT INTO cdc.outbox_events
            (source_schema, source_table, operation, record_key, record_data)
        SELECT 'lending', 'loans', 'I', jsonb_build_object('loan_id', l.loan_id), to_jsonb(l)
        FROM lending.loans AS l;
        INSERT INTO cdc.outbox_events
            (source_schema, source_table, operation, record_key, record_data)
        SELECT 'lending', 'loan_transactions', 'I',
               jsonb_build_object('transaction_id', t.transaction_id), to_jsonb(t)
        FROM lending.loan_transactions AS t;
        INSERT INTO cdc.bootstrap_runs (snapshot_name) VALUES ('lending-initial-v1');
    END IF;
END;
$$;
COMMIT;
