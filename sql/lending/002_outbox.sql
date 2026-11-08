BEGIN;
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE TABLE IF NOT EXISTS cdc.outbox_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    source_schema TEXT NOT NULL,
    source_table TEXT NOT NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
    record_key JSONB NOT NULL,
    record_data JSONB NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    exported_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_pending_events_idx
ON cdc.outbox_events (event_sequence) WHERE exported_at IS NULL;
CREATE OR REPLACE FUNCTION cdc.capture_change()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    row_data JSONB;
    key_column TEXT := TG_ARGV[0];
BEGIN
    IF TG_OP = 'DELETE' THEN row_data := to_jsonb(OLD);
    ELSE row_data := to_jsonb(NEW); END IF;
    IF TG_OP = 'UPDATE' THEN
        IF (to_jsonb(OLD) -> key_column) IS DISTINCT FROM (to_jsonb(NEW) -> key_column) THEN
            RAISE EXCEPTION 'Changing primary key % is not supported', key_column;
        END IF;
    END IF;
    INSERT INTO cdc.outbox_events
        (source_schema, source_table, operation, record_key, record_data)
    VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, substring(TG_OP FROM 1 FOR 1),
            jsonb_build_object(key_column, row_data -> key_column), row_data);
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION cdc.capture_change() FROM PUBLIC;
DROP TRIGGER IF EXISTS loans_outbox ON lending.loans;
CREATE TRIGGER loans_outbox AFTER INSERT OR UPDATE OR DELETE ON lending.loans
FOR EACH ROW EXECUTE FUNCTION cdc.capture_change('loan_id');
DROP TRIGGER IF EXISTS transactions_outbox ON lending.loan_transactions;
CREATE TRIGGER transactions_outbox AFTER INSERT OR UPDATE OR DELETE ON lending.loan_transactions
FOR EACH ROW EXECUTE FUNCTION cdc.capture_change('transaction_id');
COMMIT;
