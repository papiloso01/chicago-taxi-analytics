-- Run as postgres. Set a private password separately in your SQL client.
-- Do not commit a password or use the unused DMS account as the exporter.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cdc_exporter') THEN
        CREATE ROLE cdc_exporter LOGIN;
    END IF;
END;
$$;
GRANT CONNECT ON DATABASE transactions TO cdc_exporter;
GRANT USAGE ON SCHEMA cdc TO cdc_exporter;
GRANT SELECT ON cdc.outbox_events TO cdc_exporter;
GRANT UPDATE (exported_at) ON cdc.outbox_events TO cdc_exporter;
