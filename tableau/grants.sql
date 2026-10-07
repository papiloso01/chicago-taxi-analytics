-- Run as an administrator after the gold tables exist.
-- Create this group role once; create the actual login separately with a private password.
CREATE ROLE tableau_reader NOLOGIN;
GRANT CONNECT ON DATABASE taxi TO tableau_reader;
GRANT USAGE ON SCHEMA gold TO tableau_reader;
GRANT SELECT ON gold.mart_tableau_trips TO tableau_reader;
-- GRANT tableau_reader TO your_separately_provisioned_login;
-- Replace database "taxi" if POSTGRES_DB differs. No silver/bronze access is granted.
