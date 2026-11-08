-- After Glue execution becomes available and the job has run successfully.
-- Athena engine v3, database lending_cdc. State includes tombstones by design.
CREATE OR REPLACE VIEW lending_cdc.loans_current AS
SELECT source_system, loan_id, principal_amount, currency, status, created_at, updated_at
FROM lending_cdc.loans_state WHERE NOT is_deleted;

CREATE OR REPLACE VIEW lending_cdc.loan_transactions_current AS
SELECT source_system, transaction_id, loan_id, transaction_type, amount, occurred_at, updated_at
FROM lending_cdc.loan_transactions_state WHERE NOT is_deleted;

-- Expected guided baseline: 2 loans, 3 transactions, 8 ledger events, 1 tombstone.
SELECT count(*) AS loans FROM lending_cdc.loans_current;
SELECT count(*) AS transactions FROM lending_cdc.loan_transactions_current;
SELECT count(*) AS events FROM lending_cdc.event_ledger;
SELECT count(*) AS deleted_rows FROM lending_cdc.loan_transactions_state WHERE is_deleted;

-- Orphans can be transient during independent table commits; reconcile after a successful run.
SELECT t.source_system, t.transaction_id, t.loan_id
FROM lending_cdc.loan_transactions_current t
LEFT JOIN lending_cdc.loans_current l
ON t.source_system = l.source_system AND t.loan_id = l.loan_id
WHERE l.loan_id IS NULL;

SELECT source_system, currency, sum(principal_amount) AS principal,
       sum(repaid_amount) AS repaid, sum(disbursed_amount) AS disbursed
FROM lending_cdc.loan_portfolio GROUP BY source_system, currency;
