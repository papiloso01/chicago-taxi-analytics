SELECT
    (SELECT COUNT(*) FROM lending_cdc_athena.event_ledger) AS ledger_events,
    (SELECT COUNT(*) FROM lending_cdc_athena.loans_state WHERE NOT is_deleted) AS current_loans,
    (SELECT COUNT(*) FROM lending_cdc_athena.loan_transactions_state WHERE NOT is_deleted) AS current_transactions,
    (SELECT COUNT(*) FROM lending_cdc_athena.loan_transactions_state WHERE is_deleted) AS transaction_tombstones,
    (SELECT COUNT(*)
     FROM lending_cdc_athena.loan_transactions_state AS t
     LEFT JOIN lending_cdc_athena.loans_state AS l
       ON t.source_system = l.source_system AND t.loan_id = l.loan_id AND NOT l.is_deleted
     WHERE NOT t.is_deleted AND l.loan_id IS NULL) AS orphan_transactions,
    (SELECT COUNT(*) FROM lending_cdc_athena.loan_portfolio) AS portfolio_loans,
    (SELECT COALESCE(SUM(transaction_count), 0) FROM lending_cdc_athena.loan_portfolio) AS portfolio_transactions;
