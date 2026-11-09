CREATE OR REPLACE VIEW lending_cdc_athena.loan_portfolio AS
WITH transaction_totals AS (
    SELECT source_system, loan_id, COUNT(*) AS transaction_count,
           SUM(CASE WHEN transaction_type = 'repayment'
                    THEN amount ELSE DECIMAL '0.00' END) AS repaid_amount,
           SUM(CASE WHEN transaction_type = 'disbursement'
                    THEN amount ELSE DECIMAL '0.00' END) AS disbursed_amount
    FROM lending_cdc_athena.loan_transactions_state
    WHERE is_deleted = false
    GROUP BY source_system, loan_id
)
SELECT l.source_system, l.loan_id, l.principal_amount, l.currency, l.status,
       COALESCE(t.transaction_count, 0) AS transaction_count,
       COALESCE(t.repaid_amount, DECIMAL '0.00') AS repaid_amount,
       COALESCE(t.disbursed_amount, DECIMAL '0.00') AS disbursed_amount
FROM lending_cdc_athena.loans_state AS l
LEFT JOIN transaction_totals AS t
ON l.source_system = t.source_system AND l.loan_id = t.loan_id
WHERE l.is_deleted = false;
