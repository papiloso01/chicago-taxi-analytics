-- Run as the source schema owner (postgres in the guided lab).
BEGIN;
CREATE SCHEMA IF NOT EXISTS lending;
CREATE TABLE IF NOT EXISTS lending.loans (
    loan_id BIGINT PRIMARY KEY,
    principal_amount NUMERIC(12, 2) NOT NULL CHECK (principal_amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'GBP',
    status VARCHAR(20) NOT NULL CHECK (status IN ('active', 'closed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS lending.loan_transactions (
    transaction_id BIGINT PRIMARY KEY,
    loan_id BIGINT NOT NULL REFERENCES lending.loans(loan_id),
    transaction_type VARCHAR(20) NOT NULL CHECK (transaction_type IN ('disbursement', 'repayment')),
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO lending.loans (loan_id, principal_amount, status)
VALUES (1001, 1000.00, 'active'), (1002, 2000.00, 'active')
ON CONFLICT (loan_id) DO NOTHING;
INSERT INTO lending.loan_transactions (transaction_id, loan_id, transaction_type, amount)
VALUES (5001, 1001, 'disbursement', 1000.00), (5002, 1002, 'disbursement', 2000.00),
       (5003, 1001, 'repayment', 100.00)
ON CONFLICT (transaction_id) DO NOTHING;
COMMIT;
