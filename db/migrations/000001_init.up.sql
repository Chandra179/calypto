-- 1. ACCOUNTS TABLE (The "Current State")
-- This is what the user sees on their home screen.
CREATE TABLE accounts (
    id             BIGINT PRIMARY KEY, -- Snowflake ID
    user_id        UUID NOT NULL,      -- Mapped from internal Identity Service
    currency       CHAR(3) NOT NULL,   -- 'USD', 'IDR', etc.
    
    -- FINANCIAL STATE
    balance        BIGINT NOT NULL DEFAULT 0, -- Available to spend (in cents)
    hold_balance   BIGINT NOT NULL DEFAULT 0, -- Locked for pending txns
    
    -- CONCURRENCY CONTROL (Optimistic Locking)
    version        INT NOT NULL DEFAULT 1,    -- Increments on every update
    last_updated   TIMESTAMPTZ DEFAULT NOW(),
    
    -- CONSTRAINT: Balance can never be negative (unless overdraft allowed)
    CONSTRAINT check_positive_balance CHECK (balance >= 0)
);

-- 2. TRANSACTIONS TABLE (The "Intent")
-- Represents the request (e.g., "Alice sends $10 to Bob").
CREATE TABLE transactions (
    id             BIGINT PRIMARY KEY, -- Snowflake ID
    idempotency_key UUID UNIQUE NOT NULL, -- Safety mechanism
    
    reference      VARCHAR(255),      -- "Invoice #123" or "Payment for Coffee"
    status         VARCHAR(20) NOT NULL, -- PENDING, POSTED, FAILED
    
    metadata       JSONB,             -- Store extra data (IP, Device ID)
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- 3. LEDGER POSTINGS (The "Immutable History")
-- Double-Entry Implementation. Rows here are NEVER updated or deleted.
CREATE TABLE ledger_postings (
    id             BIGINT PRIMARY KEY, -- Snowflake ID
    transaction_id BIGINT NOT NULL REFERENCES transactions(id),
    
    account_id     BIGINT NOT NULL REFERENCES accounts(id),
    amount         BIGINT NOT NULL,    -- Negative for Debit, Positive for Credit
    
    direction      VARCHAR(10) NOT NULL, -- 'DEBIT' or 'CREDIT'
    
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

-- INDEXES for Performance
CREATE INDEX idx_accounts_user ON accounts(user_id);
CREATE INDEX idx_postings_account ON ledger_postings(account_id);