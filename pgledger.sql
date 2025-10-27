-- uuidv7 is a new function in PostgreSQL 18:
-- https://www.postgresql.org/docs/release/18.0/
CREATE FUNCTION pgledger_uuidv7_exists() RETURNS BOOL
AS $$
    SELECT EXISTS(SELECT * FROM pg_proc WHERE proname = 'uuidv7');
$$ LANGUAGE sql IMMUTABLE;

-- Function to generate uuidv7 at microsecond precision. It's not monotonic,
-- but hopefully close enough at microsecond precision.
--   From: https://postgresql.verite.pro/blog/2024/07/15/uuid-v7-pure-sql.html
-- This will only be used in PostgreSQL versions below 18 when the builtin
-- uuidv7() function does not exist (which is monotonic).
CREATE FUNCTION pgledger_uuidv7_microsecond() RETURNS UUID
AS $$
    select encode(
        substring(int8send(floor(t_ms)::int8) from 3) ||
        int2send((7<<12)::int2 | ((t_ms-floor(t_ms))*4096)::int2) ||
        substring(uuid_send(gen_random_uuid()) from 9 for 8)
        , 'hex')::uuid
    from (select extract(epoch from clock_timestamp())*1000 as t_ms) s
$$ LANGUAGE sql VOLATILE;

CREATE FUNCTION pgledger_uuidv7() RETURNS UUID
AS $$
DECLARE
    result uuid;
BEGIN
    IF pgledger_uuidv7_exists() THEN
        EXECUTE 'select uuidv7()' INTO result;
        RETURN result;
    ELSE
        RETURN pgledger_uuidv7_microsecond();
    END IF;
end
$$ LANGUAGE plpgsql VOLATILE;

CREATE FUNCTION pgledger_generate_id(prefix TEXT) RETURNS TEXT
AS $$
    SELECT prefix || '_' || uuid_to_ulid(pgledger_uuidv7())
$$ LANGUAGE sql VOLATILE;

CREATE TABLE pgledger_accounts (
    id TEXT PRIMARY KEY DEFAULT pgledger_generate_id('pgla'),
    name TEXT NOT NULL,
    currency TEXT NOT NULL,
    balance NUMERIC NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    allow_negative_balance BOOLEAN NOT NULL,
    allow_positive_balance BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE pgledger_transfers (
    id TEXT PRIMARY KEY DEFAULT pgledger_generate_id('pglt'),
    from_account_id TEXT NOT NULL REFERENCES pgledger_accounts (id),
    to_account_id TEXT NOT NULL REFERENCES pgledger_accounts (id),
    amount NUMERIC NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    metadata JSONB,
    CHECK (amount > 0 AND from_account_id != to_account_id)
);

CREATE INDEX ON pgledger_transfers (from_account_id);
CREATE INDEX ON pgledger_transfers (to_account_id);
CREATE INDEX ON pgledger_transfers (event_at);

CREATE TABLE pgledger_entries (
    id TEXT PRIMARY KEY DEFAULT pgledger_generate_id('pgle'),
    account_id TEXT NOT NULL REFERENCES pgledger_accounts (id),
    transfer_id TEXT NOT NULL REFERENCES pgledger_transfers (id),
    amount NUMERIC NOT NULL,
    account_previous_balance NUMERIC NOT NULL,
    account_current_balance NUMERIC NOT NULL,
    account_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON pgledger_entries (account_id);
CREATE INDEX ON pgledger_entries (transfer_id);

CREATE VIEW pgledger_accounts_view AS
SELECT
    id,
    name,
    currency,
    balance,
    version,
    allow_negative_balance,
    allow_positive_balance,
    created_at,
    updated_at
FROM pgledger_accounts;

CREATE VIEW pgledger_transfers_view AS
SELECT
    id,
    from_account_id,
    to_account_id,
    amount,
    created_at,
    event_at,
    metadata
FROM pgledger_transfers;

CREATE VIEW pgledger_entries_view AS
SELECT
    e.id,
    e.account_id,
    e.transfer_id,
    e.amount,
    e.account_previous_balance,
    e.account_current_balance,
    e.account_version,
    e.created_at,
    t.event_at,
    t.metadata
FROM pgledger_entries e
INNER JOIN pgledger_transfers t ON e.transfer_id = t.id;

CREATE OR REPLACE FUNCTION pgledger_create_account(
    name TEXT,
    currency TEXT,
    allow_negative_balance BOOLEAN DEFAULT TRUE,
    allow_positive_balance BOOLEAN DEFAULT TRUE
)
RETURNS SETOF PGLEDGER_ACCOUNTS_VIEW
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO pgledger_accounts (name, currency, allow_negative_balance, allow_positive_balance, created_at, updated_at)
    VALUES (name, currency, allow_negative_balance, allow_positive_balance, now(), now())
    RETURNING *;
END;
$$ LANGUAGE plpgsql;

-- Helper function to check account balance constraints
CREATE OR REPLACE FUNCTION pgledger_check_account_balance_constraints(account PGLEDGER_ACCOUNTS) RETURNS VOID AS $$
BEGIN
    -- If account doesn't allow negative balance and balance is negative, raise an error
    IF NOT account.allow_negative_balance AND (account.balance < 0) THEN
        RAISE EXCEPTION 'Account (id=%, name=%) does not allow negative balance', account.id, account.name;
    END IF;

    -- If account doesn't allow positive balance and balance is positive, raise an error
    IF NOT account.allow_positive_balance AND (account.balance > 0) THEN
        RAISE EXCEPTION 'Account (id=%, name=%) does not allow positive balance', account.id, account.name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Define a composite type for transfer requests
CREATE TYPE transfer_request AS (
    from_account_id TEXT,
    to_account_id TEXT,
    amount NUMERIC
);

CREATE OR REPLACE FUNCTION pgledger_create_transfer(
    from_account_id TEXT,
    to_account_id TEXT,
    amount NUMERIC,
    event_at TIMESTAMPTZ DEFAULT NULL,
    metadata JSONB DEFAULT NULL
)
RETURNS SETOF PGLEDGER_TRANSFERS_VIEW
AS $$
BEGIN
    -- Simply call pgledger_create_transfers with a single transfer
    RETURN QUERY
    SELECT * FROM pgledger_create_transfers(
        transfer_requests => array[(from_account_id, to_account_id, amount)::TRANSFER_REQUEST],
        event_at => event_at,
        metadata => metadata
    );
END;
$$ LANGUAGE plpgsql;

-- Function to create multiple transfers in a single transaction without an event_at
CREATE OR REPLACE FUNCTION pgledger_create_transfers(VARIADIC transfer_requests TRANSFER_REQUEST [])
RETURNS SETOF PGLEDGER_TRANSFERS_VIEW
AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM pgledger_create_transfers(transfer_requests);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgledger_create_transfers(
    transfer_requests TRANSFER_REQUEST [],
    event_at TIMESTAMPTZ DEFAULT NULL,
    metadata JSONB DEFAULT NULL
)
RETURNS SETOF PGLEDGER_TRANSFERS_VIEW
AS $$
DECLARE
    transfer_request transfer_request;
    transfer_ids TEXT[] := '{}';
    transfer_id TEXT;
    from_account pgledger_accounts;
    to_account pgledger_accounts;
    from_account_id TEXT;
    to_account_id TEXT;
    all_account_ids TEXT[] := '{}';
BEGIN
    -- Collect all unique account IDs and sort them to prevent deadlocks
    FOREACH transfer_request IN ARRAY transfer_requests LOOP
        all_account_ids := array_append(all_account_ids, transfer_request.from_account_id);
        all_account_ids := array_append(all_account_ids, transfer_request.to_account_id);
    END LOOP;

    -- Remove duplicates and sort
    SELECT ARRAY(SELECT DISTINCT unnest FROM unnest(all_account_ids) ORDER BY unnest)
    INTO all_account_ids;

    -- Lock all accounts in order
    FOREACH from_account_id IN ARRAY all_account_ids LOOP
        PERFORM pgledger_accounts.id
        FROM pgledger_accounts
        WHERE pgledger_accounts.id = from_account_id
        FOR UPDATE;
    END LOOP;

    -- Process each transfer
    FOREACH transfer_request IN ARRAY transfer_requests LOOP
        -- Preliminary checks
        IF transfer_request.amount <= 0 THEN
            RAISE EXCEPTION 'Amount (%) must be positive', transfer_request.amount;
        END IF;

        IF transfer_request.from_account_id = transfer_request.to_account_id THEN
            RAISE EXCEPTION 'Cannot transfer to the same account (id=%)', transfer_request.from_account_id;
        END IF;

        -- Update account balances
        UPDATE pgledger_accounts
        SET balance = balance - transfer_request.amount,
            version = version + 1,
            updated_at = now()
        WHERE pgledger_accounts.id = transfer_request.from_account_id
        RETURNING * INTO from_account;

        -- Check balance constraints for the source account
        PERFORM pgledger_check_account_balance_constraints(from_account);

        UPDATE pgledger_accounts
        SET balance = balance + transfer_request.amount,
            version = version + 1,
            updated_at = now()
        WHERE pgledger_accounts.id = transfer_request.to_account_id
        RETURNING * INTO to_account;

        -- Check balance constraints for the destination account
        PERFORM pgledger_check_account_balance_constraints(to_account);

        -- Check that currencies match
        IF from_account.currency != to_account.currency THEN
            RAISE EXCEPTION 'Cannot transfer between different currencies (% and %)', from_account.currency, to_account.currency;
        END IF;

        -- Create transfer record
        INSERT INTO pgledger_transfers (from_account_id, to_account_id, amount, created_at, event_at, metadata)
        VALUES (transfer_request.from_account_id, transfer_request.to_account_id, transfer_request.amount, now(), coalesce(event_at, now()), metadata)
        RETURNING pgledger_transfers.id INTO transfer_id;

        transfer_ids := array_append(transfer_ids, transfer_id);

        -- Create entry for the source account (negative amount)
        INSERT INTO pgledger_entries (account_id, transfer_id, amount, account_previous_balance, account_current_balance, account_version, created_at)
        VALUES (transfer_request.from_account_id, transfer_id, -transfer_request.amount, from_account.balance + transfer_request.amount, from_account.balance, from_account.version, now());

        -- Create entry for the destination account (positive amount)
        INSERT INTO pgledger_entries (account_id, transfer_id, amount, account_previous_balance, account_current_balance, account_version, created_at)
        VALUES (transfer_request.to_account_id, transfer_id, transfer_request.amount, to_account.balance - transfer_request.amount, to_account.balance, to_account.version, now());
    END LOOP;

    -- Return all created transfers
    RETURN QUERY
    SELECT *
    FROM pgledger_transfers_view
    WHERE id = ANY(transfer_ids)
    ORDER BY id;
END;
$$ LANGUAGE plpgsql;
