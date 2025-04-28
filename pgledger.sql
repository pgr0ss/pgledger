-- Function to generate uuidv7 at microsecond precision. It's not monotonic,
-- but hopefully close enough at microsecond precision.
--   From: https://postgresql.verite.pro/blog/2024/07/15/uuid-v7-pure-sql.html
-- This can be replaced by the builtin uuidv7() function when it's released in
-- PostgreSQL 18. That one will will be monotonic.
CREATE FUNCTION pgledger_generate_id() RETURNS uuid
AS $$
 select encode(
   substring(int8send(floor(t_ms)::int8) from 3) ||
   int2send((7<<12)::int2 | ((t_ms-floor(t_ms))*4096)::int2) ||
   substring(uuid_send(gen_random_uuid()) from 9 for 8)
  , 'hex')::uuid
  from (select extract(epoch from clock_timestamp())*1000 as t_ms) s
$$ LANGUAGE sql volatile;

CREATE TABLE pgledger_accounts (
    id UUID PRIMARY KEY DEFAULT pgledger_generate_id(),
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
    id UUID PRIMARY KEY DEFAULT pgledger_generate_id(),
    from_account_id UUID NOT NULL REFERENCES pgledger_accounts(id),
    to_account_id UUID NOT NULL REFERENCES pgledger_accounts(id),
    amount NUMERIC NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (amount > 0 AND from_account_id != to_account_id)
);

CREATE INDEX ON pgledger_transfers(from_account_id);
CREATE INDEX ON pgledger_transfers(to_account_id);

CREATE TABLE pgledger_entries (
    id UUID PRIMARY KEY DEFAULT pgledger_generate_id(),
    account_id UUID NOT NULL REFERENCES pgledger_accounts(id),
    transfer_id UUID NOT NULL REFERENCES pgledger_transfers(id),
    amount NUMERIC NOT NULL,
    account_previous_balance NUMERIC NOT NULL,
    account_current_balance NUMERIC NOT NULL,
    account_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX ON pgledger_entries(account_id);
CREATE INDEX ON pgledger_entries(transfer_id);

CREATE OR REPLACE FUNCTION pgledger_add_account(name_param TEXT, currency_param TEXT) RETURNS TABLE(id UUID, name TEXT, currency TEXT, balance NUMERIC) AS $$
BEGIN
    RETURN QUERY
    INSERT INTO pgledger_accounts (name, currency, allow_negative_balance, allow_positive_balance, created_at, updated_at)
    VALUES (name_param, currency_param, TRUE, TRUE, now(), now())
    RETURNING pgledger_accounts.id, pgledger_accounts.name, pgledger_accounts.currency, pgledger_accounts.balance;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgledger_create_account(
    name_param TEXT,
    currency_param TEXT,
    allow_negative_balance_param BOOLEAN DEFAULT TRUE,
    allow_positive_balance_param BOOLEAN DEFAULT TRUE
)
RETURNS TABLE(id UUID, name TEXT, currency TEXT, balance NUMERIC, version BIGINT, allow_negative_balance BOOLEAN, allow_positive_balance BOOLEAN, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ) AS $$
BEGIN
    RETURN QUERY
    INSERT INTO pgledger_accounts (name, currency, allow_negative_balance, allow_positive_balance, created_at, updated_at)
    VALUES (name_param, currency_param, allow_negative_balance_param, allow_positive_balance_param, now(), now())
    RETURNING pgledger_accounts.id, pgledger_accounts.name, pgledger_accounts.currency, pgledger_accounts.balance, pgledger_accounts.version,
              pgledger_accounts.allow_negative_balance, pgledger_accounts.allow_positive_balance,
              pgledger_accounts.created_at, pgledger_accounts.updated_at;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgledger_get_account(id_param UUID)
RETURNS TABLE(id UUID, name TEXT, currency TEXT, balance NUMERIC, version BIGINT, allow_negative_balance BOOLEAN, allow_positive_balance BOOLEAN, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ) AS $$
BEGIN
    RETURN QUERY
    SELECT pgledger_accounts.id, pgledger_accounts.name, pgledger_accounts.currency, pgledger_accounts.balance, pgledger_accounts.version,
           pgledger_accounts.allow_negative_balance, pgledger_accounts.allow_positive_balance,
           pgledger_accounts.created_at, pgledger_accounts.updated_at
    FROM pgledger_accounts
    WHERE pgledger_accounts.id = id_param;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgledger_get_transfer(id_param UUID) RETURNS TABLE(id UUID, from_account_id UUID, to_account_id UUID, amount NUMERIC, created_at TIMESTAMPTZ) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.id,
        t.from_account_id,
        t.to_account_id,
        t.amount,
        t.created_at
    FROM pgledger_transfers t
    WHERE t.id = id_param;
END;
$$ LANGUAGE plpgsql;

-- Helper function to check account balance constraints
CREATE OR REPLACE FUNCTION pgledger_check_account_balance_constraints(account pgledger_accounts) RETURNS VOID AS $$
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
    from_account_id UUID,
    to_account_id UUID,
    amount NUMERIC
);

CREATE OR REPLACE FUNCTION pgledger_create_transfer(from_account_id_param UUID, to_account_id_param UUID, amount_param NUMERIC) RETURNS TABLE(id UUID, from_account_id UUID, to_account_id UUID, amount NUMERIC, created_at TIMESTAMPTZ) AS $$
BEGIN
    -- Simply call pgledger_create_transfers with a single transfer
    RETURN QUERY
    SELECT * FROM pgledger_create_transfers(
        ROW(from_account_id_param, to_account_id_param, amount_param)::transfer_request
    );
END;
$$ LANGUAGE plpgsql;

-- Function to create multiple transfers in a single transaction
CREATE OR REPLACE FUNCTION pgledger_create_transfers(
    VARIADIC transfers transfer_request[]
) RETURNS TABLE(id UUID, from_account_id UUID, to_account_id UUID, amount NUMERIC, created_at TIMESTAMPTZ) AS $$
DECLARE
    transfer transfer_request;
    transfer_ids UUID[] := '{}';
    transfer_id UUID;
    from_account pgledger_accounts;
    to_account pgledger_accounts;
    from_account_id_param UUID;
    to_account_id_param UUID;
    amount_param NUMERIC;
    all_account_ids UUID[] := '{}';
BEGIN
    -- Collect all unique account IDs and sort them to prevent deadlocks
    FOREACH transfer IN ARRAY transfers LOOP
        all_account_ids := array_append(all_account_ids, transfer.from_account_id);
        all_account_ids := array_append(all_account_ids, transfer.to_account_id);
    END LOOP;

    -- Remove duplicates and sort
    SELECT ARRAY(SELECT DISTINCT unnest FROM unnest(all_account_ids) ORDER BY unnest)
    INTO all_account_ids;

    -- Lock all accounts in order
    FOREACH from_account_id_param IN ARRAY all_account_ids LOOP
        PERFORM pgledger_accounts.id
        FROM pgledger_accounts
        WHERE pgledger_accounts.id = from_account_id_param
        FOR UPDATE;
    END LOOP;

    -- Process each transfer
    FOREACH transfer IN ARRAY transfers LOOP
        from_account_id_param := transfer.from_account_id;
        to_account_id_param := transfer.to_account_id;
        amount_param := transfer.amount;

        -- Preliminary checks
        IF amount_param <= 0 THEN
            RAISE EXCEPTION 'Amount (%) must be positive', amount_param;
        END IF;

        IF from_account_id_param = to_account_id_param THEN
            RAISE EXCEPTION 'Cannot transfer to the same account (id=%)', from_account_id_param;
        END IF;

        -- Update account balances
        UPDATE pgledger_accounts
        SET balance = balance - amount_param,
            version = version + 1,
            updated_at = now()
        WHERE pgledger_accounts.id = from_account_id_param
        RETURNING * INTO from_account;

        -- Check balance constraints for the source account
        PERFORM pgledger_check_account_balance_constraints(from_account);

        UPDATE pgledger_accounts
        SET balance = balance + amount_param,
            version = version + 1,
            updated_at = now()
        WHERE pgledger_accounts.id = to_account_id_param
        RETURNING * INTO to_account;

        -- Check balance constraints for the destination account
        PERFORM pgledger_check_account_balance_constraints(to_account);

        -- Check that currencies match
        IF from_account.currency != to_account.currency THEN
            RAISE EXCEPTION 'Cannot transfer between different currencies (% and %)', from_account.currency, to_account.currency;
        END IF;

        -- Create transfer record
        INSERT INTO pgledger_transfers (from_account_id, to_account_id, amount, created_at)
        VALUES (from_account_id_param, to_account_id_param, amount_param, now())
        RETURNING pgledger_transfers.id INTO transfer_id;

        transfer_ids := array_append(transfer_ids, transfer_id);

        -- Create entry for the source account (negative amount)
        INSERT INTO pgledger_entries (account_id, transfer_id, amount, account_previous_balance, account_current_balance, account_version, created_at)
        VALUES (from_account_id_param, transfer_id, -amount_param, from_account.balance + amount_param, from_account.balance, from_account.version, now());

        -- Create entry for the destination account (positive amount)
        INSERT INTO pgledger_entries (account_id, transfer_id, amount, account_previous_balance, account_current_balance, account_version, created_at)
        VALUES (to_account_id_param, transfer_id, amount_param, to_account.balance - amount_param, to_account.balance, to_account.version, now());
    END LOOP;

    -- Return all created transfers
    RETURN QUERY
    SELECT
        t.id,
        t.from_account_id,
        t.to_account_id,
        t.amount,
        t.created_at
    FROM pgledger_transfers t
    WHERE t.id = ANY(transfer_ids)
    ORDER BY t.id;
END;
$$ LANGUAGE plpgsql;
