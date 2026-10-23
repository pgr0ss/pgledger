-- This is a fully working example script that shows how to use pgledger
--
-- Note that it uses `\gset` to store sql responses as variables. For example,
-- `\gset foo_` creates variables for each column in the response like
-- `foo_col1`, `foo_col2`, etc. These variables can then be used like
-- `:'foo1_col`.

-- The entire script can be passed to psql. If you are running postgres via the
-- pgledger docker compose, you can run this script with:
--
--   cat idempotency.sql | \
--     docker compose exec --no-TTY postgres psql -U pgledger --echo-all --no-psqlrc
--

-- Create a couple of accounts for testing
SELECT id FROM pgledger_create_account('account1', 'USD') \gset account1_
SELECT id FROM pgledger_create_account('account2', 'USD') \gset account2_

-- Create an idempotency_key (normally, you'll do this in application code)
SELECT uuidv4() AS idempotency_key \gset

SELECT :'idempotency_key';

-- Create a transfer using the idempotency_key
SELECT * FROM pgledger_create_transfer(
    :'account1_id',
    :'account2_id',
    10.00,
    metadata => jsonb_build_object('idempotency_key', :'idempotency_key')
);

-- Another transfer with the same key will return the existing transfer
SELECT * FROM pgledger_create_transfer(
    :'account1_id',
    :'account2_id',
    10.00,
    metadata => jsonb_build_object('idempotency_key', :'idempotency_key')
);

-- But if the arguments are different, it will give an error:
SELECT * FROM pgledger_create_transfer(
    :'account1_id',
    :'account2_id',
    99.00,
    metadata => jsonb_build_object('idempotency_key', :'idempotency_key')
);

-- We can see that there's exactly one transfer for this idempotency_key
SELECT * FROM pgledger_transfers_view
WHERE metadata ->> 'idempotency_key' = :'idempotency_key';

-- Idempotency also works for batches of transfers:
SELECT * FROM pgledger_create_transfers(
    event_at => '2026-10-25T12:45:54.123Z',
    metadata => jsonb_build_object('idempotency_key', :'idempotency_key' || '-2'),
    transfer_requests => ARRAY[
        (:'account1_id', :'account2_id', '1.00'),
        (:'account1_id', :'account2_id', '2.00')
    ]::TRANSFER_REQUEST[]
);

SELECT * FROM pgledger_create_transfers(
    event_at => '2026-10-25T12:45:54.123Z',
    metadata => jsonb_build_object('idempotency_key', :'idempotency_key' || '-2'),
    transfer_requests => ARRAY[
        (:'account1_id', :'account2_id', '1.00'),
        (:'account1_id', :'account2_id', '2.00')
    ]::TRANSFER_REQUEST[]
);

-- A missing or unique idempotency_key always creates a new transfer
SELECT * FROM pgledger_create_transfer(
    :'account1_id',
    :'account2_id',
    11.00,
    metadata => '{}'
);

SELECT * FROM pgledger_create_transfer(
    :'account1_id',
    :'account2_id',
    12.00,
    metadata => jsonb_build_object('idempotency_key', uuidv4())
);

-- Idempotency keys need to be strings:
SELECT * FROM pgledger_create_transfer(
    :'account1_id',
    :'account2_id',
    10.00,
    metadata => '{"idempotency_key": 123}'
);
