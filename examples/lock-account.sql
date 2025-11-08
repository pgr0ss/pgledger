-- This is a fully working example script which shows a strategy for locking an
-- account
--
-- Note that it uses `\gset` to store sql responses as variables. For example,
-- `\gset foo_` creates variables for each column in the response like
-- `foo_col1`, `foo_col2`, etc. These variables can then be used like
-- `:'foo1_col`.

-- The entire script can be passed to psql. If you are running postgres via the
-- pgledger docker compose, you can run this script with:
--
--   cat lock-account.sql | \
--     docker compose exec --no-TTY postgres psql -U pgledger --echo-queries --no-psqlrc
--

-- Create a couple of accounts for testing
SELECT id FROM pgledger_create_account('account1', 'USD') \gset account1_
SELECT id FROM pgledger_create_account('account2', 'USD') \gset account2_

-- Create a transfer to set the balances to non-zero
SELECT * FROM pgledger_create_transfer(:'account1_id',:'account2_id', 10.00);

-- Now, update account2 to disallow both negative and positive balances, which
-- means the balance must be zero. This is only checked on transfer, so it will
-- work even if the current balance is not zero.
UPDATE pgledger_accounts
SET
    allow_negative_balance = 'false',
    allow_positive_balance = 'false'
WHERE id =:'account2_id' RETURNING *;

-- This should fail now since it would take the balance from 10 to 20
SELECT * FROM pgledger_create_transfer(:'account1_id',:'account2_id', 10.00);

-- But this will work since it zeroes out the balance:
SELECT * FROM pgledger_create_transfer(:'account2_id',:'account1_id', 10.00);

-- But no other transfers to or from account2 will work now:
SELECT * FROM pgledger_create_transfer(:'account2_id',:'account1_id', 10.00);

-- Now, at query time, you can consider accounts in this state as 'inactive' or
-- whatever status you like:
SELECT
    name,
    allow_negative_balance,
    allow_positive_balance,
    CASE
        WHEN allow_positive_balance = 'false' AND allow_negative_balance = 'false' THEN 'inactive'
        ELSE 'active'
    END AS status
FROM pgledger_accounts_view
WHERE id IN (:'account2_id',:'account1_id')
ORDER BY id;
