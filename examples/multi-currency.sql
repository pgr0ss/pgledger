-- This is a fully working example script which demonstrates how to separate
-- accounts by currency and transfer between them.
--
-- Note that it uses `\gset` to store sql responses as variables. For example,
-- `\gset foo_` creates variables for each column in the response like
-- `foo_col1`, `foo_col2`, etc. These variables can then be used like
-- `:'foo1_col`.

-- The entire script can be passed to psql. If you are running postgres via the
-- pgledger docker compose, you can run this script with:
--
--   cat multi-currency.sql | \
--     docker compose exec --no-TTY postgres psql -U pgledger --echo-queries --no-psqlrc
--

-- We're going to simulate a user holding balances in multiple currencies.
-- First, we create an account per currency for the user, since each account is
-- tied to a single currency. One strategy is to use hierarchical account
-- naming to make it clear these accounts are related:
SELECT id FROM pgledger_create_account('user2.usd', 'USD') \gset user2_usd_
SELECT id FROM pgledger_create_account('user2.eur', 'EUR') \gset user2_eur_

-- This style of naming makes it easy to see related accounts:
SELECT * FROM pgledger_accounts_view
WHERE name LIKE 'user2.%';

-- And you can even use PostgreSQL's ltree functionality for querying
--   https://www.postgresql.org/docs/current/ltree.html
CREATE EXTENSION ltree;

SELECT * FROM pgledger_accounts_view
WHERE name::LTREE <@ 'user2';

-- Now, we can see that pgledger prevents transfers between accounts of different currencies:
SELECT * FROM pgledger_create_transfer(:'user2_usd_id',:'user2_eur_id', 10.00);

-- Instead, we need to create liquidity accounts per currency and use those for the transfers:
SELECT id FROM pgledger_create_account('liquidity.usd', 'USD') \gset liquidity_usd_
SELECT id FROM pgledger_create_account('liquidity.eur', 'EUR') \gset liquidity_eur_

-- Now, a currency conversion consist of 2 transfers using the 4 accounts. The
-- difference between these two different amounts (10.00 vs 9.26) is the
-- exchange rate.
SELECT * FROM pgledger_create_transfers(
    (:'user2_usd_id',:'liquidity_usd_id', '10.00'),
    (:'liquidity_eur_id',:'user2_eur_id', '9.26')
);

-- Note that this used the plural `pgledger_create_transfers` instead of the
-- singular `pgledger_create_transfer` function. It is also possible to call
-- `pgledger_create_transfer` twice in a database transaction, but that is more
-- likely to result in deadlocks since bidrectional transfers will lock the
-- same accounts in reverse order.

-- It is also possible to specify the event_at with `pgledger_create_transfers` using named arguments:
SELECT * FROM pgledger_create_transfers( -- noqa
    event_at => '2025-07-21T12:45:54.123Z',
    VARIADIC transfer_requests => ARRAY[
        (:'user2_usd_id',:'liquidity_usd_id', '10.00'),
        (:'liquidity_eur_id',:'user2_eur_id', '9.26')
    ]::TRANSFER_REQUEST []
);

-- Here is what the transfers look like holistically:
SELECT
    t.id,
    t.created_at,
    t.event_at,
    acc_from.name AS acc_from,
    acc_to.name AS acc_to,
    t.amount
FROM pgledger_transfers_view t
LEFT JOIN pgledger_accounts_view acc_from ON t.from_account_id = acc_from.id
LEFT JOIN pgledger_accounts_view acc_to ON t.to_account_id = acc_to.id
WHERE acc_from.name LIKE 'user2.%' OR acc_from.name LIKE 'liquidity.%';
