-- This is a fully working example script that shows reconcilation strategies.
--
-- Note that it uses `\gset` to store sql responses as variables. For example,
-- `\gset foo_` creates variables for each column in the response like
-- `foo_col1`, `foo_col2`, etc. These variables can then be used like
-- `:'foo1_col`.

-- The entire script can be passed to psql. If you are running postgres via the
-- pgledger docker compose, you can run this script with:
--
--   cat reconciliation.sql | \
--     docker compose exec --no-TTY postgres psql -U pgledger --echo-queries --no-psqlrc
--

-- The goal of this script is to show some reconcilation examples. Reconcilation is the process
-- by which we check that our view of the world matches what we expect.

-- Let's start with a simple payment flow example, similar to the basic-example.sql script:
SELECT id FROM pgledger_create_account('user1.external', 'USD') \gset user1_external_
SELECT id FROM pgledger_create_account('user1.receivables', 'USD') \gset user1_receivables_
SELECT id FROM pgledger_create_account('user1.available', 'USD') \gset user1_available_
SELECT id FROM pgledger_create_account('user1.pending_outbound', 'USD') \gset user1_pending_outbound_

-- The first step in the flow is the user initiates a $50 payment, and we are
-- waiting for funds to arrive. The difference this time is we'll use some
-- metadata to help us track what's going on:
SELECT * FROM pgledger_create_transfer(
    :'user1_external_id',
    :'user1_receivables_id',
    50.00,
    metadata => '{"kind": "payment_created", "payment_id": "p_123"}'
);

-- The user also creates another $50 payment:
SELECT * FROM pgledger_create_transfer(
    :'user1_external_id',
    :'user1_receivables_id',
    50.00,
    metadata => '{"kind": "payment_created", "payment_id": "p_456"}'
);

-- Next, the funds arrive in our account for one of the payments, so we remove
-- them from receivables and make them available:
SELECT * FROM pgledger_create_transfer(
    :'user1_receivables_id',
    :'user1_available_id',
    50.00,
    metadata => '{"kind": "payment_received", "payment_id": "p_456"}'
);

-- Now, we can query the receivables account and see that the balance is still
-- $50, meaning we are waiting on more funds to arrive:
SELECT balance FROM pgledger_accounts_view
WHERE id =:'user1_receivables_id';

-- But how do we know which payment we're still waiting for? If we use metadata
-- on each transfer which ties it to a payment_id, then we can do interesting
-- rollups, such as summing entries by payment_id. Any payment we've received
-- funds for will zero out (since the incoming $50 and outgoing -$50 sum to 0):
SELECT
    metadata ->> 'payment_id' AS payment_id,
    sum(amount) AS sum
FROM pgledger_entries_view
WHERE account_id =:'user1_receivables_id'
GROUP BY 1;

-- This strategy can help us find other issues, such as when the amount of
-- funds we received weren't what we expectd. For example, say we eventually
-- received the funds for the first payment but it was short:
SELECT * FROM pgledger_create_transfer(
    :'user1_receivables_id',
    :'user1_available_id',
    49.50,
    metadata => '{"kind": "payment_received", "payment_id": "p_123"}'
);

-- Now, this discrepency will show up in the rollup, and it will tell us how much it's off by:
SELECT
    metadata ->> 'payment_id' AS payment_id,
    sum(amount) AS sum
FROM pgledger_entries_view
WHERE account_id =:'user1_receivables_id'
GROUP BY 1
HAVING sum(amount) != 0;

-- Continuing the example, let's issue a partial refund of the payment. When we
-- issue the refund, we move the money into the pending_outbound account to
-- hold it until we get confirmation that it was sent
SELECT * FROM pgledger_create_transfer(
    :'user1_available_id',
    :'user1_pending_outbound_id',
    20.00,
    metadata => '{"kind": "refund_created", "payment_id": "p_123"}'
);

-- Once we get confirmation that that refund was sent, We can move the money
-- back to the user's external account (e.g. their credit/debit card). The
-- metadata can be whatever JSON we want, so we can include as many fields as
-- will be helpful:
SELECT * FROM pgledger_create_transfer(
    :'user1_pending_outbound_id',
    :'user1_external_id',
    20.00,
    event_at => '2025-07-21T12:45:54.123Z',
    metadata => '{
        "kind": "refund_sent",
        "payment_id": "p_123",
        "webhook_id": "webhook_123"
    }'
);

-- Metadata gives us a powerful way to query the ledger, For example, we can
-- track the history of a specific payment through the various accounts which
-- can help us understand the state of a payment or account:
SELECT
    e.transfer_id,
    e.metadata ->> 'kind' AS kind,
    a.name,
    e.amount
FROM pgledger_entries_view e
INNER JOIN pgledger_accounts_view a ON e.account_id = a.id
WHERE e.metadata ->> 'payment_id' = 'p_123'
ORDER BY 1;

-- And then we can visualize that data in various ways. For example, we can
-- take the previous query and display in a column-oriented view, making it
-- really easy to see the flow of money:
SELECT
    concat_ws(' - ', e.transfer_id, e.metadata ->> 'kind') AS transfer,
    a.name,
    e.amount
FROM pgledger_entries_view e
INNER JOIN pgledger_accounts_view a ON e.account_id = a.id
WHERE e.metadata ->> 'payment_id' = 'p_123'
ORDER BY 1
\crosstabview transfer name amount

-- We can even get fancier and sum the entries for each account in the table:
WITH entries AS ( -- noqa: PRS, the \crosstabview above breaks parsing, so we have to ignore sqlfluff from here
    SELECT
        concat_ws(' - ', e.transfer_id, e.metadata ->> 'kind') AS transfer,
        a.name,
        e.amount
    FROM pgledger_entries_view e
    INNER JOIN pgledger_accounts_view a ON e.account_id = a.id
    WHERE e.metadata ->> 'payment_id' = 'p_123'
)

SELECT * FROM (
    SELECT * FROM entries
    UNION
    SELECT
        '--- SUMS ---' AS transfer,
        name,
        sum(amount)
    FROM entries
    GROUP BY 1, 2
)
ORDER BY transfer
\crosstabview transfer name amount
