-- This is a fully working example script that shows how to use pgledger
--
-- Note that it uses `\gset` to store sql responses as variables. For example,
-- `\gset foo_` creates variables for each column in the response like
-- `foo_col1`, `foo_col2`, etc. These variables can then be used like
-- `:'foo1_col`.

-- The entire script can be passed to psql. If you are running postgres via the
-- pgledger docker compose, you can run this script with:
--
--   cat basic-example.sql | \
--     docker compose exec --no-TTY postgres psql -U pgledger --echo-queries --no-psqlrc
--

-- We're going to simulate a simple payment flow. First, we create our accounts:
SELECT id FROM pgledger_create_account('user1.external', 'USD') \gset user1_external_
SELECT id FROM pgledger_create_account('user1.receivables', 'USD') \gset user1_receivables_

-- Note that we may want to prevent some accounts from going negative or positive:
SELECT id FROM pgledger_create_account('user1.available', 'USD', allow_negative_balance => FALSE) \gset user1_available_

SELECT id FROM pgledger_create_account('user1.pending_outbound', 'USD') \gset user1_pending_outbound_

-- We can query accounts to see what they looks like at the beginning.
SELECT * FROM pgledger_accounts_view
WHERE id IN (:'user1_external_id',:'user1_available_id');

-- The first step in the flow is a $50 payment is created and we are waiting for funds to arrive:
SELECT * FROM pgledger_create_transfer(:'user1_external_id',:'user1_receivables_id', 50.00);

-- Next, the funds arrive in our account, so we remove them from receivables and make them available:
SELECT * FROM pgledger_create_transfer(:'user1_receivables_id',:'user1_available_id', 50.00);

-- Now, we can query the accounts and see the balances. We aren't waiting on
-- any more funds, so the receivables balance is 0:
SELECT balance FROM pgledger_accounts_view
WHERE id =:'user1_receivables_id';

-- And we can see the entries for the receivables account:
SELECT * FROM pgledger_entries_view
WHERE account_id =:'user1_receivables_id'
ORDER BY account_version;

-- Continuing the example, let's issue a partial refund of the payment. When we
-- issue the refund, we move the money into the pending_outbound account to
-- hold it until we get confirmation that it was sent
SELECT * FROM pgledger_create_transfer(:'user1_available_id',:'user1_pending_outbound_id', 20.00);

-- Once we get confirmation that that refund was sent, We can move the money
-- back to the user's external account (e.g. their credit/debit card). Often,
-- this confirmation will come as a webhook or bank file or similar, so we can
-- record the event time in the confirmation separately from the time we record
-- the ledger transfer (event_at vs created_at). We can also record extra metadata
-- as JSON that helps us tie it all together:
SELECT *
FROM
    pgledger_create_transfer(
        :'user1_pending_outbound_id',
        :'user1_external_id',
        20.00,
        event_at => '2025-07-21T12:45:54.123Z',
        metadata => '{"webhook_id": "webhook_123"}'
    );

-- Now, we can query the current state. The external account has -$30 ($50
-- payment minus $20 refund) and our account for the user has $30. Nothing is
-- in flight, so the receivables and pending accounts are 0.
SELECT
    name,
    balance
FROM pgledger_accounts_view
WHERE id IN (:'user1_external_id',:'user1_receivables_id',:'user1_available_id',:'user1_pending_outbound_id');

-- Next, we can simulate an unexpected case. Let's say we initiate a payment
-- for $10 but we only receive $8 (e.g. due to unexpected fees):
SELECT * FROM pgledger_create_transfer(:'user1_external_id',:'user1_receivables_id', 10.00);
SELECT * FROM pgledger_create_transfer(:'user1_receivables_id',:'user1_available_id', 8.00);

-- Now, we can see that our receivables balance is not $0 like we expect:
SELECT balance FROM pgledger_accounts_view
WHERE id =:'user1_receivables_id';

-- And we can look at the entries to figure out what happened:
SELECT * FROM pgledger_entries_view
WHERE account_id =:'user1_receivables_id'
ORDER BY account_version;

-- We can also see that the `allow_negative_balance => false` flag on our
-- available account prevents transfers which are more than the current
-- balance:
SELECT * FROM pgledger_create_transfer(:'user1_available_id',:'user1_pending_outbound_id', 50.00);
