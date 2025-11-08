-- Create a couple of accounts for testing
SELECT id FROM pgledger_create_account('account1', 'USD') \gset account1_
SELECT id FROM pgledger_create_account('account2', 'USD') \gset account2_

-- Create a transfer to set the balances to non-zero
SELECT * FROM pgledger_create_transfer(:'account1_id',:'account2_id', 10.00);

-- Now, update account2 to disallow negative and positive balances. This is only
-- checked on transfer, so it will work even if the current balance is not zero.
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


-- Now you could even make a virtual column for this
ALTER TABLE pgledger_accounts
ADD COLUMN status TEXT
GENERATED ALWAYS AS (
    CASE
    WHEN allow_positive_balance = 'false' AND allow_negative_balance = 'false' THEN 'inactive'
    ELSE 'active'
    END
);

SELECT name, status, allow_negative_balance, allow_positive_balance FROM pgledger_accounts
WHERE id IN (:'account2_id',:'account1_id')
ORDER BY id;
