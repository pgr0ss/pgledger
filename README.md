# pgledger

A ledger implementation in PostgreSQL.

The implementation is currently in a single file: [pgledger.sql](/pgledger.sql).

## Usage

Set up your accounts:

```sql
select id from pgledger_create_account('account_1'); -- save this as account_1_id
select id from pgledger_create_account('account_2'); -- save this as account_2_id
```

Create transfers:

```sql
select * from pgledger_create_transfer($account_1_id, $account_2_id, 12.34);
select * from pgledger_create_transfer($account_1_id, $account_2_id, 56.78);

select * from pgledger_create_transfer(41, 42, 12.34);
select * from pgledger_create_transfer(41, 42, 56.78);
```

See updated balances:

```sql
select name, balance, version from pgledger_get_account($account_2_id);

   name    | balance | version
-----------+---------+---------
 account_2 |   69.12 |       2
```

See ledger entries:

```sql
select * from pgledger_entries where account_id = $account_2_id;

  id   | account_id | transfer_id | amount | account_previous_balance | account_current_balance | account_version |          created_at
-------+------------+-------------+--------+--------------------------+-------------------------+-----------------+-------------------------------
 96198 |         42 |       48103 |  12.34 |                     0.00 |                   12.34 |               1 | 2025-03-19 21:31:03.596426+00
 96200 |         42 |       48104 |  56.78 |                    12.34 |                   69.12 |               2 | 2025-03-19 21:31:21.615916+00
(2 rows)
```

## TODO

- Add more constraints
  - Transfer amounts are positive
  - From/To aren't the same account
- Add GitHub Action to run tests
  - Add golangci-lint?
  - Look into sql linter?
- Add effective date to transfers (for when the transfer is recorded now, but it's related to something from the past)
- Add metadata to accounts and transfers - json column?
- Better primary keys - UUIDs? ULIDs? with prefixes or not? ideally monotonic
- Query via versioned views
  - `select * from pgledger_transfers_v1`
  - This way I can iterate on the underlying tables without breaking queries
  - Later, add `pgledger_transfers_v2`
  - Rename tables to be internal? `pgledger_internal_transfers`
  - Add version to functions? `pgledger_create_transfer_v1`
- Add currency to accounts - don’t let transfers cross currency boundaries
- How to do currency conversions with 4 accounts?
  - With 2 separate transfers easy to get deadlocks if they are done in reverse - usd to eur and eur to usd
  - Write a test for concurrency with multiple currency conversions going in opposite directions
  - Do we need a function which is just for conversions?
  - Or a `create_transfers` function which takes multiple transfers, locks them all, and then executes them?
- Structure sql as a series of numbered migrations (e.g. pgledger_01.sql) so new versions can be applied to existing databases
  - Also dump an overall pgledger.sql so it's easier to review in one place
- Add postgres documentation comments?
- Show how name can be used like ltree: https://www.postgresql.org/docs/current/ltree.html
