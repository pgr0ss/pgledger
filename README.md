# pgledger

A [double entry ledger](https://en.wikipedia.org/wiki/Double-entry_bookkeeping) implementation in PostgreSQL.

For more information on the background and rationale for this project, check out the blog post: [Ledger Implementation in PostgreSQL](https://pgrs.net/2025/03/24/pgledger-ledger-implementation-in-postgresql/).

The implementation is currently in a single file: [pgledger.sql](/pgledger.sql).

## Usage

Set up your accounts:

```sql
select id from pgledger_create_account('account_1', 'USD'); -- save this as account_1_id
select id from pgledger_create_account('account_2', 'USD'); -- save this as account_2_id
```

Create transfers:

```sql
select * from pgledger_create_transfer($account_1_id, $account_2_id, 12.34);
select * from pgledger_create_transfer($account_1_id, $account_2_id, 56.78);
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

### Currencies

Each account is single currency. If you want to maintain balances in multiple currencies, use multiple accounts.

An exchange between two currencies will use 4 accounts, including 2 system or liquidity accounts (one for each currency). That way, the total debits and credits for each currency still add up to 0. For example, say a user has a USD and an EUR account:

```sql
select id from pgledger_create_account('user1.USD', 'USD');
select id from pgledger_create_account('user1.EUR', 'EUR');
```

You'll also need system or liquidity accounts for each currency:

```sql
select id from pgledger_create_account('liquidity.USD', 'USD');
select id from pgledger_create_account('liquidity.EUR', 'EUR');
```

Now, to transfer between the user's USD and EUR accounts, you create 2 simultaneous transfers (totalling 4 entries, 2 in each currency). You can use the `pgledger_create_transfers` function to create multiple transfers in a single call:

```sql
select * from pgledger_create_transfers(($user1_usd, $liquidity_usd, '10.00'), ($liquidity_eur, $user1_eur, '9.26'));
```

## TODO

- Add effective date to transfers (for when the transfer is recorded now, but it's related to something from the past)
- Make create_transfers function return account balances as well
- Add metadata to accounts and transfers - json column?
- Better primary keys - UUIDs? ULIDs? with prefixes or not? ideally monotonic
- Query via versioned views
  - `select * from pgledger_transfers_v1`
  - This way I can iterate on the underlying tables without breaking queries
  - Later, add `pgledger_transfers_v2`
  - Rename tables to be internal? `pgledger_internal_transfers`
  - Add version to functions? `pgledger_create_transfer_v1`
- Structure sql as a series of numbered migrations (e.g. pgledger_01.sql) so new versions can be applied to existing databases
  - Also dump an overall pgledger.sql so it's easier to review in one place
- Add postgres documentation comments?
- Show how name can be used like ltree: https://www.postgresql.org/docs/current/ltree.html
  - Allow create_transfers to take account name instead of id? Or a separate function for this?
