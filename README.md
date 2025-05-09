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
select created_at, account_version, amount, account_previous_balance, account_current_balance from pgledger_entries where account_id = $account_2_id order by id;

          created_at           | account_version | amount | account_previous_balance | account_current_balance
-------------------------------+-----------------+--------+--------------------------+-------------------------
 2025-04-28 04:24:50.787722+00 |               1 |  12.34 |                     0.00 |                   12.34
 2025-04-28 04:24:53.25815+00  |               2 |  56.78 |                    12.34 |                   69.12
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

### IDs

IDs for all tables are represented as prefixed [ULIDs](https://github.com/ulid/spec), such as `pgla_01JTVST7XAES5BXHWZN4KR4VEZ` for a ledger account and `pglt_01JTVR1WKXEKCRG7N6YD7XCZA6` for a ledger transfer.

These prefixed ULIDs have the following benefits:

- The prefix makes it easy to see what kind of ID it is, so you are less likely to use the wrong kind of ID
- The values are monotonically increasing, which means the IDs are generated in sorted order
- ULIDs can be converted into UUIDs, so there can be future optimizations where we store the underlying values as UUIDs instead of TEXT to save space
- ULIDs have a nicer format than UUIDs (e.g. URL safe and shorter)

For more info about prefixed ULIDs as Ids, check out this blog post: [ULID Identifiers and ULID Tools Website](https://pgrs.net/2023/01/10/ulid-identifiers-and-ulid-tools-website/).

## TODO

- Add effective date to transfers (for when the transfer is recorded now, but it's related to something from the past)
- Make create_transfers function return account balances as well
- Add metadata to accounts and transfers - json column?
- Better primary keys
  - Currently using UUIDv7, but would love a prefixed ULID or similar. Prefixes like `pgla`, `pglt`, `pgle`, etc.
  - If we use a type with an embedded time, do we need a `created_at` column? Or maybe we could add a view that includes it by parsing out the UUIDv7/ULID?
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
- Add a function to get account balance at a specific point in time (select balance from most recent entry before the time)
