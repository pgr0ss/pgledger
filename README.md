# pgledger

A [double entry ledger](https://en.wikipedia.org/wiki/Double-entry_bookkeeping) implementation in PostgreSQL.

For more information on the background and rationale for this project, check out these blog posts:

- [Ledger Implementation in PostgreSQL](https://pgrs.net/2025/03/24/pgledger-ledger-implementation-in-postgresql/)
- [A Ledger In PostgreSQL Is Fast!](https://pgrs.net/2025/05/16/pgledger-in-postgresql-is-fast/)
- [Double-Entry Ledgers: The Missing Primitive in Modern Software](https://pgrs.net/2025/06/17/double-entry-ledgers-missing-primitive-in-modern-software/)

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
select name, balance, version from pgledger_accounts_view where id = $account_2_id;

   name    | balance | version
-----------+---------+---------
 account_2 |   69.12 |       2
```

See ledger entries:

```sql
select created_at, account_version, amount, account_previous_balance, account_current_balance from pgledger_entries_view where account_id = $account_2_id order by id;

          created_at           | account_version | amount | account_previous_balance | account_current_balance
-------------------------------+-----------------+--------+--------------------------+-------------------------
 2025-04-28 04:24:50.787722+00 |               1 |  12.34 |                     0.00 |                   12.34
 2025-04-28 04:24:53.25815+00  |               2 |  56.78 |                    12.34 |                   69.12
(2 rows)
```

### Event Timestamp

Transfers take an optional `event_at`, which should be used to record when the ledgerable event occurred if it is not now. For example, if you are recording ledger transfers in response to webhooks (such as money arriving in your bank account), then you can set the `event_at` to be the timestamp from the webhook.

If `event_at` is not provided, it is defaulted to `now()`, the same as the `created_at`:

```sql
-- No explicit event_at, so event_at == created_at:
select created_at, event_at from pgledger_create_transfer($account_1_id, $account_2_id, 12.34);

          created_at           |           event_at
-------------------------------+-------------------------------
 2025-07-02 23:35:56.666739+00 | 2025-07-02 23:35:56.666739+00

-- With explicit event_at:
select created_at, event_at from pgledger_create_transfer($account_1_id, $account_2_id, 12.34, '2025-07-01T12:34:56Z');

          created_at           |        event_at
-------------------------------+------------------------
 2025-07-02 23:36:16.511164+00 | 2025-07-01 12:34:56+00

-- Or with the => syntax:
select created_at, event_at from pgledger_create_transfer($account_1_id, $account_2_id, 12.34, event_at => '2025-07-01T12:34:56Z');
```

The `event_at` provides extra information in the ledger about when the real-world events happened, which may be far in the past. And it can help when querying entries for a defined time period.

For example, let's say you have a ledger account which represents a real-world bank account. When the bank sends you webhooks, you record ledger transfers. The ledger account history should match the bank account history.

Now say that on July 1 at 12:05 am you receive a webhook that money arrived in the account on June 30 at 11:55 pm. You record the ledger transfer, but the `created_at` is now `2025-07-01T00:05:00Z`. When you get the June bank statement, it will include this transfer, but when you query your ledger for transfers in June, it will not.

This is where even though you received the webhook on July 1, it should contain the timestamp of the event itself, so you can set that as the `event_at` in the ledger transfer. Now, when comparing ledger transfers or entries against the bank statement, you can query for all transfers where the `event_at` is in June, not the `created_at`:

```sql
select * from pgledger_entries_view
where account_id = $account_id
and event_at >= '2025-06-01'
and event_at < '2025-07-01'
order by event_at;
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

ULIDs are generated by first generating a [UUIDv7](<https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_7_(timestamp_and_random)>) (time-based, ordered UUID) and then converting that to a ULID via the SQL functions from https://github.com/scoville/pgsql-ulid (included in the [vendor](vendor) directory).

### Historical Balances

Each entry row records the previous and current balance for the account. This means you can look up historical account balances by finding the most recent row before the desired time.

There's an example test to show this behavior:

https://github.com/pgr0ss/pgledger/blob/1352114895bd4dcf44b0789751bed698203348ec/go/test/db_test.go#L479-L528

### Performance

Performance is a notoriously hard thing to measure, since different usage patterns and different hardware can yield very different results. I have been iterating on a script in this repository to help measure performance: [performance_check.go](go/performance_check.go), so this may be a good starting point if you want to measure performance in your own setup. The numbers included below are only a guideline.

Here are some baseline performance numbers on my M3 Macbook Air, with a vanilla, unoptimized PostgreSQL. I set up PostgreSQL with:

```bash
brew install postgresql@17
brew services start postgresql@17
```

The script can be configured with different numbers of workers and accounts. Each worker runs a loop where it picks 2 accounts at random and creates a transfer between them. To simulate a scenario where there isn't much account contention, we can ensure there are many more accounts than workers. That way, concurrent workers rarely try to transfer between the same accounts. For example:

```bash
> go run performance_check.go --accounts=50 --workers=20 --duration=30s

Completed transfers: 319105
Elapsed time in seconds: 30.0
Database size before: 1795 MB
Database size after:  2021 MB
Database size growth in bytes: 237223936
Transfers/second: 10636.8
Milliseconds/transfer: 1.9
Bytes/transfer: 743
```

We can also simulate more account contention, where workers may need to wait on other workers currently using the same accounts:

```bash
> go run performance_check.go --accounts=10 --workers=20 --duration=30s

Completed transfers: 226767
Elapsed time in seconds: 30.0
Database size before: 2021 MB
Database size after:  2182 MB
Database size growth in bytes: 168566784
Transfers/second: 7558.9
Milliseconds/transfer: 2.6
Bytes/transfer: 743
```

## TODO

- Make the transfer and account views include the account names as well as the ids
- Make create_transfers function return account balances as well
- Add metadata to accounts and transfers - json column?
  - Once we have transfer metadata, update receivables example to show how we can group by payment_id to see when incoming and outgoing don't match
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
- Potential performance improvements
  - Since ULIDs have embedded time, do we need created_at columns? Could we use a virtual generated column instead?
  - We could also deconstruct the ULID and store it as a UUID, but would need a special type or view to reconstruct the prefixed ULID form
  - Do we need from/to accounts on transfers? Could we make a queryable view for transfers instead which queries from the entries?
  - If event_date is not provided, we could store it as null (saving 8 bytes?), and then update the view to do a `COALESCE(event_at, created_at)`. We would need an index on the COALESCE statement, so not sure how much it would actually save
