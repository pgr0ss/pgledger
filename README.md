# pgledger

A ledger implementation in PostgreSQL

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
