# pgledger

Double-entry bookkeeping ledger implemented entirely as PostgreSQL functions and views. No application-level code — install the SQL, call the functions.

## Architecture

Three tables, three views, and a small set of SQL functions:

- **pgledger_accounts** — each account has a name, currency, balance, version, and optional balance constraints
- **pgledger_transfers** — each transfer moves a positive amount from one account to another (same currency)
- **pgledger_entries** — two entries per transfer (debit and credit), recording previous and current balance

Views (`pgledger_accounts_view`, `pgledger_transfers_view`, `pgledger_entries_view`) are the public query interface. The entries view joins to transfers to include `event_at` and `metadata`.

All IDs are prefixed ULIDs: `pgla_*` (accounts), `pglt_*` (transfers), `pgle_*` (entries). Generated from UUIDv7 converted to ULID.

## Core API

```sql
-- Create an account (returns pgledger_accounts_view row)
pgledger_create_account(
    name TEXT,
    currency TEXT,
    allow_negative_balance BOOLEAN DEFAULT TRUE,
    allow_positive_balance BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT NULL
) → SETOF pgledger_accounts_view

-- Create a single transfer (returns pgledger_transfers_view row)
pgledger_create_transfer(
    from_account_id TEXT,
    to_account_id TEXT,
    amount NUMERIC,
    event_at TIMESTAMPTZ DEFAULT NULL,
    metadata JSONB DEFAULT NULL
) → SETOF pgledger_transfers_view

-- Create multiple transfers atomically (returns pgledger_transfers_view rows)
pgledger_create_transfers(
    transfer_requests TRANSFER_REQUEST[],
    event_at TIMESTAMPTZ DEFAULT NULL,
    metadata JSONB DEFAULT NULL
) → SETOF pgledger_transfers_view

-- VARIADIC convenience form
pgledger_create_transfers(
    VARIADIC transfer_requests TRANSFER_REQUEST[]
) → SETOF pgledger_transfers_view
```

`transfer_request` is a composite type: `(from_account_id TEXT, to_account_id TEXT, amount NUMERIC)`.

## Key constraints and design decisions

- Amounts must be positive finite numbers — `0`, negatives, `NULL`, `NaN`, `Infinity`, and `-Infinity` are rejected; direction comes from from/to
- from_account_id must differ from to_account_id
- A `NULL` account id, and a `NULL` `transfer_requests` array, are rejected with a pgledger message rather than a constraint or plpgsql error
- An account id that matches no account is rejected with `Account (id=…) does not exist` rather than a foreign key violation
- Transfers between accounts with different currencies are rejected
- Multi-currency exchange uses 4 accounts (2 user + 2 liquidity) and `pgledger_create_transfers`
- Accounts lock in sorted ID order to prevent deadlocks
- `event_at` records when the real-world event happened; `created_at` records when the ledger entry was written
- Balance constraints are checked after each update — set both `allow_negative_balance=false` and `allow_positive_balance=false` to freeze an account
- `pgledger_create_transfers` applies requests in array order and checks each account after every request, so batch order is significant — the same multiset can succeed in one order and fail in another
- Account `version` increments on every transfer touching that account

## Implementation

- `pgledger.sql` — the entire implementation (tables, views, functions)
- `vendor/scoville-pgsql-ulid/` — ULID↔UUID conversion functions (must be installed first)

## Development

Requires Docker (for PostgreSQL) and mise (for tool versions).

```bash
mise install           # install tool versions (go, uv, just, golangci-lint)
docker compose up      # start PostgreSQL
just check             # full suite: dbreset, clean, tidy, format-sql, test, lint
just property-tests    # Hegel property suite; run separately from check
```

`POSTGRES_PORT` (default 5432) sets the host port Docker Compose publishes and the port the Go tests and `performance_check` connect to. The justfile has `set dotenv-load`, so a `.env` file with `POSTGRES_PORT=5433` gives a checkout its own database container; real environment variables take precedence over `.env`.

Common commands:

| Command | What it does |
|---------|-------------|
| `just test` | Run the example tests (`go test -v ./test/` in `go/`) |
| `just property-tests` | Run the Hegel property suite (`go test ./propertytest/`) |
| `just property-tests-continuous 5m` | Soak the property suite for a duration |
| `just dbreset` | Drop, recreate, and reload the schema |
| `just lint` | Run deadcode, sqlfluff, and golangci-lint |
| `just format-sql` | Format SQL with sqlfluff |
| `just benchmark` | Run Go benchmarks |
| `just run-examples` | Execute example SQL scripts and update `.out` files |
| `just psql` | Open a psql shell |

## Testing

Tests are Go integration tests using pgx and testify. They connect to PostgreSQL at `postgres://pgledger:pgledger@localhost:5432/pgledger`, where the port comes from `POSTGRES_PORT` (default 5432) via `go/internal/dburl`. All tests call the SQL functions directly and verify results through the views.

- `go/test/` — example tests: one scenario per test, exact expected values.
- `go/propertytest/` — Hegel property tests: generators, an in-memory oracle (`model`) that predicts acceptance and balances, structural invariants over the views, and two state machines (sequential against the oracle, concurrent against conservation and the balance constraints). `PGLEDGER_HEGEL_CASES` (default 100) sets the per-test case budget; `PGLEDGER_SOAK_DURATION` (default 60s) bounds `propertytest/continuous`.

Shared helpers live in `go/testhelpers/`, which defines Go structs (Account, Transfer, Entry) mapping to the view columns plus helpers for creating accounts, transfers, and querying entries.

Tests run in parallel against a shared database, so property assertions are scoped to the account ids each test case created. The schema must be loaded before running tests (`just dbreset`).

PostgreSQL versions 15, 16, 17, and 18 are tested in CI.

## SQL formatting

SQL files are formatted with sqlfluff (via `uvx sqlfluff@4.0.0`). Configuration is in `.sqlfluff`.

## Examples

The `examples/` directory contains executable SQL scripts with `.sql.out` files showing their output:
- `basic-example.sql` — payment flow with receivables and pending accounts
- `multi-currency.sql` — currency exchange using liquidity accounts
- `lock-account.sql` — freezing an account via balance constraints
- `reconciliation.sql` — metadata-based reconciliation patterns
