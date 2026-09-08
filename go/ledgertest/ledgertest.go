// Package ledgertest is the shared database harness for the pgledger test
// suites: the example tests in test/ and the property tests in propertytest/.
package ledgertest

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/internal/dburl"
	"github.com/stretchr/testify/assert"
)

// TestingT is satisfied by *testing.T, *testing.B and *hegel.T, so the helpers
// below work from example tests, benchmarks and property tests alike.
type TestingT interface {
	Context() context.Context
	Cleanup(f func())
	Errorf(format string, args ...any)
	FailNow()
}

type Account struct {
	ID                   string
	Name                 string
	Currency             string
	Balance              string
	Version              int
	AllowNegativeBalance bool
	AllowPositiveBalance bool
	Metadata             *string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type Transfer struct {
	ID            string
	FromAccountID string
	ToAccountID   string
	Amount        string
	CreatedAt     time.Time
	EventAt       time.Time
	Metadata      *string
}

type Entry struct {
	ID                     string
	AccountID              string
	TransferID             string
	Amount                 string
	AccountPreviousBalance string
	AccountCurrentBalance  string
	AccountVersion         int
	CreatedAt              time.Time
	EventAt                time.Time
	Metadata               *string
}

func Setup(t *testing.T) *pgxpool.Pool {
	t.Parallel()
	return Dbconn(t)
}

func Dbconn(t TestingT) *pgxpool.Pool {
	dbpool, err := Connect(t.Context())
	assert.NoError(t, err)

	t.Cleanup(dbpool.Close)

	return dbpool
}

// Connect opens a pool without registering any cleanup, for callers that have
// no TestingT to hang one on (the continuous property-test binary).
func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, dburl.URL())
}

func CreateAccount(t TestingT, conn *pgxpool.Pool, name string, currency string) *Account {
	return QueryOne[Account](t, conn, "select * from pgledger_create_account($1, $2)", name, currency)
}

func GetAccount(t TestingT, conn *pgxpool.Pool, id string) *Account {
	return QueryOne[Account](t, conn, "select * from pgledger_accounts_view where id = $1", id)
}

func GetTransfer(t TestingT, conn *pgxpool.Pool, id string) *Transfer {
	return QueryOne[Transfer](t, conn, "select * from pgledger_transfers_view where id = $1", id)
}

func CreateTransfer(t TestingT, conn *pgxpool.Pool, fromAccountID, toAccountID, amount string) *Transfer {
	transfer, err := CreateTransferReturnErr(t.Context(), conn, fromAccountID, toAccountID, amount)
	assert.NoError(t, err)

	return transfer
}

func CreateTransferReturnErr(ctx context.Context, conn *pgxpool.Pool, fromAccountID, toAccountID, amount string) (*Transfer, error) {
	rows, err := conn.Query(ctx, "select * from pgledger_create_transfers(($1, $2, $3))", fromAccountID, toAccountID, amount)
	if err != nil {
		return nil, err
	}

	transfer, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Transfer])
	if err != nil {
		return nil, err
	}

	return transfer, nil
}

func GetEntries(t TestingT, conn *pgxpool.Pool, accountID string) []Entry {
	rows, err := conn.Query(t.Context(), "select * from pgledger_entries_view where account_id = $1 order by id", accountID)
	assert.NoError(t, err)

	entries, err := pgx.CollectRows(rows, pgx.RowToStructByName[Entry])
	assert.NoError(t, err)

	return entries
}

func QueryOne[T any](t TestingT, conn *pgxpool.Pool, sql string, args ...any) *T {
	rows, err := conn.Query(t.Context(), sql, args...)
	assert.NoError(t, err)

	account, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[T])
	assert.NoError(t, err)

	return account
}
