package test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// Make an interface so I can use dbconn with both testing.T and testing.B
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

func setupTest(t *testing.T) *pgxpool.Pool {
	t.Parallel()
	return dbconn(t)
}

func dbconn(t TestingT) *pgxpool.Pool {
	dbpool, err := pgxpool.New(context.Background(), "postgres://pgledger:pgledger@localhost:5432/pgledger")
	assert.NoError(t, err)

	t.Cleanup(dbpool.Close)

	return dbpool
}

func createAccount(t TestingT, conn *pgxpool.Pool, name string, currency string) *Account {
	rows, err := conn.Query(t.Context(), "select * from pgledger_create_account($1, $2)", name, currency)
	assert.NoError(t, err)

	account, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Account])
	assert.NoError(t, err)

	return account
}

func getAccount(t TestingT, conn *pgxpool.Pool, id string) *Account {
	rows, err := conn.Query(t.Context(), "select * from pgledger_accounts_view where id = $1", id)
	assert.NoError(t, err)

	account, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Account])
	assert.NoError(t, err)

	return account
}

func getTransfer(t TestingT, conn *pgxpool.Pool, id string) *Transfer {
	rows, err := conn.Query(t.Context(), "select * from pgledger_transfers_view where id = $1", id)
	assert.NoError(t, err)

	transfer, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Transfer])
	assert.NoError(t, err)

	return transfer
}

func createTransfer(t TestingT, conn *pgxpool.Pool, fromAccountID, toAccountID, amount string) *Transfer {
	transfer, err := createTransferReturnErr(t.Context(), conn, fromAccountID, toAccountID, amount)
	assert.NoError(t, err)

	return transfer
}

func createTransferReturnErr(ctx context.Context, conn *pgxpool.Pool, fromAccountID, toAccountID, amount string) (*Transfer, error) {
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

func getEntries(t TestingT, conn *pgxpool.Pool, accountID string) []Entry {
	rows, err := conn.Query(t.Context(), "select * from pgledger_entries_view where account_id = $1 order by id", accountID)
	assert.NoError(t, err)

	entries, err := pgx.CollectRows(rows, pgx.RowToStructByName[Entry])
	assert.NoError(t, err)

	return entries
}
