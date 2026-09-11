package test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/pgr0ss/pgledger/ledgertest"
)

func TestAddAccount(t *testing.T) {
	conn := ledgertest.Setup(t)

	account := ledgertest.CreateAccount(t, conn, "account 1", "USD")

	assert.Regexp(t, "^pgla_\\w+$", account.ID)
	assert.Equal(t, "account 1", account.Name)
	assert.Equal(t, "USD", account.Currency)
	assert.Equal(t, "0", account.Balance)
	assert.Equal(t, 0, account.Version)
	assert.WithinDuration(t, time.Now(), account.CreatedAt, time.Minute)
	assert.Equal(t, account.CreatedAt, account.UpdatedAt)
}

func TestAccountsThatCannotBeNegative(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account('positive-only', 'USD', allow_negative_balance => false)")
	account2 := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account('account 2', 'USD')")

	_, err := ledgertest.CreateTransferReturnErr(t.Context(), conn, account1.ID, account2.ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", account1.ID, "positive-only"))

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestAccountsThatCannotBePositive(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.QueryOne[ledgertest.Account](t, conn, `SELECT * FROM pgledger_create_account('negative-only', 'USD', allow_positive_balance => false)`)
	account2 := ledgertest.QueryOne[ledgertest.Account](t, conn, `SELECT * FROM pgledger_create_account('account 2', 'USD')`)

	_, err := ledgertest.CreateTransferReturnErr(t.Context(), conn, account2.ID, account1.ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", account1.ID, "negative-only"))

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestAccountMetadata(t *testing.T) {
	conn := ledgertest.Setup(t)

	// No metadata
	account1 := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account($1, $2)", "no-metadata", "USD")

	// With regular parameter
	account2 := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account($1, $2, $3, $4, $5)", "no-metadata", "USD", true, true, `{"a": "b"}`)

	// With named parameter
	account3 := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account($1, $2, metadata => $3)", "no-metadata", "USD", `{"c": "d"}`)

	assert.Nil(t, account1.Metadata)
	assert.Equal(t, `{"a": "b"}`, *account2.Metadata)
	assert.Equal(t, `{"c": "d"}`, *account3.Metadata)
}

func TestCreateTransfer(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	transfer := ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "12.34")

	assert.Regexp(t, "^pglt_\\w+$", transfer.ID)
	assert.Equal(t, account1.ID, transfer.FromAccountID)
	assert.Equal(t, account2.ID, transfer.ToAccountID)
	assert.Equal(t, "12.34", transfer.Amount)
	assert.WithinDuration(t, time.Now(), transfer.CreatedAt, time.Minute)

	foundTransfer := ledgertest.GetTransfer(t, conn, transfer.ID)
	assert.Regexp(t, transfer.ID, foundTransfer.ID)
	assert.Equal(t, account1.ID, foundTransfer.FromAccountID)
	assert.Equal(t, account2.ID, foundTransfer.ToAccountID)
	assert.Equal(t, "12.34", foundTransfer.Amount)
	assert.WithinDuration(t, time.Now(), foundTransfer.CreatedAt, time.Minute)

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "-12.34", foundAccount1.Balance)
	assert.Equal(t, "12.34", foundAccount2.Balance)
	assert.Equal(t, 1, foundAccount1.Version)
	assert.Equal(t, 1, foundAccount2.Version)
	assert.Greater(t, foundAccount1.UpdatedAt, foundAccount1.CreatedAt)
	assert.Greater(t, foundAccount2.UpdatedAt, foundAccount2.CreatedAt)
}

func TestCreateTransferWithAndWithoutEventAt(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	eventAt, err := time.Parse(time.RFC3339, "2025-07-01T12:34:56Z")
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10)", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10, $3)", account1.ID, account2.ID, eventAt)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10, event_at => $3)", account1.ID, account2.ID, eventAt)
	assert.NoError(t, err)

	rows, err := conn.Query(t.Context(), "select * from pgledger_transfers where from_account_id = $1", account1.ID)
	assert.NoError(t, err)

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ledgertest.Transfer])
	assert.NoError(t, err)

	assert.Len(t, transfers, 3)

	assert.WithinDuration(t, time.Now(), transfers[0].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[1].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[2].CreatedAt, time.Minute)

	assert.Equal(t, transfers[0].CreatedAt, transfers[0].EventAt) // Defaults to now(), matching created_at
	assert.Equal(t, eventAt, transfers[1].EventAt.UTC())
	assert.Equal(t, eventAt, transfers[2].EventAt.UTC())

	// Entries view also has EventAt field
	entries := ledgertest.GetEntries(t, conn, account1.ID)
	assert.NoError(t, err)
	assert.Len(t, entries, 3)

	assert.Equal(t, entries[0].CreatedAt, entries[0].EventAt)
	assert.Equal(t, eventAt, entries[1].EventAt.UTC())
	assert.Equal(t, eventAt, entries[2].EventAt.UTC())
}

func TestCreateTransfersWithAndWithoutEventAt(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	eventAt, err := time.Parse(time.RFC3339, "2025-07-01T12:34:56Z")
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfers(($1, $2, 10))", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfers(array[($1, $2, 10)::transfer_request], $3)", account1.ID, account2.ID, eventAt)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfers(array[($1, $2, 10), ($1, $2, 10)]::transfer_request[], $3)", account1.ID, account2.ID, eventAt)
	assert.NoError(t, err)

	// With named parameters
	_, err = conn.Exec(t.Context(), `select pgledger_create_transfers(
			event_at => $3,
 			transfer_requests => array[
				($1, $2, '30'),
				($1, $2, '40')
			]::transfer_request[])`,
		account1.ID, account2.ID, eventAt)
	assert.NoError(t, err)

	rows, err := conn.Query(t.Context(), "select * from pgledger_transfers where from_account_id = $1", account1.ID)
	assert.NoError(t, err)

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ledgertest.Transfer])
	assert.NoError(t, err)

	assert.Len(t, transfers, 6)

	assert.WithinDuration(t, time.Now(), transfers[0].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[1].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[2].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[3].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[4].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[5].CreatedAt, time.Minute)

	assert.Equal(t, transfers[0].CreatedAt, transfers[0].EventAt) // Defaults to now(), matching created_at
	assert.Equal(t, eventAt, transfers[1].EventAt.UTC())
	assert.Equal(t, eventAt, transfers[2].EventAt.UTC())
	assert.Equal(t, eventAt, transfers[3].EventAt.UTC())
	assert.Equal(t, eventAt, transfers[4].EventAt.UTC())
	assert.Equal(t, eventAt, transfers[5].EventAt.UTC())
}

func TestCreateTransferWithAndWithoutMetadata(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	_, err := conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10)", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10, null, $3)", account1.ID, account2.ID, `{"a": "b"}`)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10, metadata => $3)", account1.ID, account2.ID, `{"c": "d"}`)
	assert.NoError(t, err)

	rows, err := conn.Query(t.Context(), "select * from pgledger_transfers where from_account_id = $1", account1.ID)
	assert.NoError(t, err)

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ledgertest.Transfer])
	assert.NoError(t, err)

	assert.Len(t, transfers, 3)

	assert.Nil(t, transfers[0].Metadata)
	assert.Equal(t, `{"a": "b"}`, *transfers[1].Metadata)
	assert.Equal(t, `{"c": "d"}`, *transfers[2].Metadata)

	// Entries view also has Metadata field
	entries := ledgertest.GetEntries(t, conn, account1.ID)
	assert.NoError(t, err)
	assert.Len(t, entries, 3)

	assert.Nil(t, entries[0].Metadata)
	assert.Equal(t, `{"a": "b"}`, *entries[1].Metadata)
	assert.Equal(t, `{"c": "d"}`, *entries[2].Metadata)
}

func TestCreateTransfersWithAndWithoutMetadata(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	_, err := conn.Exec(t.Context(), "select pgledger_create_transfers(($1, $2, 10))", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfers(array[($1, $2, 10)::transfer_request], null, $3)", account1.ID, account2.ID, `{"a": "b"}`)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfers(array[($1, $2, 10), ($1, $2, 10)]::transfer_request[], null, $3)", account1.ID, account2.ID, `{"c": "d"}`)
	assert.NoError(t, err)

	// With named parameters
	_, err = conn.Exec(t.Context(), `select pgledger_create_transfers(
			metadata => $3,
 			transfer_requests => array[
				($1, $2, '30'),
				($1, $2, '40')
			]::transfer_request[])`,
		account1.ID, account2.ID, `{"e": "f"}`)
	assert.NoError(t, err)

	rows, err := conn.Query(t.Context(), "select * from pgledger_transfers where from_account_id = $1", account1.ID)
	assert.NoError(t, err)

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ledgertest.Transfer])
	assert.NoError(t, err)

	assert.Len(t, transfers, 6)

	assert.Nil(t, transfers[0].Metadata)
	assert.Equal(t, `{"a": "b"}`, *transfers[1].Metadata)
	assert.Equal(t, `{"c": "d"}`, *transfers[2].Metadata)
	assert.Equal(t, `{"c": "d"}`, *transfers[3].Metadata)
	assert.Equal(t, `{"e": "f"}`, *transfers[4].Metadata)
	assert.Equal(t, `{"e": "f"}`, *transfers[5].Metadata)
}

func TestCreateMultipleTransfers(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")
	account3 := ledgertest.CreateAccount(t, conn, "account 3", "USD")

	_, err := conn.Exec(t.Context(), `
		select * from pgledger_create_transfers(
			($1, $2, '10'),
			($2, $3, '20'),
			($3, $1, '50'))`,
		account1.ID, account2.ID, account3.ID)
	assert.NoError(t, err)

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, account2.ID)
	foundAccount3 := ledgertest.GetAccount(t, conn, account3.ID)

	assert.Equal(t, "40", foundAccount1.Balance)
	assert.Equal(t, "-10", foundAccount2.Balance)
	assert.Equal(t, "-30", foundAccount3.Balance)

	assert.Equal(t, 2, foundAccount1.Version)
	assert.Equal(t, 2, foundAccount2.Version)
	assert.Equal(t, 2, foundAccount3.Version)
}

func TestMultipleTransfersRollsBackIfOneIsBad(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	account3 := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account('negative-only', 'USD', allow_positive_balance => false)")

	_, err := conn.Exec(t.Context(), `
		select * from pgledger_create_transfers(
			($1, $2, '10'),
			($2, $3, '20'),
			($3, $1, '50'))`,
		account1.ID, account2.ID, account3.ID)
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", account3.ID, "negative-only"))

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, account2.ID)
	foundAccount3 := ledgertest.GetAccount(t, conn, account3.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
	assert.Equal(t, "0", foundAccount3.Balance)

	assert.Equal(t, 0, foundAccount1.Version)
	assert.Equal(t, 0, foundAccount2.Version)
	assert.Equal(t, 0, foundAccount3.Version)
}

func TestTransfersRollbackIfTransctionRollback(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	tx, err := conn.Begin(t.Context())
	assert.NoError(t, err)

	_, err = tx.Exec(t.Context(), "select * from pgledger_create_transfers(($1, $2, '10'))", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = tx.Exec(t.Context(), "select 1/0")
	assert.ErrorContains(t, err, "division by zero")

	err = tx.Commit(t.Context())
	assert.ErrorContains(t, err, "rollback")

	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, account1.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, account2.ID).Balance)
}

func TestCreateMultipleTransfersRollbackOnFailure(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")

	positiveOnlyAccount := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account('positive-only', 'USD', allow_negative_balance => false)")

	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		BEGIN;
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10');
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '20');
		select * from pgledger_create_transfer('%[2]s', '%[1]s', '50');
		COMMIT;
		`, account1.ID, positiveOnlyAccount.ID))
	assert.ErrorContains(t, err, "does not allow negative balance")

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, positiveOnlyAccount.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)

	assert.Equal(t, 0, foundAccount1.Version)
	assert.Equal(t, 0, foundAccount2.Version)
}

func TestTransferWithInvalidAccountID(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")

	_, err := ledgertest.CreateTransferReturnErr(t.Context(), conn, account1.ID, "bad_id", "12.34")
	assert.ErrorContains(t, err, "violates foreign key constraint")

	_, err = ledgertest.CreateTransferReturnErr(t.Context(), conn, "bad_id", account1.ID, "12.34")
	assert.ErrorContains(t, err, "violates foreign key constraint")
}

func TestEntries(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	t1 := ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "5")
	t2 := ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "10")
	t3 := ledgertest.CreateTransfer(t, conn, account2.ID, account1.ID, "20")

	entries := ledgertest.GetEntries(t, conn, account1.ID)

	assert.Len(t, entries, 3)

	assert.Regexp(t, "^pgle_\\w+$", entries[0].ID)
	assert.Equal(t, t1.ID, entries[0].TransferID)
	assert.Equal(t, "-5", entries[0].Amount)
	assert.Equal(t, "0", entries[0].AccountPreviousBalance)
	assert.Equal(t, "-5", entries[0].AccountCurrentBalance)
	assert.Equal(t, 1, entries[0].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[0].CreatedAt, time.Minute)
	assert.Equal(t, entries[0].CreatedAt, entries[0].EventAt)

	assert.Regexp(t, "^pgle_\\w+$", entries[1].ID)
	assert.Equal(t, t2.ID, entries[1].TransferID)
	assert.Equal(t, "-10", entries[1].Amount)
	assert.Equal(t, "-5", entries[1].AccountPreviousBalance)
	assert.Equal(t, "-15", entries[1].AccountCurrentBalance)
	assert.Equal(t, 2, entries[1].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[1].CreatedAt, time.Minute)
	assert.Equal(t, entries[1].CreatedAt, entries[1].EventAt)

	assert.Regexp(t, "^pgle_\\w+$", entries[2].ID)
	assert.Equal(t, t3.ID, entries[2].TransferID)
	assert.Equal(t, "20", entries[2].Amount)
	assert.Equal(t, "-15", entries[2].AccountPreviousBalance)
	assert.Equal(t, "5", entries[2].AccountCurrentBalance)
	assert.Equal(t, 3, entries[2].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[2].CreatedAt, time.Minute)
	assert.Equal(t, entries[2].CreatedAt, entries[2].EventAt)

	entries = ledgertest.GetEntries(t, conn, account2.ID)

	assert.Len(t, entries, 3)

	assert.Regexp(t, "^pgle_\\w+$", entries[0].ID)
	assert.Equal(t, t1.ID, entries[0].TransferID)
	assert.Equal(t, "5", entries[0].Amount)
	assert.Equal(t, "0", entries[0].AccountPreviousBalance)
	assert.Equal(t, "5", entries[0].AccountCurrentBalance)
	assert.Equal(t, 1, entries[0].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[0].CreatedAt, time.Minute)
	assert.Equal(t, entries[0].CreatedAt, entries[0].EventAt)

	assert.Regexp(t, "^pgle_\\w+$", entries[1].ID)
	assert.Equal(t, t2.ID, entries[1].TransferID)
	assert.Equal(t, "10", entries[1].Amount)
	assert.Equal(t, "5", entries[1].AccountPreviousBalance)
	assert.Equal(t, "15", entries[1].AccountCurrentBalance)
	assert.Equal(t, 2, entries[1].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[1].CreatedAt, time.Minute)
	assert.Equal(t, entries[1].CreatedAt, entries[1].EventAt)

	assert.Regexp(t, "^pgle_\\w+$", entries[2].ID)
	assert.Equal(t, t3.ID, entries[2].TransferID)
	assert.Equal(t, "-20", entries[2].Amount)
	assert.Equal(t, "15", entries[2].AccountPreviousBalance)
	assert.Equal(t, "-5", entries[2].AccountCurrentBalance)
	assert.Equal(t, 3, entries[2].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[2].CreatedAt, time.Minute)
	assert.Equal(t, entries[2].CreatedAt, entries[2].EventAt)
}

func TestTransferAmountsMustBePositiveAndFinite(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	transfer := ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "10")
	assert.Equal(t, "10", transfer.Amount)

	// NaN and Infinity compare greater than every finite number, so a bare
	// amount > 0 guard accepts them and the balance becomes unrecoverable.
	for _, amount := range []string{"0", "-0.01", "NaN", "Infinity", "-Infinity"} {
		_, err := ledgertest.CreateTransferReturnErr(t.Context(), conn, account1.ID, account2.ID, amount)
		assert.ErrorContains(t, err, fmt.Sprintf("Amount (%s) must be a positive finite number", amount))
	}

	_, err := conn.Exec(t.Context(),
		"select * from pgledger_create_transfers(($1::text, $2::text, $3::numeric))",
		account1.ID, account2.ID, nil)
	assert.ErrorContains(t, err, "Amount (<NULL>) must be a positive finite number")

	assert.Equal(t, "-10", ledgertest.GetAccount(t, conn, account1.ID).Balance)
	assert.Equal(t, "10", ledgertest.GetAccount(t, conn, account2.ID).Balance)
}

func TestCannotTransferBetweenDifferentCurrencies(t *testing.T) {
	conn := ledgertest.Setup(t)

	// Create two accounts with different currencies
	accountUSD := ledgertest.CreateAccount(t, conn, "USD account", "USD")
	accountEUR := ledgertest.CreateAccount(t, conn, "EUR account", "EUR")

	_, err := ledgertest.CreateTransferReturnErr(t.Context(), conn, accountUSD.ID, accountEUR.ID, "10.00")
	assert.ErrorContains(t, err, "Cannot transfer between different currencies (USD and EUR)")

	// Verify account balances remain unchanged
	foundAccountUSD := ledgertest.GetAccount(t, conn, accountUSD.ID)
	foundAccountEUR := ledgertest.GetAccount(t, conn, accountEUR.ID)

	assert.Equal(t, "0", foundAccountUSD.Balance)
	assert.Equal(t, "0", foundAccountEUR.Balance)
}

func TestTransferBetweenCurrenciesWithExtraAccounts(t *testing.T) {
	conn := ledgertest.Setup(t)

	// Create two accounts with different currencies
	userUSD := ledgertest.CreateAccount(t, conn, "user.USD", "USD")
	userEUR := ledgertest.CreateAccount(t, conn, "user.EUR", "EUR")

	// Liquidity accounts for the conversion
	liquidityUSD := ledgertest.CreateAccount(t, conn, "liquidity.USD", "USD")
	liquidityEUR := ledgertest.CreateAccount(t, conn, "liquidity.EUR", "EUR")

	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		BEGIN;
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10.00');
		select * from pgledger_create_transfer('%[3]s', '%[4]s', '9.26');
		COMMIT;
		`, userUSD.ID, liquidityUSD.ID, liquidityEUR.ID, userEUR.ID))
	assert.NoError(t, err)

	assert.Equal(t, "-10.00", ledgertest.GetAccount(t, conn, userUSD.ID).Balance)
	assert.Equal(t, "10.00", ledgertest.GetAccount(t, conn, liquidityUSD.ID).Balance)
	assert.Equal(t, "-9.26", ledgertest.GetAccount(t, conn, liquidityEUR.ID).Balance)
	assert.Equal(t, "9.26", ledgertest.GetAccount(t, conn, userEUR.ID).Balance)
}

func TestTransfersUseDifferentAccounts(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")

	_, err := ledgertest.CreateTransferReturnErr(t.Context(), conn, account1.ID, account1.ID, "10")
	assert.ErrorContains(t, err, fmt.Sprintf("Cannot transfer to the same account (id=%s)", account1.ID))
}

func TestConcurrency(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 500 {
			_ = ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "100")
		}
	})

	wg.Go(func() {
		for range 500 {
			_ = ledgertest.CreateTransfer(t, conn, account2.ID, account1.ID, "100")
		}
	})

	// Wait for all goroutines to complete
	wg.Wait()

	foundAccount1 := ledgertest.GetAccount(t, conn, account1.ID)
	foundAccount2 := ledgertest.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestConcurrencyWithCurrencyExchange(t *testing.T) {
	conn := ledgertest.Setup(t)

	// Create two accounts with different currencies
	userUSD := ledgertest.CreateAccount(t, conn, "user.USD", "USD")
	userEUR := ledgertest.CreateAccount(t, conn, "user.EUR", "EUR")

	// Liquidity accounts for the conversion
	liquidityUSD := ledgertest.CreateAccount(t, conn, "liquidity.USD", "USD")
	liquidityEUR := ledgertest.CreateAccount(t, conn, "liquidity.EUR", "EUR")

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 500 {
			_, err := conn.Exec(t.Context(),
				"select * from pgledger_create_transfers(($1, $2, '10.00'), ($3, $4, '9.26'))",
				userUSD.ID, liquidityUSD.ID, liquidityEUR.ID, userEUR.ID)
			assert.NoError(t, err)
		}
	})

	wg.Go(func() {
		for range 500 {
			_, err := conn.Exec(t.Context(),
				"select * from pgledger_create_transfers(($1, $2, '9.26'), ($3, $4, '10.00'))",
				userEUR.ID, liquidityEUR.ID, liquidityUSD.ID, userUSD.ID)
			assert.NoError(t, err)
		}
	})

	// Wait for all goroutines to complete
	wg.Wait()

	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, userUSD.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, userEUR.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, liquidityUSD.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, liquidityEUR.ID).Balance)
}

func TestIdsAreMonotonic(t *testing.T) {
	conn := ledgertest.Setup(t)

	// This query generates a series of ids, and then checks their sort order
	// against the order in which they were generated
	sql := `select i, id, row_number() over(order by id) from
   (select i, pgledger_generate_id('prefix') as id from generate_series(1, 20) as i) as series
   order by i;`
	result, err := conn.Query(t.Context(), sql)
	assert.NoError(t, err)

	type Row struct {
		I         int
		ID        string
		RowNumber int
	}

	rows, err := pgx.CollectRows(result, pgx.RowToStructByName[Row])
	assert.NoError(t, err)

	assert.Len(t, rows, 20)

	for _, row := range rows {
		assert.Equal(t, row.I, row.RowNumber)
	}
}

func TestFindHistoricalBalanceAtGivenTime(t *testing.T) {
	conn := ledgertest.Setup(t)

	account1 := ledgertest.CreateAccount(t, conn, "account 1", "USD")
	account2 := ledgertest.CreateAccount(t, conn, "account 2", "USD")

	_ = ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "10")
	_ = ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "20")
	_ = ledgertest.CreateTransfer(t, conn, account1.ID, account2.ID, "50")

	entries := ledgertest.GetEntries(t, conn, account2.ID)
	assert.Len(t, entries, 3)

	// Normally, we would never update the ledger. But here I'm doing it to make testing easier.
	_, err := conn.Exec(t.Context(), "update pgledger_entries set created_at = $1 where id = $2", "2025-06-01T12:00:00Z", entries[0].ID)
	assert.NoError(t, err)
	_, err = conn.Exec(t.Context(), "update pgledger_entries set created_at = $1 where id = $2", "2025-06-01T13:00:00Z", entries[1].ID)
	assert.NoError(t, err)
	_, err = conn.Exec(t.Context(), "update pgledger_entries set created_at = $1 where id = $2", "2025-06-01T14:00:00Z", entries[2].ID)
	assert.NoError(t, err)

	// Current balance
	assert.Equal(t, "80", ledgertest.GetAccount(t, conn, account2.ID).Balance)

	// Historical balances
	assert.Equal(t, "10", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T12:00:00Z"))
	assert.Equal(t, "10", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T12:15:00Z"))
	assert.Equal(t, "30", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T13:00:00Z"))
	assert.Equal(t, "30", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T13:15:00Z"))
	assert.Equal(t, "80", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T14:00:00Z"))
	assert.Equal(t, "80", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T14:15:00Z"))
}

func TestBatchOrderIsSignificant(t *testing.T) {
	conn := ledgertest.Setup(t)

	createAccounts := func() (*ledgertest.Account, *ledgertest.Account, *ledgertest.Account) {
		x := ledgertest.CreateAccount(t, conn, "x", "USD")
		y := ledgertest.QueryOne[ledgertest.Account](t, conn, "select * from pgledger_create_account($1, $2, allow_negative_balance => false)", "y positive-only", "USD")
		z := ledgertest.CreateAccount(t, conn, "z", "USD")

		return x, y, z
	}

	// Balance constraints are checked after each request, not at the end of the
	// batch, so the same multiset of requests passes or fails depending on order.
	x, y, z := createAccounts()

	rows, err := conn.Query(t.Context(), `
		select * from pgledger_create_transfers(array[
			($1, $2, '10'),
			($2, $3, '10')
		]::transfer_request[])`,
		x.ID, y.ID, z.ID)
	assert.NoError(t, err)

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ledgertest.Transfer])
	assert.NoError(t, err)
	assert.Len(t, transfers, 2)
	assert.Equal(t, x.ID, transfers[0].FromAccountID)
	assert.Equal(t, y.ID, transfers[0].ToAccountID)
	assert.Equal(t, y.ID, transfers[1].FromAccountID)
	assert.Equal(t, z.ID, transfers[1].ToAccountID)

	assert.Equal(t, "-10", ledgertest.GetAccount(t, conn, x.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, y.ID).Balance)
	assert.Equal(t, "10", ledgertest.GetAccount(t, conn, z.ID).Balance)

	reversedX, reversedY, reversedZ := createAccounts()

	_, err = conn.Exec(t.Context(), `
		select * from pgledger_create_transfers(array[
			($2, $3, '10'),
			($1, $2, '10')
		]::transfer_request[])`,
		reversedX.ID, reversedY.ID, reversedZ.ID)
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", reversedY.ID, "y positive-only"))

	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, reversedX.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, reversedY.ID).Balance)
	assert.Equal(t, "0", ledgertest.GetAccount(t, conn, reversedZ.ID).Balance)

	assert.Equal(t, 0, ledgertest.GetAccount(t, conn, reversedX.ID).Version)
	assert.Equal(t, 0, ledgertest.GetAccount(t, conn, reversedY.ID).Version)
	assert.Equal(t, 0, ledgertest.GetAccount(t, conn, reversedZ.ID).Version)
}

func TestNullTransferRequestsArrayIsRejected(t *testing.T) {
	conn := ledgertest.Setup(t)

	// This is a raw plpgsql error leaking through rather than a pgledger
	// message, pinned here so replacing it becomes a deliberate change.
	rows, err := conn.Query(t.Context(), "select * from pgledger_create_transfers($1::transfer_request[])", nil)
	assert.NoError(t, err)

	_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[ledgertest.Transfer])
	assert.ErrorContains(t, err, "FOREACH expression must not be null")

	// SQLSTATE 22004 (null_value_not_allowed) rather than the P0001 a pgledger
	// RAISE EXCEPTION would produce.
	var pgErr *pgconn.PgError
	assert.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "22004", pgErr.Code)
}

func TestAccountNameAndCurrencyExtremes(t *testing.T) {
	conn := ledgertest.Setup(t)

	longName := strings.Repeat("a", 10*1024)
	unicodeName := "café ☕ 日本語 🏦 Ωμέγα"
	unicodeCurrency := strings.Repeat("₿🇯🇵日本円", 10)

	emptyNameAccount := ledgertest.CreateAccount(t, conn, "", "USD")
	longNameAccount := ledgertest.CreateAccount(t, conn, longName, "USD")
	unicodeAccount := ledgertest.CreateAccount(t, conn, unicodeName, unicodeCurrency)

	assert.Equal(t, "", ledgertest.GetAccount(t, conn, emptyNameAccount.ID).Name)
	assert.Equal(t, longName, ledgertest.GetAccount(t, conn, longNameAccount.ID).Name)
	assert.Equal(t, unicodeName, ledgertest.GetAccount(t, conn, unicodeAccount.ID).Name)
	assert.Equal(t, unicodeCurrency, ledgertest.GetAccount(t, conn, unicodeAccount.ID).Currency)

	otherUnicodeAccount := ledgertest.CreateAccount(t, conn, unicodeName+" 2", unicodeCurrency)

	transfer := ledgertest.CreateTransfer(t, conn, unicodeAccount.ID, otherUnicodeAccount.ID, "12.34")
	assert.Equal(t, "12.34", transfer.Amount)

	assert.Equal(t, "-12.34", ledgertest.GetAccount(t, conn, unicodeAccount.ID).Balance)
	assert.Equal(t, "12.34", ledgertest.GetAccount(t, conn, otherUnicodeAccount.ID).Balance)
}

func accountBalanceAtTime(t *testing.T, conn *pgxpool.Pool, accountID string, datetime string) string {
	rows, err := conn.Query(t.Context(), `
		select account_current_balance
		from pgledger_entries
		where account_id = $1
		and created_at <= $2
		order by created_at desc
		limit 1`,
		accountID, datetime)
	assert.NoError(t, err)

	balance, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	return balance
}
