package test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/pgr0ss/pgledger/testhelpers"
)

func TestAddAccount(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account := testhelpers.CreateAccount(t, conn, "account 1", "USD")

	assert.Regexp(t, "^pgla_\\w+$", account.ID)
	assert.Equal(t, "account 1", account.Name)
	assert.Equal(t, "USD", account.Currency)
	assert.Equal(t, "0", account.Balance)
	assert.Equal(t, 0, account.Version)
	assert.WithinDuration(t, time.Now(), account.CreatedAt, time.Minute)
	assert.Equal(t, account.CreatedAt, account.UpdatedAt)
}

func TestAccountsThatCannotBeNegative(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account('positive-only', 'USD', allow_negative_balance => false)")
	account2 := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account('account 2', 'USD')")

	_, err := testhelpers.CreateTransferReturnErr(t.Context(), conn, account1.ID, account2.ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", account1.ID, "positive-only"))

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestAccountsThatCannotBePositive(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.QueryOne[testhelpers.Account](t, conn, `SELECT * FROM pgledger_create_account('negative-only', 'USD', allow_positive_balance => false)`)
	account2 := testhelpers.QueryOne[testhelpers.Account](t, conn, `SELECT * FROM pgledger_create_account('account 2', 'USD')`)

	_, err := testhelpers.CreateTransferReturnErr(t.Context(), conn, account2.ID, account1.ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", account1.ID, "negative-only"))

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestAccountMetadata(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	// No metadata
	account1 := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account($1, $2)", "no-metadata", "USD")

	// With regular parameter
	account2 := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account($1, $2, $3, $4, $5)", "no-metadata", "USD", true, true, `{"a": "b"}`)

	// With named parameter
	account3 := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account($1, $2, metadata => $3)", "no-metadata", "USD", `{"c": "d"}`)

	assert.Nil(t, account1.Metadata)
	assert.Equal(t, `{"a": "b"}`, *account2.Metadata)
	assert.Equal(t, `{"c": "d"}`, *account3.Metadata)
}

func TestCreateTransfer(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	transfer := testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "12.34")

	assert.Regexp(t, "^pglt_\\w+$", transfer.ID)
	assert.Equal(t, account1.ID, transfer.FromAccountID)
	assert.Equal(t, account2.ID, transfer.ToAccountID)
	assert.Equal(t, "12.34", transfer.Amount)
	assert.WithinDuration(t, time.Now(), transfer.CreatedAt, time.Minute)

	foundTransfer := testhelpers.GetTransfer(t, conn, transfer.ID)
	assert.Regexp(t, transfer.ID, foundTransfer.ID)
	assert.Equal(t, account1.ID, foundTransfer.FromAccountID)
	assert.Equal(t, account2.ID, foundTransfer.ToAccountID)
	assert.Equal(t, "12.34", foundTransfer.Amount)
	assert.WithinDuration(t, time.Now(), foundTransfer.CreatedAt, time.Minute)

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "-12.34", foundAccount1.Balance)
	assert.Equal(t, "12.34", foundAccount2.Balance)
	assert.Equal(t, 1, foundAccount1.Version)
	assert.Equal(t, 1, foundAccount2.Version)
	assert.Greater(t, foundAccount1.UpdatedAt, foundAccount1.CreatedAt)
	assert.Greater(t, foundAccount2.UpdatedAt, foundAccount2.CreatedAt)
}

func TestCreateTransferWithAndWithoutEventAt(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

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

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[testhelpers.Transfer])
	assert.NoError(t, err)

	assert.Len(t, transfers, 3)

	assert.WithinDuration(t, time.Now(), transfers[0].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[1].CreatedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), transfers[2].CreatedAt, time.Minute)

	assert.Equal(t, transfers[0].CreatedAt, transfers[0].EventAt) // Defaults to now(), matching created_at
	assert.Equal(t, eventAt, transfers[1].EventAt.UTC())
	assert.Equal(t, eventAt, transfers[2].EventAt.UTC())

	// Entries view also has EventAt field
	entries := testhelpers.GetEntries(t, conn, account1.ID)
	assert.NoError(t, err)
	assert.Len(t, entries, 3)

	assert.Equal(t, entries[0].CreatedAt, entries[0].EventAt)
	assert.Equal(t, eventAt, entries[1].EventAt.UTC())
	assert.Equal(t, eventAt, entries[2].EventAt.UTC())
}

func TestCreateTransfersWithAndWithoutEventAt(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

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

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[testhelpers.Transfer])
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
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	_, err := conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10)", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10, null, $3)", account1.ID, account2.ID, `{"a": "b"}`)
	assert.NoError(t, err)

	_, err = conn.Exec(t.Context(), "select pgledger_create_transfer($1, $2, 10, metadata => $3)", account1.ID, account2.ID, `{"c": "d"}`)
	assert.NoError(t, err)

	rows, err := conn.Query(t.Context(), "select * from pgledger_transfers where from_account_id = $1", account1.ID)
	assert.NoError(t, err)

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[testhelpers.Transfer])
	assert.NoError(t, err)

	assert.Len(t, transfers, 3)

	assert.Nil(t, transfers[0].Metadata)
	assert.Equal(t, `{"a": "b"}`, *transfers[1].Metadata)
	assert.Equal(t, `{"c": "d"}`, *transfers[2].Metadata)

	// Entries view also has Metadata field
	entries := testhelpers.GetEntries(t, conn, account1.ID)
	assert.NoError(t, err)
	assert.Len(t, entries, 3)

	assert.Nil(t, entries[0].Metadata)
	assert.Equal(t, `{"a": "b"}`, *entries[1].Metadata)
	assert.Equal(t, `{"c": "d"}`, *entries[2].Metadata)
}

func TestCreateTransfersWithAndWithoutMetadata(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

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

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[testhelpers.Transfer])
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
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	account3 := testhelpers.CreateAccount(t, conn, "account 3", "USD")

	_, err := conn.Exec(t.Context(), `
		select * from pgledger_create_transfers(
			($1, $2, '10'),
			($2, $3, '20'),
			($3, $1, '50'))`,
		account1.ID, account2.ID, account3.ID)
	assert.NoError(t, err)

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)
	foundAccount3 := testhelpers.GetAccount(t, conn, account3.ID)

	assert.Equal(t, "40", foundAccount1.Balance)
	assert.Equal(t, "-10", foundAccount2.Balance)
	assert.Equal(t, "-30", foundAccount3.Balance)

	assert.Equal(t, 2, foundAccount1.Version)
	assert.Equal(t, 2, foundAccount2.Version)
	assert.Equal(t, 2, foundAccount3.Version)
}

func TestMultipleTransfersRollsBackIfOneIsBad(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	account3 := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account('negative-only', 'USD', allow_positive_balance => false)")

	_, err := conn.Exec(t.Context(), `
		select * from pgledger_create_transfers(
			($1, $2, '10'),
			($2, $3, '20'),
			($3, $1, '50'))`,
		account1.ID, account2.ID, account3.ID)
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", account3.ID, "negative-only"))

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)
	foundAccount3 := testhelpers.GetAccount(t, conn, account3.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
	assert.Equal(t, "0", foundAccount3.Balance)

	assert.Equal(t, 0, foundAccount1.Version)
	assert.Equal(t, 0, foundAccount2.Version)
	assert.Equal(t, 0, foundAccount3.Version)
}

func TestTransfersRollbackIfTransctionRollback(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	tx, err := conn.Begin(t.Context())
	assert.NoError(t, err)

	_, err = tx.Exec(t.Context(), "select * from pgledger_create_transfers(($1, $2, '10'))", account1.ID, account2.ID)
	assert.NoError(t, err)

	_, err = tx.Exec(t.Context(), "select 1/0")
	assert.ErrorContains(t, err, "division by zero")

	err = tx.Commit(t.Context())
	assert.ErrorContains(t, err, "rollback")

	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account1.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestCreateMultipleTransfersRollbackOnFailure(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")

	positiveOnlyAccount := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account('positive-only', 'USD', allow_negative_balance => false)")

	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		BEGIN;
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10');
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '20');
		select * from pgledger_create_transfer('%[2]s', '%[1]s', '50');
		COMMIT;
		`, account1.ID, positiveOnlyAccount.ID))
	assert.ErrorContains(t, err, "does not allow negative balance")

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, positiveOnlyAccount.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)

	assert.Equal(t, 0, foundAccount1.Version)
	assert.Equal(t, 0, foundAccount2.Version)
}

func TestTransferWithInvalidAccountID(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")

	_, err := testhelpers.CreateTransferReturnErr(t.Context(), conn, account1.ID, "bad_id", "12.34")
	assert.ErrorContains(t, err, "Account (id=bad_id) does not exist")

	_, err = testhelpers.CreateTransferReturnErr(t.Context(), conn, "bad_id", account1.ID, "12.34")
	assert.ErrorContains(t, err, "Account (id=bad_id) does not exist")
}

func TestEntries(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	t1 := testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "5")
	t2 := testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "10")
	t3 := testhelpers.CreateTransfer(t, conn, account2.ID, account1.ID, "20")

	entries := testhelpers.GetEntries(t, conn, account1.ID)

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

	entries = testhelpers.GetEntries(t, conn, account2.ID)

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
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	transfer := testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "10")
	assert.Equal(t, "10", transfer.Amount)

	// NaN and Infinity compare greater than every finite number, so a bare
	// amount > 0 guard accepts them and the balance becomes unrecoverable.
	for _, amount := range []string{"0", "-0.01", "NaN", "Infinity", "-Infinity"} {
		_, err := testhelpers.CreateTransferReturnErr(t.Context(), conn, account1.ID, account2.ID, amount)
		assert.ErrorContains(t, err, fmt.Sprintf("Amount (%s) must be a positive finite number", amount))
	}

	_, err := conn.Exec(t.Context(),
		"select * from pgledger_create_transfers(($1::text, $2::text, $3::numeric))",
		account1.ID, account2.ID, nil)
	assert.ErrorContains(t, err, "Amount (<NULL>) must be a positive finite number")

	assert.Equal(t, "-10", testhelpers.GetAccount(t, conn, account1.ID).Balance)
	assert.Equal(t, "10", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestCannotTransferBetweenDifferentCurrencies(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	// Create two accounts with different currencies
	accountUSD := testhelpers.CreateAccount(t, conn, "USD account", "USD")
	accountEUR := testhelpers.CreateAccount(t, conn, "EUR account", "EUR")

	_, err := testhelpers.CreateTransferReturnErr(t.Context(), conn, accountUSD.ID, accountEUR.ID, "10.00")
	assert.ErrorContains(t, err, "Cannot transfer between different currencies (USD and EUR)")

	// Verify account balances remain unchanged
	foundAccountUSD := testhelpers.GetAccount(t, conn, accountUSD.ID)
	foundAccountEUR := testhelpers.GetAccount(t, conn, accountEUR.ID)

	assert.Equal(t, "0", foundAccountUSD.Balance)
	assert.Equal(t, "0", foundAccountEUR.Balance)
}

func TestTransferBetweenCurrenciesWithExtraAccounts(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	// Create two accounts with different currencies
	userUSD := testhelpers.CreateAccount(t, conn, "user.USD", "USD")
	userEUR := testhelpers.CreateAccount(t, conn, "user.EUR", "EUR")

	// Liquidity accounts for the conversion
	liquidityUSD := testhelpers.CreateAccount(t, conn, "liquidity.USD", "USD")
	liquidityEUR := testhelpers.CreateAccount(t, conn, "liquidity.EUR", "EUR")

	_, err := conn.Exec(t.Context(), fmt.Sprintf(`
		BEGIN;
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10.00');
		select * from pgledger_create_transfer('%[3]s', '%[4]s', '9.26');
		COMMIT;
		`, userUSD.ID, liquidityUSD.ID, liquidityEUR.ID, userEUR.ID))
	assert.NoError(t, err)

	assert.Equal(t, "-10.00", testhelpers.GetAccount(t, conn, userUSD.ID).Balance)
	assert.Equal(t, "10.00", testhelpers.GetAccount(t, conn, liquidityUSD.ID).Balance)
	assert.Equal(t, "-9.26", testhelpers.GetAccount(t, conn, liquidityEUR.ID).Balance)
	assert.Equal(t, "9.26", testhelpers.GetAccount(t, conn, userEUR.ID).Balance)
}

func TestTransfersUseDifferentAccounts(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")

	_, err := testhelpers.CreateTransferReturnErr(t.Context(), conn, account1.ID, account1.ID, "10")
	assert.ErrorContains(t, err, fmt.Sprintf("Cannot transfer to the same account (id=%s)", account1.ID))
}

func TestConcurrency(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	var wg sync.WaitGroup

	wg.Go(func() {
		for range 500 {
			_ = testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "100")
		}
	})

	wg.Go(func() {
		for range 500 {
			_ = testhelpers.CreateTransfer(t, conn, account2.ID, account1.ID, "100")
		}
	})

	// Wait for all goroutines to complete
	wg.Wait()

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestConcurrencyWithCurrencyExchange(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	// Create two accounts with different currencies
	userUSD := testhelpers.CreateAccount(t, conn, "user.USD", "USD")
	userEUR := testhelpers.CreateAccount(t, conn, "user.EUR", "EUR")

	// Liquidity accounts for the conversion
	liquidityUSD := testhelpers.CreateAccount(t, conn, "liquidity.USD", "USD")
	liquidityEUR := testhelpers.CreateAccount(t, conn, "liquidity.EUR", "EUR")

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

	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, userUSD.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, userEUR.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, liquidityUSD.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, liquidityEUR.ID).Balance)
}

func TestIdsAreMonotonic(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

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
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	_ = testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "10")
	_ = testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "20")
	_ = testhelpers.CreateTransfer(t, conn, account1.ID, account2.ID, "50")

	entries := testhelpers.GetEntries(t, conn, account2.ID)
	assert.Len(t, entries, 3)

	// Normally, we would never update the ledger. But here I'm doing it to make testing easier.
	_, err := conn.Exec(t.Context(), "update pgledger_entries set created_at = $1 where id = $2", "2025-06-01T12:00:00Z", entries[0].ID)
	assert.NoError(t, err)
	_, err = conn.Exec(t.Context(), "update pgledger_entries set created_at = $1 where id = $2", "2025-06-01T13:00:00Z", entries[1].ID)
	assert.NoError(t, err)
	_, err = conn.Exec(t.Context(), "update pgledger_entries set created_at = $1 where id = $2", "2025-06-01T14:00:00Z", entries[2].ID)
	assert.NoError(t, err)

	// Current balance
	assert.Equal(t, "80", testhelpers.GetAccount(t, conn, account2.ID).Balance)

	// Historical balances
	assert.Equal(t, "10", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T12:00:00Z"))
	assert.Equal(t, "10", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T12:15:00Z"))
	assert.Equal(t, "30", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T13:00:00Z"))
	assert.Equal(t, "30", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T13:15:00Z"))
	assert.Equal(t, "80", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T14:00:00Z"))
	assert.Equal(t, "80", accountBalanceAtTime(t, conn, account2.ID, "2025-06-01T14:15:00Z"))
}

func accountBalanceAtTime(t *testing.T, conn *pgxpool.Pool, accountID string, datetime string) string {
	rows, err := conn.Query(t.Context(), `
		select account_current_balance
		from pgledger_entries
		where account_id = $1
		and created_at <= $2
		order by account_version desc
		limit 1`,
		accountID, datetime)
	assert.NoError(t, err)

	balance, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	return balance
}

func TestBatchOrderIsSignificant(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	createAccounts := func() (*testhelpers.Account, *testhelpers.Account, *testhelpers.Account) {
		x := testhelpers.CreateAccount(t, conn, "x", "USD")
		y := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account($1, $2, allow_negative_balance => false)", "y positive-only", "USD")
		z := testhelpers.CreateAccount(t, conn, "z", "USD")

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

	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[testhelpers.Transfer])
	assert.NoError(t, err)
	assert.Len(t, transfers, 2)
	assert.Equal(t, x.ID, transfers[0].FromAccountID)
	assert.Equal(t, y.ID, transfers[0].ToAccountID)
	assert.Equal(t, y.ID, transfers[1].FromAccountID)
	assert.Equal(t, z.ID, transfers[1].ToAccountID)

	assert.Equal(t, "-10", testhelpers.GetAccount(t, conn, x.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, y.ID).Balance)
	assert.Equal(t, "10", testhelpers.GetAccount(t, conn, z.ID).Balance)

	reversedX, reversedY, reversedZ := createAccounts()

	_, err = conn.Exec(t.Context(), `
		select * from pgledger_create_transfers(array[
			($2, $3, '10'),
			($1, $2, '10')
		]::transfer_request[])`,
		reversedX.ID, reversedY.ID, reversedZ.ID)
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", reversedY.ID, "y positive-only"))

	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, reversedX.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, reversedY.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, reversedZ.ID).Balance)

	assert.Equal(t, 0, testhelpers.GetAccount(t, conn, reversedX.ID).Version)
	assert.Equal(t, 0, testhelpers.GetAccount(t, conn, reversedY.ID).Version)
	assert.Equal(t, 0, testhelpers.GetAccount(t, conn, reversedZ.ID).Version)
}

func TestNullsAreRejected(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	rows, err := conn.Query(t.Context(), "select * from pgledger_create_transfers($1::transfer_request[])", nil)
	assert.NoError(t, err)

	_, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[testhelpers.Transfer])
	assert.ErrorContains(t, err, "Transfer requests must not be null")

	var pgErr *pgconn.PgError
	assert.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "P0001", pgErr.Code)

	for _, args := range [][]any{{account1.ID, nil}, {nil, account2.ID}} {
		_, err = conn.Exec(t.Context(),
			"select * from pgledger_create_transfers(($1::text, $2::text, 10))", args...)
		assert.ErrorContains(t, err, "must not be null")
	}

	// A wholly NULL request element has a NULL amount, so the amount guard
	// catches it first.
	_, err = conn.Exec(t.Context(), "select * from pgledger_create_transfers(array[null]::transfer_request[])")
	assert.ErrorContains(t, err, "Amount (<NULL>) must be a positive finite number")

	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account1.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestMetadataWithNullCharacterIsRejected(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	// jsonb cannot store \u0000, so PostgreSQL rejects this, not pgledger.
	_, err := conn.Exec(t.Context(),
		"select * from pgledger_create_transfer($1, $2, 10, metadata => $3::jsonb)",
		account1.ID, account2.ID, `{"k": "a\u0000b"}`)
	assert.ErrorContains(t, err, "unsupported Unicode escape sequence")

	var pgErr *pgconn.PgError
	assert.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "22P05", pgErr.Code)

	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account1.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestAccountNameAndCurrencyExtremes(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	longName := strings.Repeat("a", 10*1024)
	unicodeName := "café ☕ 日本語 🏦 Ωμέγα"
	unicodeCurrency := strings.Repeat("₿🇯🇵日本円", 10)

	emptyNameAccount := testhelpers.CreateAccount(t, conn, "", "USD")
	longNameAccount := testhelpers.CreateAccount(t, conn, longName, "USD")
	unicodeAccount := testhelpers.CreateAccount(t, conn, unicodeName, unicodeCurrency)

	assert.Equal(t, "", testhelpers.GetAccount(t, conn, emptyNameAccount.ID).Name)
	assert.Equal(t, longName, testhelpers.GetAccount(t, conn, longNameAccount.ID).Name)
	assert.Equal(t, unicodeName, testhelpers.GetAccount(t, conn, unicodeAccount.ID).Name)
	assert.Equal(t, unicodeCurrency, testhelpers.GetAccount(t, conn, unicodeAccount.ID).Currency)

	otherUnicodeAccount := testhelpers.CreateAccount(t, conn, unicodeName+" 2", unicodeCurrency)

	transfer := testhelpers.CreateTransfer(t, conn, unicodeAccount.ID, otherUnicodeAccount.ID, "12.34")
	assert.Equal(t, "12.34", transfer.Amount)

	assert.Equal(t, "-12.34", testhelpers.GetAccount(t, conn, unicodeAccount.ID).Balance)
	assert.Equal(t, "12.34", testhelpers.GetAccount(t, conn, otherUnicodeAccount.ID).Balance)
}

func TestIdempotencyKeyReturnsExistingTransfer(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	idempotencyKey := "transfer-" + account1.ID

	created, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '12.34', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Len(t, created, 1)
	assert.Equal(t, []testhelpers.IdempotencyKey{{IdempotencyKey: idempotencyKey, RequestIndex: 1, TransferID: created[0].ID}}, testhelpers.GetIdempotencyKeys(t, conn, idempotencyKey))

	retried, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '12.34', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, retried)

	// pgledger_create_transfer is a one-request pgledger_create_transfers, so
	// they share keys.
	batched, err := queryTransfers(t, conn, "select * from pgledger_create_transfers(array[($1, $2, '12.34')]::transfer_request[], idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, batched)

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)

	assert.Equal(t, "-12.34", foundAccount1.Balance)
	assert.Equal(t, "12.34", foundAccount2.Balance)
	assert.Equal(t, 1, foundAccount1.Version)
	assert.Equal(t, 1, foundAccount2.Version)
	assert.Len(t, testhelpers.GetEntries(t, conn, account1.ID), 1)
	assert.Len(t, testhelpers.GetEntries(t, conn, account2.ID), 1)
}

func TestIdempotencyKeyReturnsExistingBatch(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	account3 := testhelpers.CreateAccount(t, conn, "account 3", "USD")
	idempotencyKey := "batch-" + account1.ID
	metadata := `{"note": "batch"}`

	eventAt, err := time.Parse(time.RFC3339, "2025-07-01T12:34:56Z")
	assert.NoError(t, err)

	created, err := queryTransfers(t, conn, `
		select * from pgledger_create_transfers(array[
			($1, $2, '10'),
			($2, $3, '20'),
			($1, $2, '10')
		]::transfer_request[], $4, $5, $6)`,
		account1.ID, account2.ID, account3.ID, eventAt, metadata, idempotencyKey)
	assert.NoError(t, err)
	assert.Len(t, created, 3)
	assert.Equal(t, []testhelpers.IdempotencyKey{
		{IdempotencyKey: idempotencyKey, RequestIndex: 1, TransferID: created[0].ID},
		{IdempotencyKey: idempotencyKey, RequestIndex: 2, TransferID: created[1].ID},
		{IdempotencyKey: idempotencyKey, RequestIndex: 3, TransferID: created[2].ID},
	}, testhelpers.GetIdempotencyKeys(t, conn, idempotencyKey))

	retried, err := queryTransfers(t, conn, `
		select * from pgledger_create_transfers(array[
			($1, $2, '10'),
			($2, $3, '20'),
			($1, $2, '10')
		]::transfer_request[], $4, $5, $6)`,
		account1.ID, account2.ID, account3.ID, eventAt, metadata, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, retried)

	// Amounts compare numerically, and the stored transfers come back as they
	// were first written.
	respelled, err := queryTransfers(t, conn, `
		select * from pgledger_create_transfers(array[
			($1, $2, '10.0'),
			($2, $3, '20.00'),
			($1, $2, '10')
		]::transfer_request[], $4, $5, $6)`,
		account1.ID, account2.ID, account3.ID, eventAt, metadata, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, respelled)

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)
	foundAccount3 := testhelpers.GetAccount(t, conn, account3.ID)

	assert.Equal(t, "-20", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
	assert.Equal(t, "20", foundAccount3.Balance)
	assert.Equal(t, 2, foundAccount1.Version)
	assert.Equal(t, 3, foundAccount2.Version)
	assert.Equal(t, 1, foundAccount3.Version)
}

func TestIdempotencyKeyWithDifferentRequestIsRejected(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	account3 := testhelpers.CreateAccount(t, conn, "account 3", "USD")
	idempotencyKey := "mismatch-" + account1.ID

	eventAt, err := time.Parse(time.RFC3339, "2025-07-01T12:34:56Z")
	assert.NoError(t, err)

	created, err := queryTransfers(t, conn,
		"select * from pgledger_create_transfers(array[($1, $2, '10'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
		account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey)
	assert.NoError(t, err)
	assert.Len(t, created, 2)

	for _, retry := range []struct {
		name string
		sql  string
		args []any
	}{
		{
			"different amount",
			"select * from pgledger_create_transfers(array[($1, $2, '11'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey},
		},
		{
			"different account",
			"select * from pgledger_create_transfers(array[($1, $3, '10'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey},
		},
		{
			"reversed direction",
			"select * from pgledger_create_transfers(array[($2, $1, '10'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey},
		},
		{
			"reordered requests",
			"select * from pgledger_create_transfers(array[($2, $3, '20'), ($1, $2, '10')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey},
		},
		{
			"missing request",
			"select * from pgledger_create_transfer($1, $2, '10', $3, idempotency_key => $4)",
			[]any{account1.ID, account2.ID, eventAt, idempotencyKey},
		},
		{
			"extra request",
			"select * from pgledger_create_transfers(array[($1, $2, '10'), ($2, $3, '20'), ($1, $2, '10')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey},
		},
		{
			"empty batch",
			"select * from pgledger_create_transfers(array[]::transfer_request[], $1, idempotency_key => $2)",
			[]any{eventAt, idempotencyKey},
		},
		{
			"invalid amount",
			"select * from pgledger_create_transfers(array[($1, $2, '0'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey},
		},
		{
			"different event_at",
			"select * from pgledger_create_transfers(array[($1, $2, '10'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt.Add(time.Second), idempotencyKey},
		},
		{
			"default event_at",
			"select * from pgledger_create_transfers(array[($1, $2, '10'), ($2, $3, '20')]::transfer_request[], idempotency_key => $4)",
			[]any{account1.ID, account2.ID, account3.ID, idempotencyKey},
		},
		{
			"different metadata",
			"select * from pgledger_create_transfers(array[($1, $2, '10'), ($2, $3, '20')]::transfer_request[], $4, $5, $6)",
			[]any{account1.ID, account2.ID, account3.ID, eventAt, `{"note": "retry"}`, idempotencyKey},
		},
	} {
		_, err := queryTransfers(t, conn, retry.sql, retry.args...)
		assert.ErrorContains(t, err, fmt.Sprintf("Idempotency key (%s) was already used with a different request", idempotencyKey), retry.name)

		var pgErr *pgconn.PgError
		if assert.ErrorAs(t, err, &pgErr, retry.name) {
			assert.Equal(t, "P0001", pgErr.Code, retry.name)
		}
	}

	foundAccount1 := testhelpers.GetAccount(t, conn, account1.ID)
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)
	foundAccount3 := testhelpers.GetAccount(t, conn, account3.ID)

	assert.Equal(t, "-10", foundAccount1.Balance)
	assert.Equal(t, "-10", foundAccount2.Balance)
	assert.Equal(t, "20", foundAccount3.Balance)
	assert.Equal(t, 1, foundAccount1.Version)
	assert.Equal(t, 2, foundAccount2.Version)
	assert.Equal(t, 1, foundAccount3.Version)

	retried, err := queryTransfers(t, conn,
		"select * from pgledger_create_transfers(array[($1, $2, '10'), ($2, $3, '20')]::transfer_request[], $4, idempotency_key => $5)",
		account1.ID, account2.ID, account3.ID, eventAt, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, retried)
}

func TestIdempotencyKeyWithDefaultEventAt(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	idempotencyKey := "default-event-at-" + account1.ID

	created, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Len(t, created, 1)
	assert.Equal(t, created[0].CreatedAt, created[0].EventAt)

	retried, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, retried)

	eventAt, err := time.Parse(time.RFC3339, "2025-07-01T12:34:56Z")
	assert.NoError(t, err)

	_, err = queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '10', $3, idempotency_key => $4)", account1.ID, account2.ID, eventAt, idempotencyKey)
	assert.ErrorContains(t, err, fmt.Sprintf("Idempotency key (%s) was already used with a different request", idempotencyKey))

	assert.Equal(t, "10", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestRejectedTransferDoesNotClaimIdempotencyKey(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	positiveOnly := testhelpers.QueryOne[testhelpers.Account](t, conn, "select * from pgledger_create_account('positive-only', 'USD', allow_negative_balance => false)")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	idempotencyKey := "rejected-" + positiveOnly.ID

	outOfPositiveOnly := "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)"

	_, err := queryTransfers(t, conn, outOfPositiveOnly, positiveOnly.ID, account2.ID, idempotencyKey)
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", positiveOnly.ID, "positive-only"))

	// The rejected call rolled back, so a different request can claim the key.
	created, err := queryTransfers(t, conn, outOfPositiveOnly, account2.ID, positiveOnly.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Len(t, created, 1)

	retried, err := queryTransfers(t, conn, outOfPositiveOnly, account2.ID, positiveOnly.ID, idempotencyKey)
	assert.NoError(t, err)
	assert.Equal(t, created, retried)

	// positive-only now holds 10, so only the key stops the original request.
	_, err = queryTransfers(t, conn, outOfPositiveOnly, positiveOnly.ID, account2.ID, idempotencyKey)
	assert.ErrorContains(t, err, fmt.Sprintf("Idempotency key (%s) was already used with a different request", idempotencyKey))

	assert.Equal(t, "10", testhelpers.GetAccount(t, conn, positiveOnly.ID).Balance)
	assert.Equal(t, "-10", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestIdempotencyKeyWithinOneTransaction(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	idempotencyKey := "transaction-" + account1.ID

	tx, err := conn.Begin(t.Context())
	assert.NoError(t, err)

	var ids []string
	for range 2 {
		rows, err := tx.Query(t.Context(), "select id from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
		assert.NoError(t, err)

		id, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
		assert.NoError(t, err)
		ids = append(ids, id)
	}

	assert.NoError(t, tx.Commit(t.Context()))

	assert.Len(t, ids, 2)
	assert.Equal(t, ids[0], ids[1])
	assert.Equal(t, "10", testhelpers.GetAccount(t, conn, account2.ID).Balance)
}

func TestIdempotencyKeyConcurrency(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	idempotencyKey := "concurrent-" + account1.ID

	type result struct {
		amount    string
		transfers []testhelpers.Transfer
		err       error
	}
	results := make([]result, 20)
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			amount := []string{"10", "20"}[i%2]
			transfers, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, $3, idempotency_key => $4)", account1.ID, account2.ID, amount, idempotencyKey)
			results[i] = result{amount: amount, transfers: transfers, err: err}
		})
	}
	wg.Wait()

	// Whichever amount claimed the key first wins; every call with the other
	// amount is a mismatch.
	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)
	winner := foundAccount2.Balance
	assert.Contains(t, []string{"10", "20"}, winner)
	assert.Equal(t, 1, foundAccount2.Version)

	ids := map[string]bool{}
	for _, r := range results {
		if r.amount == winner {
			assert.NoError(t, r.err)
			assert.Len(t, r.transfers, 1)
			for _, transfer := range r.transfers {
				ids[transfer.ID] = true
			}
		} else {
			assert.ErrorContains(t, r.err, fmt.Sprintf("Idempotency key (%s) was already used with a different request", idempotencyKey))
		}
	}
	assert.Len(t, ids, 1)
}

// A snapshot taken before a concurrent claim commits cannot see it, so the
// lookup misses; the primary key on the idempotency table still rejects the
// second claim, even for a request on unrelated accounts.
func TestIdempotencyKeyIsClaimedOnceWithAStaleSnapshot(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
	account3 := testhelpers.CreateAccount(t, conn, "account 3", "USD")
	account4 := testhelpers.CreateAccount(t, conn, "account 4", "USD")

	for _, isolation := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		idempotencyKey := fmt.Sprintf("stale-snapshot-%s-%s", isolation, account1.ID)

		stale, err := conn.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: isolation})
		assert.NoError(t, err)
		_, err = stale.Exec(t.Context(), "select 1")
		assert.NoError(t, err)

		claimed, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
		assert.NoError(t, err)
		assert.Len(t, claimed, 1)

		_, err = stale.Exec(t.Context(), "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account3.ID, account4.ID, idempotencyKey)
		var pgErr *pgconn.PgError
		if assert.ErrorAs(t, err, &pgErr, isolation) {
			assert.Equal(t, "23505", pgErr.Code, isolation)
			assert.Equal(t, `duplicate key value violates unique constraint "pgledger_idempotency_keys_pkey"`, pgErr.Message)
			assert.Equal(t, "pgledger_idempotency_keys_pkey", pgErr.ConstraintName, isolation)
		}
		assert.NoError(t, stale.Rollback(t.Context()))

		assert.Equal(t, []testhelpers.IdempotencyKey{{IdempotencyKey: idempotencyKey, RequestIndex: 1, TransferID: claimed[0].ID}}, testhelpers.GetIdempotencyKeys(t, conn, idempotencyKey), isolation)
	}

	assert.Equal(t, "20", testhelpers.GetAccount(t, conn, account2.ID).Balance)
	assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account4.ID).Balance)
}

// READ COMMITTED takes a new snapshot for each statement, and PostgreSQL runs
// READ UNCOMMITTED as READ COMMITTED, so a transaction that began before the
// claim committed still sees it and replays the claimed transfer.
func TestIdempotencyKeyReplaysInATransactionStartedBeforeTheClaim(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
	account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")

	for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted, pgx.ReadUncommitted} {
		idempotencyKey := fmt.Sprintf("earlier-transaction-%s-%s", isolation, account1.ID)

		earlier, err := conn.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: isolation})
		assert.NoError(t, err)
		_, err = earlier.Exec(t.Context(), "select 1")
		assert.NoError(t, err)

		claimed, err := queryTransfers(t, conn, "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
		assert.NoError(t, err)
		assert.Len(t, claimed, 1)

		replayed, err := queryTransfers(t, earlier, "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)", account1.ID, account2.ID, idempotencyKey)
		assert.NoError(t, err, isolation)
		assert.Equal(t, claimed, replayed, isolation)
		assert.NoError(t, earlier.Commit(t.Context()))

		assert.Equal(t, []testhelpers.IdempotencyKey{{IdempotencyKey: idempotencyKey, RequestIndex: 1, TransferID: claimed[0].ID}}, testhelpers.GetIdempotencyKeys(t, conn, idempotencyKey), isolation)
	}

	foundAccount2 := testhelpers.GetAccount(t, conn, account2.ID)
	assert.Equal(t, "20", foundAccount2.Balance)
	assert.Equal(t, 2, foundAccount2.Version)
}

// Each waiter's call blocks on the key's lock until the claiming call commits.
// Under READ COMMITTED the lookup after the wait sees the claim, so a retry
// replays it and a different request is a mismatch. Under REPEATABLE READ and
// SERIALIZABLE the waiter's snapshot was taken as its call started, before the
// claim committed, so neither replays: a retry fails to lock the accounts the
// claim updated, and a request on other accounts fails on the primary key. A
// new transaction afterwards replays the claim at every level.
func TestIdempotencyKeyWaitersByIsolationLevel(t *testing.T) {
	conn := testhelpers.SetupParallel(t)

	for _, tt := range []struct {
		isolation pgx.TxIsoLevel
		retryCode string // empty when the retry replays the claim
		otherCode string
	}{
		{pgx.ReadCommitted, "", "P0001"},
		{pgx.RepeatableRead, "40001", "23505"},
		{pgx.Serializable, "40001", "23505"},
	} {
		t.Run(string(tt.isolation), func(t *testing.T) {
			ctx := t.Context()
			account1 := testhelpers.CreateAccount(t, conn, "account 1", "USD")
			account2 := testhelpers.CreateAccount(t, conn, "account 2", "USD")
			account3 := testhelpers.CreateAccount(t, conn, "account 3", "USD")
			account4 := testhelpers.CreateAccount(t, conn, "account 4", "USD")
			idempotencyKey := "waiters-" + account1.ID
			createTransfer := "select * from pgledger_create_transfer($1, $2, '10', idempotency_key => $3)"

			createTransferInTx := func(from, to string) ([]testhelpers.Transfer, error) {
				tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: tt.isolation})
				if err != nil {
					return nil, err
				}
				defer func() { _ = tx.Rollback(ctx) }()

				transfers, err := queryTransfers(t, tx, createTransfer, from, to, idempotencyKey)
				if err != nil {
					return nil, err
				}
				return transfers, tx.Commit(ctx)
			}

			claimer, err := conn.Begin(ctx)
			assert.NoError(t, err)
			claimed, err := queryTransfers(t, claimer, createTransfer, account1.ID, account2.ID, idempotencyKey)
			assert.NoError(t, err)
			assert.Len(t, claimed, 1)

			var retried, other []testhelpers.Transfer
			var retryErr, otherErr error
			var wg sync.WaitGroup
			wg.Go(func() { retried, retryErr = createTransferInTx(account1.ID, account2.ID) })
			wg.Go(func() { other, otherErr = createTransferInTx(account3.ID, account4.ID) })

			assert.Eventually(t, func() bool { return idempotencyKeyWaiters(t, conn, idempotencyKey) == 2 }, 5*time.Second, 10*time.Millisecond)
			assert.NoError(t, claimer.Commit(ctx))
			wg.Wait()

			if tt.retryCode == "" {
				assert.NoError(t, retryErr)
				assert.Equal(t, claimed, retried)
			} else {
				assertIdempotencyRejection(t, retryErr, tt.retryCode)
			}
			assertIdempotencyRejection(t, otherErr, tt.otherCode)
			assert.Empty(t, other)

			replayed, err := createTransferInTx(account1.ID, account2.ID)
			assert.NoError(t, err)
			assert.Equal(t, claimed, replayed)

			assert.Equal(t, []testhelpers.IdempotencyKey{{IdempotencyKey: idempotencyKey, RequestIndex: 1, TransferID: claimed[0].ID}}, testhelpers.GetIdempotencyKeys(t, conn, idempotencyKey))
			assert.Equal(t, "10", testhelpers.GetAccount(t, conn, account2.ID).Balance)
			assert.Equal(t, "0", testhelpers.GetAccount(t, conn, account4.ID).Balance)
		})
	}
}

func assertIdempotencyRejection(t *testing.T, err error, code string) {
	t.Helper()

	var pgErr *pgconn.PgError
	if !assert.ErrorAs(t, err, &pgErr) {
		return
	}
	assert.Equal(t, code, pgErr.Code, pgErr.Message)
	switch code {
	case "P0001":
		assert.Contains(t, pgErr.Message, "was already used with a different request")
	case "23505":
		assert.Equal(t, "pgledger_idempotency_keys_pkey", pgErr.ConstraintName)
	}
}

// idempotencyKeyWaiters counts the sessions blocked on the key's advisory
// lock, which pg_locks splits into the high (classid) and low (objid) halves.
func idempotencyKeyWaiters(t *testing.T, conn *pgxpool.Pool, idempotencyKey string) int {
	var waiters int
	err := conn.QueryRow(t.Context(), `
		select count(*) from pg_locks
		where locktype = 'advisory' and objsubid = 1 and not granted
			and (classid::bigint << 32) | objid::bigint = hashtextextended($1, 0)`, idempotencyKey).Scan(&waiters)
	assert.NoError(t, err)
	return waiters
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func queryTransfers(t *testing.T, conn querier, sql string, args ...any) ([]testhelpers.Transfer, error) {
	rows, err := conn.Query(t.Context(), sql, args...)
	if err != nil {
		return nil, err
	}

	return pgx.CollectRows(rows, pgx.RowToStructByName[testhelpers.Transfer])
}
