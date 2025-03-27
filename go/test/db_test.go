package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestAddAccount(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account := createAccount(ctx, t, conn, "account 1", "USD")

	assert.Regexp(t, "\\d+", account.ID)
	assert.Equal(t, "account 1", account.Name)
	assert.Equal(t, "USD", account.Currency)
	assert.Equal(t, "0", account.Balance)
	assert.Equal(t, 0, account.Version)
	assert.WithinDuration(t, time.Now(), account.CreatedAt, time.Minute)
	assert.Equal(t, account.CreatedAt, account.UpdatedAt)
}

func TestAccountsThatCannotBeNegative(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	rows, err := conn.Query(ctx, "select id from pgledger_create_account('positive-only', 'USD', allow_negative_balance_param => false)")
	assert.NoError(t, err)

	account1ID, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	account2 := createAccount(ctx, t, conn, "account 2", "USD")

	_, err = createTransferReturnErr(ctx, conn, account1ID, account2.ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", account1ID, "positive-only"))

	foundAccount1 := getAccount(ctx, t, conn, account1ID)
	foundAccount2 := getAccount(ctx, t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestAccountsThatCannotBePositive(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	rows, err := conn.Query(ctx, `SELECT id FROM pgledger_create_account('negative-only', 'USD', allow_positive_balance_param => false)`)
	assert.NoError(t, err)

	account1ID, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	account2 := createAccount(ctx, t, conn, "account 2", "USD")

	_, err = createTransferReturnErr(ctx, conn, account2.ID, account1ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", account1ID, "negative-only"))

	foundAccount1 := getAccount(ctx, t, conn, account1ID)
	foundAccount2 := getAccount(ctx, t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestCreateTransfer(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")
	account2 := createAccount(ctx, t, conn, "account 2", "USD")

	transfer := createTransfer(ctx, t, conn, account1.ID, account2.ID, "12.34")

	assert.Regexp(t, "\\d+", transfer.ID)
	assert.Equal(t, account1.ID, transfer.FromAccountID)
	assert.Equal(t, account2.ID, transfer.ToAccountID)
	assert.Equal(t, "12.34", transfer.Amount)
	assert.WithinDuration(t, time.Now(), transfer.CreatedAt, time.Minute)

	foundTransfer := getTransfer(ctx, t, conn, transfer.ID)
	assert.Regexp(t, transfer.ID, foundTransfer.ID)
	assert.Equal(t, account1.ID, foundTransfer.FromAccountID)
	assert.Equal(t, account2.ID, foundTransfer.ToAccountID)
	assert.Equal(t, "12.34", foundTransfer.Amount)
	assert.WithinDuration(t, time.Now(), foundTransfer.CreatedAt, time.Minute)

	foundAccount1 := getAccount(ctx, t, conn, account1.ID)
	foundAccount2 := getAccount(ctx, t, conn, account2.ID)

	assert.Equal(t, "-12.34", foundAccount1.Balance)
	assert.Equal(t, "12.34", foundAccount2.Balance)
	assert.Equal(t, 1, foundAccount1.Version)
	assert.Equal(t, 1, foundAccount2.Version)
	assert.Greater(t, foundAccount1.UpdatedAt, foundAccount1.CreatedAt)
	assert.Greater(t, foundAccount2.UpdatedAt, foundAccount2.CreatedAt)
}

func TestCreateMultipleTransfers(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")
	account2 := createAccount(ctx, t, conn, "account 2", "USD")
	account3 := createAccount(ctx, t, conn, "account 3", "USD")

	_, err := conn.Exec(ctx, fmt.Sprintf(`
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10');
		select * from pgledger_create_transfer('%[2]s', '%[3]s', '20');
		select * from pgledger_create_transfer('%[3]s', '%[1]s', '50');
		`, account1.ID, account2.ID, account3.ID))
	assert.NoError(t, err)

	foundAccount1 := getAccount(ctx, t, conn, account1.ID)
	foundAccount2 := getAccount(ctx, t, conn, account2.ID)
	foundAccount3 := getAccount(ctx, t, conn, account3.ID)

	assert.Equal(t, "40", foundAccount1.Balance)
	assert.Equal(t, "-10", foundAccount2.Balance)
	assert.Equal(t, "-30", foundAccount3.Balance)

	assert.Equal(t, 2, foundAccount1.Version)
	assert.Equal(t, 2, foundAccount2.Version)
	assert.Equal(t, 2, foundAccount3.Version)
}

func TestCreateMultipleTransfersRollbackOnFailure(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")

	rows, err := conn.Query(ctx, "select id from pgledger_create_account('positive-only', 'USD', allow_negative_balance_param => false)")
	assert.NoError(t, err)

	positiveOnlyAccountID, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	_, err = conn.Exec(ctx, fmt.Sprintf(`
		BEGIN;
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10');
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '20');
		select * from pgledger_create_transfer('%[2]s', '%[1]s', '50');
		COMMIT;
		`, account1.ID, positiveOnlyAccountID))
	assert.ErrorContains(t, err, "does not allow negative balance")

	foundAccount1 := getAccount(ctx, t, conn, account1.ID)
	foundAccount2 := getAccount(ctx, t, conn, positiveOnlyAccountID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)

	assert.Equal(t, 0, foundAccount1.Version)
	assert.Equal(t, 0, foundAccount2.Version)
}

func TestTransferWithInvalidAccountID(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")

	_, err := createTransferReturnErr(ctx, conn, account1.ID, "999", "12.34")
	assert.ErrorContains(t, err, "violates foreign key constraint")

	_, err = createTransferReturnErr(ctx, conn, "999", account1.ID, "12.34")
	assert.ErrorContains(t, err, "violates foreign key constraint")
}

func TestEntries(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")
	account2 := createAccount(ctx, t, conn, "account 2", "USD")

	t1 := createTransfer(ctx, t, conn, account1.ID, account2.ID, "5")
	t2 := createTransfer(ctx, t, conn, account1.ID, account2.ID, "10")
	t3 := createTransfer(ctx, t, conn, account2.ID, account1.ID, "20")

	entries := getEntries(ctx, t, conn, account1.ID)

	assert.Len(t, entries, 3)

	assert.Equal(t, t1.ID, entries[0].TransferID)
	assert.Equal(t, "-5", entries[0].Amount)
	assert.Equal(t, "0", entries[0].AccountPreviousBalance)
	assert.Equal(t, "-5", entries[0].AccountCurrentBalance)
	assert.Equal(t, 1, entries[0].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[0].CreatedAt, time.Minute)

	assert.Equal(t, t2.ID, entries[1].TransferID)
	assert.Equal(t, "-10", entries[1].Amount)
	assert.Equal(t, "-5", entries[1].AccountPreviousBalance)
	assert.Equal(t, "-15", entries[1].AccountCurrentBalance)
	assert.Equal(t, 2, entries[1].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[1].CreatedAt, time.Minute)

	assert.Equal(t, t3.ID, entries[2].TransferID)
	assert.Equal(t, "20", entries[2].Amount)
	assert.Equal(t, "-15", entries[2].AccountPreviousBalance)
	assert.Equal(t, "5", entries[2].AccountCurrentBalance)
	assert.Equal(t, 3, entries[2].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[2].CreatedAt, time.Minute)

	entries = getEntries(ctx, t, conn, account2.ID)

	assert.Len(t, entries, 3)

	assert.Equal(t, t1.ID, entries[0].TransferID)
	assert.Equal(t, "5", entries[0].Amount)
	assert.Equal(t, "0", entries[0].AccountPreviousBalance)
	assert.Equal(t, "5", entries[0].AccountCurrentBalance)
	assert.Equal(t, 1, entries[0].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[0].CreatedAt, time.Minute)

	assert.Equal(t, t2.ID, entries[1].TransferID)
	assert.Equal(t, "10", entries[1].Amount)
	assert.Equal(t, "5", entries[1].AccountPreviousBalance)
	assert.Equal(t, "15", entries[1].AccountCurrentBalance)
	assert.Equal(t, 2, entries[1].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[1].CreatedAt, time.Minute)

	assert.Equal(t, t3.ID, entries[2].TransferID)
	assert.Equal(t, "-20", entries[2].Amount)
	assert.Equal(t, "15", entries[2].AccountPreviousBalance)
	assert.Equal(t, "-5", entries[2].AccountCurrentBalance)
	assert.Equal(t, 3, entries[2].AccountVersion)
	assert.WithinDuration(t, time.Now(), entries[2].CreatedAt, time.Minute)
}

func TestTransferAmountsArePositive(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")
	account2 := createAccount(ctx, t, conn, "account 2", "USD")

	_, err := createTransferReturnErr(ctx, conn, account1.ID, account2.ID, "0")
	assert.ErrorContains(t, err, "Amount (0) must be positive")

	_, err = createTransferReturnErr(ctx, conn, account1.ID, account2.ID, "-0.01")
	assert.ErrorContains(t, err, "Amount (-0.01) must be positive")
}

func TestCannotTransferBetweenDifferentCurrencies(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	// Create two accounts with different currencies
	accountUSD := createAccount(ctx, t, conn, "USD account", "USD")
	accountEUR := createAccount(ctx, t, conn, "EUR account", "EUR")

	_, err := createTransferReturnErr(ctx, conn, accountUSD.ID, accountEUR.ID, "10.00")
	assert.ErrorContains(t, err, "Cannot transfer between different currencies (USD and EUR)")

	// Verify account balances remain unchanged
	foundAccountUSD := getAccount(ctx, t, conn, accountUSD.ID)
	foundAccountEUR := getAccount(ctx, t, conn, accountEUR.ID)

	assert.Equal(t, "0", foundAccountUSD.Balance)
	assert.Equal(t, "0", foundAccountEUR.Balance)
}

func TestTransferBetweenCurrenciesWithExtraAccounts(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	// Create two accounts with different currencies
	userUSD := createAccount(ctx, t, conn, "user.USD", "USD")
	userEUR := createAccount(ctx, t, conn, "user.EUR", "EUR")

	// Liquidity accounts for the conversion
	liquidityUSD := createAccount(ctx, t, conn, "liquidity.USD", "USD")
	liquidityEUR := createAccount(ctx, t, conn, "liquidity.EUR", "EUR")

	_, err := conn.Exec(ctx, fmt.Sprintf(`
		BEGIN;
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10.00');
		select * from pgledger_create_transfer('%[3]s', '%[4]s', '9.26');
		COMMIT;
		`, userUSD.ID, liquidityUSD.ID, liquidityEUR.ID, userEUR.ID))
	assert.NoError(t, err)

	assert.Equal(t, "-10.00", getAccount(ctx, t, conn, userUSD.ID).Balance)
	assert.Equal(t, "10.00", getAccount(ctx, t, conn, liquidityUSD.ID).Balance)
	assert.Equal(t, "-9.26", getAccount(ctx, t, conn, liquidityEUR.ID).Balance)
	assert.Equal(t, "9.26", getAccount(ctx, t, conn, userEUR.ID).Balance)
}

func TestTransfersUseDifferentAccounts(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")

	_, err := createTransferReturnErr(ctx, conn, account1.ID, account1.ID, "10")
	assert.ErrorContains(t, err, fmt.Sprintf("Cannot transfer to the same account (id=%s)", account1.ID))
}

func TestConcurrency(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1 := createAccount(ctx, t, conn, "account 1", "USD")
	account2 := createAccount(ctx, t, conn, "account 2", "USD")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 500 {
			_ = createTransfer(ctx, t, conn, account1.ID, account2.ID, "100")
		}
	}()

	go func() {
		defer wg.Done()
		for range 500 {
			_ = createTransfer(ctx, t, conn, account2.ID, account1.ID, "100")
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	foundAccount1 := getAccount(ctx, t, conn, account1.ID)
	foundAccount2 := getAccount(ctx, t, conn, account2.ID)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}
