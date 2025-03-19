package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestAddAccount(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	assert.Regexp(t, "\\d+", account.ID)
	assert.Equal(t, "account 1", account.Name)
	assert.Equal(t, "0", account.Balance)
	assert.Equal(t, 0, account.Version)
	assert.WithinDuration(t, time.Now(), account.CreatedAt, time.Minute)
	assert.Equal(t, account.CreatedAt, account.UpdatedAt)
}

func TestAccountsThatCannotBeNegative(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	rows, err := conn.Query(ctx, "select id from pgledger_create_account('positive-only', allow_negative_balance_param => false)")
	assert.NoError(t, err)

	account1ID, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	_, err = createTransfer(ctx, conn, account1ID, account2.ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", account1ID, "positive-only"))

	foundAccount1, err := getAccount(ctx, conn, account1ID)
	assert.NoError(t, err)

	foundAccount2, err := getAccount(ctx, conn, account2.ID)
	assert.NoError(t, err)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestAccountsThatCannotBePositive(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	rows, err := conn.Query(ctx, `SELECT id FROM pgledger_create_account('negative-only', allow_positive_balance_param => false)`)
	assert.NoError(t, err)

	account1ID, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	_, err = createTransfer(ctx, conn, account2.ID, account1ID, "12.34")
	assert.ErrorContains(t, err, fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", account1ID, "negative-only"))

	foundAccount1, err := getAccount(ctx, conn, account1ID)
	assert.NoError(t, err)

	foundAccount2, err := getAccount(ctx, conn, account2.ID)
	assert.NoError(t, err)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

func TestCreateTransfer(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	transfer, err := createTransfer(ctx, conn, account1.ID, account2.ID, "12.34")
	assert.NoError(t, err)

	assert.Regexp(t, "\\d+", transfer.ID)
	assert.Equal(t, account1.ID, transfer.FromAccountID)
	assert.Equal(t, account2.ID, transfer.ToAccountID)
	assert.Equal(t, "12.34", transfer.Amount)
	assert.WithinDuration(t, time.Now(), transfer.CreatedAt, time.Minute)

	foundTransfer, err := getTransfer(ctx, conn, transfer.ID)
	assert.NoError(t, err)
	assert.Regexp(t, transfer.ID, foundTransfer.ID)
	assert.Equal(t, account1.ID, foundTransfer.FromAccountID)
	assert.Equal(t, account2.ID, foundTransfer.ToAccountID)
	assert.Equal(t, "12.34", foundTransfer.Amount)
	assert.WithinDuration(t, time.Now(), foundTransfer.CreatedAt, time.Minute)

	foundAccount1, err := getAccount(ctx, conn, account1.ID)
	assert.NoError(t, err)

	foundAccount2, err := getAccount(ctx, conn, account2.ID)
	assert.NoError(t, err)

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

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	account3, err := createAccount(ctx, conn, "account 3")
	assert.NoError(t, err)

	_, err = conn.Exec(ctx, fmt.Sprintf(`
		select * from pgledger_create_transfer('%[1]s', '%[2]s', '10');
		select * from pgledger_create_transfer('%[2]s', '%[3]s', '20');
		select * from pgledger_create_transfer('%[3]s', '%[1]s', '50');
		`, account1.ID, account2.ID, account3.ID))
	assert.NoError(t, err)

	foundAccount1, err := getAccount(ctx, conn, account1.ID)
	assert.NoError(t, err)

	foundAccount2, err := getAccount(ctx, conn, account2.ID)
	assert.NoError(t, err)

	foundAccount3, err := getAccount(ctx, conn, account3.ID)
	assert.NoError(t, err)

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

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	rows, err := conn.Query(ctx, "select id from pgledger_create_account('positive-only', allow_negative_balance_param => false)")
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

	foundAccount1, err := getAccount(ctx, conn, account1.ID)
	assert.NoError(t, err)

	foundAccount2, err := getAccount(ctx, conn, positiveOnlyAccountID)
	assert.NoError(t, err)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)

	assert.Equal(t, 0, foundAccount1.Version)
	assert.Equal(t, 0, foundAccount2.Version)
}

func TestTransferWithInvalidAccountID(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	_, err = createTransfer(ctx, conn, account1.ID, "999", "12.34")
	assert.ErrorContains(t, err, "violates foreign key constraint")

	_, err = createTransfer(ctx, conn, "999", account1.ID, "12.34")
	assert.ErrorContains(t, err, "violates foreign key constraint")
}

func TestEntries(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	t1, err := createTransfer(ctx, conn, account1.ID, account2.ID, "5")
	assert.NoError(t, err)

	t2, err := createTransfer(ctx, conn, account1.ID, account2.ID, "10")
	assert.NoError(t, err)

	t3, err := createTransfer(ctx, conn, account2.ID, account1.ID, "20")
	assert.NoError(t, err)

	entries, err := getEntries(ctx, conn, account1.ID)
	assert.NoError(t, err)

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

	entries, err = getEntries(ctx, conn, account2.ID)
	assert.NoError(t, err)

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

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	_, err = createTransfer(ctx, conn, account1.ID, account2.ID, "0")
	assert.ErrorContains(t, err, "Amount (0) must be positive")

	_, err = createTransfer(ctx, conn, account1.ID, account2.ID, "-0.01")
	assert.ErrorContains(t, err, "Amount (-0.01) must be positive")
}

func TestTransfersUseDifferentAccounts(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	_, err = createTransfer(ctx, conn, account1.ID, account1.ID, "10")
	assert.ErrorContains(t, err, fmt.Sprintf("Cannot transfer to the same account (id=%s)", account1.ID))
}

func TestConcurrency(t *testing.T) {
	conn := dbconn(t)
	ctx := t.Context()

	account1, err := createAccount(ctx, conn, "account 1")
	assert.NoError(t, err)

	account2, err := createAccount(ctx, conn, "account 2")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 500 {
			_, err1 := createTransfer(ctx, conn, account1.ID, account2.ID, "100")
			assert.NoError(t, err1)

			_, err1 = createTransfer(ctx, conn, account2.ID, account1.ID, "100")
			assert.NoError(t, err1)
		}
	}()

	go func() {
		defer wg.Done()
		for range 500 {
			_, err2 := createTransfer(ctx, conn, account2.ID, account1.ID, "100")
			assert.NoError(t, err2)

			_, err2 = createTransfer(ctx, conn, account1.ID, account2.ID, "100")
			assert.NoError(t, err2)
		}
	}()

	// Wait for all goroutines to complete
	wg.Wait()

	foundAccount1, err := getAccount(ctx, conn, account1.ID)
	assert.NoError(t, err)

	foundAccount2, err := getAccount(ctx, conn, account2.ID)
	assert.NoError(t, err)

	assert.Equal(t, "0", foundAccount1.Balance)
	assert.Equal(t, "0", foundAccount2.Balance)
}

type Account struct {
	ID                   string
	Name                 string
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
}

func createAccount(ctx context.Context, conn *pgxpool.Pool, name string) (*Account, error) {
	rows, err := conn.Query(ctx, "select * from pgledger_create_account($1)", name)
	if err != nil {
		return nil, err
	}

	account, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Account])
	if err != nil {
		return nil, err
	}

	return account, nil
}

func getAccount(ctx context.Context, conn *pgxpool.Pool, id string) (*Account, error) {
	rows, err := conn.Query(ctx, "select * from pgledger_get_account($1)", id)
	if err != nil {
		return nil, err
	}

	account, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Account])
	if err != nil {
		return nil, err
	}

	return account, nil
}

func getTransfer(ctx context.Context, conn *pgxpool.Pool, id string) (*Transfer, error) {
	rows, err := conn.Query(ctx, "select * from pgledger_get_transfer($1)", id)
	if err != nil {
		return nil, err
	}

	transfer, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Transfer])
	if err != nil {
		return nil, err
	}

	return transfer, nil
}

func createTransfer(ctx context.Context, conn *pgxpool.Pool, fromAccountID, toAccountID, amount string) (*Transfer, error) {
	rows, err := conn.Query(ctx, "select * from pgledger_create_transfer($1, $2, $3)", fromAccountID, toAccountID, amount)
	if err != nil {
		return nil, err
	}

	transfer, err := pgx.CollectExactlyOneRow(rows, pgx.RowToAddrOfStructByName[Transfer])
	if err != nil {
		return nil, err
	}

	return transfer, nil
}

func getEntries(ctx context.Context, conn *pgxpool.Pool, accountID string) ([]Entry, error) {
	rows, err := conn.Query(ctx, "select * from pgledger_entries where account_id = $1 order by id", accountID)
	if err != nil {
		return nil, err
	}

	entries, err := pgx.CollectRows(rows, pgx.RowToStructByName[Entry])
	if err != nil {
		return nil, err
	}

	return entries, nil
}
