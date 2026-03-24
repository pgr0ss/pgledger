//go:build property

package test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// accountModel tracks the expected state of one account created during a property test run.
type accountModel struct {
	id                   string
	currency             string
	balance              int64 // tracked in integer units; amounts are always integers 1–999
	version              int
	allowNegativeBalance bool
}

// ledgerMachine holds the state for a single rapid test run.
type ledgerMachine struct {
	conn        *pgxpool.Pool
	accounts    []*accountModel
	transferIDs []string
}

// propCounter gives each account a unique name across all concurrent runs.
var propCounter atomic.Int64

func (m *ledgerMachine) setup(t *rapid.T) {
	m.conn = dbconn(t)
	// Two accounts needed so sameCurrencyPair succeeds on first action.
	m.doCreateAccount(t, "USD", true)
	m.doCreateAccount(t, "USD", true)
}

func (m *ledgerMachine) doCreateAccount(t *rapid.T, currency string, allowNegative bool) {
	n := propCounter.Add(1)
	var acc *Account
	if allowNegative {
		acc = createAccount(t, m.conn, fmt.Sprintf("prop-%d", n), currency)
	} else {
		acc = queryOne[Account](t, m.conn,
			"select * from pgledger_create_account($1, $2, allow_negative_balance => false)",
			fmt.Sprintf("prop-%d", n), currency)
	}
	m.accounts = append(m.accounts, &accountModel{
		id:                   acc.ID,
		currency:             currency,
		allowNegativeBalance: allowNegative,
	})
}

// createAccount is a rapid action that adds a new USD or EUR account.
func (m *ledgerMachine) createAccount(t *rapid.T) {
	currency := rapid.SampledFrom([]string{"USD", "EUR"}).Draw(t, "currency")
	m.doCreateAccount(t, currency, true)
}

// createConstrainedAccount creates an account with allow_negative_balance=false.
func (m *ledgerMachine) createConstrainedAccount(t *rapid.T) {
	currency := rapid.SampledFrom([]string{"USD", "EUR"}).Draw(t, "currency")
	m.doCreateAccount(t, currency, false)
}

// pickPool selects a pool of >=2 same-currency unconstrained accounts, or returns nil.
func (m *ledgerMachine) pickPool(t *rapid.T, label string) []*accountModel {
	var usd, eur []*accountModel
	for _, a := range m.accounts {
		if !a.allowNegativeBalance {
			continue
		}
		switch a.currency {
		case "USD":
			usd = append(usd, a)
		case "EUR":
			eur = append(eur, a)
		}
	}

	switch {
	case len(usd) >= 2 && len(eur) >= 2:
		if rapid.Bool().Draw(t, label) {
			return usd
		}
		return eur
	case len(usd) >= 2:
		return usd
	case len(eur) >= 2:
		return eur
	default:
		return nil
	}
}

// createTransfer is a rapid action that moves a random integer amount between
// two same-currency accounts chosen from the current pool.
func (m *ledgerMachine) createTransfer(t *rapid.T) {
	pool := m.pickPool(t, "use-usd")
	if pool == nil {
		t.Skip("no same-currency pair available")
	}

	fromIdx := rapid.IntRange(0, len(pool)-1).Draw(t, "from")
	toIdx := rapid.IntRange(0, len(pool)-2).Draw(t, "to")
	if toIdx >= fromIdx {
		toIdx++
	}
	from := pool[fromIdx]
	to := pool[toIdx]
	amount := rapid.IntRange(1, 999).Draw(t, "amount")

	transfer := createTransfer(t, m.conn, from.id, to.id, strconv.Itoa(amount))
	m.transferIDs = append(m.transferIDs, transfer.ID)
	from.balance -= int64(amount)
	from.version++
	to.balance += int64(amount)
	to.version++
}

// createBatchTransfers calls pgledger_create_transfers with 2-3 transfer requests.
func (m *ledgerMachine) createBatchTransfers(t *rapid.T) {
	pool := m.pickPool(t, "batch-use-usd")
	if pool == nil {
		t.Skip("no same-currency pair available for batch")
	}

	count := rapid.IntRange(2, 3).Draw(t, "batch-count")
	args := make([]any, 0, count*3)
	transferParts := []string{}
	for i := range count {
		fromIdx := rapid.IntRange(0, len(pool)-1).Draw(t, fmt.Sprintf("batch-from-%d", i))
		toIdx := rapid.IntRange(0, len(pool)-2).Draw(t, fmt.Sprintf("batch-to-%d", i))
		if toIdx >= fromIdx {
			toIdx++
		}
		amount := rapid.IntRange(1, 999).Draw(t, fmt.Sprintf("batch-amount-%d", i))

		base := i * 3
		args = append(args, pool[fromIdx].id, pool[toIdx].id, strconv.Itoa(amount))
		transferParts = append(transferParts, fmt.Sprintf("($%d, $%d, $%d)", base+1, base+2, base+3))

		pool[fromIdx].balance -= int64(amount)
		pool[fromIdx].version++
		pool[toIdx].balance += int64(amount)
		pool[toIdx].version++
	}

	sql := fmt.Sprintf("select * from pgledger_create_transfers(%s)", strings.Join(transferParts, ", "))
	rows, err := m.conn.Query(t.Context(), sql, args...)
	assert.NoError(t, err)
	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[Transfer])
	assert.NoError(t, err)
	for _, tr := range transfers {
		m.transferIDs = append(m.transferIDs, tr.ID)
	}
}

// createTransferFromConstrainedExpectFail attempts a transfer from a constrained
// account with insufficient balance. Expects failure and unchanged state.
func (m *ledgerMachine) createTransferFromConstrainedExpectFail(t *rapid.T) {
	var constrained []*accountModel
	for _, a := range m.accounts {
		if !a.allowNegativeBalance && a.balance <= 0 {
			constrained = append(constrained, a)
		}
	}
	if len(constrained) == 0 {
		t.Skip("no constrained account with non-positive balance")
	}

	from := constrained[rapid.IntRange(0, len(constrained)-1).Draw(t, "constrained-from")]

	var targets []*accountModel
	for _, a := range m.accounts {
		if a.id != from.id && a.currency == from.currency {
			targets = append(targets, a)
		}
	}
	if len(targets) == 0 {
		t.Skip("no target for constrained transfer")
	}

	to := targets[rapid.IntRange(0, len(targets)-1).Draw(t, "constrained-to")]
	amount := rapid.IntRange(1, 999).Draw(t, "constrained-amount")

	_, err := createTransferReturnErr(t.Context(), m.conn, from.id, to.id, strconv.Itoa(amount))
	assert.ErrorContains(t, err, "does not allow negative balance")
}

// checkInvariants runs all invariant checks after each action.
// Account and entry data is fetched once per account and shared across checks.
func (m *ledgerMachine) checkInvariants(t *rapid.T) {
	for _, acc := range m.accounts {
		dbAcc := getAccount(t, m.conn, acc.id)
		entries := getEntries(t, m.conn, acc.id)
		assertAccountInvariants(t, acc, dbAcc, entries)
	}
	m.checkZeroSum(t)
	m.checkTwoEntriesPerTransfer(t)
}

// assertAccountInvariants checks all per-account invariants given pre-fetched data.
func assertAccountInvariants(t TestingT, acc *accountModel, dbAcc *Account, entries []Entry) {
	// Balance matches model
	dbBalance, err := strconv.ParseInt(dbAcc.Balance, 10, 64)
	assert.NoError(t, err)
	assert.Equal(t, acc.balance, dbBalance, "account %s: DB balance != model", acc.id)

	// Version matches model
	assert.Equal(t, acc.version, dbAcc.Version,
		"account %s: DB version %d != model version %d", acc.id, dbAcc.Version, acc.version)

	// Entry chain
	if len(entries) == 0 {
		assert.Equal(t, int64(0), acc.balance, "account %s: no entries but balance is non-zero", acc.id)
		return
	}

	assert.Equal(t, "0", entries[0].AccountPreviousBalance,
		"account %s: first entry previous_balance should be 0", acc.id)

	for i, e := range entries {
		prev, _ := strconv.ParseInt(e.AccountPreviousBalance, 10, 64)
		amt, _ := strconv.ParseInt(e.Amount, 10, 64)
		curr, _ := strconv.ParseInt(e.AccountCurrentBalance, 10, 64)
		assert.Equal(t, prev+amt, curr,
			"account %s entry[%d]: prev(%d) + amt(%d) should equal curr", acc.id, i, prev, amt)

		// Version monotonicity
		assert.Equal(t, i+1, e.AccountVersion,
			"account %s entry[%d]: expected version %d, got %d", acc.id, i, i+1, e.AccountVersion)
	}

	lastCurr, _ := strconv.ParseInt(entries[len(entries)-1].AccountCurrentBalance, 10, 64)
	assert.Equal(t, acc.balance, lastCurr,
		"account %s: last entry current_balance != model balance", acc.id)
}

// checkZeroSum verifies that the sum of all account balances per currency is zero.
func (m *ledgerMachine) checkZeroSum(t *rapid.T) {
	assertZeroSum(t, m.conn, m.accountIDs())
}

func (m *ledgerMachine) accountIDs() []string {
	ids := make([]string, len(m.accounts))
	for i, a := range m.accounts {
		ids[i] = a.id
	}
	return ids
}

func assertZeroSum(t TestingT, conn *pgxpool.Pool, accountIDs []string) {
	rows, err := conn.Query(t.Context(),
		"SELECT currency, SUM(balance) FROM pgledger_accounts_view WHERE id = ANY($1) GROUP BY currency",
		accountIDs)
	assert.NoError(t, err)

	type currencySum struct {
		Currency string
		Sum      string
	}
	sums, err := pgx.CollectRows(rows, pgx.RowToStructByPos[currencySum])
	assert.NoError(t, err)

	for _, cs := range sums {
		assert.Equal(t, "0", cs.Sum, "zero-sum violated for currency %s", cs.Currency)
	}
}

type transferEntryStats struct {
	TransferID string
	Count      int
	Sum        string
}

// checkTwoEntriesPerTransfer verifies every transfer has exactly 2 entries summing to 0.
func (m *ledgerMachine) checkTwoEntriesPerTransfer(t *rapid.T) {
	assertTwoEntriesPerTransfer(t, m.conn, m.transferIDs)
}

func assertTwoEntriesPerTransfer(t TestingT, conn *pgxpool.Pool, transferIDs []string) {
	if len(transferIDs) == 0 {
		return
	}

	rows, err := conn.Query(t.Context(),
		"SELECT transfer_id, COUNT(*), SUM(amount) FROM pgledger_entries WHERE transfer_id = ANY($1) GROUP BY transfer_id",
		transferIDs)
	assert.NoError(t, err)

	stats, err := pgx.CollectRows(rows, pgx.RowToStructByPos[transferEntryStats])
	assert.NoError(t, err)

	assert.Len(t, stats, len(transferIDs), "some transfers missing entries")
	for _, s := range stats {
		assert.Equal(t, 2, s.Count, "transfer %s: expected 2 entries, got %d", s.TransferID, s.Count)
		assert.Equal(t, "0", s.Sum, "transfer %s: entry amounts don't sum to 0", s.TransferID)
	}
}

// TestLedgerStateMachine runs a stateful property test using rapid.
// It generates random sequences of actions and checks all invariants after every step.
func TestLedgerStateMachine(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		m := &ledgerMachine{}
		m.setup(t)
		t.Repeat(map[string]func(*rapid.T){
			"CreateAccount":                     m.createAccount,
			"CreateConstrainedAccount":          m.createConstrainedAccount,
			"CreateTransfer":                    m.createTransfer,
			"CreateBatchTransfers":              m.createBatchTransfers,
			"TransferFromConstrainedExpectFail": m.createTransferFromConstrainedExpectFail,
			"":                                  m.checkInvariants,
		})
	})
}

// TestLedgerConcurrentStateMachine runs multiple goroutines performing transfers
// against shared accounts, then checks all invariants hold.
func TestLedgerConcurrentStateMachine(t *testing.T) {
	t.Parallel()
	conn := dbconn(t)
	rapid.Check(t, func(t *rapid.T) {
		// Create a pool of shared accounts
		numAccounts := rapid.IntRange(3, 6).Draw(t, "num-accounts")
		accounts := make([]*accountModel, numAccounts)
		for i := range numAccounts {
			n := propCounter.Add(1)
			acc := createAccount(t, conn, fmt.Sprintf("concurrent-%d", n), "USD")
			accounts[i] = &accountModel{id: acc.ID, currency: "USD", allowNegativeBalance: true}
		}

		// Pre-generate all transfer parameters (rapid.T is not thread-safe)
		numWorkers := rapid.IntRange(2, 4).Draw(t, "num-workers")
		transfersPerWorker := rapid.IntRange(5, 20).Draw(t, "transfers-per-worker")

		type transferParam struct {
			fromID string
			toID   string
			amount string
		}
		workerParams := make([][]transferParam, numWorkers)
		for w := range numWorkers {
			workerParams[w] = make([]transferParam, transfersPerWorker)
			for j := range transfersPerWorker {
				fromIdx := rapid.IntRange(0, numAccounts-1).Draw(t, fmt.Sprintf("w%d-t%d-from", w, j))
				toIdx := rapid.IntRange(0, numAccounts-2).Draw(t, fmt.Sprintf("w%d-t%d-to", w, j))
				if toIdx >= fromIdx {
					toIdx++
				}
				amount := rapid.IntRange(1, 100).Draw(t, fmt.Sprintf("w%d-t%d-amt", w, j))
				workerParams[w][j] = transferParam{
					fromID: accounts[fromIdx].id,
					toID:   accounts[toIdx].id,
					amount: strconv.Itoa(amount),
				}
			}
		}

		var allTransferIDs sync.Map
		var wg sync.WaitGroup

		for w := range numWorkers {
			params := workerParams[w]
			wg.Go(func() {
				for _, p := range params {
					transfer, err := createTransferReturnErr(t.Context(), conn,
						p.fromID, p.toID, p.amount)
					if err == nil {
						allTransferIDs.Store(transfer.ID, true)
					}
				}
			})
		}
		wg.Wait()

		// Reuse shared invariant helpers
		accountIDs := make([]string, numAccounts)
		for i, a := range accounts {
			accountIDs[i] = a.id
		}
		assertZeroSum(t, conn, accountIDs)

		// Check per-account invariants (without model balance/version since
		// concurrent transfers make the model non-deterministic)
		for _, acc := range accounts {
			entries := getEntries(t, conn, acc.id)
			if len(entries) == 0 {
				continue
			}
			assert.Equal(t, "0", entries[0].AccountPreviousBalance,
				"account %s: first entry previous_balance should be 0", acc.id)
			for i, e := range entries {
				prev, _ := strconv.ParseInt(e.AccountPreviousBalance, 10, 64)
				amt, _ := strconv.ParseInt(e.Amount, 10, 64)
				curr, _ := strconv.ParseInt(e.AccountCurrentBalance, 10, 64)
				assert.Equal(t, prev+amt, curr,
					"account %s entry[%d]: chain broken", acc.id, i)
				assert.Equal(t, i+1, e.AccountVersion,
					"account %s entry[%d]: version not monotonic", acc.id, i)
			}
			dbAcc := getAccount(t, conn, acc.id)
			lastCurr := entries[len(entries)-1].AccountCurrentBalance
			assert.Equal(t, dbAcc.Balance, lastCurr,
				"account %s: last entry balance != account balance", acc.id)
		}

		var transferIDs []string
		allTransferIDs.Range(func(key, _ any) bool {
			transferIDs = append(transferIDs, key.(string))
			return true
		})
		assertTwoEntriesPerTransfer(t, conn, transferIDs)
	})
}
