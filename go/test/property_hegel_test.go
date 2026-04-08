//go:build property

package test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"hegel.dev/go/hegel"
)

// hegelMachine holds the state for a single hegel property test run.
type hegelMachine struct {
	conn        *pgxpool.Pool
	accounts    []*accountModel
	transferIDs []string
}

func (m *hegelMachine) setup(ht *hegel.T, dbconn *pgxpool.Pool) {
	m.conn = dbconn
	m.doCreateAccount(ht, "USD", true)
	m.doCreateAccount(ht, "USD", true)
}

func (m *hegelMachine) doCreateAccount(ht *hegel.T, currency string, allowNegative bool) {
	n := propCounter.Add(1)
	var acc *Account
	if allowNegative {
		acc = createAccount(ht, m.conn, fmt.Sprintf("prop-%d", n), currency)
	} else {
		acc = queryOne[Account](ht, m.conn,
			"select * from pgledger_create_account($1, $2, allow_negative_balance => false)",
			fmt.Sprintf("prop-%d", n), currency)
	}
	m.accounts = append(m.accounts, &accountModel{
		id:                   acc.ID,
		currency:             currency,
		allowNegativeBalance: allowNegative,
	})
}

func (m *hegelMachine) createAccount(ht *hegel.T) {
	currency := hegel.Draw(ht, hegel.SampledFrom([]string{"USD", "EUR"}))
	m.doCreateAccount(ht, currency, true)
}

func (m *hegelMachine) createConstrainedAccount(ht *hegel.T) {
	currency := hegel.Draw(ht, hegel.SampledFrom([]string{"USD", "EUR"}))
	m.doCreateAccount(ht, currency, false)
}

func (m *hegelMachine) pickPool(ht *hegel.T) []*accountModel {
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
		if hegel.Draw(ht, hegel.Booleans()) {
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

func (m *hegelMachine) createTransfer(ht *hegel.T) {
	pool := m.pickPool(ht)
	if pool == nil {
		return
	}

	fromIdx := hegel.Draw(ht, hegel.Integers(0, len(pool)-1))
	toIdx := hegel.Draw(ht, hegel.Integers(0, len(pool)-2))
	if toIdx >= fromIdx {
		toIdx++
	}
	from := pool[fromIdx]
	to := pool[toIdx]
	amount := hegel.Draw(ht, hegel.Integers(1, 999))

	transfer := createTransfer(ht, m.conn, from.id, to.id, strconv.Itoa(amount))
	m.transferIDs = append(m.transferIDs, transfer.ID)
	from.balance -= int64(amount)
	from.version++
	to.balance += int64(amount)
	to.version++
}

func (m *hegelMachine) createBatchTransfers(ht *hegel.T) {
	pool := m.pickPool(ht)
	if pool == nil {
		return
	}

	count := hegel.Draw(ht, hegel.Integers(2, 3))
	args := make([]any, 0, count*3)
	transferParts := []string{}
	for i := range count {
		fromIdx := hegel.Draw(ht, hegel.Integers(0, len(pool)-1))
		toIdx := hegel.Draw(ht, hegel.Integers(0, len(pool)-2))
		if toIdx >= fromIdx {
			toIdx++
		}
		amount := hegel.Draw(ht, hegel.Integers(1, 999))

		base := i * 3
		args = append(args, pool[fromIdx].id, pool[toIdx].id, strconv.Itoa(amount))
		transferParts = append(transferParts, fmt.Sprintf("($%d, $%d, $%d)", base+1, base+2, base+3))

		pool[fromIdx].balance -= int64(amount)
		pool[fromIdx].version++
		pool[toIdx].balance += int64(amount)
		pool[toIdx].version++
	}

	sql := fmt.Sprintf("select * from pgledger_create_transfers(%s)", strings.Join(transferParts, ", "))
	rows, err := m.conn.Query(ht.Context(), sql, args...)
	assert.NoError(ht, err)
	transfers, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[Transfer])
	assert.NoError(ht, err)
	for _, tr := range transfers {
		m.transferIDs = append(m.transferIDs, tr.ID)
	}
}

func (m *hegelMachine) createTransferFromConstrainedExpectFail(ht *hegel.T) {
	var constrained []*accountModel
	for _, a := range m.accounts {
		if !a.allowNegativeBalance && a.balance <= 0 {
			constrained = append(constrained, a)
		}
	}
	if len(constrained) == 0 {
		return
	}

	from := constrained[hegel.Draw(ht, hegel.Integers(0, len(constrained)-1))]

	var targets []*accountModel
	for _, a := range m.accounts {
		if a.id != from.id && a.currency == from.currency {
			targets = append(targets, a)
		}
	}
	if len(targets) == 0 {
		return
	}

	to := targets[hegel.Draw(ht, hegel.Integers(0, len(targets)-1))]
	amount := hegel.Draw(ht, hegel.Integers(1, 999))

	_, err := createTransferReturnErr(ht.Context(), m.conn, from.id, to.id, strconv.Itoa(amount))
	assert.ErrorContains(ht, err, "does not allow negative balance")
}

func (m *hegelMachine) checkInvariants(ht *hegel.T) {
	for _, acc := range m.accounts {
		dbAcc := getAccount(ht, m.conn, acc.id)
		entries := getEntries(ht, m.conn, acc.id)
		assertAccountInvariants(ht, acc, dbAcc, entries)
	}
	m.checkZeroSum(ht)
	m.checkTwoEntriesPerTransfer(ht)
}

func (m *hegelMachine) checkZeroSum(ht *hegel.T) {
	assertZeroSum(ht, m.conn, m.accountIDs())
}

func (m *hegelMachine) accountIDs() []string {
	ids := make([]string, len(m.accounts))
	for i, a := range m.accounts {
		ids[i] = a.id
	}
	return ids
}

func (m *hegelMachine) checkTwoEntriesPerTransfer(ht *hegel.T) {
	assertTwoEntriesPerTransfer(ht, m.conn, m.transferIDs)
}

// TestHegelLedgerStateMachine runs a stateful property test using hegel.
// It generates random sequences of actions and checks all invariants after every step.
func TestHegelLedgerStateMachine(t *testing.T) {
	t.Parallel()
	actions := []string{
		"CreateAccount",
		"CreateConstrainedAccount",
		"CreateTransfer",
		"CreateBatchTransfers",
		"TransferFromConstrainedExpectFail",
	}
	conn := dbconn(t)
	t.Run("state machine", hegel.Case(func(ht *hegel.T) {
		m := &hegelMachine{}
		m.setup(ht, conn)

		numSteps := hegel.Draw(ht, hegel.Integers(1, 50))
		for range numSteps {
			action := hegel.Draw(ht, hegel.SampledFrom(actions))
			switch action {
			case "CreateAccount":
				m.createAccount(ht)
			case "CreateConstrainedAccount":
				m.createConstrainedAccount(ht)
			case "CreateTransfer":
				m.createTransfer(ht)
			case "CreateBatchTransfers":
				m.createBatchTransfers(ht)
			case "TransferFromConstrainedExpectFail":
				m.createTransferFromConstrainedExpectFail(ht)
			}
			m.checkInvariants(ht)
		}
	}))
}

// TestHegelLedgerConcurrentStateMachine runs multiple goroutines performing transfers
// against shared accounts, then checks all invariants hold.
func TestHegelLedgerConcurrentStateMachine(t *testing.T) {
	t.Parallel()
	conn := dbconn(t)
	t.Run("concurrent state machine", hegel.Case(func(ht *hegel.T) {
		numAccounts := hegel.Draw(ht, hegel.Integers(3, 6))
		accounts := make([]*accountModel, numAccounts)
		for i := range numAccounts {
			n := propCounter.Add(1)
			acc := createAccount(ht, conn, fmt.Sprintf("concurrent-%d", n), "USD")
			accounts[i] = &accountModel{id: acc.ID, currency: "USD", allowNegativeBalance: true}
		}

		numWorkers := hegel.Draw(ht, hegel.Integers(2, 4))
		transfersPerWorker := hegel.Draw(ht, hegel.Integers(5, 20))

		type transferParam struct {
			fromID string
			toID   string
			amount string
		}
		workerParams := make([][]transferParam, numWorkers)
		for w := range numWorkers {
			workerParams[w] = make([]transferParam, transfersPerWorker)
			for j := range transfersPerWorker {
				fromIdx := hegel.Draw(ht, hegel.Integers(0, numAccounts-1))
				toIdx := hegel.Draw(ht, hegel.Integers(0, numAccounts-2))
				if toIdx >= fromIdx {
					toIdx++
				}
				amount := hegel.Draw(ht, hegel.Integers(1, 100))
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
					transfer, err := createTransferReturnErr(ht.Context(), conn,
						p.fromID, p.toID, p.amount)
					if err == nil {
						allTransferIDs.Store(transfer.ID, true)
					}
				}
			})
		}
		wg.Wait()

		accountIDs := make([]string, numAccounts)
		for i, a := range accounts {
			accountIDs[i] = a.id
		}
		assertZeroSum(ht, conn, accountIDs)

		for _, acc := range accounts {
			entries := getEntries(ht, conn, acc.id)
			if len(entries) == 0 {
				continue
			}
			assert.Equal(ht, "0", entries[0].AccountPreviousBalance,
				"account %s: first entry previous_balance should be 0", acc.id)
			for i, e := range entries {
				prev, _ := strconv.ParseInt(e.AccountPreviousBalance, 10, 64)
				amt, _ := strconv.ParseInt(e.Amount, 10, 64)
				curr, _ := strconv.ParseInt(e.AccountCurrentBalance, 10, 64)
				assert.Equal(ht, prev+amt, curr,
					"account %s entry[%d]: chain broken", acc.id, i)
				assert.Equal(ht, i+1, e.AccountVersion,
					"account %s entry[%d]: version not monotonic", acc.id, i)
			}
			dbAcc := getAccount(ht, conn, acc.id)
			lastCurr := entries[len(entries)-1].AccountCurrentBalance
			assert.Equal(ht, dbAcc.Balance, lastCurr,
				"account %s: last entry balance != account balance", acc.id)
		}

		var transferIDs []string
		allTransferIDs.Range(func(key, _ any) bool {
			transferIDs = append(transferIDs, key.(string))
			return true
		})
		assertTwoEntriesPerTransfer(ht, conn, transferIDs)
	}))
}
