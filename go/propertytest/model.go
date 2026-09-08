package propertytest

import (
	"fmt"
	"math/big"
)

// modelAccount mirrors one row of pgledger_accounts, with an exact balance.
type modelAccount struct {
	Currency string
	AllowNeg bool
	AllowPos bool
	Balance  *big.Rat
	Version  int64
}

func (a *modelAccount) checkConstraints() string {
	if !a.AllowNeg && a.Balance.Sign() < 0 {
		return "does not allow negative balance"
	}
	if !a.AllowPos && a.Balance.Sign() > 0 {
		return "does not allow positive balance"
	}
	return ""
}

// model is the in-memory oracle: an independent implementation of
// pgledger_create_transfers' semantics, used to predict both the resulting
// balances and whether a call is accepted at all.
type model struct {
	accounts []modelAccount
}

func newModel(specs []accountSpec) *model {
	m := &model{}
	for _, spec := range specs {
		m.addAccount(spec)
	}
	return m
}

func (m *model) addAccount(spec accountSpec) {
	m.accounts = append(m.accounts, modelAccount{
		Currency: spec.Currency,
		AllowNeg: spec.AllowNeg,
		AllowPos: spec.AllowPos,
		Balance:  new(big.Rat),
	})
}

func (m *model) clone() *model {
	out := &model{accounts: make([]modelAccount, len(m.accounts))}
	for i, a := range m.accounts {
		out.accounts[i] = a
		out.accounts[i].Balance = new(big.Rat).Set(a.Balance)
	}
	return out
}

// applyBatch mirrors pgledger_create_transfers request by request: debit and
// check the source, credit and check the destination, then compare currencies.
// It returns the first rejection reason, or "" when the whole batch is
// accepted. A rejected batch leaves the model untouched, mirroring the
// transactional rollback.
func (m *model) applyBatch(reqs []request) string {
	trial := m.clone()

	for _, r := range reqs {
		amount, ok := new(big.Rat).SetString(r.Amount)
		if !ok {
			return fmt.Sprintf("amount %q is not a finite number", r.Amount)
		}
		if amount.Sign() <= 0 {
			return fmt.Sprintf("amount %q must be positive", r.Amount)
		}
		if r.FromIdx == r.ToIdx {
			return "cannot transfer to the same account"
		}

		from := &trial.accounts[r.FromIdx]
		to := &trial.accounts[r.ToIdx]

		from.Balance.Sub(from.Balance, amount)
		from.Version++
		if reason := from.checkConstraints(); reason != "" {
			return fmt.Sprintf("source account %d %s", r.FromIdx, reason)
		}

		to.Balance.Add(to.Balance, amount)
		to.Version++
		if reason := to.checkConstraints(); reason != "" {
			return fmt.Sprintf("destination account %d %s", r.ToIdx, reason)
		}

		// pgledger compares currencies only after both balances are updated.
		if from.Currency != to.Currency {
			return fmt.Sprintf("cannot transfer between different currencies (%s and %s)",
				from.Currency, to.Currency)
		}
	}

	*m = *trial
	return ""
}
