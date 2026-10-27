package propertytest

import (
	"fmt"
	"math/big"
	"slices"
)

type modelAccount struct {
	ID       string
	Name     string
	Currency string
	AllowNeg bool
	AllowPos bool
	Balance  *big.Rat
	Version  int64
}

// checkConstraints mirrors pgledger_check_account_balance_constraints.
func (a *modelAccount) checkConstraints() string {
	if !a.AllowNeg && a.Balance.Sign() < 0 {
		return fmt.Sprintf("Account (id=%s, name=%s) does not allow negative balance", a.ID, a.Name)
	}
	if !a.AllowPos && a.Balance.Sign() > 0 {
		return fmt.Sprintf("Account (id=%s, name=%s) does not allow positive balance", a.ID, a.Name)
	}
	return ""
}

// model is the oracle: an independent implementation of
// pgledger_create_transfers that predicts balances and rejection messages.
// claims maps each claimed idempotency key to the call that claimed it.
type model struct {
	accounts []modelAccount
	claims   map[string]call
}

func (m *model) addAccount(a scopeAccount) {
	m.accounts = append(m.accounts, modelAccount{
		ID:       a.ID,
		Name:     a.Name,
		Currency: a.Spec.Currency,
		AllowNeg: a.Spec.AllowNeg,
		AllowPos: a.Spec.AllowPos,
		Balance:  new(big.Rat),
	})
}

func (m *model) clone() *model {
	out := &model{accounts: make([]modelAccount, len(m.accounts)), claims: m.claims}
	for i, a := range m.accounts {
		out.accounts[i] = a
		out.accounts[i].Balance = new(big.Rat).Set(a.Balance)
	}
	return out
}

func (m *model) accountID(idx int) string {
	switch idx {
	case nullAccount:
		return "<NULL>"
	case unknownAccount:
		return unknownAccountID
	}
	return m.accounts[idx].ID
}

// applyBatch returns the first message pgledger would raise, or "" when the
// batch is accepted. A rejected batch leaves m untouched, like the rollback.
func (m *model) applyBatch(reqs []request) string {
	trial := m.clone()

	for _, r := range reqs {
		amount, ok := new(big.Rat).SetString(r.Amount)
		if !ok || amount.Sign() <= 0 {
			return fmt.Sprintf("Amount (%s) must be a positive finite number", r.Amount)
		}

		if r.hasAccount(nullAccount) {
			return fmt.Sprintf("Account ids (from=%s, to=%s) must not be null",
				trial.accountID(r.FromIdx), trial.accountID(r.ToIdx))
		}

		if r.FromIdx == r.ToIdx {
			return fmt.Sprintf("Cannot transfer to the same account (id=%s)", trial.accountID(r.FromIdx))
		}

		if r.FromIdx == unknownAccount {
			return fmt.Sprintf("Account (id=%s) does not exist", unknownAccountID)
		}
		from := &trial.accounts[r.FromIdx]
		from.Balance.Sub(from.Balance, amount)
		from.Version++
		if message := from.checkConstraints(); message != "" {
			return message
		}

		if r.ToIdx == unknownAccount {
			return fmt.Sprintf("Account (id=%s) does not exist", unknownAccountID)
		}
		to := &trial.accounts[r.ToIdx]
		to.Balance.Add(to.Balance, amount)
		to.Version++
		if message := to.checkConstraints(); message != "" {
			return message
		}

		// pgledger compares currencies only after both balances are updated.
		if from.Currency != to.Currency {
			return fmt.Sprintf("Cannot transfer between different currencies (%s and %s)",
				from.Currency, to.Currency)
		}
	}

	*m = *trial
	return ""
}

func idempotencyMismatch(idempotencyKey string) string {
	return fmt.Sprintf("Idempotency key (%s) was already used with a different request", idempotencyKey)
}

// applyCall checks c's key before any request: a claimed key either replays
// (the returned transfers are the claiming call's) or rejects. Only an
// accepted call with at least one request claims its key.
func (m *model) applyCall(c call) (replayed bool, message string) {
	if c.IdempotencyKey != nil {
		if prior, ok := m.claims[*c.IdempotencyKey]; ok {
			if prior.matches(c) {
				return true, ""
			}
			return false, idempotencyMismatch(*c.IdempotencyKey)
		}
	}

	if message := m.applyBatch(c.Reqs); message != "" {
		return false, message
	}
	if c.IdempotencyKey != nil && len(c.Reqs) > 0 {
		if m.claims == nil {
			m.claims = map[string]call{}
		}
		m.claims[*c.IdempotencyKey] = c
	}
	return false, ""
}

// matches compares requests in order with amounts compared numerically, then
// event_at and metadata as jsonb would.
func (c call) matches(other call) bool {
	if (c.EventAt == nil) != (other.EventAt == nil) ||
		(c.EventAt != nil && !c.EventAt.Equal(*other.EventAt)) {
		return false
	}
	ours, theirs := c.metadataJSON(), other.metadataJSON()
	if (ours == nil) != (theirs == nil) || (ours != nil && *ours != *theirs) {
		return false
	}

	a, aOK := c.requestKeys()
	b, bOK := other.requestKeys()
	return aOK && bOK && slices.Equal(a, b)
}

// requestKeys is false when an amount is not a number, which no claimed call
// can hold.
func (c call) requestKeys() ([]string, bool) {
	out := make([]string, 0, len(c.Reqs))
	for _, r := range c.Reqs {
		amount, ok := new(big.Rat).SetString(r.Amount)
		if !ok {
			return nil, false
		}
		out = append(out, fmt.Sprintf("%d->%d %s", r.FromIdx, r.ToIdx, ratString(amount)))
	}
	return out, true
}
