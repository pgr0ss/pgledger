package propertytest

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/ledgertest"
	"hegel.dev/go/hegel"
)

func describe(reqs []request) string {
	parts := make([]string, 0, len(reqs))
	for _, r := range reqs {
		parts = append(parts, fmt.Sprintf("%d->%d %s", r.FromIdx, r.ToIdx, r.Amount))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// assertParity is the accept/reject comparison between the oracle and
// pgledger, plus the rollback check that a rejected call left no trace.
func assertParity(tc hegel.TestCase, s *scope, reqs []request, before snapshot, reason string, err error) {
	switch {
	case reason == "" && err != nil:
		tc.Errorf("model accepted %s but pgledger rejected it: %v", describe(reqs), err)
	case reason != "" && err == nil:
		tc.Errorf("model rejected %s (%s) but pgledger accepted it", describe(reqs), reason)
	case reason != "":
		if diff := before.diff(s.snapshot(tc)); diff != "" {
			tc.Errorf("rejected batch %s mutated the ledger: %s", describe(reqs), diff)
		}
	}
}

// LedgerMachine drives the three transfer entry points against the oracle. It
// is sequential: accept/reject parity is only predictable when the model sees
// the operations in the order pgledger applied them.
//
// LedgerMachine and its constructor are exported so propertytest/continuous
// can drive them; identifiers declared in _test.go files are unreachable from
// a main package.
type LedgerMachine struct {
	scope *scope
	model *model
}

// DefaultSpecs is the account set the continuous runner starts from: same
// currency, one account that may not go negative so the constraint path is
// exercised.
func DefaultSpecs() []accountSpec {
	return []accountSpec{
		{Currency: "USD", AllowNeg: true, AllowPos: true},
		{Currency: "USD", AllowNeg: false, AllowPos: true},
		{Currency: "EUR", AllowNeg: true, AllowPos: true},
	}
}

func NewLedgerMachine(tc hegel.TestCase, conn *pgxpool.Pool, ctx context.Context, specs []accountSpec) *LedgerMachine {
	return &LedgerMachine{
		scope: newScope(tc, conn, ctx, specs),
		model: newModel(specs),
	}
}

func (m *LedgerMachine) RuleCreateAccount(tc hegel.TestCase) {
	spec := hegel.Draw(tc, accountSpecGen())
	m.scope.addAccount(tc, spec)
	m.model.addAccount(spec)
}

func (m *LedgerMachine) RuleTransfers(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	reqs := hegel.Draw(tc, batchGen(m.scope.size(), amountGen()))
	before := m.scope.snapshot(tc)
	reason := m.model.applyBatch(reqs)
	_, err := m.scope.createTransfers(reqs, nil, nil)
	assertParity(tc, m.scope, reqs, before, reason, err)
}

func (m *LedgerMachine) RuleTransferSingleAPI(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	req := hegel.Draw(tc, requestGen(m.scope.size(), amountGen()))
	reqs := []request{req}
	before := m.scope.snapshot(tc)
	reason := m.model.applyBatch(reqs)
	_, err := m.scope.createTransfer(req, nil, nil)
	assertParity(tc, m.scope, reqs, before, reason, err)
}

func (m *LedgerMachine) RuleTransfersVariadicAPI(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	reqs := hegel.Draw(tc, batchGen(m.scope.size(), amountGen()))
	before := m.scope.snapshot(tc)
	reason := m.model.applyBatch(reqs)
	_, err := m.scope.createTransfersVariadic(reqs)
	assertParity(tc, m.scope, reqs, before, reason, err)
}

// RuleEmptyBatch pins the contract that an empty request array is accepted and
// changes nothing.
func (m *LedgerMachine) RuleEmptyBatch(tc hegel.TestCase) {
	before := m.scope.snapshot(tc)
	returned, err := m.scope.createTransfers(nil, nil, nil)
	if err != nil {
		tc.Errorf("empty batch was rejected: %v", err)
	}
	if len(returned) != 0 {
		tc.Errorf("empty batch returned %d transfers", len(returned))
	}
	if diff := before.diff(m.scope.snapshot(tc)); diff != "" {
		tc.Errorf("empty batch mutated the ledger: %s", diff)
	}
}

func (m *LedgerMachine) InvariantBalancesMatchModel(tc hegel.TestCase) {
	m.scope.assertMatchesModel(tc, m.model)
}

func (m *LedgerMachine) InvariantStructure(tc hegel.TestCase) {
	m.scope.assertInvariants(tc)
}

// ConcurrentLedger runs transfers from several workers at once over accounts
// that permit any balance. Accept/reject is interleaving-dependent, so the
// oracle is dropped: what must hold is that no call fails, that value is
// conserved, and that no update is lost.
type ConcurrentLedger struct {
	scope    *scope
	accounts *hegel.Pool[string]
}

func NewConcurrentLedger(tc hegel.TestCase, conn *pgxpool.Pool, ctx context.Context) *ConcurrentLedger {
	return &ConcurrentLedger{
		scope:    newScope(tc, conn, ctx, nil),
		accounts: hegel.NewPool[string](tc),
	}
}

func (m *ConcurrentLedger) RuleOpenAccount(tc hegel.TestCase) {
	id := m.scope.addAccount(tc, accountSpec{Currency: "USD", AllowNeg: true, AllowPos: true})
	m.accounts.Add(tc, id)
}

func (m *ConcurrentLedger) RuleTransfer(tc hegel.TestCase) {
	from := hegel.Draw(tc, m.accounts.ValuesReusable())
	to := hegel.Draw(tc, m.accounts.ValuesReusable())
	tc.Assume(from != to)
	amount := hegel.Draw(tc, amountGen())

	if _, err := ledgertest.CreateTransferReturnErr(m.scope.ctx, m.scope.conn, from, to, amount); err != nil {
		// The sorted lock order in pgledger_create_transfers is what makes a
		// single-statement call deadlock-free, so any error here is a finding
		// (40P01 in particular).
		tc.Errorf("concurrent transfer %s -> %s of %s failed: %v", from, to, amount, err)
	}
}

// InvariantConservation runs at a round barrier, never concurrently with
// rules, which is the quiescent point where conservation must hold.
func (m *ConcurrentLedger) InvariantConservation(tc hegel.TestCase) {
	m.scope.assertInvariants(tc)
}
