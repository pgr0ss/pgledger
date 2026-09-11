package propertytest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/ledgertest"
	"hegel.dev/go/hegel"
)

// pgledgerRejections are the messages pgledger raises itself. A rejected call
// must carry one of them: anything else — a not-null violation from a NULL
// falling through the guards, a foreign key violation, a plpgsql internal
// error — is a hole in the input validation rather than a contract.
var pgledgerRejections = []string{
	"must be a positive finite number",
	"Cannot transfer to the same account",
	"Cannot transfer between different currencies",
	"does not allow negative balance",
	"does not allow positive balance",
}

func requirePgledgerError(tc hegel.TestCase, err error) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		tc.Errorf("expected a PostgreSQL error, got %T: %v", err, err)
		return
	}
	if pgErr.Code != "P0001" {
		tc.Errorf("expected a pgledger exception (P0001), got %s: %s", pgErr.Code, pgErr.Message)
		return
	}
	for _, message := range pgledgerRejections {
		if strings.Contains(pgErr.Message, message) {
			return
		}
	}
	tc.Errorf("unrecognised pgledger rejection: %s", pgErr.Message)
}

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
	scope    *scope
	model    *model
	currency string
	accepted int
}

// DefaultSpecs is the account set the machines start from: one currency, and
// one account per constraint flag so every rejection path the oracle predicts
// is reachable.
//
// Every account the machine creates shares this currency. A machine with mixed
// currencies spends almost every step re-proving the currency check — measured
// at 28 transfers landed per 500 rule invocations — and the cross-currency
// contract is covered by TestCrossCurrencyTransfersAreRejectedWithoutTrace.
func DefaultSpecs() []accountSpec {
	return []accountSpec{
		{Currency: "USD", AllowNeg: true, AllowPos: true},
		{Currency: "USD", AllowNeg: false, AllowPos: true},
		{Currency: "USD", AllowNeg: true, AllowPos: false},
	}
}

func NewLedgerMachine(tc hegel.TestCase, conn *pgxpool.Pool, ctx context.Context, specs []accountSpec) *LedgerMachine {
	currency := "USD"
	if len(specs) > 0 {
		currency = specs[0].Currency
	}
	return &LedgerMachine{
		scope:    newScope(tc, conn, ctx, specs),
		model:    newModel(specs),
		currency: currency,
	}
}

func (m *LedgerMachine) RuleCreateAccount(tc hegel.TestCase) {
	spec := accountSpec{
		Currency: m.currency,
		AllowNeg: hegel.Draw(tc, hegel.Booleans()),
		AllowPos: hegel.Draw(tc, hegel.Booleans()),
	}
	m.scope.addAccount(tc, spec)
	m.model.addAccount(spec)
}

// recordAccepted accumulates the transfers the machine has landed. The
// invariants are trivially true while every call is rejected, so this count is
// the signal the engine can search on to keep the machine non-vacuous.
func (m *LedgerMachine) recordAccepted(n int, err error) {
	if err == nil {
		m.accepted += n
	}
}

// Accepted is the number of transfers the machine has landed, reported to the
// engine as a target once per test case. RunStateful rejects exported methods
// that take a TestCase without a Rule/Invariant prefix, so the observation
// itself is made by the caller.
func (m *LedgerMachine) Accepted() int {
	return m.accepted
}

func (m *LedgerMachine) RuleTransfers(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	reqs := hegel.Draw(tc, batchGen(m.scope.size(), amountGen()))
	before := m.scope.snapshot(tc)
	reason := m.model.applyBatch(reqs)
	_, err := m.scope.createTransfers(reqs, nil, nil)
	m.recordAccepted(len(reqs), err)
	assertParity(tc, m.scope, reqs, before, reason, err)
}

func (m *LedgerMachine) RuleTransferSingleAPI(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	req := hegel.Draw(tc, requestGen(m.scope.size(), amountGen()))
	reqs := []request{req}
	before := m.scope.snapshot(tc)
	reason := m.model.applyBatch(reqs)
	_, err := m.scope.createTransfer(req, nil, nil)
	m.recordAccepted(1, err)
	assertParity(tc, m.scope, reqs, before, reason, err)
}

func (m *LedgerMachine) RuleTransfersVariadicAPI(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	reqs := hegel.Draw(tc, batchGen(m.scope.size(), amountGen()))
	before := m.scope.snapshot(tc)
	reason := m.model.applyBatch(reqs)
	_, err := m.scope.createTransfersVariadic(reqs)
	m.recordAccepted(len(reqs), err)
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

// Findings collects the failures a concurrent state machine reported.
//
// hegel v0.6.33 logs a concurrent machine's failures — it prints "Concurrent
// state machine detected: this run is nondeterministic, so failures are
// reported from the execution that discovered them" — but the Go test still
// passes: an Errorf from a rule or invariant running under
// WithBoundedConcurrency does not fail the test the way the sequential path
// does. Measured with a deliberately failing invariant, which printed its
// failure and left the test green. So the concurrent machine records every
// failure here and the caller fails on them once the run is over.
type Findings struct {
	mu   sync.Mutex
	list []string
}

func (f *Findings) Failures() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.list...)
}

// watch wraps tc so failures land in f as well as in hegel's own report.
func (f *Findings) watch(tc hegel.TestCase) hegel.TestCase {
	return &watched{TestCase: tc, findings: f}
}

type watched struct {
	hegel.TestCase
	findings *Findings
}

func (w *watched) Errorf(format string, args ...any) {
	w.findings.mu.Lock()
	w.findings.list = append(w.findings.list, fmt.Sprintf(format, args...))
	w.findings.mu.Unlock()
	w.TestCase.Errorf(format, args...)
}

// ConcurrentLedger runs transfers from several workers at once. Accept/reject
// is interleaving-dependent, so the oracle is dropped; what must hold is that
// unconstrained transfers never fail, that constrained ones only ever fail
// with a pgledger rejection (never a deadlock), that a negative-forbidden
// account never ends up negative, that value is conserved and that no update
// is lost.
type ConcurrentLedger struct {
	scope       *scope
	accounts    *hegel.Pool[string]
	constrained *hegel.Pool[string]
	found       *Findings
}

func NewConcurrentLedger(tc hegel.TestCase, conn *pgxpool.Pool, ctx context.Context, found *Findings) *ConcurrentLedger {
	tc = found.watch(tc)
	return &ConcurrentLedger{
		scope:       newScope(tc, conn, ctx, nil),
		accounts:    hegel.NewPool[string](tc),
		constrained: hegel.NewPool[string](tc),
		found:       found,
	}
}

func (m *ConcurrentLedger) RuleOpenAccount(tc hegel.TestCase) {
	id := m.scope.addAccount(m.found.watch(tc), accountSpec{Currency: "USD", AllowNeg: true, AllowPos: true})
	m.accounts.Add(tc, id)
}

// RuleOpenNegativeForbiddenAccount adds an account that may never go negative.
// Its balance is the reachability assertion InvariantConstraintsHold checks.
func (m *ConcurrentLedger) RuleOpenNegativeForbiddenAccount(tc hegel.TestCase) {
	id := m.scope.addAccount(m.found.watch(tc), accountSpec{Currency: "USD", AllowNeg: false, AllowPos: true})
	m.constrained.Add(tc, id)
}

// RuleTransferToConstrainedAccount funds a negative-forbidden account, and
// RuleTransferFromConstrainedAccount drains it. Draining may legitimately be
// rejected depending on the interleaving, so only the error class is asserted.
func (m *ConcurrentLedger) RuleTransferToConstrainedAccount(tc hegel.TestCase) {
	from := hegel.Draw(tc, m.accounts.ValuesReusable())
	to := hegel.Draw(tc, m.constrained.ValuesReusable())
	amount := hegel.Draw(tc, amountGen())

	if _, err := ledgertest.CreateTransferReturnErr(m.scope.ctx, m.scope.conn, from, to, amount); err != nil {
		m.found.watch(tc).Errorf("funding constrained account %s from %s with %s failed: %v", to, from, amount, err)
	}
}

func (m *ConcurrentLedger) RuleTransferFromConstrainedAccount(tc hegel.TestCase) {
	from := hegel.Draw(tc, m.constrained.ValuesReusable())
	to := hegel.Draw(tc, m.accounts.ValuesReusable())
	amount := hegel.Draw(tc, amountGen())

	if _, err := ledgertest.CreateTransferReturnErr(m.scope.ctx, m.scope.conn, from, to, amount); err != nil {
		requirePgledgerError(m.found.watch(tc), err)
	}
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
		m.found.watch(tc).Errorf("concurrent transfer %s -> %s of %s failed: %v", from, to, amount, err)
	}
}

// InvariantConservation runs at a round barrier, never concurrently with
// rules, which is the quiescent point where conservation must hold.
func (m *ConcurrentLedger) InvariantConservation(tc hegel.TestCase) {
	m.scope.assertInvariants(m.found.watch(tc))
}

// InvariantConstraintsHold is the reachability half: whatever the interleaving
// did, no account that forbids a negative balance may hold one.
func (m *ConcurrentLedger) InvariantConstraintsHold(tc hegel.TestCase) {
	m.scope.reportViolations(m.found.watch(tc), "constrained account holds a forbidden balance", `
		select format('account %s (negative %s / positive %s) holds %s',
			id, allow_negative_balance, allow_positive_balance, balance)
		from pgledger_accounts_view
		where id = any($1)
			and ((not allow_negative_balance and balance < 0)
				or (not allow_positive_balance and balance > 0))`, m.scope.accountIDs())
}
