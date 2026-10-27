package propertytest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/testhelpers"
	"hegel.dev/go/hegel"
)

func requireRejection(tc hegel.TestCase, err error, messages ...string) {
	pgErr := requireRaise(tc, err)
	for _, message := range messages {
		if strings.Contains(pgErr.Message, message) {
			return
		}
	}
	tc.Errorf("expected a rejection matching %v, got: %s", messages, pgErr.Message)
}

func requireRaise(tc hegel.TestCase, err error) *pgconn.PgError {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		tc.Errorf("expected a PostgreSQL error, got %T: %v", err, err)
		return nil
	}
	if pgErr.Code != "P0001" {
		tc.Errorf("expected a pgledger exception (P0001), got %s: %s", pgErr.Code, pgErr.Message)
		return nil
	}
	return pgErr
}

func describe(reqs []request) string {
	parts := make([]string, 0, len(reqs))
	for _, r := range reqs {
		parts = append(parts, fmt.Sprintf("%d->%d %s", r.FromIdx, r.ToIdx, r.Amount))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func assertParity(tc hegel.TestCase, s *scope, reqs []request, before snapshot, want string, err error) {
	switch {
	case want == "" && err != nil:
		tc.Errorf("model accepted %s but pgledger rejected it: %v", describe(reqs), err)
	case want != "" && err == nil:
		tc.Errorf("model rejected %s (%s) but pgledger accepted it", describe(reqs), want)
	case want != "":
		if got := requireRaise(tc, err).Message; got != want {
			tc.Errorf("batch %s was rejected with %q, model predicted %q", describe(reqs), got, want)
		}
		if diff := before.diff(s.snapshot(tc)); diff != "" {
			tc.Errorf("rejected batch %s mutated the ledger: %s", describe(reqs), diff)
		}
	}
}

// keyedCalls remembers, across one test's calls, the transfers each claimed
// key returned and the keyed calls made so far, for replays and mutations.
type keyedCalls struct {
	claimed map[string][]testhelpers.Transfer
	made    []call
}

func newKeyedCalls() *keyedCalls {
	return &keyedCalls{claimed: map[string][]testhelpers.Transfer{}}
}

// assert runs c against pgledger and the model, and returns the model's
// prediction, which assertParity has held pgledger to. A replay must return
// the claiming call's transfers unchanged and leave the ledger untouched.
func (k *keyedCalls) assert(tc hegel.TestCase, s *scope, m *model, c call) (replayed bool, want string) {
	before := s.snapshot(tc)
	replayed, want = m.applyCall(c)
	returned, err := s.createCall(c, hegel.Draw(tc, hegel.Booleans()))
	assertParity(tc, s, c.Reqs, before, want, err)
	if c.IdempotencyKey == nil {
		return replayed, want
	}
	k.made = append(k.made, c)

	switch {
	case err != nil || want != "":
	case replayed:
		if got, claimed := transferRows(returned), transferRows(k.claimed[*c.IdempotencyKey]); !slices.Equal(got, claimed) {
			tc.Errorf("replay of %s with key %s returned %v, but the claiming call returned %v",
				describe(c.Reqs), *c.IdempotencyKey, got, claimed)
		}
		if diff := before.diff(s.snapshot(tc)); diff != "" {
			tc.Errorf("replay of %s with key %s mutated the ledger: %s", describe(c.Reqs), *c.IdempotencyKey, diff)
		}
	case len(c.Reqs) > 0:
		k.claimed[*c.IdempotencyKey] = returned
	}
	return replayed, want
}

func transferRows(transfers []testhelpers.Transfer) []string {
	rows := make([]string, 0, len(transfers))
	for _, t := range transfers {
		metadata := "<null>"
		if t.Metadata != nil {
			metadata = *t.Metadata
		}
		rows = append(rows, fmt.Sprintf("%s %s->%s %s created %s event %s %s",
			t.ID, t.FromAccountID, t.ToAccountID, t.Amount,
			t.CreatedAt.UTC().Format(time.RFC3339Nano), t.EventAt.UTC().Format(time.RFC3339Nano), metadata))
	}
	return rows
}

// LedgerMachine is sequential: accept/reject parity is only predictable when
// the model sees operations in the order pgledger applied them.
type LedgerMachine struct {
	scope           *scope
	model           *model
	currency        string
	accepted        int
	idempotencyKeys []string
	keyed           *keyedCalls
}

// DefaultSpecs has one account per constraint flag, all in one currency: with
// mixed currencies nearly every step re-proves the currency check, which
// TestCrossCurrencyTransfersAreRejectedWithoutTrace already covers.
func DefaultSpecs() []accountSpec {
	return []accountSpec{
		{Currency: "USD", AllowNeg: true, AllowPos: true},
		{Currency: "USD", AllowNeg: false, AllowPos: true},
		{Currency: "USD", AllowNeg: true, AllowPos: false},
	}
}

func LedgerMachineOptions() []hegel.StateMachineOption {
	return []hegel.StateMachineOption{
		hegel.WithAlwaysCheckInvariants("InvariantBalancesMatchModel", "InvariantStructure"),
	}
}

func NewLedgerMachine(tc hegel.TestCase, ctx context.Context, conn *pgxpool.Pool, specs []accountSpec) *LedgerMachine {
	currency := "USD"
	if len(specs) > 0 {
		currency = specs[0].Currency
	}
	s := newScope(tc, ctx, conn, specs)
	return &LedgerMachine{
		scope:           s,
		model:           s.newModel(),
		currency:        currency,
		idempotencyKeys: s.idempotencyKeys(3),
		keyed:           newKeyedCalls(),
	}
}

func (m *LedgerMachine) RuleCreateAccount(tc hegel.TestCase) {
	spec := accountSpec{
		Currency: m.currency,
		AllowNeg: hegel.Draw(tc, hegel.Booleans()),
		AllowPos: hegel.Draw(tc, hegel.Booleans()),
	}
	m.model.addAccount(m.scope.addAccount(tc, spec))
}

func (m *LedgerMachine) recordAccepted(n int, err error) {
	if err == nil {
		m.accepted += n
	}
}

// Accepted is reported as a hegel target to steer away from runs where every
// call is rejected and the invariants hold vacuously. The caller reports it
// because RunStateful rejects exported TestCase methods without a
// Rule/Invariant prefix.
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

func (m *LedgerMachine) RuleIdempotentTransfers(tc hegel.TestCase) {
	tc.Assume(m.scope.size() >= 2)
	c := hegel.Draw(tc, nextCallGen(m.scope.size(), m.idempotencyKeys, m.keyed.made))
	if replayed, want := m.keyed.assert(tc, m.scope, m.model, c); !replayed && want == "" {
		m.accepted += len(c.Reqs)
	}
}

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

// Findings records concurrent-machine failures because hegel v0.9.23 reports
// success when a run ends with RUN_STATUS_FAILED_NONDETERMINISTIC
// (runWithContext returns io.Copy's nil error), which concurrent runs are
// likeliest to hit.
type Findings struct {
	mu   sync.Mutex
	list []string
}

func (f *Findings) Failures() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.list...)
}

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

// ConcurrentLedger has no oracle, since accept/reject depends on the
// interleaving. Unconstrained transfers must never fail (no deadlocks),
// constrained ones may fail only with their own constraint's message, and
// keyed ones only as an idempotency mismatch.
type ConcurrentLedger struct {
	scope             *scope
	accounts          *hegel.Pool[string]
	negativeForbidden *hegel.Pool[string]
	positiveForbidden *hegel.Pool[string]
	found             *Findings
	idempotencyKeys   []string

	mu sync.Mutex
	// intents is the first request drawn for each key, which later calls
	// mostly resend.
	intents map[string][3]string
	// claimed is the transfer id each key's accepted calls returned.
	claimed map[string]string
}

func NewConcurrentLedger(tc hegel.TestCase, ctx context.Context, conn *pgxpool.Pool, found *Findings) *ConcurrentLedger {
	tc = found.watch(tc)
	s := newScope(tc, ctx, conn, nil)
	return &ConcurrentLedger{
		scope:             s,
		accounts:          hegel.NewPool[string](tc),
		negativeForbidden: hegel.NewPool[string](tc),
		positiveForbidden: hegel.NewPool[string](tc),
		found:             found,
		idempotencyKeys:   s.idempotencyKeys(3),
		intents:           map[string][3]string{},
		claimed:           map[string]string{},
	}
}

func ConcurrentLedgerOptions(maxConcurrency int) []hegel.StateMachineOption {
	return []hegel.StateMachineOption{
		hegel.WithBoundedConcurrency(maxConcurrency),
		hegel.WithAlwaysCheckInvariants("InvariantStructure", "InvariantIdempotencyKeys"),
		hegel.WithRuleGroup("ledger",
			"RuleOpenAccount",
			"RuleOpenNegativeForbiddenAccount",
			"RuleOpenPositiveForbiddenAccount",
			"RuleTransfer",
			"RuleTransferRing",
			"RuleTransferToNegativeForbiddenAccount",
			"RuleTransferFromNegativeForbiddenAccount",
			"RuleTransferToPositiveForbiddenAccount",
			"RuleTransferFromPositiveForbiddenAccount",
			"RuleIdempotentTransfer",
		),
	}
}

func (m *ConcurrentLedger) RuleOpenAccount(tc hegel.TestCase) {
	account := m.scope.addAccount(m.found.watch(tc), accountSpec{Currency: "USD", AllowNeg: true, AllowPos: true})
	m.accounts.Add(tc, account.ID)
}

func (m *ConcurrentLedger) RuleOpenNegativeForbiddenAccount(tc hegel.TestCase) {
	account := m.scope.addAccount(m.found.watch(tc), accountSpec{Currency: "USD", AllowNeg: false, AllowPos: true})
	m.negativeForbidden.Add(tc, account.ID)
}

func (m *ConcurrentLedger) RuleOpenPositiveForbiddenAccount(tc hegel.TestCase) {
	account := m.scope.addAccount(m.found.watch(tc), accountSpec{Currency: "USD", AllowNeg: true, AllowPos: false})
	m.positiveForbidden.Add(tc, account.ID)
}

func (m *ConcurrentLedger) RuleTransferToNegativeForbiddenAccount(tc hegel.TestCase) {
	m.transferMustSucceed(tc, m.accounts, m.negativeForbidden)
}

func (m *ConcurrentLedger) RuleTransferFromNegativeForbiddenAccount(tc hegel.TestCase) {
	m.transferMayBeRejected(tc, m.negativeForbidden, m.accounts, "does not allow negative balance")
}

func (m *ConcurrentLedger) RuleTransferToPositiveForbiddenAccount(tc hegel.TestCase) {
	m.transferMayBeRejected(tc, m.accounts, m.positiveForbidden, "does not allow positive balance")
}

func (m *ConcurrentLedger) RuleTransferFromPositiveForbiddenAccount(tc hegel.TestCase) {
	m.transferMustSucceed(tc, m.positiveForbidden, m.accounts)
}

func (m *ConcurrentLedger) RuleTransfer(tc hegel.TestCase) {
	m.transferMustSucceed(tc, m.accounts, m.accounts)
}

// RuleTransferRing visits accounts in drawn order, so overlapping concurrent
// rings deadlock unless pgledger_create_transfers locks in sorted order.
func (m *ConcurrentLedger) RuleTransferRing(tc hegel.TestCase) {
	ring := hegel.Draw(tc, hegel.UniqueLists(m.accounts.ValuesReusable()).MinSize(3).MaxSize(5))

	rows := make([][3]any, len(ring))
	for i, from := range ring {
		rows[i] = [3]any{from, ring[(i+1)%len(ring)], hegel.Draw(tc, amountGen())}
	}
	if _, err := m.scope.createTransfersBetween(rows); err != nil {
		m.found.watch(tc).Errorf("concurrent ring %v failed: %v", rows, err)
	}
}

// RuleIdempotentTransfer mostly resends the first request drawn for its key,
// so concurrent calls race to claim the key with matching and mismatching
// requests. Whichever claims it, every accepted call must return its one
// transfer.
func (m *ConcurrentLedger) RuleIdempotentTransfer(tc hegel.TestCase) {
	idempotencyKey := hegel.Draw(tc, hegel.SampledFrom(m.idempotencyKeys))
	from, to, amount := m.drawTransfer(tc, m.accounts, m.accounts)
	resend := hegel.Draw(tc, hegel.WeightedBooleans(0.75))

	m.mu.Lock()
	if intent, ok := m.intents[idempotencyKey]; !ok {
		m.intents[idempotencyKey] = [3]string{from, to, amount}
	} else if resend {
		from, to, amount = intent[0], intent[1], intent[2]
	}
	m.mu.Unlock()

	watched := m.found.watch(tc)
	returned, err := m.scope.query(`select * from pgledger_create_transfer($1, $2, $3, idempotency_key => $4)`,
		from, to, amount, idempotencyKey)
	if err != nil {
		requireRejection(watched, err, idempotencyMismatch(idempotencyKey))
		return
	}
	if len(returned) != 1 {
		watched.Errorf("key %s returned %d transfers, want 1", idempotencyKey, len(returned))
		return
	}
	got := returned[0]
	if got.FromAccountID != from || got.ToAccountID != to ||
		mustRat(watched, "returned amount", got.Amount).Cmp(mustRat(watched, "requested amount", amount)) != 0 {
		watched.Errorf("key %s returned %s -> %s of %s for a request of %s -> %s of %s",
			idempotencyKey, got.FromAccountID, got.ToAccountID, got.Amount, from, to, amount)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if prior, ok := m.claimed[idempotencyKey]; !ok {
		m.claimed[idempotencyKey] = got.ID
	} else if prior != got.ID {
		watched.Errorf("key %s returned transfer %s, but an earlier call with it returned %s", idempotencyKey, got.ID, prior)
	}
}

func (m *ConcurrentLedger) transferMustSucceed(tc hegel.TestCase, fromPool, toPool *hegel.Pool[string]) {
	from, to, amount := m.drawTransfer(tc, fromPool, toPool)
	if _, err := testhelpers.CreateTransferReturnErr(m.scope.ctx, m.scope.conn, from, to, amount); err != nil {
		m.found.watch(tc).Errorf("concurrent transfer %s -> %s of %s failed: %v", from, to, amount, err)
	}
}

func (m *ConcurrentLedger) transferMayBeRejected(tc hegel.TestCase, fromPool, toPool *hegel.Pool[string], allowed string) {
	from, to, amount := m.drawTransfer(tc, fromPool, toPool)
	if _, err := testhelpers.CreateTransferReturnErr(m.scope.ctx, m.scope.conn, from, to, amount); err != nil {
		requireRejection(m.found.watch(tc), err, allowed)
	}
}

func (m *ConcurrentLedger) drawTransfer(tc hegel.TestCase, fromPool, toPool *hegel.Pool[string]) (from, to, amount string) {
	from = hegel.Draw(tc, fromPool.ValuesReusable())
	to = hegel.Draw(tc, toPool.ValuesReusable())
	tc.Assume(from != to)
	return from, to, hegel.Draw(tc, amountGen())
}

// InvariantStructure runs at round barriers, the quiescent point where
// conservation must hold.
func (m *ConcurrentLedger) InvariantStructure(tc hegel.TestCase) {
	m.scope.assertInvariants(m.found.watch(tc))
}

// InvariantIdempotencyKeys checks that each claimed key maps to exactly the one
// transfer its accepted calls returned.
func (m *ConcurrentLedger) InvariantIdempotencyKeys(tc hegel.TestCase) {
	tc = m.found.watch(tc)
	m.mu.Lock()
	claimed := maps.Clone(m.claimed)
	m.mu.Unlock()

	for _, idempotencyKey := range slices.Sorted(maps.Keys(claimed)) {
		rows, err := m.scope.conn.Query(m.scope.ctx,
			`select transfer_id from pgledger_idempotency_keys_view where idempotency_key = $1 order by request_index`, idempotencyKey)
		if err != nil {
			tc.Errorf("transfers with key %s: %v", idempotencyKey, err)
			return
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			tc.Errorf("transfers with key %s: %v", idempotencyKey, err)
			return
		}
		if !slices.Equal(ids, []string{claimed[idempotencyKey]}) {
			tc.Errorf("key %s maps to transfers %v, but its accepted calls returned %s", idempotencyKey, ids, claimed[idempotencyKey])
		}
	}
}
