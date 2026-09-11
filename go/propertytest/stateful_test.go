//go:build property

package propertytest

import (
	"strings"
	"testing"

	"github.com/pgr0ss/pgledger/ledgertest"
	"hegel.dev/go/hegel"
)

// TestLedgerStateMachine drives arbitrary sequences of account creation and
// the three transfer entry points against the oracle, re-checking every
// invariant after each step.
func TestLedgerStateMachine(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		m := NewLedgerMachine(ht, conn, ctx, DefaultSpecs())
		hegel.RunStateful(ht, m)
		ht.Target(float64(m.Accepted()), "accepted transfers")
	}, hegel.WithTestCases(propertyCases()),
		hegel.WithStatefulStepCount(20),
		hegel.WithDatabase("testdata/hegel"))
}

// TestLedgerUnderConcurrency runs transfers from several workers at once. The
// sorted lock order inside pgledger_create_transfers is what must make this
// deadlock-free, and conservation and the balance constraints must hold at
// every round barrier.
//
// No WithDatabase here: hegel disables shrinking, replay and example-database
// persistence for concurrent state machines. It also does not fail the Go test
// on a concurrent machine's failures, which is why the run is judged by the
// Findings it collected rather than by hegel's own verdict.
func TestLedgerUnderConcurrency(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	var found Findings
	hegel.Test(t, func(ht *hegel.T) {
		hegel.RunStateful(ht, NewConcurrentLedger(ht, conn, ctx, &found),
			hegel.WithBoundedConcurrency(4),
			hegel.WithRuleGroup("ledger",
				"RuleOpenAccount",
				"RuleOpenNegativeForbiddenAccount",
				"RuleTransfer",
				"RuleTransferToConstrainedAccount",
				"RuleTransferFromConstrainedAccount",
			),
		)
	}, hegel.WithTestCases(10),
		hegel.WithStatefulStepCount(30))

	if failures := found.Failures(); len(failures) != 0 {
		t.Fatalf("concurrent run reported %d failure(s):\n%s", len(failures), strings.Join(failures, "\n"))
	}
}
