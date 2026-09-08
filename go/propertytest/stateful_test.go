//go:build property

package propertytest

import (
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
		hegel.RunStateful(ht, NewLedgerMachine(ht, conn, ctx, DefaultSpecs()))
	}, hegel.WithTestCases(propertyCases()),
		hegel.WithStatefulStepCount(20),
		hegel.WithDatabase("testdata/hegel"))
}

// TestLedgerUnderConcurrency runs transfers from several workers at once. The
// sorted lock order inside pgledger_create_transfers is what must make this
// deadlock-free, and conservation must hold at every round barrier.
func TestLedgerUnderConcurrency(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		hegel.RunStateful(ht, NewConcurrentLedger(ht, conn, ctx),
			hegel.WithBoundedConcurrency(4),
			hegel.WithRuleGroup("ledger", "RuleOpenAccount", "RuleTransfer"),
		)
	}, hegel.WithTestCases(10),
		hegel.WithStatefulStepCount(30),
		hegel.WithDatabase("testdata/hegel"))
}
