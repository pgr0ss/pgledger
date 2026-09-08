package propertytest

import (
	"strings"
	"testing"

	"github.com/pgr0ss/pgledger/testhelpers"
	"hegel.dev/go/hegel"
)

func TestLedgerStateMachine(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		m := NewLedgerMachine(ht, ctx, conn, DefaultSpecs())
		hegel.RunStateful(ht, m, append(LedgerMachineOptions(), hegel.WithStatefulStepCount(20))...)
		ht.Target(float64(m.Accepted()), "accepted transfers")
	})
}

func TestLedgerUnderConcurrency(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	var found Findings
	// Each case runs a whole machine, so it gets a tenth of the budget.
	check(t, func(ht *T) {
		hegel.RunStateful(ht, NewConcurrentLedger(ht, ctx, conn, &found),
			append(ConcurrentLedgerOptions(4), hegel.WithStatefulStepCount(30))...)
	}, hegel.WithTestCases(max(1, propertyCases()/10)))

	if failures := found.Failures(); !t.Failed() && len(failures) != 0 {
		t.Fatalf("hegel reported success but the concurrent run recorded %d failure(s):\n%s", len(failures), strings.Join(failures, "\n"))
	}
}
