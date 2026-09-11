// Command continuous runs the pgledger property suite as a standalone,
// unbounded workload — the long-running counterpart to `just property-tests`.
package main

import (
	"context"
	"log"
	"runtime"
	"strings"

	"github.com/pgr0ss/pgledger/ledgertest"
	"github.com/pgr0ss/pgledger/propertytest"
	"hegel.dev/go/hegel"
)

func main() {
	// Workload parses --test-cases, --derandomize, --database and
	// --single-test-case, and runs unbounded by default.
	hegel.Workload(func(tc hegel.TestCase) {
		ctx := context.Background()
		conn, err := ledgertest.Connect(ctx)
		if err != nil {
			log.Fatalf("connect: %v", err)
		}
		defer conn.Close()

		// The continuous runner drives the concurrent machine: LedgerMachine's
		// rules are sequential-only, because its oracle predicts rejection.
		var found propertytest.Findings
		hegel.RunStateful(tc, propertytest.NewConcurrentLedger(tc, conn, ctx, &found),
			hegel.WithBoundedConcurrency(runtime.GOMAXPROCS(0)),
			hegel.WithRuleGroup("ledger",
				"RuleOpenAccount",
				"RuleOpenNegativeForbiddenAccount",
				"RuleTransfer",
				"RuleTransferToConstrainedAccount",
				"RuleTransferFromConstrainedAccount",
			),
		)

		// hegel does not fail a concurrent state machine's test case, so the
		// workload has to exit on the failures the machine recorded — otherwise
		// a soak run reports success no matter what it found.
		if failures := found.Failures(); len(failures) != 0 {
			log.Fatalf("concurrent run reported %d failure(s):\n%s", len(failures), strings.Join(failures, "\n"))
		}
	})
}
