// Command continuous runs the pgledger property suite as a standalone,
// unbounded workload — the long-running counterpart to `just property-tests`.
package main

import (
	"context"
	"log"
	"runtime"

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
		hegel.RunStateful(tc, propertytest.NewConcurrentLedger(tc, conn, ctx),
			hegel.WithBoundedConcurrency(runtime.GOMAXPROCS(0)),
			hegel.WithRuleGroup("ledger", "RuleOpenAccount", "RuleTransfer"),
		)
	})
}
