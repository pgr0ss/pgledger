// Command continuous runs the pgledger property suite as a soak workload.
//
// PGLEDGER_SOAK_DURATION (default 60s) bounds the run. It is an environment
// variable because hegel.Workload owns the command line and rejects unknown
// flags.
package main

import (
	"context"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/pgr0ss/pgledger/propertytest"
	"github.com/pgr0ss/pgledger/testhelpers"
	"hegel.dev/go/hegel"
)

func soakDuration() time.Duration {
	value := os.Getenv("PGLEDGER_SOAK_DURATION")
	if value == "" {
		return time.Minute
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		log.Fatalf("PGLEDGER_SOAK_DURATION=%q: %v", value, err)
	}
	return duration
}

func main() {
	duration := soakDuration()
	deadline := time.Now().Add(duration)

	ctx := context.Background()
	conn, err := testhelpers.Connect(ctx)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	// Findings catches failures hegel reports as success (see
	// propertytest.Findings). The deadline exits from inside the callback, so
	// it must be checked there as well as after Workload returns.
	var found propertytest.Findings
	report := func() {
		if failures := found.Failures(); len(failures) != 0 {
			log.Fatalf("concurrent run reported %d failure(s):\n%s", len(failures), strings.Join(failures, "\n"))
		}
	}

	// Workload runs unbounded by default; the deadline is what ends the run.
	hegel.Workload(func(tc hegel.TestCase) {
		if time.Now().After(deadline) {
			report()
			log.Printf("ran for %s with no property failures", duration)
			os.Exit(0)
		}

		if hegel.Draw(tc, hegel.Booleans()) {
			m := propertytest.NewLedgerMachine(tc, ctx, conn, propertytest.DefaultSpecs())
			hegel.RunStateful(tc, m, propertytest.LedgerMachineOptions()...)
			tc.Target(float64(m.Accepted()), "accepted transfers")
			return
		}
		hegel.RunStateful(tc, propertytest.NewConcurrentLedger(tc, ctx, conn, &found),
			propertytest.ConcurrentLedgerOptions(runtime.GOMAXPROCS(0))...)
	})

	report()
}
