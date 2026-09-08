package propertytest

import (
	"context"
	"path/filepath"
	"testing"

	"hegel.dev/go/hegel"
)

// T replaces *hegel.T: hegel.Test (v0.9.13) calls t.Fail() without a message
// when the engine itself ends the run (failed health check, nondeterminism,
// engine error), so properties run through hegel.Run, which returns the error.
// Bodies fail with Errorf, not a Fatalf wrapper here: hegel tells distinct
// failures apart by origin.
type T struct {
	hegel.TestCase
	t *testing.T
}

func (p *T) Context() context.Context { return p.t.Context() }

func (p *T) Cleanup(f func()) { p.t.Cleanup(f) }

// hegel.Run takes no database key, so each test gets its own example-database
// directory. Hegel's notes and failing examples go to stdout, not t's log.
func check(t *testing.T, body func(*T), opts ...hegel.Option) {
	t.Helper()

	opts = append([]hegel.Option{
		hegel.WithTestCases(propertyCases()),
		hegel.WithDatabase(filepath.Join("testdata", "hegel", t.Name())),
	}, opts...)
	if err := hegel.Run(func(tc hegel.TestCase) { body(&T{TestCase: tc, t: t}) }, opts...); err != nil {
		t.Fatal(err)
	}
}
