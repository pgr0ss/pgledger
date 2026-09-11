// Package propertytest holds the pgledger property-based test suite: value
// generators, an in-memory oracle, and the Hegel state machines. The go test
// entry points live in the //go:build property files alongside these; the
// machines are exported so the continuous binary can drive the same
// definitions.
package propertytest

import (
	"math"
	"math/big"
	"os"
	"strconv"
	"time"

	"hegel.dev/go/hegel"
)

// propertyCases is the per-test case budget: small enough for local iteration
// via `just property-tests`, overridable for CI and continuous runs.
func propertyCases() int {
	if v := os.Getenv("PGLEDGER_HEGEL_CASES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

// mustRat parses a NUMERIC read back from PostgreSQL as an exact decimal. It
// fails the test case when the value is not one, which is how a NaN or
// Infinity balance is detected: those are not numbers, and no later transfer
// can restore an account that holds one.
//
// hegel.TestCase.Errorf aborts the test case, so the zero value returned after
// it is unreachable; it exists so a caller can never dereference a nil *big.Rat
// if that ever changes.
func mustRat(tc hegel.TestCase, what, s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		tc.Errorf("%s is not a finite number: %q", what, s)
		return new(big.Rat)
	}
	return r
}

func ratString(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	return r.FloatString(40)
}

// amountGen draws a positive finite decimal with a realistic mix of scales.
// The constraint lives in the generator: no Assume, no rejected test cases.
func amountGen() hegel.Generator[string] {
	return hegel.Composite(func(tc hegel.TestCase) string {
		units := hegel.Draw(tc, hegel.Integers[int64](1, 1_000_000_000))
		scale := hegel.Draw(tc, hegel.Integers(0, 6))
		r := new(big.Rat).SetFrac(
			big.NewInt(units),
			new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil),
		)
		return r.FloatString(scale)
	})
}

// hostileAmountGen widens amountGen to everything NUMERIC accepts, including
// the values the ledger has no business storing.
func hostileAmountGen() hegel.Generator[string] {
	return hegel.OneOf(
		amountGen(),
		hegel.SampledFrom([]string{
			"0", "0.00", "-1", "-0.01",
			"NaN", "Infinity", "-Infinity",
			"1e-40", "1e100",
		}),
	)
}

// currencies includes 'usd' alongside 'USD': currency comparison is
// case-sensitive, so those two must never transfer to each other.
var currencies = []string{"USD", "EUR", "JPY", "usd"}

func currencyGen() hegel.Generator[string] {
	return hegel.SampledFrom(currencies)
}

type accountSpec struct {
	Currency string
	AllowNeg bool
	AllowPos bool
}

// unconstrainedSpecGen draws accounts that permit any balance, for the
// properties where a rejection would only add noise.
func unconstrainedSpecGen() hegel.Generator[accountSpec] {
	return hegel.Composite(func(tc hegel.TestCase) accountSpec {
		return accountSpec{Currency: hegel.Draw(tc, currencyGen()), AllowNeg: true, AllowPos: true}
	})
}

// sameCurrencySpecsGen draws an account set in a single currency, optionally
// with varied balance constraints.
//
// Independent rejection causes are deliberately kept out of one generator: a
// mixed-currency set rejects most requests on the currency check, which then
// masks whatever the property is really about (the amount domain, or
// constraint parity).
func sameCurrencySpecsGen(minAccounts, maxAccounts int, constrained bool) hegel.Generator[[]accountSpec] {
	return hegel.Composite(func(tc hegel.TestCase) []accountSpec {
		currency := hegel.Draw(tc, currencyGen())
		specs := make([]accountSpec, hegel.Draw(tc, hegel.Integers(minAccounts, maxAccounts)))
		for i := range specs {
			specs[i] = accountSpec{Currency: currency, AllowNeg: true, AllowPos: true}
			if constrained {
				specs[i].AllowNeg = hegel.Draw(tc, hegel.Booleans())
				specs[i].AllowPos = hegel.Draw(tc, hegel.Booleans())
			}
		}
		return specs
	})
}

// differentCurrencySpecsGen draws two accounts whose currencies differ, which
// is the only shape that reaches the currency check.
func differentCurrencySpecsGen() hegel.Generator[[]accountSpec] {
	return hegel.Composite(func(tc hegel.TestCase) []accountSpec {
		from := hegel.Draw(tc, hegel.Integers(0, len(currencies)-1))
		to := hegel.Draw(tc, hegel.Integers(0, len(currencies)-2))
		if to >= from {
			to++
		}
		return []accountSpec{
			{Currency: currencies[from], AllowNeg: true, AllowPos: true},
			{Currency: currencies[to], AllowNeg: true, AllowPos: true},
		}
	})
}

// jsonTextGen draws strings for metadata keys and values. A \u0000 in a string
// is rejected by jsonb itself rather than by pgledger, so it is excluded here
// and pinned by an example test instead.
func jsonTextGen() hegel.Generator[string] {
	return hegel.Text().MaxSize(6).ExcludeCategories([]string{"Cs"}).ExcludeCharacters("\x00")
}

// jsonValueGen draws an arbitrary JSON value, nesting up to depth levels of
// objects and arrays. Integers span the int64 range because jsonb stores
// numerics, not machine integers.
func jsonValueGen(depth int) hegel.Generator[any] {
	return hegel.Composite(func(tc hegel.TestCase) any {
		const scalarKinds = 3
		maxKind := scalarKinds
		if depth > 0 {
			maxKind = scalarKinds + 2
		}

		switch hegel.Draw(tc, hegel.Integers(0, maxKind)) {
		case 0:
			return nil
		case 1:
			return hegel.Draw(tc, hegel.Booleans())
		case 2:
			return hegel.Draw(tc, hegel.Integers[int64](math.MinInt64, math.MaxInt64))
		case 3:
			return hegel.Draw(tc, jsonTextGen())
		case 4:
			items := make([]any, hegel.Draw(tc, hegel.Integers(0, 3)))
			for i := range items {
				items[i] = hegel.Draw(tc, jsonValueGen(depth-1))
			}
			return items
		default:
			object := map[string]any{}
			for range hegel.Draw(tc, hegel.Integers(0, 3)) {
				object[hegel.Draw(tc, jsonTextGen())] = hegel.Draw(tc, jsonValueGen(depth-1))
			}
			return object
		}
	})
}

// metadataGen draws a JSON object, which is the shape callers actually pass as
// transfer metadata.
func metadataGen() hegel.Generator[map[string]any] {
	return hegel.Composite(func(tc hegel.TestCase) map[string]any {
		object := map[string]any{}
		for range hegel.Draw(tc, hegel.Integers(0, 4)) {
			object[hegel.Draw(tc, jsonTextGen())] = hegel.Draw(tc, jsonValueGen(2))
		}
		return object
	})
}

// eventAtGen draws a timestamp spanning a few years either side of a fixed
// instant: event_at records when the real-world event happened, so past and
// future values are both legitimate.
func eventAtGen() hegel.Generator[time.Time] {
	base := time.Date(2020, time.June, 15, 12, 0, 0, 0, time.UTC)
	return hegel.Composite(func(tc hegel.TestCase) time.Time {
		offset := hegel.Draw(tc, hegel.Integers[int64](-100_000_000, 100_000_000))
		return base.Add(time.Duration(offset) * time.Second)
	})
}

type request struct {
	FromIdx int
	ToIdx   int
	Amount  string
}

// requestGen picks two distinct account indices by construction rather than by
// drawing a pair and rejecting the equal ones.
func requestGen(n int, amounts hegel.Generator[string]) hegel.Generator[request] {
	return hegel.Composite(func(tc hegel.TestCase) request {
		from := hegel.Draw(tc, hegel.Integers(0, n-1))
		to := hegel.Draw(tc, hegel.Integers(0, n-2))
		if to >= from {
			to++
		}
		return request{FromIdx: from, ToIdx: to, Amount: hegel.Draw(tc, amounts)}
	})
}

func batchGen(n int, amounts hegel.Generator[string]) hegel.Generator[[]request] {
	return hegel.Lists(requestGen(n, amounts)).MinSize(1).MaxSize(5)
}
