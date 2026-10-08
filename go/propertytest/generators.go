// Package propertytest is the pgledger property-based test suite. The state
// machines live outside _test.go files so propertytest/continuous can drive
// them.
package propertytest

import (
	"math"
	"math/big"
	"os"
	"slices"
	"strconv"
	"time"

	"hegel.dev/go/hegel"
)

func propertyCases() int {
	if v := os.Getenv("PGLEDGER_HEGEL_CASES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

// mustRat fails the test case on a non-numeric value, which is how a NaN or
// Infinity balance is detected.
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

// nullAmount is sent as SQL NULL. It is spelled the way RAISE renders NULL, so
// the model's rejection message needs no special case.
const nullAmount = "<NULL>"

var rejectedAmounts = []string{
	"0", "0.00", "-1", "-0.01",
	"NaN", "Infinity", "-Infinity", nullAmount,
}

// extremeAmounts get their own generator branch: sharing one with
// rejectedAmounts made them too rare for their coverage floor.
var extremeAmounts = []string{"1e-40", "1e100"}

func hostileAmountGen() hegel.Generator[string] {
	return hegel.OneOf(
		amountGen(),
		hegel.SampledFrom(rejectedAmounts),
		hegel.SampledFrom(extremeAmounts),
	)
}

func isHostileAmount(amount string) bool {
	return slices.Contains(rejectedAmounts, amount) || slices.Contains(extremeAmounts, amount)
}

// 'usd' and 'USD' differ: currency comparison is case-sensitive.
var currencies = []string{"USD", "EUR", "JPY", "usd"}

func currencyGen() hegel.Generator[string] {
	return hegel.SampledFrom(currencies)
}

type accountSpec struct {
	Currency string
	AllowNeg bool
	AllowPos bool
}

func unconstrainedSpecGen() hegel.Generator[accountSpec] {
	return hegel.Composite(func(tc hegel.TestCase) accountSpec {
		return accountSpec{Currency: hegel.Draw(tc, currencyGen()), AllowNeg: true, AllowPos: true}
	})
}

// sameCurrencySpecsGen keeps to one currency: a mixed set rejects most
// requests on the currency check, masking what the property is about.
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

// jsonTextGen excludes \u0000, which jsonb itself rejects (see
// TestMetadataWithNullCharacterIsRejected).
func jsonTextGen() hegel.Generator[string] {
	return hegel.Text().MaxSize(6).ExcludeCategories([]string{"Cs"}).ExcludeCharacters("\x00")
}

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

func metadataGen() hegel.Generator[map[string]any] {
	return hegel.Composite(func(tc hegel.TestCase) map[string]any {
		object := map[string]any{}
		for range hegel.Draw(tc, hegel.Integers(0, 4)) {
			object[hegel.Draw(tc, jsonTextGen())] = hegel.Draw(tc, jsonValueGen(2))
		}
		return object
	})
}

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

// Sentinel request indices: send a NULL account id, or an id no account has.
const (
	nullAccount    = -1
	unknownAccount = -2
)

const unknownAccountID = "pgla_00000000000000000000000000"

func hostileRequestGen(n int) hegel.Generator[request] {
	return hegel.Composite(func(tc hegel.TestCase) request {
		req := hegel.Draw(tc, requestGen(n, hostileAmountGen()))
		// WeightedBooleans, not an Integers draw: hegel skews Integers towards
		// its bounds per test case, so a rare kind could vanish from a whole run.
		if !hegel.Draw(tc, hegel.WeightedBooleans(0.3)) {
			return req
		}
		if hegel.Draw(tc, hegel.WeightedBooleans(1.0/3)) {
			req.ToIdx = req.FromIdx
			return req
		}
		sentinel := func() int {
			if hegel.Draw(tc, hegel.Booleans()) {
				return nullAccount
			}
			return unknownAccount
		}
		switch {
		case hegel.Draw(tc, hegel.WeightedBooleans(1.0/3)):
			req.FromIdx = sentinel()
		case hegel.Draw(tc, hegel.WeightedBooleans(0.5)):
			req.ToIdx = sentinel()
		default:
			req.FromIdx = sentinel()
			req.ToIdx = sentinel()
		}
		return req
	})
}

func (r request) hasAccount(idx int) bool {
	return r.FromIdx == idx || r.ToIdx == idx
}

func (r request) isSelfTransfer() bool {
	return r.FromIdx == r.ToIdx
}

func (r request) isHostile() bool {
	return r.hasAccount(nullAccount) || r.hasAccount(unknownAccount) || r.isSelfTransfer() || isHostileAmount(r.Amount)
}

func batchGen(n int, amounts hegel.Generator[string]) hegel.Generator[[]request] {
	return hegel.Lists(requestGen(n, amounts)).MinSize(1).MaxSize(5)
}
