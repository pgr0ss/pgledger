//go:build property

package propertytest

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/ledgertest"
	"hegel.dev/go/hegel"
)

// coverage counts how many test cases reached the situation a property is
// about. Hegel reports nothing when a generator drifts into a corner where
// every call is rejected and the assertions hold trivially, so each property
// that can go vacuous states the fraction of cases it needs.
type coverage struct {
	cases   atomic.Int64
	covered atomic.Int64
}

func (c *coverage) record(covered bool) {
	c.cases.Add(1)
	if covered {
		c.covered.Add(1)
	}
}

func (c *coverage) require(t *testing.T, what string, minFraction float64) {
	t.Helper()

	cases, covered := c.cases.Load(), c.covered.Load()
	if cases == 0 {
		t.Fatalf("no test cases ran, so nothing %s", what)
	}
	if fraction := float64(covered) / float64(cases); fraction < minFraction {
		t.Fatalf("only %d of %d test cases %s (%.0f%%, want at least %.0f%%): the property is near-vacuous",
			covered, cases, what, fraction*100, minFraction*100)
	}
}

// TestLedgerInvariantsHoldForWellFormedBatches is the Tier A property: over a
// random ledger and random batches of well-formed amounts, conservation, the
// entry fold, version counting, per-transfer entry pairs and entry chain
// continuity all hold, and an accepted batch returns its transfers in request
// order.
//
// The accounts share a currency and permit any balance: with random currencies
// and random constraint flags, almost every batch is rejected and the
// invariants then hold over an empty ledger. Rejection paths belong to the
// properties below that are about rejection.
func TestLedgerInvariantsHoldForWellFormedBatches(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	var landed coverage
	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 6, false))
		s := newScope(ht, conn, ctx, specs)

		accepted := 0
		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), amountGen())).MinSize(1).MaxSize(4)) {
			returned, err := s.createTransfers(batch, nil, nil)
			if err != nil {
				ht.Note("rejected " + describe(batch) + ": " + err.Error())
				requirePgledgerError(ht, err)
			} else {
				accepted += len(returned)
				assertReturnedMatchesRequests(ht, s, batch, returned)
			}
			s.assertInvariants(ht)
		}

		landed.record(accepted > 0)
		ht.Target(float64(accepted), "accepted transfers")
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))

	landed.require(t, "landed a transfer", 0.7)
}

// TestHostileAmountsNeverCorruptTheLedger is the Tier D property: whatever the
// NUMERIC type accepts, a call either succeeds and leaves every touched
// balance a finite number, or is rejected and changes nothing.
//
// The accounts permit any balance, so nothing but the amount itself can cause
// a rejection: a balance constraint firing first would mask how the amount was
// handled.
func TestHostileAmountsNeverCorruptTheLedger(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	var accepted, rejected coverage
	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, conn, ctx, specs)

		// One or two requests, not a long batch: every extra well-formed
		// request only lowers the odds that the hostile one is reached.
		batch := hegel.Draw(ht, hegel.Lists(requestGen(len(specs), hostileAmountGen())).MinSize(1).MaxSize(2))
		before := s.snapshot(ht)

		_, err := s.createTransfers(batch, nil, nil)
		accepted.record(err == nil)
		rejected.record(err != nil)
		if err != nil {
			requirePgledgerError(ht, err)
			if diff := before.diff(s.snapshot(ht)); diff != "" {
				ht.Fatalf("rejected batch %s mutated the ledger: %s", describe(batch), diff)
			}
			return
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))

	accepted.require(t, "accepted the batch", 0.2)
	rejected.require(t, "rejected the batch", 0.2)
}

// TestModelPredictsAcceptanceAndBalances is the Tier B oracle property:
// pgledger accepts a batch exactly when the model does, ends with exactly the
// model's balances and versions, and leaves nothing behind when it rejects.
//
// The axis under test is the balance constraints, so the amounts are
// well-formed: drawing hostile amounts here as well means the amount guard
// rejects nearly every call before a constraint can, which the coverage floor
// below reports as vacuity. Hostile amounts have their own property.
func TestModelPredictsAcceptanceAndBalances(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	var accepted, rejected coverage
	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 5, true))
		s := newScope(ht, conn, ctx, specs)
		m := newModel(specs)

		landed, refused := 0, 0
		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), amountGen())).MinSize(1).MaxSize(4)) {
			before := s.snapshot(ht)
			reason := m.applyBatch(batch)
			_, err := s.createTransfers(batch, nil, nil)
			assertParity(ht, s, batch, before, reason, err)
			s.assertMatchesModel(ht, m)

			if err == nil {
				landed += len(batch)
			} else {
				refused++
			}
		}

		accepted.record(landed > 0)
		rejected.record(refused > 0)
		ht.Target(float64(landed), "accepted transfers")
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))

	accepted.require(t, "accepted a batch", 0.3)
	rejected.require(t, "rejected a batch", 0.3)
}

// TestCrossCurrencyTransfersAreRejectedWithoutTrace is the other half of the
// oracle: the currency check runs after both balance UPDATEs have already run,
// so the whole statement has to roll back.
func TestCrossCurrencyTransfersAreRejectedWithoutTrace(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, differentCurrencySpecsGen())
		s := newScope(ht, conn, ctx, specs)

		// A same-currency transfer first, so the rejection below is compared
		// against a ledger that is not empty.
		third := accountSpec{Currency: specs[0].Currency, AllowNeg: true, AllowPos: true}
		s.addAccount(ht, third)
		if _, err := s.createTransfer(request{FromIdx: 0, ToIdx: 2, Amount: hegel.Draw(ht, amountGen())}, nil, nil); err != nil {
			ht.Fatalf("same-currency transfer between %s accounts: %v", specs[0].Currency, err)
		}

		before := s.snapshot(ht)
		req := request{FromIdx: 0, ToIdx: 1, Amount: hegel.Draw(ht, amountGen())}
		_, err := s.createTransfer(req, nil, nil)
		if err == nil {
			ht.Fatalf("transfer from %s to %s was accepted", specs[0].Currency, specs[1].Currency)
		}
		requirePgledgerError(ht, err)
		if !strings.Contains(err.Error(), "Cannot transfer between different currencies") {
			ht.Fatalf("unexpected rejection for %s -> %s: %v", specs[0].Currency, specs[1].Currency, err)
		}
		if diff := before.diff(s.snapshot(ht)); diff != "" {
			ht.Fatalf("rejected cross-currency transfer mutated the ledger: %s", diff)
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestTransferAndReverseRestoreBalances is the Tier C inverse property. The
// comparison is numeric: NUMERIC scale is not normalised, so a 10.500 out and
// 10.5 back leaves the balance reading "0.000".
func TestTransferAndReverseRestoreBalances(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		spec := hegel.Draw(ht, unconstrainedSpecGen())
		s := newScope(ht, conn, ctx, []accountSpec{spec, spec})

		amount := hegel.Draw(ht, amountGen())
		forward := request{FromIdx: 0, ToIdx: 1, Amount: amount}
		back := request{FromIdx: 1, ToIdx: 0, Amount: amount}

		if _, err := s.createTransfer(forward, nil, nil); err != nil {
			ht.Fatalf("forward transfer of %s: %v", amount, err)
		}
		if _, err := s.createTransfer(back, nil, nil); err != nil {
			ht.Fatalf("reverse transfer of %s: %v", amount, err)
		}

		for i, id := range s.accountIDs() {
			account := ledgertest.GetAccount(ht, conn, id)
			if mustRat(ht, "balance of account "+id, account.Balance).Sign() != 0 {
				ht.Fatalf("account %d balance is %s after transfer and reverse of %s", i, account.Balance, amount)
			}
			if account.Version != 2 {
				ht.Fatalf("account %d version is %d, want 2", i, account.Version)
			}
			if entries := ledgertest.GetEntries(ht, conn, id); len(entries) != 2 {
				ht.Fatalf("account %d has %d entries, want 2", i, len(entries))
			}
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestSplitTransferEqualsSingleTransfer is the Tier C split property: two
// transfers of x1 and x2 leave the same balances as one transfer of x1+x2.
func TestSplitTransferEqualsSingleTransfer(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		// Four accounts in one currency: 0->1 gets the split pair, 2->3 the sum.
		specs := make([]accountSpec, 4)
		currency := hegel.Draw(ht, currencyGen())
		for i := range specs {
			specs[i] = accountSpec{Currency: currency, AllowNeg: true, AllowPos: true}
		}
		s := newScope(ht, conn, ctx, specs)

		first := hegel.Draw(ht, amountGen())
		second := hegel.Draw(ht, amountGen())
		sum := new(big.Rat).Add(
			mustRat(ht, "first", first),
			mustRat(ht, "second", second),
		)

		if _, err := s.createTransfers([]request{
			{FromIdx: 0, ToIdx: 1, Amount: first},
			{FromIdx: 0, ToIdx: 1, Amount: second},
		}, nil, nil); err != nil {
			ht.Fatalf("split transfers of %s and %s: %v", first, second, err)
		}
		if _, err := s.createTransfer(request{FromIdx: 2, ToIdx: 3, Amount: ratString(sum)}, nil, nil); err != nil {
			ht.Fatalf("summed transfer of %s: %v", ratString(sum), err)
		}

		ids := s.accountIDs()
		split := mustRat(ht, "split destination", ledgertest.GetAccount(ht, conn, ids[1]).Balance)
		single := mustRat(ht, "single destination", ledgertest.GetAccount(ht, conn, ids[3]).Balance)
		if split.Cmp(single) != 0 {
			ht.Fatalf("%s + %s split to %s but summed to %s",
				first, second, ratString(split), ratString(single))
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestBatchOrderDoesNotChangeBalances is the Tier C permutation property,
// conditional on accounts that permit any balance: with no constraint to trip,
// a batch and any permutation of it end with the same balances. The
// order-sensitive half — constrained accounts, where permutation changes
// accept/reject — is pinned by TestBatchOrderIsSignificant in the example
// suite, because it is specified behaviour rather than an invariant.
func TestBatchOrderDoesNotChangeBalances(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		// Two disjoint halves of the same shape: the first runs the batch as
		// drawn, the second runs the permutation.
		half := hegel.Draw(ht, hegel.Integers(2, 4))
		currency := hegel.Draw(ht, currencyGen())
		specs := make([]accountSpec, 2*half)
		for i := range specs {
			specs[i] = accountSpec{Currency: currency, AllowNeg: true, AllowPos: true}
		}
		s := newScope(ht, conn, ctx, specs)

		batch := hegel.Draw(ht, hegel.Lists(requestGen(half, amountGen())).MinSize(2).MaxSize(5))
		permuted := make([]request, len(batch))
		copy(permuted, batch)
		for i := len(permuted) - 1; i > 0; i-- {
			j := hegel.Draw(ht, hegel.Integers(0, i))
			permuted[i], permuted[j] = permuted[j], permuted[i]
		}
		shifted := make([]request, len(permuted))
		for i, r := range permuted {
			shifted[i] = request{FromIdx: r.FromIdx + half, ToIdx: r.ToIdx + half, Amount: r.Amount}
		}
		ht.Note("as drawn " + describe(batch) + ", permuted " + describe(permuted))

		if _, err := s.createTransfers(batch, nil, nil); err != nil {
			ht.Fatalf("batch %s: %v", describe(batch), err)
		}
		if _, err := s.createTransfers(shifted, nil, nil); err != nil {
			ht.Fatalf("permuted batch %s: %v", describe(permuted), err)
		}

		ids := s.accountIDs()
		for i := range half {
			asDrawn := ledgertest.GetAccount(ht, conn, ids[i]).Balance
			asPermuted := ledgertest.GetAccount(ht, conn, ids[i+half]).Balance
			if mustRat(ht, "balance as drawn", asDrawn).Cmp(mustRat(ht, "balance permuted", asPermuted)) != 0 {
				ht.Fatalf("account %d holds %s as drawn but %s permuted", i, asDrawn, asPermuted)
			}
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestTransfersAreNotIdempotent pins the absence of an idempotency key: the
// same call issued twice creates two transfers and moves the balance twice. A
// future dedupe feature must break this test rather than land silently.
func TestTransfersAreNotIdempotent(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		spec := hegel.Draw(ht, unconstrainedSpecGen())
		s := newScope(ht, conn, ctx, []accountSpec{spec, spec})

		amount := hegel.Draw(ht, amountGen())
		req := request{FromIdx: 0, ToIdx: 1, Amount: amount}
		eventAt := hegel.Draw(ht, eventAtGen())
		metadata := `{"idempotency-key": "same"}`

		first, err := s.createTransfer(req, &eventAt, &metadata)
		if err != nil {
			ht.Fatalf("first transfer of %s: %v", amount, err)
		}
		second, err := s.createTransfer(req, &eventAt, &metadata)
		if err != nil {
			ht.Fatalf("repeated transfer of %s: %v", amount, err)
		}
		if first[0].ID == second[0].ID {
			ht.Fatalf("both calls returned transfer %s", first[0].ID)
		}

		want := new(big.Rat).Mul(mustRat(ht, "amount", amount), big.NewRat(2, 1))
		destination := ledgertest.GetAccount(ht, conn, s.accountIDs()[1])
		if mustRat(ht, "destination balance", destination.Balance).Cmp(want) != 0 {
			ht.Fatalf("two transfers of %s left %s, want %s", amount, destination.Balance, ratString(want))
		}
		if destination.Version != 2 {
			ht.Fatalf("destination version is %d after two transfers, want 2", destination.Version)
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestEventAtAndMetadataApplyToEveryTransferInBatch covers the timestamp
// contract: created_at falls inside the call, event_at defaults to created_at,
// and one event_at/metadata argument fans out to every transfer in the batch.
func TestEventAtAndMetadataApplyToEveryTransferInBatch(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	clock := func(tc hegel.TestCase) time.Time {
		var now time.Time
		if err := conn.QueryRow(ctx, "select clock_timestamp()").Scan(&now); err != nil {
			tc.Errorf("read server clock: %v", err)
		}
		return now
	}

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, conn, ctx, specs)
		batch := hegel.Draw(ht, batchGen(len(specs), amountGen()))

		before := clock(ht)
		defaulted, err := s.createTransfers(batch, nil, nil)
		after := clock(ht)
		if err != nil {
			ht.Fatalf("batch %s: %v", describe(batch), err)
		}
		for i, transfer := range defaulted {
			if transfer.CreatedAt.Before(before) || transfer.CreatedAt.After(after) {
				ht.Fatalf("transfer %d created_at %s is outside the call window [%s, %s]",
					i, transfer.CreatedAt, before, after)
			}
			if !transfer.EventAt.Equal(transfer.CreatedAt) {
				ht.Fatalf("transfer %d defaulted event_at to %s but created_at is %s",
					i, transfer.EventAt, transfer.CreatedAt)
			}
			if !transfer.CreatedAt.Equal(defaulted[0].CreatedAt) {
				ht.Fatalf("transfer %d created_at %s differs from the first row's %s",
					i, transfer.CreatedAt, defaulted[0].CreatedAt)
			}
			if transfer.Metadata != nil {
				ht.Fatalf("transfer %d has metadata %s but none was supplied", i, *transfer.Metadata)
			}
		}

		eventAt := hegel.Draw(ht, eventAtGen())
		metadata := `{"batch": true}`
		supplied, err := s.createTransfers(batch, &eventAt, &metadata)
		if err != nil {
			ht.Fatalf("batch %s with event_at %s: %v", describe(batch), eventAt, err)
		}
		for i, transfer := range supplied {
			if !transfer.EventAt.Equal(eventAt) {
				ht.Fatalf("transfer %d has event_at %s, want the supplied %s", i, transfer.EventAt, eventAt)
			}
			if transfer.Metadata == nil || *transfer.Metadata != metadata {
				ht.Fatalf("transfer %d has metadata %v, want %s", i, transfer.Metadata, metadata)
			}
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestMetadataRoundTripsAsJsonb is the Tier C roundtrip property over
// arbitrary nested JSON. jsonb normalises key order and drops duplicate keys,
// so the comparison is made by PostgreSQL as jsonb rather than on text.
func TestMetadataRoundTripsAsJsonb(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		spec := hegel.Draw(ht, unconstrainedSpecGen())
		s := newScope(ht, conn, ctx, []accountSpec{spec, spec})

		raw := hegel.Draw(ht, metadataGen())
		encoded, err := json.Marshal(raw)
		if err != nil {
			ht.Fatalf("marshal metadata %v: %v", raw, err)
		}
		metadata := string(encoded)

		transfers, err := s.createTransfers(
			[]request{{FromIdx: 0, ToIdx: 1, Amount: hegel.Draw(ht, amountGen())}},
			nil, &metadata,
		)
		if err != nil {
			ht.Fatalf("transfer with metadata %s: %v", metadata, err)
		}

		var same bool
		if err := conn.QueryRow(ctx,
			`select metadata = $1::jsonb from pgledger_transfers_view where id = $2`,
			metadata, transfers[0].ID).Scan(&same); err != nil {
			ht.Fatalf("comparing metadata %s: %v", metadata, err)
		}
		if !same {
			stored := "<null>"
			if transfers[0].Metadata != nil {
				stored = *transfers[0].Metadata
			}
			ht.Fatalf("metadata %s round-tripped as %s", metadata, stored)
		}

		// assertInvariants checks that the entries view denormalises this
		// metadata from the transfer row it joins to.
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestHistoricalBalancesReplayFromEntries covers the documented reconciliation
// query: the newest entry at or before an instant carries the balance the
// account held then. One transfer per call keeps created_at distinct per
// account, which is what makes "order by created_at desc limit 1" well defined.
func TestHistoricalBalancesReplayFromEntries(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, conn, ctx, specs)
		m := newModel(specs)

		type checkpoint struct {
			at       time.Time
			balances []*big.Rat
		}
		var history []checkpoint

		for _, req := range hegel.Draw(ht, hegel.Lists(requestGen(len(specs), amountGen())).MinSize(1).MaxSize(6)) {
			if reason := m.applyBatch([]request{req}); reason != "" {
				ht.Fatalf("model rejected %s: %s", describe([]request{req}), reason)
			}
			if _, err := s.createTransfer(req, nil, nil); err != nil {
				ht.Fatalf("transfer %s: %v", describe([]request{req}), err)
			}

			var at time.Time
			if err := conn.QueryRow(ctx, "select clock_timestamp()").Scan(&at); err != nil {
				ht.Fatalf("read server clock: %v", err)
			}
			balances := make([]*big.Rat, len(m.accounts))
			for i, account := range m.accounts {
				balances[i] = new(big.Rat).Set(account.Balance)
			}
			history = append(history, checkpoint{at: at, balances: balances})
		}

		ids := s.accountIDs()
		for step, point := range history {
			for i, id := range ids {
				got, err := balanceAt(ctx, conn, id, point.at)
				if err != nil {
					ht.Fatalf("historical balance of account %d: %v", i, err)
				}
				if mustRat(ht, "historical balance", got).Cmp(point.balances[i]) != 0 {
					ht.Fatalf("account %d held %s after step %d, want %s",
						i, got, step, ratString(point.balances[i]))
				}
			}
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// balanceAt is the query documented in examples/reconciliation.sql. An account
// with no entry yet held nothing.
func balanceAt(ctx context.Context, conn *pgxpool.Pool, accountID string, at time.Time) (string, error) {
	var balance string
	err := conn.QueryRow(ctx, `
		select account_current_balance::text
		from pgledger_entries
		where account_id = $1 and created_at <= $2
		order by created_at desc
		limit 1`, accountID, at).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return "0", nil
	}
	return balance, err
}

// assertReturnedMatchesRequests is property A6: pgledger_create_transfers
// returns its rows ORDER BY id, which is only the request order if ULIDs are
// monotonic with insertion.
func assertReturnedMatchesRequests(tc hegel.TestCase, s *scope, reqs []request, returned []ledgertest.Transfer) {
	if len(returned) != len(reqs) {
		tc.Errorf("returned %d transfers for %d requests", len(returned), len(reqs))
		return
	}
	ids := s.accountIDs()
	for i, r := range reqs {
		got := returned[i]
		if got.FromAccountID != ids[r.FromIdx] || got.ToAccountID != ids[r.ToIdx] {
			tc.Errorf("request %d was %d->%d but came back %s->%s",
				i, r.FromIdx, r.ToIdx, got.FromAccountID, got.ToAccountID)
			return
		}
		want := mustRat(tc, "requested amount", r.Amount)
		if mustRat(tc, "returned amount", got.Amount).Cmp(want) != 0 {
			tc.Errorf("request %d asked for %s but came back as %s", i, r.Amount, got.Amount)
			return
		}
	}
}
