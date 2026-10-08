package propertytest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/testhelpers"
	"hegel.dev/go/hegel"
)

// coverage guards against vacuous properties: hegel reports nothing when a
// generator drifts to where every call is rejected and assertions hold
// trivially.
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
		t.Errorf("no test cases ran, so nothing %s", what)
		return
	}
	if fraction := float64(covered) / float64(cases); fraction < minFraction {
		t.Errorf("only %d of %d test cases %s (%.0f%%, want at least %.0f%%): the property is near-vacuous",
			covered, cases, what, fraction*100, minFraction*100)
	}
}

// The accounts share a currency and permit any balance: otherwise nearly every
// batch is rejected and the invariants hold over an empty ledger.
func TestLedgerInvariantsHoldForWellFormedBatches(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 6, false))
		s := newScope(ht, ctx, conn, specs)

		accepted := 0
		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), amountGen())).MinSize(1).MaxSize(4)) {
			returned, err := s.createTransfers(batch, nil, nil)
			if err != nil {
				ht.Errorf("well-formed batch %s was rejected: %v", describe(batch), err)
			}
			accepted += len(returned)
			assertReturnedMatchesRequests(ht, s, batch, returned)
			s.assertInvariants(ht)
		}

		ht.Target(float64(accepted), "accepted transfers")
	})
}

// The accounts permit any balance, so a balance constraint cannot mask how a
// malformed request was handled.
func TestHostileRequestsNeverCorruptTheLedger(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	// Outcome floors alone pass on a run that never draws a hostile value.
	var accepted, rejected, hostileDrawn, nullDrawn, unknownDrawn, selfDrawn, extremeAccepted coverage
	check(t, func(ht *T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, ctx, conn, specs)
		m := s.newModel()

		// Short batches: each well-formed request lowers the odds of reaching
		// the hostile one.
		batch := hegel.Draw(ht, hegel.Lists(hostileRequestGen(len(specs))).MinSize(1).MaxSize(2))
		hostile := slices.ContainsFunc(batch, request.isHostile)
		extreme := slices.ContainsFunc(batch, func(r request) bool {
			return slices.Contains(extremeAmounts, r.Amount)
		})
		nullID := slices.ContainsFunc(batch, func(r request) bool { return r.hasAccount(nullAccount) })
		unknownID := slices.ContainsFunc(batch, func(r request) bool { return r.hasAccount(unknownAccount) })
		self := slices.ContainsFunc(batch, request.isSelfTransfer)
		before := s.snapshot(ht)

		want := m.applyBatch(batch)
		returned, err := s.createTransfers(batch, nil, nil)
		accepted.record(err == nil)
		rejected.record(err != nil)
		hostileDrawn.record(hostile)
		extremeAccepted.record(extreme && err == nil)
		nullDrawn.record(nullID)
		unknownDrawn.record(unknownID)
		selfDrawn.record(self)

		assertParity(ht, s, batch, before, want, err)
		if err == nil {
			assertReturnedMatchesRequests(ht, s, batch, returned)
		}
		s.assertMatchesModel(ht, m)
		s.assertInvariants(ht)
	})

	// Draws are correlated within a run, so rates swing widely: over 55 runs of
	// 100 cases, each malformed-id kind and extreme acceptance averaged 14-20%
	// but dipped to 1-5%; acceptance averaged 34% but dipped to 15%. The floors
	// only catch a generator that never reaches the case.
	accepted.require(t, "accepted the batch", 0.1)
	rejected.require(t, "rejected the batch", 0.2)
	hostileDrawn.require(t, "drew a malformed request", 0.3)
	nullDrawn.require(t, "drew a NULL account id", 0.01)
	unknownDrawn.require(t, "drew an unknown account id", 0.01)
	selfDrawn.require(t, "drew a self-transfer", 0.01)
	extremeAccepted.require(t, "accepted an extreme-but-legal amount", 0.01)
}

// Amounts are well-formed: hostile ones would be rejected by the amount guard
// before any balance constraint could fire.
func TestModelPredictsAcceptanceAndBalances(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	var accepted, rejected coverage
	check(t, func(ht *T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 5, true))
		s := newScope(ht, ctx, conn, specs)
		m := s.newModel()

		landed, refused := 0, 0
		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), amountGen())).MinSize(1).MaxSize(4)) {
			before := s.snapshot(ht)
			want := m.applyBatch(batch)
			_, err := s.createTransfers(batch, nil, nil)
			assertParity(ht, s, batch, before, want, err)
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
	})

	accepted.require(t, "accepted a batch", 0.3)
	rejected.require(t, "rejected a batch", 0.3)
}

// The currency check runs after both balance UPDATEs, so the whole statement
// has to roll back.
func TestCrossCurrencyTransfersAreRejectedWithoutTrace(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		specs := hegel.Draw(ht, differentCurrencySpecsGen())
		s := newScope(ht, ctx, conn, specs)

		// A same-currency transfer first, so the rejection lands on a non-empty
		// ledger.
		third := accountSpec{Currency: specs[0].Currency, AllowNeg: true, AllowPos: true}
		s.addAccount(ht, third)
		if _, err := s.createTransfer(request{FromIdx: 0, ToIdx: 2, Amount: hegel.Draw(ht, amountGen())}, nil, nil); err != nil {
			ht.Errorf("same-currency transfer between %s accounts: %v", specs[0].Currency, err)
		}

		before := s.snapshot(ht)
		req := request{FromIdx: 0, ToIdx: 1, Amount: hegel.Draw(ht, amountGen())}
		_, err := s.createTransfer(req, nil, nil)
		if err == nil {
			ht.Errorf("transfer from %s to %s was accepted", specs[0].Currency, specs[1].Currency)
		}
		requireRejection(ht, err, "Cannot transfer between different currencies")
		if diff := before.diff(s.snapshot(ht)); diff != "" {
			ht.Errorf("rejected cross-currency transfer mutated the ledger: %s", diff)
		}
		s.assertInvariants(ht)
	})
}

// Compared numerically: NUMERIC keeps scale, so 10.500 out and 10.5 back
// leaves the balance reading "0.000".
func TestTransferAndReverseRestoreBalances(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		spec := hegel.Draw(ht, unconstrainedSpecGen())
		s := newScope(ht, ctx, conn, []accountSpec{spec, spec})

		amount := hegel.Draw(ht, amountGen())
		forward := request{FromIdx: 0, ToIdx: 1, Amount: amount}
		back := request{FromIdx: 1, ToIdx: 0, Amount: amount}

		if _, err := s.createTransfer(forward, nil, nil); err != nil {
			ht.Errorf("forward transfer of %s: %v", amount, err)
		}
		if _, err := s.createTransfer(back, nil, nil); err != nil {
			ht.Errorf("reverse transfer of %s: %v", amount, err)
		}

		for i, id := range s.accountIDs() {
			account := testhelpers.GetAccount(ht, conn, id)
			if mustRat(ht, "balance of account "+id, account.Balance).Sign() != 0 {
				ht.Errorf("account %d balance is %s after transfer and reverse of %s", i, account.Balance, amount)
			}
			if account.Version != 2 {
				ht.Errorf("account %d version is %d, want 2", i, account.Version)
			}
			if entries := testhelpers.GetEntries(ht, conn, id); len(entries) != 2 {
				ht.Errorf("account %d has %d entries, want 2", i, len(entries))
			}
		}
		s.assertInvariants(ht)
	})
}

func TestSplitTransferEqualsSingleTransfer(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		// Four accounts in one currency: 0->1 gets the split pair, 2->3 the sum.
		specs := make([]accountSpec, 4)
		currency := hegel.Draw(ht, currencyGen())
		for i := range specs {
			specs[i] = accountSpec{Currency: currency, AllowNeg: true, AllowPos: true}
		}
		s := newScope(ht, ctx, conn, specs)

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
			ht.Errorf("split transfers of %s and %s: %v", first, second, err)
		}
		if _, err := s.createTransfer(request{FromIdx: 2, ToIdx: 3, Amount: ratString(sum)}, nil, nil); err != nil {
			ht.Errorf("summed transfer of %s: %v", ratString(sum), err)
		}

		ids := s.accountIDs()
		split := mustRat(ht, "split destination", testhelpers.GetAccount(ht, conn, ids[1]).Balance)
		single := mustRat(ht, "single destination", testhelpers.GetAccount(ht, conn, ids[3]).Balance)
		if split.Cmp(single) != 0 {
			ht.Errorf("%s + %s split to %s but summed to %s",
				first, second, ratString(split), ratString(single))
		}
		s.assertInvariants(ht)
	})
}

// Order only stops mattering without balance constraints; the constrained case
// is pinned by TestBatchOrderIsSignificant.
func TestBatchOrderDoesNotChangeBalances(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		// Two disjoint halves of the same shape: the first runs the batch as
		// drawn, the second runs the permutation.
		half := hegel.Draw(ht, hegel.Integers(2, 4))
		currency := hegel.Draw(ht, currencyGen())
		specs := make([]accountSpec, 2*half)
		for i := range specs {
			specs[i] = accountSpec{Currency: currency, AllowNeg: true, AllowPos: true}
		}
		s := newScope(ht, ctx, conn, specs)

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
			ht.Errorf("batch %s: %v", describe(batch), err)
		}
		if _, err := s.createTransfers(shifted, nil, nil); err != nil {
			ht.Errorf("permuted batch %s: %v", describe(permuted), err)
		}

		ids := s.accountIDs()
		for i := range half {
			asDrawn := testhelpers.GetAccount(ht, conn, ids[i]).Balance
			asPermuted := testhelpers.GetAccount(ht, conn, ids[i+half]).Balance
			if mustRat(ht, "balance as drawn", asDrawn).Cmp(mustRat(ht, "balance permuted", asPermuted)) != 0 {
				ht.Errorf("account %d holds %s as drawn but %s permuted", i, asDrawn, asPermuted)
			}
		}
		s.assertInvariants(ht)
	})
}

// No idempotency key: a future dedupe feature must break this test rather than
// land silently.
func TestTransfersAreNotIdempotent(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		spec := hegel.Draw(ht, unconstrainedSpecGen())
		s := newScope(ht, ctx, conn, []accountSpec{spec, spec})

		amount := hegel.Draw(ht, amountGen())
		req := request{FromIdx: 0, ToIdx: 1, Amount: amount}
		eventAt := hegel.Draw(ht, eventAtGen())
		metadata := `{"idempotency-key": "same"}`

		first, err := s.createTransfer(req, &eventAt, &metadata)
		if err != nil {
			ht.Errorf("first transfer of %s: %v", amount, err)
		}
		second, err := s.createTransfer(req, &eventAt, &metadata)
		if err != nil {
			ht.Errorf("repeated transfer of %s: %v", amount, err)
		}
		if first[0].ID == second[0].ID {
			ht.Errorf("both calls returned transfer %s", first[0].ID)
		}

		want := new(big.Rat).Mul(mustRat(ht, "amount", amount), big.NewRat(2, 1))
		destination := testhelpers.GetAccount(ht, conn, s.accountIDs()[1])
		if mustRat(ht, "destination balance", destination.Balance).Cmp(want) != 0 {
			ht.Errorf("two transfers of %s left %s, want %s", amount, destination.Balance, ratString(want))
		}
		if destination.Version != 2 {
			ht.Errorf("destination version is %d after two transfers, want 2", destination.Version)
		}
		s.assertInvariants(ht)
	})
}

func TestEventAtAndMetadataApplyToEveryTransferInBatch(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	clock := func(tc hegel.TestCase) time.Time {
		var now time.Time
		if err := conn.QueryRow(ctx, "select clock_timestamp()").Scan(&now); err != nil {
			tc.Errorf("read server clock: %v", err)
		}
		return now
	}

	check(t, func(ht *T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, ctx, conn, specs)
		batch := hegel.Draw(ht, batchGen(len(specs), amountGen()))

		before := clock(ht)
		defaulted, err := s.createTransfers(batch, nil, nil)
		after := clock(ht)
		if err != nil {
			ht.Errorf("batch %s: %v", describe(batch), err)
		}
		for i, transfer := range defaulted {
			if transfer.CreatedAt.Before(before) || transfer.CreatedAt.After(after) {
				ht.Errorf("transfer %d created_at %s is outside the call window [%s, %s]",
					i, transfer.CreatedAt, before, after)
			}
			if !transfer.EventAt.Equal(transfer.CreatedAt) {
				ht.Errorf("transfer %d defaulted event_at to %s but created_at is %s",
					i, transfer.EventAt, transfer.CreatedAt)
			}
			if !transfer.CreatedAt.Equal(defaulted[0].CreatedAt) {
				ht.Errorf("transfer %d created_at %s differs from the first row's %s",
					i, transfer.CreatedAt, defaulted[0].CreatedAt)
			}
			if transfer.Metadata != nil {
				ht.Errorf("transfer %d has metadata %s but none was supplied", i, *transfer.Metadata)
			}
		}

		eventAt := hegel.Draw(ht, eventAtGen())
		metadata := `{"batch": true}`
		supplied, err := s.createTransfers(batch, &eventAt, &metadata)
		if err != nil {
			ht.Errorf("batch %s with event_at %s: %v", describe(batch), eventAt, err)
		}
		for i, transfer := range supplied {
			if !transfer.EventAt.Equal(eventAt) {
				ht.Errorf("transfer %d has event_at %s, want the supplied %s", i, transfer.EventAt, eventAt)
			}
			if transfer.Metadata == nil || *transfer.Metadata != metadata {
				ht.Errorf("transfer %d has metadata %v, want %s", i, transfer.Metadata, metadata)
			}
		}
		s.assertInvariants(ht)
	})
}

// jsonb normalises key order and drops duplicate keys, so PostgreSQL compares
// as jsonb rather than as text.
func TestMetadataRoundTripsAsJsonb(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		spec := hegel.Draw(ht, unconstrainedSpecGen())
		s := newScope(ht, ctx, conn, []accountSpec{spec, spec})

		raw := hegel.Draw(ht, metadataGen())
		encoded, err := json.Marshal(raw)
		if err != nil {
			ht.Errorf("marshal metadata %v: %v", raw, err)
		}
		metadata := string(encoded)

		transfers, err := s.createTransfers(
			[]request{{FromIdx: 0, ToIdx: 1, Amount: hegel.Draw(ht, amountGen())}},
			nil, &metadata,
		)
		if err != nil {
			ht.Errorf("transfer with metadata %s: %v", metadata, err)
		}

		var same bool
		if err := conn.QueryRow(ctx,
			`select metadata = $1::jsonb from pgledger_transfers_view where id = $2`,
			metadata, transfers[0].ID).Scan(&same); err != nil {
			ht.Errorf("comparing metadata %s: %v", metadata, err)
		}
		if !same {
			stored := "<null>"
			if transfers[0].Metadata != nil {
				stored = *transfers[0].Metadata
			}
			ht.Errorf("metadata %s round-tripped as %s", metadata, stored)
		}

		// assertInvariants checks the entries view's copy of this metadata.
		s.assertInvariants(ht)
	})
}

// An account touched twice in one call has entries sharing one created_at;
// account_version orders them.
func TestHistoricalBalancesReplayFromEntries(t *testing.T) {
	conn := testhelpers.SetupParallel(t)
	ctx := t.Context()

	check(t, func(ht *T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, ctx, conn, specs)
		m := s.newModel()

		type checkpoint struct {
			at       time.Time
			balances []*big.Rat
		}
		var history []checkpoint

		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), amountGen())).MinSize(1).MaxSize(4)) {
			if reason := m.applyBatch(batch); reason != "" {
				ht.Errorf("model rejected %s: %s", describe(batch), reason)
			}
			if _, err := s.createTransfers(batch, nil, nil); err != nil {
				ht.Errorf("batch %s: %v", describe(batch), err)
			}

			var at time.Time
			if err := conn.QueryRow(ctx, "select clock_timestamp()").Scan(&at); err != nil {
				ht.Errorf("read server clock: %v", err)
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
					ht.Errorf("historical balance of account %d: %v", i, err)
				}
				if mustRat(ht, "historical balance", got).Cmp(point.balances[i]) != 0 {
					ht.Errorf("account %d held %s after step %d, want %s",
						i, got, step, ratString(point.balances[i]))
				}
			}
		}
		s.assertInvariants(ht)
	})
}

// balanceAt is the README's Historical Balances query. No entry yet means "0".
func balanceAt(ctx context.Context, conn *pgxpool.Pool, accountID string, at time.Time) (string, error) {
	var balance string
	err := conn.QueryRow(ctx, `
		select account_current_balance::text
		from pgledger_entries
		where account_id = $1 and created_at <= $2
		order by account_version desc
		limit 1`, accountID, at).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return "0", nil
	}
	return balance, err
}

// assertReturnedMatchesRequests compares as a multiset: rows come back ORDER BY
// id, and below PostgreSQL 18 ids are not monotonic within a microsecond
// (pgledger_uuidv7_microsecond).
func assertReturnedMatchesRequests(tc hegel.TestCase, s *scope, reqs []request, returned []testhelpers.Transfer) {
	ids := s.accountIDs()

	want := make([]string, 0, len(reqs))
	for _, r := range reqs {
		want = append(want, fmt.Sprintf("%s->%s %s",
			ids[r.FromIdx], ids[r.ToIdx], ratString(mustRat(tc, "requested amount", r.Amount))))
	}
	got := make([]string, 0, len(returned))
	for _, t := range returned {
		got = append(got, fmt.Sprintf("%s->%s %s",
			t.FromAccountID, t.ToAccountID, ratString(mustRat(tc, "returned amount", t.Amount))))
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		tc.Errorf("requested %v but got back %v", want, got)
	}
}
