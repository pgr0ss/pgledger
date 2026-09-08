//go:build property

package propertytest

import (
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgr0ss/pgledger/ledgertest"
	"hegel.dev/go/hegel"
)

// TestLedgerInvariantsHoldForWellFormedBatches is the Tier A property: over a
// random ledger and random accepted or rejected batches of well-formed
// amounts, conservation, the entry fold, version counting and entry chain
// continuity all hold, and an accepted batch returns its transfers in request
// order.
func TestLedgerInvariantsHoldForWellFormedBatches(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, hegel.Lists(accountSpecGen()).MinSize(2).MaxSize(6))
		s := newScope(ht, conn, ctx, specs)

		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), amountGen())).MinSize(1).MaxSize(4)) {
			returned, err := s.createTransfers(batch, nil, nil)
			if err != nil {
				ht.Note("rejected " + describe(batch) + ": " + err.Error())
				requirePgledgerError(ht, err)
			} else {
				assertReturnedMatchesRequests(ht, s, batch, returned)
			}
			s.assertInvariants(ht)
		}
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
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

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 4, false))
		s := newScope(ht, conn, ctx, specs)

		batch := hegel.Draw(ht, batchGen(len(specs), hostileAmountGen()))
		before := s.snapshot(ht)

		_, err := s.createTransfers(batch, nil, nil)
		if err != nil {
			requirePgledgerError(ht, err)
			if diff := before.diff(s.snapshot(ht)); diff != "" {
				ht.Fatalf("rejected batch %s mutated the ledger: %s", describe(batch), diff)
			}
			return
		}
		s.assertInvariants(ht)
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestModelPredictsAcceptanceAndBalances is the Tier B oracle property:
// pgledger accepts a batch exactly when the model does, ends with exactly the
// model's balances and versions, and leaves nothing behind when it rejects.
func TestModelPredictsAcceptanceAndBalances(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, sameCurrencySpecsGen(2, 5, true))
		s := newScope(ht, conn, ctx, specs)
		m := newModel(specs)

		for _, batch := range hegel.Draw(ht, hegel.Lists(batchGen(len(specs), hostileAmountGen())).MinSize(1).MaxSize(4)) {
			before := s.snapshot(ht)
			reason := m.applyBatch(batch)
			_, err := s.createTransfers(batch, nil, nil)
			assertParity(ht, s, batch, before, reason, err)
			s.assertMatchesModel(ht, m)
		}
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// TestTransferAndReverseRestoreBalances is the Tier C inverse property. The
// comparison is numeric: NUMERIC scale is not normalised, so a 10.500 out and
// 10.5 back leaves the balance reading "0.000".
func TestTransferAndReverseRestoreBalances(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		specs := hegel.Draw(ht, hegel.Lists(unconstrainedSpecGen()).MinSize(1).MaxSize(1))
		specs = append(specs, accountSpec{Currency: specs[0].Currency, AllowNeg: true, AllowPos: true})
		s := newScope(ht, conn, ctx, specs)

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

// TestMetadataRoundTripsAsJsonb is the Tier C roundtrip property. jsonb
// normalises key order and drops duplicate keys, so the comparison is made by
// PostgreSQL as jsonb rather than on the returned text.
func TestMetadataRoundTripsAsJsonb(t *testing.T) {
	conn := ledgertest.Setup(t)
	ctx := t.Context()

	hegel.Test(t, func(ht *hegel.T) {
		currency := hegel.Draw(ht, currencyGen())
		s := newScope(ht, conn, ctx, []accountSpec{
			{Currency: currency, AllowNeg: true, AllowPos: true},
			{Currency: currency, AllowNeg: true, AllowPos: true},
		})

		raw := hegel.Draw(ht, hegel.Maps(
			hegel.Text().MinSize(1).MaxSize(8).Categories([]string{"L", "Nd"}),
			hegel.Integers(-1000, 1000),
		).MaxSize(4))
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
			ht.Fatalf("metadata comparison: %v", err)
		}
		if !same {
			stored := "<null>"
			if transfers[0].Metadata != nil {
				stored = *transfers[0].Metadata
			}
			ht.Fatalf("metadata %s round-tripped as %s", metadata, stored)
		}

		// The entries view denormalises the transfer's metadata (A8).
		for _, id := range s.accountIDs() {
			for _, entry := range ledgertest.GetEntries(ht, conn, id) {
				if entry.Metadata == nil {
					ht.Fatalf("entry %s has no metadata", entry.ID)
				}
			}
		}
	}, hegel.WithTestCases(propertyCases()), hegel.WithDatabase("testdata/hegel"))
}

// assertReturnedMatchesRequests is property A6: pgledger_create_transfers
// returns its rows ORDER BY id, which is only the request order if ULIDs are
// monotonic with insertion.
func assertReturnedMatchesRequests(tc hegel.TestCase, s *scope, reqs []request, returned []ledgertest.Transfer) {
	if len(returned) != len(reqs) {
		tc.Errorf("returned %d transfers for %d requests", len(returned), len(reqs))
	}
	ids := s.accountIDs()
	for i, r := range reqs {
		got := returned[i]
		if got.FromAccountID != ids[r.FromIdx] || got.ToAccountID != ids[r.ToIdx] {
			tc.Errorf("request %d was %d->%d but came back %s->%s",
				i, r.FromIdx, r.ToIdx, got.FromAccountID, got.ToAccountID)
		}
		want := mustRat(tc, "requested amount", r.Amount)
		if mustRat(tc, "returned amount", got.Amount).Cmp(want) != 0 {
			tc.Errorf("request %d asked for %s but came back as %s", i, r.Amount, got.Amount)
		}
	}
}

// requirePgledgerError holds pgledger to its error contract: a rejected call
// raises a pgledger exception (P0001) or one of the integrity violations its
// schema is built on, never an internal failure such as an undefined function
// or a syntax error.
func requirePgledgerError(tc hegel.TestCase, err error) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		tc.Errorf("expected a PostgreSQL error, got %T: %v", err, err)
		return
	}
	switch pgErr.Code {
	case "P0001", // raise_exception: pgledger's own guards
		"23502", // not_null_violation
		"23503", // foreign_key_violation
		"23514", // check_violation
		"22P02": // invalid_text_representation
		return
	}
	tc.Errorf("unexpected error class %s: %s", pgErr.Code, pgErr.Message)
}
