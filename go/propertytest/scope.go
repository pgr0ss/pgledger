package propertytest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/ledgertest"
	"hegel.dev/go/hegel"
)

// scope is the set of accounts one property test case owns. Every assertion is
// restricted to these ids: the suite runs in parallel against a shared
// database, so an unscoped aggregate would read other tests' accounts.
type scope struct {
	conn    *pgxpool.Pool
	ctx     context.Context
	mu      sync.Mutex
	ids     []string
	specs   []accountSpec
	created int
}

func newScope(tc hegel.TestCase, conn *pgxpool.Pool, ctx context.Context, specs []accountSpec) *scope {
	s := &scope{conn: conn, ctx: ctx}
	for _, spec := range specs {
		s.addAccount(tc, spec)
	}
	return s
}

func (s *scope) addAccount(tc hegel.TestCase, spec accountSpec) string {
	s.mu.Lock()
	name := fmt.Sprintf("prop-%d-%d", s.created, rand.Int64())
	s.created++
	s.mu.Unlock()

	var id string
	err := s.conn.QueryRow(s.ctx,
		`select id from pgledger_create_account($1, $2, $3, $4)`,
		name, spec.Currency, spec.AllowNeg, spec.AllowPos).Scan(&id)
	if err != nil {
		tc.Errorf("create account %s: %v", name, err)
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, id)
	s.specs = append(s.specs, spec)
	return id
}

func (s *scope) accountIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ids...)
}

func (s *scope) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ids)
}

// requestArgs renders reqs as transfer_request literals over placeholders,
// resolving account indices to the ids this scope created.
func (s *scope) requestArgs(reqs []request) (literals []string, args []any) {
	ids := s.accountIDs()
	literals = make([]string, 0, len(reqs))
	for _, r := range reqs {
		literals = append(literals, fmt.Sprintf("($%d::text, $%d::text, $%d::numeric)",
			len(args)+1, len(args)+2, len(args)+3))
		args = append(args, ids[r.FromIdx], ids[r.ToIdx], r.Amount)
	}
	return literals, args
}

// createTransfers calls the array form of pgledger_create_transfers.
func (s *scope) createTransfers(reqs []request, eventAt *time.Time, metadata *string) ([]ledgertest.Transfer, error) {
	literals, args := s.requestArgs(reqs)

	arrayLit := "array[]::transfer_request[]"
	if len(literals) > 0 {
		arrayLit = "array[" + strings.Join(literals, ", ") + "]::transfer_request[]"
	}
	sql := fmt.Sprintf(`select * from pgledger_create_transfers(
		transfer_requests => %s, event_at => $%d::timestamptz, metadata => $%d::jsonb)`,
		arrayLit, len(args)+1, len(args)+2)
	args = append(args, eventAt, metadata)

	return s.query(sql, args...)
}

// createTransfersVariadic calls the VARIADIC form, which takes no event_at or
// metadata. The variadic form has no empty-batch spelling, so reqs must not be
// empty.
func (s *scope) createTransfersVariadic(reqs []request) ([]ledgertest.Transfer, error) {
	literals, args := s.requestArgs(reqs)
	sql := fmt.Sprintf("select * from pgledger_create_transfers(%s)", strings.Join(literals, ", "))
	return s.query(sql, args...)
}

// createTransfer calls the single-transfer entry point.
func (s *scope) createTransfer(req request, eventAt *time.Time, metadata *string) ([]ledgertest.Transfer, error) {
	ids := s.accountIDs()
	return s.query(`select * from pgledger_create_transfer($1::text, $2::text, $3::numeric, $4::timestamptz, $5::jsonb)`,
		ids[req.FromIdx], ids[req.ToIdx], req.Amount, eventAt, metadata)
}

func (s *scope) query(sql string, args ...any) ([]ledgertest.Transfer, error) {
	rows, err := s.conn.Query(s.ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[ledgertest.Transfer])
}

// reportViolations runs a query whose every row is one invariant violation,
// rendered as a single text column, and fails the test case for each row. A
// query that returns nothing is the property holding.
func (s *scope) reportViolations(tc hegel.TestCase, what, sql string, ids []string) {
	rows, err := s.conn.Query(s.ctx, sql, ids)
	if err != nil {
		tc.Errorf("%s query: %v", what, err)
		return
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tc.Errorf("%s query: %v", what, err)
		return
	}
	for _, detail := range found {
		tc.Errorf("%s: %s", what, detail)
	}
}

// assertInvariants checks the structural invariants that must hold after every
// accepted or rejected call, scoped to this test case's accounts: conservation
// per currency (A1), balance as the fold of its entries and version as the leg
// count (A2/A5), two entries per transfer summing to zero over the right pair
// of accounts (A3), entry chain continuity (A4), and the entries view agreeing
// with the transfers it denormalises (A8).
func (s *scope) assertInvariants(tc hegel.TestCase) {
	ids := s.accountIDs()

	rows, err := s.conn.Query(s.ctx, `
		select currency, sum(balance)::text
		from pgledger_accounts_view
		where id = any($1)
		group by currency`, ids)
	if err != nil {
		tc.Errorf("conservation query: %v", err)
		return
	}
	totals, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
		Currency string
		Total    string
	}])
	if err != nil {
		tc.Errorf("conservation query: %v", err)
		return
	}
	for _, t := range totals {
		if mustRat(tc, "sum(balance) for "+t.Currency, t.Total).Sign() != 0 {
			tc.Errorf("currency %s does not conserve: sum(balance) = %s", t.Currency, t.Total)
			return
		}
	}

	s.reportViolations(tc, "balance is not the fold of its entries", `
		select format('account %s: balance=%s sum(entries)=%s version=%s legs=%s',
			a.id, a.balance, coalesce(sum(e.amount), 0), a.version, count(e.id))
		from pgledger_accounts_view a
		left join pgledger_entries e on e.account_id = a.id
		where a.id = any($1)
		group by a.id, a.balance, a.version
		having coalesce(sum(e.amount), 0) != a.balance or count(e.id) != a.version`, ids)

	s.reportViolations(tc, "transfer does not have two balanced entries", `
		select format('transfer %s: %s entries, sum %s, accounts %s, expected %s and %s',
			t.id, count(e.id), coalesce(sum(e.amount), 0),
			array_agg(e.account_id order by e.account_id), t.from_account_id, t.to_account_id)
		from pgledger_transfers t
		left join pgledger_entries e on e.transfer_id = t.id
		where t.from_account_id = any($1)
		group by t.id, t.from_account_id, t.to_account_id
		having count(e.id) != 2
			or coalesce(sum(e.amount), 0) != 0
			or array_agg(e.account_id order by e.account_id)
				!= array(select id from unnest(array[t.from_account_id, t.to_account_id]) id order by id)`, ids)

	// Ordering is by account_version, the sequence in which the balances were
	// actually written. Entry ids are ULIDs from a microsecond-precision uuidv7
	// fallback below PostgreSQL 18, which is not monotonic within a microsecond,
	// so id order is not a safe stand-in here.
	s.reportViolations(tc, "entry chain is broken", `
		select format('account %s entry %s: previous_balance %s does not follow %s',
			account_id, id, account_previous_balance, coalesce(prev::text, '0'))
		from (
			select id, account_id, account_previous_balance,
				lag(account_current_balance) over chain as prev,
				row_number() over chain as position
			from pgledger_entries
			where account_id = any($1)
			window chain as (partition by account_id order by account_version)
		) e
		where (position = 1 and account_previous_balance != 0)
			or (prev is not null and prev != account_previous_balance)`, ids)

	s.reportViolations(tc, "entries view disagrees with its transfer", `
		select format('entry %s: event_at %s / metadata %s, transfer %s has %s / %s',
			e.id, e.event_at, e.metadata, t.id, t.event_at, t.metadata)
		from pgledger_entries_view e
		join pgledger_transfers t on t.id = e.transfer_id
		where e.account_id = any($1)
			and (e.event_at != t.event_at or e.metadata is distinct from t.metadata)`, ids)

	s.reportViolations(tc, "entries view has the wrong number of rows for a transfer", `
		select format('transfer %s has %s rows in pgledger_entries_view', transfer_id, count(*))
		from pgledger_entries_view
		where account_id = any($1)
		group by transfer_id
		having count(*) != 2`, ids)
}

type accountState struct {
	ID        string
	Balance   string
	Version   int64
	UpdatedAt string
}

type snapshot struct {
	accounts  []accountState
	transfers int64
	entries   int64
}

// snapshot records everything a rejected call must leave untouched: balances,
// versions, updated_at, and the transfer and entry row counts. updated_at is
// included because pgledger writes it in the same UPDATE as the balance, so it
// is the column a partial write would expose.
func (s *scope) snapshot(tc hegel.TestCase) snapshot {
	ids := s.accountIDs()

	rows, err := s.conn.Query(s.ctx,
		`select id, balance::text, version, updated_at::text
		from pgledger_accounts_view where id = any($1) order by id`, ids)
	if err != nil {
		tc.Errorf("snapshot query: %v", err)
		return snapshot{}
	}
	accounts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[accountState])
	if err != nil {
		tc.Errorf("snapshot query: %v", err)
		return snapshot{}
	}

	snap := snapshot{accounts: accounts}
	err = s.conn.QueryRow(s.ctx,
		`select count(*) from pgledger_transfers where from_account_id = any($1) or to_account_id = any($1)`,
		ids).Scan(&snap.transfers)
	if err != nil {
		tc.Errorf("snapshot transfers: %v", err)
		return snapshot{}
	}
	err = s.conn.QueryRow(s.ctx,
		`select count(*) from pgledger_entries where account_id = any($1)`, ids).Scan(&snap.entries)
	if err != nil {
		tc.Errorf("snapshot entries: %v", err)
		return snapshot{}
	}
	return snap
}

func (a snapshot) diff(b snapshot) string {
	if a.transfers != b.transfers {
		return fmt.Sprintf("transfer count %d -> %d", a.transfers, b.transfers)
	}
	if a.entries != b.entries {
		return fmt.Sprintf("entry count %d -> %d", a.entries, b.entries)
	}
	if len(a.accounts) != len(b.accounts) {
		return fmt.Sprintf("account count %d -> %d", len(a.accounts), len(b.accounts))
	}
	for i := range a.accounts {
		if a.accounts[i] != b.accounts[i] {
			return fmt.Sprintf("account %s: %+v -> %+v", a.accounts[i].ID, a.accounts[i], b.accounts[i])
		}
	}
	return ""
}

// assertMatchesModel compares every scope account against the oracle, on
// balance (numerically, since NUMERIC scale is not normalised) and version.
func (s *scope) assertMatchesModel(tc hegel.TestCase, m *model) {
	ids := s.accountIDs()

	rows, err := s.conn.Query(s.ctx,
		`select id, balance::text, version, updated_at::text
		from pgledger_accounts_view where id = any($1)`, ids)
	if err != nil {
		tc.Errorf("model comparison query: %v", err)
		return
	}
	states, err := pgx.CollectRows(rows, pgx.RowToStructByPos[accountState])
	if err != nil {
		tc.Errorf("model comparison query: %v", err)
		return
	}

	byID := make(map[string]accountState, len(states))
	for _, st := range states {
		byID[st.ID] = st
	}
	for i, id := range ids {
		st, ok := byID[id]
		if !ok {
			tc.Errorf("account %s (index %d) is missing from pgledger_accounts_view", id, i)
			return
		}
		want := m.accounts[i]
		if mustRat(tc, "balance of account "+id, st.Balance).Cmp(want.Balance) != 0 {
			tc.Errorf("account %d (%s) balance: pgledger %s, model %s",
				i, id, st.Balance, ratString(want.Balance))
		}
		if st.Version != want.Version {
			tc.Errorf("account %d (%s) version: pgledger %d, model %d", i, id, st.Version, want.Version)
		}
	}
}
