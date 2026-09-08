package propertytest

import (
	"context"
	"fmt"
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
	conn  *pgxpool.Pool
	ctx   context.Context
	mu    sync.Mutex
	ids   []string
	specs []accountSpec
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
	name := fmt.Sprintf("prop-%d", len(s.ids))
	s.mu.Unlock()

	var id string
	err := s.conn.QueryRow(s.ctx,
		`select id from pgledger_create_account($1, $2, $3, $4)`,
		name, spec.Currency, spec.AllowNeg, spec.AllowPos).Scan(&id)
	if err != nil {
		tc.Errorf("create account: %v", err)
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

// createTransfers calls the array form of pgledger_create_transfers.
func (s *scope) createTransfers(reqs []request, eventAt *time.Time, metadata *string) ([]ledgertest.Transfer, error) {
	ids := s.accountIDs()
	args := []any{}
	rows := make([]string, 0, len(reqs))
	for _, r := range reqs {
		rows = append(rows, fmt.Sprintf("($%d::text, $%d::text, $%d::numeric)",
			len(args)+1, len(args)+2, len(args)+3))
		args = append(args, ids[r.FromIdx], ids[r.ToIdx], r.Amount)
	}

	arrayLit := "array[]::transfer_request[]"
	if len(rows) > 0 {
		arrayLit = "array[" + strings.Join(rows, ", ") + "]::transfer_request[]"
	}
	sql := fmt.Sprintf(`select * from pgledger_create_transfers(
		transfer_requests => %s, event_at => $%d::timestamptz, metadata => $%d::jsonb)`,
		arrayLit, len(args)+1, len(args)+2)
	args = append(args, eventAt, metadata)

	return s.query(sql, args...)
}

// createTransfersVariadic calls the VARIADIC form, which takes no event_at or
// metadata.
func (s *scope) createTransfersVariadic(reqs []request) ([]ledgertest.Transfer, error) {
	ids := s.accountIDs()
	args := []any{}
	rows := make([]string, 0, len(reqs))
	for _, r := range reqs {
		rows = append(rows, fmt.Sprintf("($%d::text, $%d::text, $%d::numeric)",
			len(args)+1, len(args)+2, len(args)+3))
		args = append(args, ids[r.FromIdx], ids[r.ToIdx], r.Amount)
	}
	sql := fmt.Sprintf("select * from pgledger_create_transfers(%s)", strings.Join(rows, ", "))
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

// assertInvariants checks the structural invariants that must hold after every
// accepted or rejected call: conservation per currency (A1), balance as the
// fold of its entries and version as the leg count (A2/A5), and entry chain
// continuity (A4).
func (s *scope) assertInvariants(tc hegel.TestCase) {
	ids := s.accountIDs()

	rows, err := s.conn.Query(s.ctx, `
		select currency, sum(balance)::text
		from pgledger_accounts_view
		where id = any($1)
		group by currency`, ids)
	if err != nil {
		tc.Errorf("conservation query: %v", err)
	}
	totals, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
		Currency string
		Total    string
	}])
	if err != nil {
		tc.Errorf("conservation query: %v", err)
	}
	for _, t := range totals {
		if mustRat(tc, "sum(balance) for "+t.Currency, t.Total).Sign() != 0 {
			tc.Errorf("currency %s does not conserve: sum(balance) = %s", t.Currency, t.Total)
		}
	}

	rows, err = s.conn.Query(s.ctx, `
		select a.id, a.balance::text, coalesce(sum(e.amount), 0)::text, a.version, count(e.id)
		from pgledger_accounts_view a
		left join pgledger_entries e on e.account_id = a.id
		where a.id = any($1)
		group by a.id, a.balance, a.version
		having coalesce(sum(e.amount), 0) != a.balance or count(e.id) != a.version`, ids)
	if err != nil {
		tc.Errorf("fold query: %v", err)
	}
	folds, err := pgx.CollectRows(rows, pgx.RowToStructByPos[struct {
		ID       string
		Balance  string
		EntrySum string
		Version  int64
		Legs     int64
	}])
	if err != nil {
		tc.Errorf("fold query: %v", err)
	}
	for _, f := range folds {
		tc.Errorf("account %s: balance=%s sum(entries)=%s version=%d legs=%d",
			f.ID, f.Balance, f.EntrySum, f.Version, f.Legs)
	}

	var breaks int
	err = s.conn.QueryRow(s.ctx, `
		select count(*) from (
			select account_previous_balance,
				lag(account_current_balance) over (partition by account_id order by id) as prev
			from pgledger_entries where account_id = any($1)
		) t where prev is not null and prev != account_previous_balance`, ids).Scan(&breaks)
	if err != nil {
		tc.Errorf("chain query: %v", err)
	}
	if breaks != 0 {
		tc.Errorf("entry chain is broken in %d place(s)", breaks)
	}
}

type accountState struct {
	ID      string
	Balance string
	Version int64
}

type snapshot struct {
	accounts  []accountState
	transfers int64
	entries   int64
}

func (s *scope) snapshot(tc hegel.TestCase) snapshot {
	ids := s.accountIDs()

	rows, err := s.conn.Query(s.ctx,
		`select id, balance::text, version from pgledger_accounts_view where id = any($1) order by id`, ids)
	if err != nil {
		tc.Errorf("snapshot query: %v", err)
	}
	accounts, err := pgx.CollectRows(rows, pgx.RowToStructByPos[accountState])
	if err != nil {
		tc.Errorf("snapshot query: %v", err)
	}

	var snap snapshot
	snap.accounts = accounts
	err = s.conn.QueryRow(s.ctx,
		`select count(*) from pgledger_transfers where from_account_id = any($1) or to_account_id = any($1)`,
		ids).Scan(&snap.transfers)
	if err != nil {
		tc.Errorf("snapshot transfers: %v", err)
	}
	err = s.conn.QueryRow(s.ctx,
		`select count(*) from pgledger_entries where account_id = any($1)`, ids).Scan(&snap.entries)
	if err != nil {
		tc.Errorf("snapshot entries: %v", err)
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
		`select id, balance::text, version from pgledger_accounts_view where id = any($1)`, ids)
	if err != nil {
		tc.Errorf("model comparison query: %v", err)
	}
	states, err := pgx.CollectRows(rows, pgx.RowToStructByPos[accountState])
	if err != nil {
		tc.Errorf("model comparison query: %v", err)
	}
	if len(states) != len(ids) {
		tc.Errorf("expected %d accounts, found %d", len(ids), len(states))
	}

	byID := make(map[string]accountState, len(states))
	for _, st := range states {
		byID[st.ID] = st
	}
	for i, id := range ids {
		st, ok := byID[id]
		if !ok {
			tc.Errorf("account %s (index %d) is missing", id, i)
		}
		want := m.accounts[i]
		got := mustRat(tc, "balance of account "+id, st.Balance)
		if got.Cmp(want.Balance) != 0 {
			tc.Errorf("account %d (%s) balance: pgledger %s, model %s",
				i, id, st.Balance, ratString(want.Balance))
		}
		if st.Version != want.Version {
			tc.Errorf("account %d (%s) version: pgledger %d, model %d", i, id, st.Version, want.Version)
		}
	}
}
