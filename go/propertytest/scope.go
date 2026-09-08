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
	"github.com/pgr0ss/pgledger/testhelpers"
	"hegel.dev/go/hegel"
)

// scope is the accounts one test case owns. The suite shares a database with
// parallel tests, so every assertion must be restricted to these ids.
type scope struct {
	conn     *pgxpool.Pool
	ctx      context.Context
	mu       sync.Mutex
	accounts []scopeAccount
	created  int
}

type scopeAccount struct {
	ID   string
	Name string
	Spec accountSpec
}

func newScope(tc hegel.TestCase, ctx context.Context, conn *pgxpool.Pool, specs []accountSpec) *scope {
	s := &scope{conn: conn, ctx: ctx}
	for _, spec := range specs {
		s.addAccount(tc, spec)
	}
	return s
}

func (s *scope) addAccount(tc hegel.TestCase, spec accountSpec) scopeAccount {
	s.mu.Lock()
	name := fmt.Sprintf("prop-%d-%d", s.created, rand.Int64())
	s.created++
	s.mu.Unlock()

	account := scopeAccount{Name: name, Spec: spec}
	err := s.conn.QueryRow(s.ctx,
		`select id from pgledger_create_account($1, $2, $3, $4)`,
		name, spec.Currency, spec.AllowNeg, spec.AllowPos).Scan(&account.ID)
	if err != nil {
		tc.Errorf("create account %s: %v", name, err)
		return scopeAccount{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts = append(s.accounts, account)
	return account
}

func (s *scope) newModel() *model {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := &model{}
	for _, account := range s.accounts {
		m.addAccount(account)
	}
	return m
}

func (s *scope) accountIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, len(s.accounts))
	for i, account := range s.accounts {
		ids[i] = account.ID
	}
	return ids
}

func (s *scope) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.accounts)
}

func accountArg(ids []string, idx int) any {
	switch idx {
	case nullAccount:
		return nil
	case unknownAccount:
		return unknownAccountID
	}
	return ids[idx]
}

func amountArg(amount string) any {
	if amount == nullAmount {
		return nil
	}
	return amount
}

func (s *scope) requestArgs(reqs []request) (literals []string, args []any) {
	ids := s.accountIDs()
	rows := make([][3]any, 0, len(reqs))
	for _, r := range reqs {
		rows = append(rows, [3]any{accountArg(ids, r.FromIdx), accountArg(ids, r.ToIdx), amountArg(r.Amount)})
	}
	return transferLiterals(rows)
}

func transferLiterals(rows [][3]any) (literals []string, args []any) {
	literals = make([]string, 0, len(rows))
	for _, row := range rows {
		literals = append(literals, fmt.Sprintf("($%d::text, $%d::text, $%d::numeric)",
			len(args)+1, len(args)+2, len(args)+3))
		args = append(args, row[:]...)
	}
	return literals, args
}

func (s *scope) createTransfers(reqs []request, eventAt *time.Time, metadata *string) ([]testhelpers.Transfer, error) {
	literals, args := s.requestArgs(reqs)
	return s.createTransferArray(literals, args, eventAt, metadata)
}

func (s *scope) createTransfersBetween(rows [][3]any) ([]testhelpers.Transfer, error) {
	literals, args := transferLiterals(rows)
	return s.createTransferArray(literals, args, nil, nil)
}

func (s *scope) createTransferArray(literals []string, args []any, eventAt *time.Time, metadata *string) ([]testhelpers.Transfer, error) {
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

// createTransfersVariadic needs a non-empty reqs: VARIADIC has no empty-batch
// spelling.
func (s *scope) createTransfersVariadic(reqs []request) ([]testhelpers.Transfer, error) {
	literals, args := s.requestArgs(reqs)
	sql := fmt.Sprintf("select * from pgledger_create_transfers(%s)", strings.Join(literals, ", "))
	return s.query(sql, args...)
}

func (s *scope) createTransfer(req request, eventAt *time.Time, metadata *string) ([]testhelpers.Transfer, error) {
	ids := s.accountIDs()
	return s.query(`select * from pgledger_create_transfer($1::text, $2::text, $3::numeric, $4::timestamptz, $5::jsonb)`,
		accountArg(ids, req.FromIdx), accountArg(ids, req.ToIdx), amountArg(req.Amount), eventAt, metadata)
}

func (s *scope) query(sql string, args ...any) ([]testhelpers.Transfer, error) {
	rows, err := s.conn.Query(s.ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[testhelpers.Transfer])
}

// reportViolations fails the test case with every row of sql, each a rendered
// invariant violation.
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
	if len(found) > 0 {
		tc.Errorf("%s: %s", what, strings.Join(found, "; "))
	}
}

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

	s.reportViolations(tc, "account holds a balance its flags forbid", `
		select format('account %s (negative %s / positive %s) holds %s',
			id, allow_negative_balance, allow_positive_balance, balance)
		from pgledger_accounts_view
		where id = any($1)
			and ((not allow_negative_balance and balance < 0)
				or (not allow_positive_balance and balance > 0))`, ids)

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

	s.reportViolations(tc, "entry disagrees with its transfer", `
		select format('entry %s on account %s: amount %s at %s, transfer %s moves %s from %s to %s at %s',
			e.id, e.account_id, e.amount, e.created_at,
			t.id, t.amount, t.from_account_id, t.to_account_id, t.created_at)
		from pgledger_entries e
		join pgledger_transfers t on t.id = e.transfer_id
		where e.account_id = any($1)
			and (e.amount != case when e.account_id = t.from_account_id then -t.amount else t.amount end
				or e.created_at != t.created_at)`, ids)

	s.reportViolations(tc, "transfer crosses currencies", `
		select format('transfer %s moves %s from %s (%s) to %s (%s)',
			t.id, t.amount, f.id, f.currency, d.id, d.currency)
		from pgledger_transfers t
		join pgledger_accounts f on f.id = t.from_account_id
		join pgledger_accounts d on d.id = t.to_account_id
		where t.from_account_id = any($1) and f.currency != d.currency`, ids)

	// Order by account_version, not id: below PostgreSQL 18 entry ids come from
	// a uuidv7 fallback that is not monotonic within a microsecond.
	s.reportViolations(tc, "entry chain is broken", `
		select format('account %s entry %s (version %s, position %s): previous %s + amount %s = current %s, prior entry ended at %s',
			account_id, id, account_version, position,
			account_previous_balance, amount, account_current_balance, coalesce(prev::text, '0'))
		from (
			select id, account_id, account_version, amount,
				account_previous_balance, account_current_balance,
				lag(account_current_balance) over chain as prev,
				row_number() over chain as position
			from pgledger_entries
			where account_id = any($1)
			window chain as (partition by account_id order by account_version)
		) e
		where account_version != position
			or account_current_balance != account_previous_balance + amount
			or (position = 1 and account_previous_balance != 0)
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

// updated_at is in the snapshot because pgledger writes it in the same UPDATE
// as the balance, so it exposes a partial write.
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

func (s *scope) assertMatchesModel(tc hegel.TestCase, m *model) {
	ids := s.accountIDs()

	rows, err := s.conn.Query(s.ctx,
		`select id, balance::text, version
		from pgledger_accounts_view where id = any($1)`, ids)
	if err != nil {
		tc.Errorf("model comparison query: %v", err)
		return
	}
	type balanceState struct {
		ID      string
		Balance string
		Version int64
	}
	states, err := pgx.CollectRows(rows, pgx.RowToStructByPos[balanceState])
	if err != nil {
		tc.Errorf("model comparison query: %v", err)
		return
	}

	byID := make(map[string]balanceState, len(states))
	for _, st := range states {
		byID[st.ID] = st
	}
	for i, id := range ids {
		st, ok := byID[id]
		if !ok {
			tc.Errorf("account %s (index %d) is missing from pgledger_accounts_view", id, i)
		}
		want := m.accounts[i]
		balanceMatches := mustRat(tc, "balance of account "+id, st.Balance).Cmp(want.Balance) == 0
		if !balanceMatches || st.Version != want.Version {
			tc.Errorf("account %d (%s): pgledger balance %s version %d, model balance %s version %d",
				i, id, st.Balance, st.Version, ratString(want.Balance), want.Version)
		}
	}
}
