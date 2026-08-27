package database

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ── Fakes: full pgx.Rows over canned rows ───────────────────────────

// fakeIterRows replays a canned result set positionally. FieldDescriptions
// carry only names — decoding goes through Scan into plain Go pointers, so no
// OIDs/codecs are needed for RowToStructByNameLax (it uses FieldDescriptions
// + Scan only).
type fakeIterRows struct {
	mu         sync.Mutex
	cols       []string
	rows       [][]any
	i          int   // next row index
	errAt      int   // when >= 0, Next returns false once i reaches it
	streamErr  error // surfaced by Err() after the truncated advance
	reachedEnd bool  // set by Next when truncation triggers
	scanErr    error // returned by every Scan call
	closed     bool
}

func newFakeIterRows(cols []string, rows [][]any) *fakeIterRows {
	return &fakeIterRows{cols: cols, rows: rows, errAt: -1}
}

func (r *fakeIterRows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.errAt >= 0 && r.i >= r.errAt {
		r.reachedEnd = true // emulates pgx auto-closing on a broken read
		return false
	}
	if r.i >= len(r.rows) {
		return false
	}
	r.i++ // pgx semantics: Next advances the cursor
	return true
}

// current returns the row after the last advance (pgx convention).
func (r *fakeIterRows) current() []any {
	if r.i == 0 || r.i > len(r.rows) {
		return nil
	}
	return r.rows[r.i-1]
}

func (r *fakeIterRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.current()
	if row == nil {
		return errors.New("fakeIterRows: Scan without a successful Next")
	}
	for j := range dest {
		switch d := dest[j].(type) {
		case *int64:
			v, ok := row[j].(int64)
			if !ok {
				return errors.New("fakeIterRows: type mismatch for *int64")
			}
			*d = v
		case *string:
			v, ok := row[j].(string)
			if !ok {
				return errors.New("fakeIterRows: type mismatch for *string")
			}
			*d = v
		default:
			return errors.New("fakeIterRows: unsupported dest type")
		}
	}
	return nil
}

func (r *fakeIterRows) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.streamErr != nil && r.reachedEnd {
		return r.streamErr
	}
	return nil
}

func (r *fakeIterRows) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
}

func (*fakeIterRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *fakeIterRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(r.cols))
	for i, name := range r.cols {
		fds[i].Name = name
	}
	return fds
}

func (r *fakeIterRows) Values() ([]any, error) {
	out := append([]any(nil), r.current()...)
	return out, nil
}

func (*fakeIterRows) RawValues() [][]byte { return nil }
func (*fakeIterRows) Conn() *pgx.Conn     { return nil }

var _ pgx.Rows = (*fakeIterRows)(nil)

// iterQueryRunner is a runner whose Query returns the canned rows.
type iterQueryRunner struct {
	fakePoolGate

	mu           sync.Mutex
	rows         pgx.Rows
	queryCtx     context.Context
	ctxErrAtCall error // nil iff the context was ALIVE when Query ran
}

func (q *iterQueryRunner) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queryCtx = ctx
	q.ctxErrAtCall = ctx.Err()
	return q.rows, nil
}

// ── Tests ───────────────────────────────────────────────────────────

type iterUser struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func TestIterDecodesEveryRowByName(t *testing.T) {
	rows := newFakeIterRows([]string{"id", "name"}, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
		{int64(3), "carol"},
	})
	q := &iterQueryRunner{rows: rows}
	c := &conn{r: q, o: Options{CommandTimeout: 250 * time.Millisecond}}

	var got []iterUser
	err := runIter[iterUser](c, "SELECT id, name FROM users", nil, func(u iterUser) error {
		got = append(got, u)
		return nil
	})
	if err != nil {
		t.Fatalf("runIter: %v", err)
	}
	if len(got) != 3 || got[0].Name != "alice" || got[2].ID != 3 {
		t.Fatalf("decoded = %+v", got)
	}
	if !rows.closed {
		t.Error("rows not closed after full iteration")
	}
	if q.ctxErrAtCall != nil {
		t.Error("query received a dead context; want live stored-lineage one")
	}
}

func TestIterAbortsAndWrapsFnError(t *testing.T) {
	rows := newFakeIterRows([]string{"id", "name"}, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"}, // fn aborts here
		{int64(3), "carol"},
	})
	q := &iterQueryRunner{rows: rows}
	c := &conn{r: q, o: Options{CommandTimeout: 250 * time.Millisecond}}

	fnErr := errors.New("consumer says stop")
	calls := 0
	err := runIter[iterUser](c, "SELECT id FROM users", nil, func(iterUser) error {
		calls++
		if calls == 2 {
			return fnErr
		}
		return nil
	})

	if !errors.Is(err, fnErr) {
		t.Fatalf("err = %v; want FnErr passthrough chain (errors.Is)", err)
	}
	if !strings.Contains(err.Error(), "iterator stopped by consumer") {
		t.Errorf("err = %v; want descriptive wrap", err)
	}
	if calls != 2 {
		t.Fatalf("fn called %d times, want 2 (stream aborted)", calls)
	}
	if !rows.closed {
		t.Error("rows not closed after fn-error abort")
	}
}

func TestIterSurfacesStreamError(t *testing.T) {
	rows := newFakeIterRows([]string{"id", "name"}, [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
	})
	rows.errAt = 1 // truncate after first row, emulating a broken read
	streamBroken := errors.New("read tcp: connection reset")
	rows.streamErr = streamBroken

	q := &iterQueryRunner{rows: rows}
	c := &conn{r: q, o: Options{CommandTimeout: 250 * time.Millisecond}}

	var seen int
	err := runIter[iterUser](c, "SELECT id FROM users", nil, func(iterUser) error {
		seen++
		return nil
	})
	if !errors.Is(err, streamBroken) {
		t.Fatalf("err = %v; want stream error passthrough", err)
	}
	if seen != 1 {
		t.Errorf("fn saw %d rows before the stream broke; want 1", seen)
	}
}
