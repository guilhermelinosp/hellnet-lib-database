package database

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// ── Fakes ────────────────────────────────────────────────────────────

// recordingCopier implements runner + bulkCopier, recording the CopyFrom
// arguments and returning canned results.
type recordingCopier struct {
	fakePoolGate

	mu            sync.Mutex
	base          context.Context
	baseErrAtCall error // nil iff the context was ALIVE when CopyFrom ran
	table         pgx.Identifier
	columns       []string
	rowsCount     int

	n       int64
	copyErr error
}

func (c *recordingCopier) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base = ctx
	c.baseErrAtCall = ctx.Err()
	c.table = tableName
	c.columns = columnNames
	for rowSrc.Next() {
		if _, err := rowSrc.Values(); err != nil {
			return 0, err
		}
		c.rowsCount++
	}
	return c.n, c.copyErr
}

// newTestConn builds a Conn wired to an arbitrary runner — construction via
// New/Connect would force a real pool/link these fakes replace.
func newTestConn(r runner) *Conn {
	return &Conn{
		conn: conn{r: r},
	}
}

// ── Validation-first ────────────────────────────────────────────────

func TestBulkInsertRejectsInvalidTable(t *testing.T) {
	c := newTestConn(&fakePoolGate{})
	for _, table := range []string{"", "orders; DROP TABLE x", "orders--", `"orders"`} {
		if _, err := c.BulkInsert(table, []string{"id"}, nil); err == nil {
			t.Errorf("BulkInsert(%q) accepted an invalid table name", table)
		}
	}
}

func TestBulkInsertRequiresColumns(t *testing.T) {
	c := newTestConn(&fakePoolGate{})
	if _, err := c.BulkInsert("orders", nil, nil); err == nil || !strings.Contains(err.Error(), "at least one column") {
		t.Errorf("BulkInsert(empty columns) err = %v, want descriptive column requirement", err)
	}
}

func TestBulkInsertRejectsInvalidColumn(t *testing.T) {
	c := newTestConn(&fakePoolGate{})
	if _, err := c.BulkInsert("orders", []string{"id", "status; DROP TABLE x"}, nil); err == nil {
		t.Error("BulkInsert accepted an invalid column name")
	}
}

// ── Capability mapping ──────────────────────────────────────────────

func TestBulkInsertCapabilityMissing(t *testing.T) {
	// fakePoolGate implements only the basic runner: no CopyFrom anywhere.
	c := newTestConn(&fakePoolGate{})
	_, err := c.BulkInsert("orders", []string{"id"}, [][]any{{1}})
	if err == nil || !strings.Contains(err.Error(), "does not support COPY") {
		t.Fatalf("err = %v, want descriptive capability-missing error", err)
	}
}

// ── Happy path over a native copier ─────────────────────────────────

func TestBulkInsertUsesCopyFromWithDerivedContext(t *testing.T) {
	copier := &recordingCopier{n: 42}
	base := context.WithValue(context.Background(), ctxKeyMarker{}, "lineage")
	c := newTestConn(copier)
	c.o.CommandTimeout = 250 * time.Millisecond
	c.ctx = base

	rows := [][]any{{int64(1), "a"}, {int64(2), "b"}}
	n, err := c.BulkInsert("orders", []string{"id", "status"}, rows)
	if err != nil || n != 42 {
		t.Fatalf("BulkInsert = %d, %v; want 42, nil", n, err)
	}

	copier.mu.Lock()
	defer copier.mu.Unlock()
	if got, want := copier.table, (pgx.Identifier{"orders"}); !equalIdentifiers(got, want) {
		t.Errorf("table identifier = %v, want %v", got, want)
	}
	if strings.Join(copier.columns, ",") != "id,status" {
		t.Errorf("columns = %v, want [id status]", copier.columns)
	}
	if copier.rowsCount != 2 {
		t.Errorf("row source delivered %d rows, want 2", copier.rowsCount)
	}
	if copier.baseErrAtCall != nil {
		t.Errorf("CopyFrom received a dead ctx (Err=%v); want live CommandTimeout-derived context", copier.baseErrAtCall)
	}
	if v, ok := copier.base.Value(ctxKeyMarker{}).(string); !ok || v != "lineage" {
		t.Error("CopyFrom context lost the stored construction-time lineage")
	}
}

func TestBulkInsertWrapsCopyError(t *testing.T) {
	copierErr := errors.New("syntax error in COPY")
	copier := &recordingCopier{copyErr: copierErr}
	c := newTestConn(copier)

	_, err := c.BulkInsert("orders", []string{"id"}, [][]any{{1}})
	if err == nil || !errors.Is(err, copierErr) || !strings.Contains(err.Error(), `copy into "orders"`) {
		t.Fatalf("err = %v, want wrapped COPY error preserving cause", err)
	}
}

func equalIdentifiers(a, b pgx.Identifier) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
