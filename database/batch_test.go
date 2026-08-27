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

// ── Fakes: batch pipeline over the existing fakeTx pattern ──────────

// fakeBatchEntry is one planned result slot, in queue order.
type fakeBatchEntry struct {
	tag     string // command tag text (e.g. "INSERT 0 1"); empty → row slot
	row     []any  // values returned by RowScan when tag == ""
	execErr error
	rowErr  error
}

// fakeBatchResults emulates pgx.BatchResults popping from a canned queue.
type fakeBatchResults struct {
	mu       sync.Mutex
	entries  []fakeBatchEntry
	i        int
	closed   int
	closeErr error
}

func (f *fakeBatchResults) next() (fakeBatchEntry, error) {
	if f.i >= len(f.entries) {
		return fakeBatchEntry{}, errors.New("fakeBatchResults: result queue exhausted")
	}
	e := f.entries[f.i]
	f.i++
	return e, nil
}

func (f *fakeBatchResults) Exec() (pgconn.CommandTag, error) {
	e, err := f.next()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	if e.execErr != nil {
		return pgconn.CommandTag{}, e.execErr
	}
	return pgconn.NewCommandTag(e.tag), nil
}

func (f *fakeBatchResults) Query() (pgx.Rows, error) {
	return nil, errors.New("fakeBatchResults: query unsupported")
}

func (f *fakeBatchResults) QueryRow() pgx.Row {
	e, err := f.next()
	if err != nil {
		return fakeRow{err: err}
	}
	if e.rowErr != nil {
		return fakeRow{err: e.rowErr}
	}
	return fakeRow{vals: e.row}
}

func (f *fakeBatchResults) Close() error {
	f.mu.Lock()
	f.closed++
	err := f.closeErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	// Real pgx drains unread results; emulate by consuming the rest.
	for f.i < len(f.entries) {
		if _, err := f.next(); err != nil {
			break
		}
	}
	return nil
}

var _ pgx.BatchResults = (*fakeBatchResults)(nil)

// fakeRow scans canned values into dest pointers positionally.
type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.vals) {
		return errors.New("fakeRow: dest/values length mismatch")
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *int64:
			v, ok := r.vals[i].(int64)
			if !ok {
				return errors.New("fakeRow: type mismatch for *int64")
			}
			*d = v
		case *string:
			v, ok := r.vals[i].(string)
			if !ok {
				return errors.New("fakeRow: type mismatch for *string")
			}
			*d = v
		default:
			return errors.New("fakeRow: unsupported dest type")
		}
	}
	return nil
}

var _ pgx.Row = fakeRow{}

// fakeBatchTx extends the existing fakeTx with a SendBatch emulation that
// records the context it received and replays a canned results queue.
type fakeBatchTx struct {
	fakeTx

	mu       sync.Mutex
	br       pgx.BatchResults
	sendCtx  ctxSnapshot
	batchLen int
	noSender bool // force the capability-missing branch despite method presence
}

func (t *fakeBatchTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sendCtx = ctxSnapshot{ctx: ctx, errAtCall: ctx.Err()}
	t.batchLen = b.Len()
	if t.noSender || t.br == nil {
		return nil
	}
	return t.br
}

func (t *fakeBatchTx) snapshot() (ctxSnapshot, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sendCtx, t.batchLen
}

func newTestTx(r runner) *Tx {
	return &Tx{conn: conn{r: r}}
}

// ── Queue bookkeeping ───────────────────────────────────────────────

func TestBatchQueueLen(t *testing.T) {
	tx := newTestTx(&fakePoolGate{})
	b := tx.Batch()
	if b.Len() != 0 {
		t.Fatalf("fresh batch Len = %d, want 0", b.Len())
	}
	b.Queue("SELECT 1")
	b.Queue("INSERT INTO orders VALUES ($1)", 7)
	if b.Len() != 2 {
		t.Fatalf("Len = %d, want 2", b.Len())
	}
}

// ── Capability mapping ──────────────────────────────────────────────

func TestSendBatchCapabilityMissing(t *testing.T) {
	tx := newTestTx(&fakePoolGate{}) // plain runner: no SendBatch at all
	br := tx.SendBatch(tx.Batch())

	wantSub := "does not support batch"
	if _, err := br.ExecResult(); err == nil || !strings.Contains(err.Error(), wantSub) {
		t.Errorf("ExecResult err = %v, want %q", err, wantSub)
	}
	if err := br.RowScan(new(int64)); err == nil || !strings.Contains(err.Error(), wantSub) {
		t.Errorf("RowScan err = %v, want %q", err, wantSub)
	}
	if err := br.Close(); err == nil || !strings.Contains(err.Error(), wantSub) {
		t.Errorf("Close err = %v, want %q", err, wantSub)
	}
}

// ── Happy path: ordered consumption + context lineage + close ───────

func TestSendBatchOrderedConsumption(t *testing.T) {
	bt := &fakeBatchTx{
		br: &fakeBatchResults{
			entries: []fakeBatchEntry{
				{tag: "INSERT 0 1"},
				{tag: "INSERT 0 1"},
				{row: []any{int64(3)}},
			},
		},
	}
	base := context.WithValue(context.Background(), ctxKeyMarker{}, "lineage")

	tx := newTestTx(bt)
	tx.o.CommandTimeout = 250 * time.Millisecond
	tx.ctx = base

	b := tx.Batch()
	b.Queue("INSERT INTO orders (status) VALUES ($1)", "a")
	b.Queue("INSERT INTO orders (status) VALUES ($1)", "b")
	b.Queue("SELECT COUNT(*) FROM orders")

	br := tx.SendBatch(b)

	n, err := br.ExecResult()
	if err != nil || n != 1 {
		t.Fatalf("ExecResult#1 = %d, %v; want 1, nil", n, err)
	}
	n, err = br.ExecResult()
	if err != nil || n != 1 {
		t.Fatalf("ExecResult#2 = %d, %v; want 1, nil", n, err)
	}

	var count int64
	if err := br.RowScan(&count); err != nil || count != 3 {
		t.Fatalf("RowScan = %d, %v; want 3, nil", count, err)
	}

	snap, blen := bt.snapshot()
	if blen != 3 {
		t.Errorf("batch delivered to runner with Len=%d, want 3", blen)
	}
	if snap.ctx.Err() != nil {
		t.Error("SendBatch received a dead context; want live CommandTimeout-derived one")
	}
	if v, ok := snap.ctx.Value(ctxKeyMarker{}).(string); !ok || v != "lineage" {
		t.Error("SendBatch context lost the stored construction-time lineage")
	}

	// Close is required and must be idempotent.
	if err := br.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := br.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := bt.br.(*fakeBatchResults); got.closed < 2 {
		t.Errorf("Close passed through %d times, want >= 2", got.closed)
	}
}

func TestRowScanPropagatesErrNoRows(t *testing.T) {
	bt := &fakeBatchTx{
		br: &fakeBatchResults{
			entries: []fakeBatchEntry{{rowErr: ErrNoRows}},
		},
	}
	tx := newTestTx(bt)

	br := tx.SendBatch(tx.Batch())
	defer func() { _ = br.Close() }()

	var v int64
	err := br.RowScan(&v)
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf("RowScan err = %v, want ErrNoRows passthrough", err)
	}
}

func TestExecResultPreservesCommandError(t *testing.T) {
	cmdErr := errors.New("duplicate key value violates unique constraint")
	bt := &fakeBatchTx{
		br: &fakeBatchResults{
			entries: []fakeBatchEntry{{tag: "INSERT 0 0", execErr: cmdErr}},
		},
	}
	tx := newTestTx(bt)

	br := tx.SendBatch(tx.Batch())
	defer func() { _ = br.Close() }()

	_, err := br.ExecResult()
	if !errors.Is(err, cmdErr) {
		t.Fatalf("ExecResult err = %v, want raw command error passthrough", err)
	}
}
