package database

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ── Shared unit-test doubles ────────────────────────────────────────
//
// These fakes extend the minimal ones in transaction_test.go
// (fakePoolGate / fakeTx / newFakeDB): they RECORD every statement,
// captured context snapshot and pgx.TxOptions they receive, and serve
// canned results so the full typed core (CollectRows scanning included)
// runs without a live PostgreSQL.

// queryRecord freezes one recorded Query/QueryRow call: SQL, positional
// args and a snapshot of the handed-over context state at call time.
type queryRecord struct {
	SQL          string
	Args         []any
	CtxErrAtCall error
	CtxDeadline  time.Time // zero when the context carries no deadline
	HasDeadline  bool
	Lineage      any // value stored under ctxKeyMarker in the source context
}

// recordingPool is a scriptable Pool: Exec/Query/QueryRow/BeginTx are
// recorded, and canned responses are consumed FIFO from the queues below.
// Concurrency-safe: the routing cluster tests hammer several pools from
// multiple goroutines.
type recordingPool struct {
	fakePoolGate // promotes Begin(ctx)/Ping/Close defaults; shadowed below where needed

	mu            sync.Mutex
	execs         []queryRecord
	queryCalls    []queryRecord
	rowSets       []*rowSet    // consumed by Query (one per call)
	scalars       []scalarCell // consumed by QueryRow.Scan (one per call)
	commandTagStr string       // CommandTag text returned by Exec
	beginTxOpts   []pgx.TxOptions
	begunTxs      []*fakeTx
}

func newRecordingPool() *recordingPool {
	return &recordingPool{commandTagStr: "UPDATE 1"}
}

// ── scripting ──

func (p *recordingPool) enqueueRows(cols []string, vals []any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rowSets = append(p.rowSets, &rowSet{cols: cols, vals: vals})
}

func (p *recordingPool) enqueueScalar(v any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scalars = append(p.scalars, scalarCell{v: v})
}

// enqueueScalarError scripts a failing single-value scan (e.g. connection
// dropped mid-Exists).
func (p *recordingPool) enqueueScalarError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scalars = append(p.scalars, scalarCell{err: err})
}

func (p *recordingPool) setCommandTag(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commandTagStr = s
}

// ── inspection ──

func (p *recordingPool) execCalls() []queryRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]queryRecord(nil), p.execs...)
}

func (p *recordingPool) queries() []queryRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]queryRecord(nil), p.queryCalls...)
}

func (p *recordingPool) execCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.execs)
}

func (p *recordingPool) queryCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queryCalls)
}

func (p *recordingPool) txOptionsSeen() []pgx.TxOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]pgx.TxOptions(nil), p.beginTxOpts...)
}

func (p *recordingPool) begunTx(index int) *fakeTx {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < len(p.begunTxs) {
		return p.begunTxs[index]
	}
	return nil
}

// ── Pool implementation ──

func (p *recordingPool) snapshot(ctx context.Context) queryRecord {
	r := queryRecord{
		CtxErrAtCall: ctx.Err(),
		Lineage:      ctx.Value(ctxKeyMarker{}),
	}
	if d, ok := ctx.Deadline(); ok {
		r.CtxDeadline, r.HasDeadline = d, true
	}
	return r
}

func (p *recordingPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	rec := p.snapshot(ctx)
	rec.SQL, rec.Args = sql, args

	p.mu.Lock()
	p.execs = append(p.execs, rec)
	tag := p.commandTagStr
	p.mu.Unlock()

	return pgconn.NewCommandTag(tag), nil
}

func (p *recordingPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rec := p.snapshot(ctx)
	rec.SQL, rec.Args = sql, args

	p.mu.Lock()
	p.queryCalls = append(p.queryCalls, rec)
	var rs *rowSet
	if len(p.rowSets) > 0 {
		rs = p.rowSets[0]
		p.rowSets = p.rowSets[1:]
	}
	p.mu.Unlock()

	if rs != nil {
		return &recRows{rs: rs}, nil
	}
	return &recRows{}, nil // empty result set
}

func (p *recordingPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rec := p.snapshot(ctx)
	rec.SQL, rec.Args = sql, args

	p.mu.Lock()
	p.queryCalls = append(p.queryCalls, rec)
	var cell scalarCell
	if len(p.scalars) > 0 {
		cell = p.scalars[0]
		p.scalars = p.scalars[1:]
	} else {
		cell = scalarCell{err: errors.New("recordingPool: no scripted scalar result")}
	}
	p.mu.Unlock()

	return scalarRow{cell: cell}
}

// Begin shadows the embedded gate's default so plain transactions also
// receive a usable fakeTx (a nil tx would panic inside runTransactional).
func (p *recordingPool) Begin(context.Context) (pgx.Tx, error) {
	return p.trackTx(), nil
}

// BeginTx records the requested pgx.TxOptions and hands out a fresh fakeTx
// per call, mirroring *pgxpool.Pool.BeginTx (used by TransactionalLevel).
func (p *recordingPool) BeginTx(_ context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx := p.trackTx()

	p.mu.Lock()
	p.beginTxOpts = append(p.beginTxOpts, txOptions)
	p.mu.Unlock()

	return tx, nil
}

func (p *recordingPool) trackTx() *fakeTx {
	tx := &fakeTx{}
	p.mu.Lock()
	p.begunTxs = append(p.begunTxs, tx)
	p.mu.Unlock()
	return tx
}

var _ Pool = (*recordingPool)(nil)
var _ interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
} = (*recordingPool)(nil)

// ── canned result plumbing ──

type scalarCell struct {
	v   any
	err error
}

type scalarRow struct{ cell scalarCell }

func (r scalarRow) Scan(dest ...any) error {
	if r.cell.err != nil {
		return r.cell.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("scalarRow: want 1 destination, got %d", len(dest))
	}
	return scanAssign(dest[0], r.cell.v)
}

// rowSet is one canned result set: column descriptions plus row values
// (single-row sets suffice for every library query path exercised here).
type rowSet struct {
	cols []string
	vals []any
}

// recRows implements pgx.Rows (all methods exported, hence fully
// satisfiable out-of-package) over a rowSet. pgx's CollectOneRow /
// RowToStructByNameLax operate on the exported Rows surface, so scans go
// through this fake exactly as against real server rows. Only zero-or-one-row
// sets are needed by every library query path.
type recRows struct {
	rs      *rowSet
	cur     int // -1 == no current row; 0 == the single row is positioned
	started bool
	closed  bool
}

func (f *recRows) Next() bool {
	if f.closed || f.rs == nil {
		return false
	}
	switch {
	case !f.started:
		f.started = true
	case f.cur >= 0:
		return false // the single available row was already consumed
	}
	if len(f.rs.vals) == 0 {
		return false
	}
	f.cur = 0
	return true
}

func (f *recRows) Scan(dest ...any) error {
	if f.rs == nil || f.cur != 0 {
		return errors.New("recRows: Scan called before a successful Next")
	}
	if len(dest) != len(f.rs.cols) {
		return fmt.Errorf("recRows: want %d destinations, got %d", len(f.rs.cols), len(dest))
	}
	for i, d := range dest {
		if err := scanAssign(d, f.rs.vals[i]); err != nil {
			return err
		}
	}
	return nil
}

func (f *recRows) Values() ([]any, error) {
	if f.rs == nil {
		return nil, errors.New("recRows: no row values")
	}
	return append([]any(nil), f.rs.vals...), nil
}

func (f *recRows) RawValues() [][]byte { return make([][]byte, len(f.rs.cols)) }

func (f *recRows) FieldDescriptions() []pgconn.FieldDescription {
	if f.rs == nil {
		return nil
	}
	descs := make([]pgconn.FieldDescription, len(f.rs.cols))
	for i, c := range f.rs.cols {
		descs[i].Name = c
	}
	return descs
}

func (f *recRows) CommandTag() pgconn.CommandTag { return pgconn.NewCommandTag("SELECT 1") }

func (f *recRows) Err() error      { return nil }
func (f *recRows) Close()          { f.closed = true }
func (f *recRows) Conn() *pgx.Conn { return nil }

var (
	_ pgx.Rows           = (*recRows)(nil)
	_ pgx.CollectableRow = (*recRows)(nil)
)

// scanAssign stores src into the dest pointer using plain reflection —
// enough for the int64/string/bool/time.Time targets this library scans.
func scanAssign(dest, src any) error {
	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Pointer || dv.IsNil() {
		return fmt.Errorf("scanAssign: destination must be a non-nil pointer, got %T", dest)
	}
	elem := dv.Elem()
	if src == nil {
		elem.SetZero()
		return nil
	}
	sv := reflect.ValueOf(src)
	switch {
	case sv.Type().AssignableTo(elem.Type()):
		elem.Set(sv)
	case sv.Type().ConvertibleTo(elem.Type()):
		elem.Set(sv.Convert(elem.Type()))
	default:
		return fmt.Errorf("scanAssign: cannot scan %T into %T", src, dest)
	}
	return nil
}

// newTestDB wires a *DB around an arbitrary Pool fake with generous base
// CommandTimeout (disjoint from the tight custom timeouts asserted in the
// DBView tests) and retry disabled for deterministic single-attempt calls.
func newTestDB(base context.Context, p Pool) *DB {
	return &DB{
		conn: conn{
			r:   p,
			o:   Options{CommandTimeout: 2 * time.Second, ConnectionTimeout: 100 * time.Millisecond},
			ctx: base,
		},
		pool:  p,
		retry: NewRetryPolicy(false, 0, time.Millisecond),
	}
}
