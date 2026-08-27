package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ── Fakes: minimal pool + pgx.Tx doubles recording the contexts they receive ──

// ctxSnapshot freezes what the fake saw when the library handed over a
// context: cancelling CancelFuncs run during normal cleanup would otherwise
// make post-hoc Err() checks meaningless.
type ctxSnapshot struct {
	ctx       context.Context // values remain readable after cancellation
	errAtCall error           // nil iff the context was alive when received
}

type fakeTx struct {
	commitErr    error
	rollbackErr  error
	commitCtxs   []ctxSnapshot
	rollbackCtxs []ctxSnapshot
}

func (t *fakeTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("fakeTx: nested begin unsupported")
}

func (t *fakeTx) Commit(ctx context.Context) error {
	t.commitCtxs = append(t.commitCtxs, ctxSnapshot{ctx: ctx, errAtCall: ctx.Err()})
	return t.commitErr
}

func (t *fakeTx) Rollback(ctx context.Context) error {
	t.rollbackCtxs = append(t.rollbackCtxs, ctxSnapshot{ctx: ctx, errAtCall: ctx.Err()})
	return t.rollbackErr
}

func (t *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeTx: copy unsupported")
}

func (t *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (t *fakeTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }

func (t *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("fakeTx: prepare unsupported")
}

func (t *fakeTx) Conn() *pgx.Conn { return nil }

func (t *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("fakeTx: exec unsupported")
}

func (t *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("fakeTx: query unsupported")
}

func (t *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row { return nil }

var _ pgx.Tx = (*fakeTx)(nil)

type fakePoolGate struct {
	tx       pgx.Tx
	beginCtx context.Context
	stats    PoolStats // devolvido por Stat para o sampler de métricas
}

func (p *fakePoolGate) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (p *fakePoolGate) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("fakePoolGate: query unsupported")
}

func (p *fakePoolGate) QueryRow(context.Context, string, ...any) pgx.Row { return nil }

func (p *fakePoolGate) Begin(ctx context.Context) (pgx.Tx, error) {
	p.beginCtx = ctx
	return p.tx, nil
}

func (p *fakePoolGate) Ping(context.Context) error { return nil }
func (p *fakePoolGate) Close()                     {}

// Stat alimenta o contrato estendido de Pool (métricas de pool).
func (p *fakePoolGate) Stat() PoolStats { return p.stats }

var _ Pool = (*fakePoolGate)(nil)

// newFakeDB builds a DB wired to the fake pool/tx with an explicit base ctx —
// construction via New would normalize away exactly the nil/dead cases these
// tests exercise.
func newFakeDB(base context.Context, pool *fakePoolGate) *DB {
	return &DB{
		conn: conn{
			r:   pool,
			o:   Options{CommandTimeout: 100 * time.Millisecond, ConnectionTimeout: 100 * time.Millisecond},
			ctx: base,
		},
		pool:  pool,
		retry: NewRetryPolicy(false, 0, time.Millisecond),
	}
}

type ctxKeyMarker struct{}

// ── I1: compensating rollbacks must not reuse a dead context ───────────────

// A failed commit usually failed BECAUSE its derived context hit the deadline.
// The compensating rollback therefore gets a fresh Background-backed timeout:
// even with a fully cancelled stored base context it must arrive alive.
func TestTransactionalCommitFailureRollbackUsesFreshContext(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	cancel() // stored lineage is dead; a rollback reusing it is instantly futile

	tx := &fakeTx{commitErr: errors.New("simulated deadline exceeded")}
	gate := &fakePoolGate{tx: tx}
	db := newFakeDB(base, gate)

	err := db.Transactional(func(*Tx) error { return nil })
	if err == nil || !errors.Is(err, tx.commitErr) {
		t.Fatalf("Transactional err = %v, want wrapped commit error", err)
	}
	if len(tx.rollbackCtxs) != 1 {
		t.Fatalf("rollback calls = %d, want 1", len(tx.rollbackCtxs))
	}
	if rb := tx.rollbackCtxs[0]; rb.errAtCall != nil {
		t.Errorf("compensating rollback received a dead ctx (Err=%v); want fresh live context", rb.errAtCall)
	}
}

// When fn fails while the caller's stored context is still alive, the fn-error
// rollback keeps that lineage (marker value visible) instead of discarding it.
func TestTransactionalFnErrorRollbackKeepsLiveCallerContext(t *testing.T) {
	base := context.WithValue(context.Background(), ctxKeyMarker{}, "lineage")

	tx := &fakeTx{}
	gate := &fakePoolGate{tx: tx}
	db := newFakeDB(base, gate)

	fnErr := errors.New("fn failed")
	err := db.Transactional(func(*Tx) error { return fnErr })
	if !errors.Is(err, fnErr) {
		t.Fatalf("Transactional err = %v, want fn error", err)
	}
	if len(tx.rollbackCtxs) != 1 {
		t.Fatalf("rollback calls = %d, want 1", len(tx.rollbackCtxs))
	}
	rb := tx.rollbackCtxs[0]
	if v, ok := rb.ctx.Value(ctxKeyMarker{}).(string); !ok || v != "lineage" {
		t.Errorf("rollback ctx lost caller lineage (value=%q ok=%v)", v, ok)
	}
	if rb.errAtCall != nil {
		t.Errorf("rollback ctx Err = %v, want nil", rb.errAtCall)
	}
}

// Same branch, but the stored lineage is already done: the rollback falls back
// to a fresh Background-backed context so the release still reaches the server.
func TestTransactionalFnErrorRollbackFallsBackWhenLineageExpired(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	cancel()

	tx := &fakeTx{}
	gate := &fakePoolGate{tx: tx}
	db := newFakeDB(base, gate)

	fnErr := errors.New("fn failed")
	err := db.Transactional(func(*Tx) error { return fnErr })
	if !errors.Is(err, fnErr) {
		t.Fatalf("Transactional err = %v, want fn error", err)
	}
	if len(tx.rollbackCtxs) != 1 {
		t.Fatalf("rollback calls = %d, want 1", len(tx.rollbackCtxs))
	}
	rb := tx.rollbackCtxs[0]
	if rb.errAtCall != nil {
		t.Errorf("fallback rollback ctx Err = %v, want nil (fresh context)", rb.errAtCall)
	}
	if _, ok := rb.ctx.Value(ctxKeyMarker{}).(string); ok {
		t.Error("fallback rollback ctx should come from Background, not carry the expired lineage")
	}
}

// The deferred panic-path rollback also gets its own fresh timeout, and a
// failing rollback there is warned about rather than silently swallowed.
func TestTransactionalDeferredPanicRollbackUsesFreshContext(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	cancel()

	tx := &fakeTx{rollbackErr: errors.New("release failed too")}
	gate := &fakePoolGate{tx: tx}
	db := newFakeDB(base, gate)

	func() {
		defer func() { _ = recover() }() // fn panics; Transactional re-panics after deferred rollback
		db.Transactional(func(*Tx) error { panic("exploded") })
	}()

	if len(tx.rollbackCtxs) != 1 {
		t.Fatalf("rollback calls = %d, want 1 (deferred path)", len(tx.rollbackCtxs))
	}
	if rb := tx.rollbackCtxs[0]; rb.errAtCall != nil {
		t.Errorf("deferred rollback received a dead ctx (Err=%v), want fresh live context", rb.errAtCall)
	}
}
