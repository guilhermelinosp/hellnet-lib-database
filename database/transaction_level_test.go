package database

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// ── parseIsolationLevel ─────────────────────────────────────────────

func TestParseIsolationLevel(t *testing.T) {
	valid := map[string]pgx.TxIsoLevel{
		"read committed":    pgx.ReadCommitted,
		"READ COMMITTED":    pgx.ReadCommitted,
		"repeatable read":   pgx.RepeatableRead,
		" Repeatable Read ": pgx.RepeatableRead, // trimmed + lowercased to the documented spelling
		"serializable":      pgx.Serializable,
		"SERIALIZABLE":      pgx.Serializable,
	}
	for in, want := range valid {
		got, err := parseIsolationLevel(in)
		if err != nil || got.IsoLevel != want {
			t.Errorf("parseIsolationLevel(%q) = %+v, %v; want IsoLevel %q", in, got, err, want)
		}
	}

	invalid := []string{"", "snapshot", "read uncommitted", "chaos"}
	for _, in := range invalid {
		opts, err := parseIsolationLevel(in)
		if err == nil {
			t.Errorf("parseIsolationLevel(%q) = %+v, want error", in, opts)
			continue
		}
		for _, want := range []string{"invalid isolation level", `"read committed"`, `"repeatable read"`, `"serializable"`} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("parseIsolationLevel(%q) err missing %q: %v", in, want, err)
				break
			}
		}
		if opts.IsoLevel != "" {
			t.Errorf("parseIsolationLevel(%q) returned non-zero options on failure", in)
		}
	}
}

// ── DB.TransactionalLevel plumbing through pool.BeginTx ─────────────

func TestDBTransactionalLevelPlumbsTxOptions(t *testing.T) {
	levelCases := map[string]pgx.TxIsoLevel{
		"read committed":  pgx.ReadCommitted,
		"repeatable read": pgx.RepeatableRead,
		"serializable":    pgx.Serializable,
	}
	for level, want := range levelCases {
		pool := newRecordingPool()
		db := newTestDB(context.Background(), pool)

		err := db.TransactionalLevel(level, func(tx *Tx) error { return nil })
		if err != nil {
			t.Fatalf("TransactionalLevel(%q) err=%v", level, err)
		}

		opts := pool.txOptionsSeen()
		if len(opts) != 1 || opts[0].IsoLevel != want {
			t.Fatalf("level %q: BeginTx saw %+v, want exactly one call with IsoLevel %q", level, opts, want)
		}
		tx := pool.begunTx(0)
		if tx == nil || len(tx.commitCtxs) != 1 {
			t.Errorf("level %q: committed transaction not recorded (tx=%+v)", level, tx)
		}
	}
}

func TestDBTransactionalLevelRollsBackOnFnError(t *testing.T) {
	pool := newRecordingPool()
	db := newTestDB(context.Background(), pool)

	sentinel := context.Canceled // any error; identity is what matters
	err := db.TransactionalLevel("repeatable read", func(*Tx) error { return sentinel })
	if !errorChainContains(err, sentinel) {
		t.Errorf("err = %v, want it to wrap the fn error", err)
	}

	if opts := pool.txOptionsSeen(); len(opts) != 1 || opts[0].IsoLevel != pgx.RepeatableRead {
		t.Errorf("BeginTx options = %+v, want repeatable read", opts)
	}
	tx := pool.begunTx(0)
	if tx == nil || len(tx.rollbackCtxs) != 1 || len(tx.commitCtxs) != 0 {
		t.Errorf("rollback not recorded as expected: commit=%d rollback=%d",
			len(tx.commitCtxs), len(tx.rollbackCtxs))
	}
}

func TestDBTransactionalLevelInvalidLevelFailsBeforePool(t *testing.T) {
	pool := newRecordingPool()
	db := newTestDB(context.Background(), pool)

	called := false
	err := db.TransactionalLevel("snapshot", func(*Tx) error { called = true; return nil })
	if err == nil || !strings.Contains(err.Error(), `invalid isolation level "snapshot"`) {
		t.Fatalf("err=%v, want invalid-isolation-level error naming the input", err)
	}
	if called {
		t.Error("fn must not run when the level fails validation")
	}
	if got := len(pool.txOptionsSeen()); got != 0 {
		t.Errorf("pool.BeginTx invoked %d times despite validation failure, want 0", got)
	}
}

// fakePoolGate satisfies Pool but NOT BeginTx-with-options — exercising the
// capability assertion for legacy fakes/embedders.
func TestDBTransactionalLevelUnsupportedPool(t *testing.T) {
	gate := &fakePoolGate{}
	db := newFakeDB(context.Background(), gate)

	err := db.TransactionalLevel("serializable", func(*Tx) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "pool does not support transaction isolation levels") {
		t.Fatalf("err=%v, want unsupported-pool error", err)
	}
	if gate.beginCtx != nil {
		t.Error("plain Begin must not be reached by TransactionalLevel")
	}
}

// ── Conn.TransactionalLevel ─────────────────────────────────────────

func TestConnTransactionalLevelPlumbsOptions(t *testing.T) {
	pool := newRecordingPool()
	c := &Conn{conn: conn{r: pool, o: Options{CommandTimeout: 2 * time.Second, ConnectionTimeout: 100 * time.Millisecond}}}

	err := c.TransactionalLevel("SERIALIZABLE", func(tx *Tx) error { return nil })
	if err != nil {
		t.Fatalf("Conn.TransactionalLevel err=%v", err)
	}

	opts := pool.txOptionsSeen()
	if len(opts) != 1 || opts[0].IsoLevel != pgx.Serializable {
		t.Fatalf("conn BeginTx options = %+v, want serializable", opts)
	}
	if tx := pool.begunTx(0); tx == nil || len(tx.commitCtxs) != 1 {
		t.Error("commit not recorded on the conn-level transaction")
	}
}

func TestConnTransactionalLevelUnsupportedRunner(t *testing.T) {
	gate := &fakePoolGate{}
	c := &Conn{conn: conn{r: gate, o: Options{CommandTimeout: time.Second, ConnectionTimeout: time.Second}}}

	err := c.TransactionalLevel("read committed", func(*Tx) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "connection does not support transaction isolation levels") {
		t.Fatalf("err=%v, want unsupported-connection error", err)
	}
}

// ── small assertion helpers ─────────────────────────────────────────

func isSameText(a, b error) bool {
	return a != nil && b != nil && strings.Contains(a.Error(), b.Error())
}

func errorChainContains(err, target error) bool {
	for e := err; e != nil; {
		if errors.Is(e, target) || isSameText(e, target) {
			return true
		}
		var u interface{ Unwrap() error }
		if !errors.As(e, &u) {
			return false
		}
		e = u.Unwrap()
	}
	return false
}
