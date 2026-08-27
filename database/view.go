package database

import (
	"errors"
	"log/slog"
	"time"
)

// ── Per-operation command-timeout view ──────────────────────────────
//
// DBView is a lightweight, immutable VALUE view over an existing *DB that
// overrides selected knobs for one call chain. It exists because Options is
// process-wide: a nightly batch that must tolerate a slow aggregate should
// not force the whole service to raise its command timeout — and vice versa,
// a latency-critical endpoint should not inherit a 30s default.
//
// Caveats (v1, deliberate):
//   - Timeouts only. Pool sizing, retry policy, credentials and the captured
//     base context are shared 1:1 with the parent DB.
//   - No transactions, no Acquire/Connect, no Repository construction: views
//     describe statement execution, not lifecycle. Open a view of each DB in
//     a transaction scope instead, or pass the explicit timeout into the Tx
//     path when such support lands.
//   - No iterator surface yet (deferred to another work stream; omitted here
//     so this file stays compile-safe against those changes).
//   - Zero value is unusable by design: obtain views exclusively through
//     (*DB).WithCommandTimeout.

// minCommandTimeout is the floor applied to any requested override. Shorter
// "timeouts" mostly measure scheduler jitter and mask misconfigurations
// (milliseconds instead of seconds), so they are clamped loudly rather than
// honored silently.
const minCommandTimeout = 50 * time.Millisecond

// ErrInvalidView is returned by every DBView operation invoked on a nil or
// zero-value view (i.e. not produced via WithCommandTimeout).
var ErrInvalidView = errors.New("database: DBView not initialized; use DB.WithCommandTimeout")

// DBView aliases one *DB with an effective CommandTimeout. It carries no
// resources of its own: Close/Ping/transactions belong to the parent.
type DBView struct {
	db         *DB
	cmdTimeout time.Duration
}

// WithCommandTimeout returns a view of db whose every statement runs under d
// instead of the pool-wide CommandTimeout. Values below minCommandTimeout
// (including zero/negative) are clamped to the floor with a single warning:
// the signature cannot fail, and silent acceptance would turn a config typo
// into either instant deadlines or unbounded waits.
func (db *DB) WithCommandTimeout(d time.Duration) *DBView {
	if d < minCommandTimeout {
		slog.Warn("database: clamping command timeout to minimum",
			"requested", d.String(), "minimum", minCommandTimeout.String())
		d = minCommandTimeout
	}
	return &DBView{db: db, cmdTimeout: d}
}

// CommandTimeout reports the effective timeout used by this view's statements.
func (v *DBView) CommandTimeout() time.Duration {
	if v == nil {
		return 0
	}
	return v.cmdTimeout
}

// conn derives the shared core executor from the parent with only the
// command timeout overridden.
func (v *DBView) conn() (*conn, error) {
	if v == nil || v.db == nil {
		return nil, ErrInvalidView
	}
	o := v.db.o
	o.CommandTimeout = v.cmdTimeout
	return &conn{r: v.db.r, o: o, ctx: v.db.ctx}, nil
}

// Execute runs a command under the view's timeout. Retry behavior mirrors
// the parent DB (same policy, same captured base context bounding backoff).
func (v *DBView) Execute(sql string, args ...any) (int64, error) {
	c, err := v.conn()
	if err != nil {
		return 0, err
	}
	return retried(v.db.base(), v.db.retry, func() (int64, error) {
		return c.Execute(sql, args...)
	})
}

// ViewQuery runs a SELECT mapping every row into T under the view's timeout.
// Package-level (not a method) because Go methods cannot declare type
// parameters — the same constraint that shaped Query[T]/TxQuery[T].
func ViewQuery[T any](v *DBView, sql string, args ...any) ([]T, error) {
	c, err := v.conn()
	if err != nil {
		return nil, err
	}
	return retried(v.db.base(), v.db.retry, func() ([]T, error) {
		return runQuery[T](c, sql, args...)
	})
}

// ViewQueryRow runs at-most-one-row queries under the view's timeout;
// an empty result is (zero, false, nil), exactly like QueryRow[T].
func ViewQueryRow[T any](v *DBView, sql string, args ...any) (T, bool, error) {
	var out T
	var found bool

	c, err := v.conn()
	if err != nil {
		return out, false, err
	}

	rerr := v.db.retry.do(v.db.base(), func() error {
		val, f, rerr := runQueryRow[T](c, sql, args...)
		out, found = val, f
		return rerr
	})

	return out, found, rerr
}

// ViewScalar scans a single value under the view's timeout.
func ViewScalar[T any](v *DBView, sql string, args ...any) (T, error) {
	c, err := v.conn()
	if err != nil {
		var zero T
		return zero, err
	}
	return retried(v.db.base(), v.db.retry, func() (T, error) {
		return runScalar[T](c, sql, args...)
	})
}
