package database

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// runner is the minimal statement-execution surface shared by connection pools
// (*pgxpool.Pool) and transactions (pgx.Tx satisfies it natively). Keeping it
// this small is what lets one generic core serve both auto-commit and
// transactional paths without adapters.
type runner interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// conn carries the backend (pool or live transaction), the effective options
// and the context captured ONCE at construction time (New/Connect) — every
// operation derives its per-command context from it internally, so no public
// method takes a context.Context.
type conn struct {
	r   runner
	o   Options
	ctx context.Context
}

// base returns the construction-time context, falling back to
// context.Background() when none was stored. All internal timeouts are
// derived from it (CommandTimeout for statements, ConnectionTimeout for
// begin/acquire/connect paths).
func (c *conn) base() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// timeout bounds ctx with the given command timeout.
func timeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

// freshRollbackCtx derives a context for a COMPENSATING rollback straight from
// context.Background(), bounded by CommandTimeout. Used where the primary
// statement just failed — a failed commit very often failed exactly because
// its context hit the deadline — so reusing that context would make the
// rollback instantly futile.
func freshRollbackCtx(o Options) (context.Context, context.CancelFunc) {
	return timeout(context.Background(), o.CommandTimeout)
}

// rollbackCtx prefers the caller's construction-time context (keeping its
// values/deadline lineage) and falls back to a fresh Background-backed
// context when that lineage is already done (canceled or expired). Used for
// fn-error rollbacks, where the original work failed for its own reasons and
// a live caller context is still meaningful.
func rollbackCtx(base context.Context, o Options) (context.Context, context.CancelFunc) {
	if base.Err() != nil {
		return freshRollbackCtx(o)
	}
	return timeout(base, o.CommandTimeout)
}

// track reports slow queries against the SlowQuery diagnostic threshold.
func track(o Options, start time.Time, sql string) {
	if o.SlowQuery > 0 && time.Since(start) > o.SlowQuery {
		slog.Warn("database: slow query", "duration", time.Since(start), "sql", sql)
	}
}

// ── Generic execution core ─────────────────────────────────────────
//
// Go methods cannot declare type parameters, so the typed equivalents of
// .NET IDatabaseExecutor.QueryAsync<T> / QueryFirstOrDefaultAsync<T> /
// ExecuteScalarAsync<T> are package-level functions parameterized by a conn
// (pool or transaction) instead of a receiver.

// runQuery maps every row into T using pgx conventions: exported fields
// matched by name or by a `db:"column"` tag. The context is the one captured
// once at construction, bounded by CommandTimeout.
func runQuery[T any](c *conn, sql string, args ...any) ([]T, error) {
	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	start := time.Now()
	rows, err := c.r.Query(cctx, sql, args...)
	if err != nil {
		track(c.o, start, sql)
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
	track(c.o, start, sql)

	return out, err
}

// runQueryRow runs a query expected to return at most one row. found reports
// whether a row exists; an empty result is not an error (mirrors .NET's
// QueryFirstOrDefaultAsync returning null). The context is the one captured
// once at construction, bounded by CommandTimeout.
func runQueryRow[T any](c *conn, sql string, args ...any) (T, bool, error) {
	var zero T

	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	start := time.Now()
	rows, err := c.r.Query(cctx, sql, args...)
	if err != nil {
		track(c.o, start, sql)
		return zero, false, err
	}
	value, err := pgx.CollectOneRow(rows, pgx.RowToStructByNameLax[T])
	track(c.o, start, sql)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return zero, false, nil
	case err != nil:
		return zero, false, err
	default:
		return value, true, nil
	}
}

// runScalar scans a single-column, single-row result into T. The context is
// the one captured once at construction, bounded by CommandTimeout.
func runScalar[T any](c *conn, sql string, args ...any) (T, error) {
	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	var value T

	start := time.Now()
	err := c.r.QueryRow(cctx, sql, args...).Scan(&value)
	track(c.o, start, sql)

	return value, err
}

// Execute runs a command and returns affected rows. Shared through embedding:
// DB shadows it with a retry-wrapping version, Tx inherits it verbatim. The
// context is the one captured once at construction, bounded by CommandTimeout.
func (c *conn) Execute(sql string, args ...any) (int64, error) {
	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	start := time.Now()
	tag, err := c.r.Exec(cctx, sql, args...)
	track(c.o, start, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// retried applies p.do around fn, unifying the value-carrying retry boilerplate
// for every typed operation. The stored base context bounds the backoff sleep:
// cancellation during it surfaces as the joined ctx error (per-attempt contexts
// are derived internally from the same stored base context).
func retried[T any](ctx context.Context, p RetryPolicy, fn func() (T, error)) (T, error) {
	var out T
	err := p.do(ctx, func() error {
		var err error
		out, err = fn()
		return err
	})
	return out, err
}

// ── Public typed surface (DB) ─────────────────────────────────────

// Execute runs a command (INSERT/UPDATE/DELETE/DDL) and returns the number of
// affected rows. Transient failures are retried when retry is enabled.
func (db *DB) Execute(sql string, args ...any) (int64, error) {
	return retried(db.base(), db.retry, func() (int64, error) {
		return db.conn.Execute(sql, args...)
	})
}

// Query runs a SELECT and maps every row into a T. Transient failures are
// retried when retry is enabled.
func Query[T any](db *DB, sql string, args ...any) ([]T, error) {
	return retried(db.base(), db.retry, func() ([]T, error) {
		return runQuery[T](&db.conn, sql, args...)
	})
}

// QueryRow runs a query expected to return at most one row. Transient failures
// are retried when retry is enabled.
func QueryRow[T any](db *DB, sql string, args ...any) (T, bool, error) {
	var out T
	var found bool

	err := db.retry.do(db.base(), func() error {
		v, f, err := runQueryRow[T](&db.conn, sql, args...)
		out, found = v, f
		return err
	})

	return out, found, err
}

// Scalar runs a single-value query and scans it into T. Transient failures are
// retried when retry is enabled.
func Scalar[T any](db *DB, sql string, args ...any) (T, error) {
	return retried(db.base(), db.retry, func() (T, error) {
		return runScalar[T](&db.conn, sql, args...)
	})
}
