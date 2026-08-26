package database

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Tx is a transaction-scoped executor. It embeds conn (carrying the live
// pgx.Tx as its runner and the shared options), so operations run on the same
// connection and inherit the common execute path. Operations on Tx never
// retry: re-running partially applied work would be incorrect (the caller
// owns the retry decision for the whole unit of work).
type Tx struct {
	conn
}

// TxQuery maps every row into T. Rows see the transaction's uncommitted state.
func TxQuery[T any](tx *Tx, sql string, args ...any) ([]T, error) {
	return runQuery[T](&tx.conn, sql, args...)
}

// TxQueryRow runs a query expected to return at most one row inside the
// transaction.
func TxQueryRow[T any](tx *Tx, sql string, args ...any) (T, bool, error) {
	return runQueryRow[T](&tx.conn, sql, args...)
}

// TxScalar scans a single-value result inside the transaction.
func TxScalar[T any](tx *Tx, sql string, args ...any) (T, error) {
	return runScalar[T](&tx.conn, sql, args...)
}

// Commit commits the transaction. Use it only for transactions started via
// Conn.Begin, which the caller drives manually. Transactions managed by
// DB.Transactional / Conn.Transactional commit automatically and must NOT call
// this method. The context captured once at construction is used internally,
// bounded by CommandTimeout.
func (tx *Tx) Commit() error {
	pgxTx, ok := tx.r.(pgx.Tx)
	if !ok {
		return fmt.Errorf("database: connection does not support commit")
	}
	cctx, cancel := timeout(tx.base(), tx.o.CommandTimeout)
	defer cancel()
	return pgxTx.Commit(cctx)
}

// Rollback aborts the transaction. Use it only for transactions started via
// Conn.Begin; see Commit for the ownership rules. The context captured once at
// construction is used internally, bounded by CommandTimeout.
func (tx *Tx) Rollback() error {
	pgxTx, ok := tx.r.(pgx.Tx)
	if !ok {
		return fmt.Errorf("database: connection does not support rollback")
	}
	cctx, cancel := timeout(tx.base(), tx.o.CommandTimeout)
	defer cancel()
	return pgxTx.Rollback(cctx)
}

// Transactional runs fn atomically: fn's operations share one connection and
// commit together. If fn returns an error the transaction is rolled back and
// the error is returned; a rollback failure is joined into the returned error.
// The context captured once at New is propagated internally to begin, fn's
// statements, commit and rollback (CommandTimeout bounds the terminal
// statements, ConnectionTimeout bounds Begin). This mirrors the .NET
// IDatabaseTransaction.ExecuteAsync contract.
func (db *DB) Transactional(fn func(tx *Tx) error) error {
	cctx, cancel := timeout(db.base(), db.o.ConnectionTimeout)
	pgxconn, err := db.pool.Begin(cctx)
	cancel()
	if err != nil {
		return fmt.Errorf("database: begin transaction: %w", err)
	}

	tx := &Tx{conn: conn{r: pgxconn, o: db.o, ctx: db.ctx}}

	// done tracks whether fn resolved the transaction (commit or rollback).
	// A deferred rollback releases the pooled connection if fn panics, since a
	// panic would otherwise unwind past both Commit and the explicit Rollback
	// and leak the connection acquired by Begin. It uses a FRESH timeout:
	// whatever failure caused this path may have expired the derived contexts,
	// and a swallowed rollback error would leak the pooled connection.
	done := false
	defer func() {
		if !done {
			rctx, rcancel := freshRollbackCtx(db.o)
			defer rcancel()
			if rbErr := pgxconn.Rollback(rctx); rbErr != nil {
				slog.Warn("database: deferred rollback after panic",
					"error", errors.Join(fmt.Errorf("database: rollback: %w", rbErr)))
			}
		}
	}()

	if err := fn(tx); err != nil {
		// Keep the caller's (stored) context when still alive so rollback
		// honors its values; fall back to a fresh Background-backed context
		// when that lineage is already done.
		rctx, rcancel := rollbackCtx(db.base(), db.o)
		rbErr := pgxconn.Rollback(rctx)
		rcancel()
		if rbErr != nil {
			return errors.Join(err, fmt.Errorf("database: rollback: %w", rbErr))
		}
		done = true
		return err
	}

	mctx, mcancel := timeout(db.base(), db.o.CommandTimeout)
	comErr := pgxconn.Commit(mctx)
	mcancel()
	if comErr != nil {
		// A failed commit leaves the transaction aborted server-side; still
		// attempt a rollback to release the connection cleanly. Use a FRESH
		// Background-backed timeout: the commit usually failed exactly
		// because its context hit the deadline, making mctx instantly futile.
		rctx, rcancel := freshRollbackCtx(db.o)
		rbErr := pgxconn.Rollback(rctx)
		rcancel()
		if rbErr != nil {
			slog.Warn("database: post-commit-failure rollback",
				"error", errors.Join(comErr, fmt.Errorf("database: rollback: %w", rbErr)))
		}
		// The transaction is fully resolved (failed commit + compensation);
		// keep the deferred rollback out of it.
		done = true
		return fmt.Errorf("database: commit: %w", comErr)
	}
	done = true
	return nil
}

var (
	_ runner = (*pgxpool.Pool)(nil)
	_ runner = (pgx.Tx)(nil)
	_        = pgconn.CommandTag{}
)
