package database

import (
	"context"
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

// beginTxBounded begins a transaction bounded by ConnectionTimeout.
func beginTxBounded(base context.Context, o Options, begin func(context.Context) (pgx.Tx, error)) (pgx.Tx, error) {
	cctx, cancel := timeout(base, o.ConnectionTimeout)
	pgxTx, err := begin(cctx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("database: begin transaction: %w", err)
	}
	return pgxTx, nil
}

// runTransactional is the shared body of DB.Transactional and
// Conn.Transactional: begin (caller-supplied), run fn atomically, and resolve
// the transaction. Compensation paths use fresh or live-lineage contexts so a
// rollback never reuses an expired one, and rollback failures are warned — a
// swallowed failure would leak the pooled connection.
//
// src fornece opções, base e o registry COMPARTILHADO de hooks/métricas: a Tx
// criada aqui enxerga coletores habilitados tardiamente no DB e reporta os
// desfechos db_transactions_total / db_rollback_compensations_total.
func runTransactional(
	begin func(context.Context) (pgx.Tx, error),
	src *conn,
	fn func(tx *Tx) error,
) error {
	o := src.o

	pgxTx, err := beginTxBounded(src.base(), o, begin)
	if err != nil {
		return err
	}

	met := src.observed()
	tx := &Tx{conn: src.derive(pgxTx)}

	// done tracks whether fn resolved the transaction. A deferred rollback
	// releases the connection if fn panics: a panic would otherwise unwind
	// past both Commit and the explicit Rollback and leak it. It uses a FRESH
	// timeout: whatever failure caused this path may have expired the derived
	// contexts.
	done := false
	defer func() {
		if !done {
			met.recordRollbackCompensation()
			met.recordTx(txResultPanic)
			rctx, rcancel := freshRollbackCtx(o)
			defer rcancel()
			if rbErr := pgxTx.Rollback(rctx); rbErr != nil {
				slog.Warn("database: deferred rollback after panic",
					"error", errors.Join(fmt.Errorf("database: rollback: %w", rbErr)))
			}
		}
	}()

	if err := fn(tx); err != nil {
		rctx, rcancel := rollbackCtx(src.base(), o)
		rbErr := pgxTx.Rollback(rctx)
		rcancel()
		if rbErr != nil {
			return errors.Join(err, fmt.Errorf("database: rollback: %w", rbErr))
		}
		done = true
		met.recordTx(txResultRollback)
		return err
	}

	mctx, mcancel := timeout(src.base(), o.CommandTimeout)
	comErr := pgxTx.Commit(mctx)
	mcancel()
	if comErr != nil {
		// A failed commit leaves the transaction aborted server-side; still
		// attempt a rollback to release the connection cleanly. The commit
		// usually failed exactly because its context hit the deadline, so use
		// a FRESH Background-backed timeout.
		met.recordRollbackCompensation()
		met.recordTx(txResultRollback)
		rctx, rcancel := freshRollbackCtx(o)
		rbErr := pgxTx.Rollback(rctx)
		rcancel()
		if rbErr != nil {
			slog.Warn("database: post-commit-failure rollback",
				"error", errors.Join(comErr, fmt.Errorf("database: rollback: %w", rbErr)))
		}
		done = true
		return fmt.Errorf("database: commit: %w", comErr)
	}
	done = true
	met.recordTx(txResultCommit)
	return nil
}

// Transactional runs fn atomically on the pooled connection: fn's operations
// share one connection and commit together. If fn returns an error the
// transaction is rolled back and the error is returned; a rollback failure is
// joined into the returned error. The context captured once at New is
// propagated internally (CommandTimeout bounds the terminal statements,
// ConnectionTimeout bounds Begin). This mirrors the .NET
// IDatabaseTransaction.ExecuteAsync contract.
func (db *DB) Transactional(fn func(tx *Tx) error) error {
	return runTransactional(func(c context.Context) (pgx.Tx, error) { return db.pool.Begin(c) },
		&db.conn, fn)
}

var (
	_ runner = (*pgxpool.Pool)(nil)
	_ runner = (pgx.Tx)(nil)
	_        = pgconn.CommandTag{}
)
