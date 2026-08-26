package database

import (
	"context"
	"errors"
	"fmt"

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
func TxQuery[T any](ctx context.Context, tx *Tx, sql string, args ...any) ([]T, error) {
	return runQuery[T](ctx, &tx.conn, sql, args...)
}

// TxQueryRow runs a query expected to return at most one row inside the
// transaction.
func TxQueryRow[T any](ctx context.Context, tx *Tx, sql string, args ...any) (T, bool, error) {
	return runQueryRow[T](ctx, &tx.conn, sql, args...)
}

// TxScalar scans a single-value result inside the transaction.
func TxScalar[T any](ctx context.Context, tx *Tx, sql string, args ...any) (T, error) {
	return runScalar[T](ctx, &tx.conn, sql, args...)
}

// Transactional runs fn atomically: fn's operations share one connection and
// commit together. If fn returns an error the transaction is rolled back and
// the error is returned; a rollback failure is joined into the returned error.
// This mirrors the .NET IDatabaseTransaction.ExecuteAsync contract.
func (db *DB) Transactional(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	cctx, cancel := timeout(ctx, db.o.ConnectionTimeout)
	pgxconn, err := db.pool.Begin(cctx)
	cancel()
	if err != nil {
		return fmt.Errorf("database: begin transaction: %w", err)
	}

	tx := &Tx{conn: conn{r: pgxconn, o: db.o}}

	// done tracks whether fn resolved the transaction (commit or rollback).
	// A deferred rollback releases the pooled connection if fn panics, since a
	// panic would otherwise unwind past both Commit and the explicit Rollback
	// and leak the connection acquired by Begin.
	done := false
	defer func() {
		if !done {
			_ = pgxconn.Rollback(context.Background())
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := pgxconn.Rollback(ctx); rbErr != nil {
			return errors.Join(err, fmt.Errorf("database: rollback: %w", rbErr))
		}
		done = true
		return err
	}

	if err := pgxconn.Commit(ctx); err != nil {
		// A failed commit leaves the transaction aborted server-side; still
		// attempt a rollback to release the connection cleanly.
		_ = pgxconn.Rollback(ctx)
		return fmt.Errorf("database: commit: %w", err)
	}
	done = true
	return nil
}

var (
	_ runner = (*pgxpool.Pool)(nil)
	_ runner = (pgx.Tx)(nil)
	_        = pgconn.CommandTag{}
)
