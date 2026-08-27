package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// iterFirstRowWatchdog arms a timer that cancels the stream context if the
// FIRST row does not arrive within CommandTimeout. Streaming results must not
// carry that deadline for their whole lifetime (a 10-minute export would die
// at 30s), so as soon as one row advances, the watchdog is disarmed and the
// rest of the stream runs uncapped. Disarming happens exactly once.
func disarmAfter(firstRow bool, wd *time.Timer) bool {
	if !firstRow {
		return false
	}
	wd.Stop()
	return true
}

// runIter streams every row into fn without materializing the result set:
// rows are decoded one at a time straight from pgx.Rows, so memory stays
// constant regardless of result size (5000 or 5 million rows behave the same).
// Mapping uses the same RowToStructByNameLax convention as Query[T].
//
// Timeout semantics differ deliberately from other operations: CommandTimeout
// bounds query start + time-to-FIRST-ROW only; after the first row the stream
// is intentionally uncapped because long exports may legitimately outlive any
// per-command deadline. An fn error aborts the iteration and is returned
// wrapped ("database: iterator stopped by consumer") with errors.Is intact —
// the FnErr passthrough chain callers rely on to branch on their own errors.
func runIter[T any](c *conn, sql string, args []any, fn func(row T) error) error {
	sctx, scancel := context.WithCancel(c.base())
	defer scancel()

	start := time.Now()
	rows, err := c.r.Query(sctx, sql, args...)
	if err != nil {
		track(c.o, start, sql)
		return err
	}
	defer rows.Close()

	watchdog := time.AfterFunc(c.o.CommandTimeout, scancel)
	defer watchdog.Stop()

	firstRow := true
	for rows.Next() {
		firstRow = !disarmAfter(firstRow, watchdog)

		row, derr := pgx.RowToStructByNameLax[T](rows)
		if derr != nil {
			return derr
		}
		if ferr := fn(row); ferr != nil {
			return fmt.Errorf("database: iterator stopped by consumer: %w", ferr)
		}
	}

	if qerr := rows.Err(); qerr != nil {
		// A timeout hitting before ANY row arrived is reported as the documented
		// first-row deadline; mid-stream deadlines cannot happen (watchdog armed
		// only until the first advance).
		if firstRow && errors.Is(qerr, context.DeadlineExceeded) {
			return fmt.Errorf("database: first row not available within %s: %w", c.o.CommandTimeout, qerr)
		}
		return qerr
	}
	return nil
}

// Iter streams rows of sql through fn with constant memory usage, on the
// pooled DB. Unlike the typed queries, streams are NEVER retried: a partially
// consumed stream cannot be replayed inside fn without duplicating side
// effects. See runIter for timeout/abort semantics.
//
//	SQL string, args []any positional slice — pass nil when there are none.
func Iter[T any](db *DB, sql string, args []any, fn func(row T) error) error {
	return runIter[T](&db.conn, sql, args, fn)
}

// ConnIter is Iter pinned to a dedicated connection (no retry either).
func ConnIter[T any](c *Conn, sql string, args []any, fn func(row T) error) error {
	return runIter[T](&c.conn, sql, args, fn)
}

// TxIter is Iter inside a transaction: rows see the tx's uncommitted state.
func TxIter[T any](tx *Tx, sql string, args []any, fn func(row T) error) error {
	return runIter[T](&tx.conn, sql, args, fn)
}
