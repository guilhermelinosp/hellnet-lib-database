package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// batchSender is the native pgx pipelining support asserted on the transaction
// runner. pgx.Tx (which is what a real Tx carries) implements it natively; the
// capability check keeps unit-test fakes free of the wider surface.
type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Batch is a queue of statements sent to PostgreSQL in ONE round trip
// (pipelining). Create it with Tx.Batch, append work with Queue and dispatch
// with Tx.SendBatch — results are then consumed strictly in queue order via
// BatchResults.
//
//	b := tx.Batch()
//	b.Queue("INSERT ...", ...)
//	b.Queue("SELECT COUNT(*) ...")
//	br := tx.SendBatch(b)
//	n, err := br.ExecResult()
//	var count int64
//	err = br.RowScan(&count)
//	err = br.Close() // REQUIRED — see BatchResults.Close
type Batch struct {
	pgx *pgx.Batch
}

// Batch creates an empty statement queue bound to this transaction. Statements
// execute with the transaction's snapshot: queued writes are invisible outside
// the tx until Commit. The context captured once at construction is used
// internally when the batch is dispatched.
func (tx *Tx) Batch() *Batch {
	return &Batch{pgx: &pgx.Batch{}}
}

// Queue appends one statement (with positional arguments) to the batch. It
// never touches the network — everything is flushed at SendBatch time.
func (b *Batch) Queue(sql string, args ...any) {
	b.pgx.Queue(sql, args...)
}

// Len reports how many statements are queued so far.
func (b *Batch) Len() int {
	return b.pgx.Len()
}

// SendBatch dispatches every queued statement in a single network round trip
// and returns a BatchResults to read them back in queue order. The underlying
// connection stays busy until BatchResults.Close is called — not calling Close
// leaks/holds it. SendBatch never retries (same rule as any Tx operation).
func (tx *Tx) SendBatch(b *Batch) *BatchResults {
	sender, ok := tx.r.(batchSender)
	if !ok {
		return &BatchResults{err: fmt.Errorf("database: this transaction does not support batch")}
	}

	cctx, cancel := timeout(tx.base(), tx.o.CommandTimeout)
	res := sender.SendBatch(cctx, b.pgx)
	return &BatchResults{res: res, cancel: cancel}
}

// BatchResults reads the outcomes of a sent batch, in queue order. Every method
// consumes exactly one result slot.
type BatchResults struct {
	res    pgx.BatchResults
	cancel context.CancelFunc
	err    error
}

// ExecResult consumes the next result as a command result, reporting affected
// rows. Errors propagate raw so SQLSTATE discrimination still works at the
// call site.
func (br *BatchResults) ExecResult() (int64, error) {
	if br.err != nil {
		return 0, br.err
	}
	tag, err := br.res.Exec()
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// RowScan consumes the next result as a single row, scanning its columns into
// dest in order. ErrNoRows propagates unwrapped for errors.Is checks.
func (br *BatchResults) RowScan(dest ...any) error {
	if br.err != nil {
		return br.err
	}
	return br.res.QueryRow().Scan(dest...)
}

// Close finishes the batch operation: unread results are drained, the derived
// context is released and the connection becomes usable again. It MUST be
// called after consuming results (defer it right after SendBatch) — skipping
// it leaves the connection stuck on the batch protocol. Safe to call more than
// once; subsequent calls return the first error.
func (br *BatchResults) Close() error {
	if br.cancel != nil {
		br.cancel()
		br.cancel = nil
	}
	if br.err != nil {
		return br.err
	}
	return br.res.Close()
}

// txSendsBatch compiles only while every pgx.Tx continues to expose SendBatch —
// compile-time proof that transactions can drive pipelined batches.
var _ = func(tx pgx.Tx) batchSender { return tx }
