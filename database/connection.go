package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Conn is a dedicated database connection. Unlike the pooled DB, a Conn pins a
// single underlying connection — either borrowed from the pool via Acquire, or
// opened directly (no pool) via Connect — so every operation runs on the same
// session. This is what you want when a batch of N interactions must share one
// connection, when you need explicit open/close lifecycle control, or when you
// want to drive multiple connections yourself.
//
// A Conn reuses the exact same typed core as DB and Tx: it has Execute (via
// embedding), ConnQuery[T], ConnQueryRow[T] and ConnScalar[T]. Like Tx, it
// NEVER retries — re-running partially applied work on a pinned connection
// would be incorrect, so the caller owns the retry decision for the unit.
//
// Lifecycle: every Acquire/Connect must be paired with Close (releasing the
// connection back to the pool, or closing the physical link). For a
// transactional scope call Begin and then Commit/Rollback; keep the Conn open
// until the transaction is finished, then Close — exactly like raw pgx.
type Conn struct {
	conn
	closeFn func(context.Context) error
	beginFn func(context.Context) (pgx.Tx, error)
}

// poolAcquirer is the subset of pgxpool.Pool used to borrow a single
// connection. It is asserted at runtime so the minimal Pool interface kept on
// DB (which unit tests can fake) does not need to grow an Acquire method.
type poolAcquirer interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
}

// Acquire borrows a single connection from the pool and pins it to the returned
// Conn until Close is called. Multiple Acquire calls return independent
// connections, so explicit multi-connection scenarios work alongside the
// pool's automatic multiplexing. The context captured once at New is used
// internally, bounded by the configured ConnectionTimeout.
func (db *DB) Acquire() (*Conn, error) {
	a, ok := db.pool.(poolAcquirer)
	if !ok {
		return nil, fmt.Errorf("database: pool does not support Acquire")
	}

	cctx, cancel := timeout(db.base(), db.o.ConnectionTimeout)
	defer cancel()

	pconn, err := a.Acquire(cctx)
	if err != nil {
		return nil, fmt.Errorf("database: acquire connection: %w", err)
	}
	return &Conn{
		conn:    conn{r: pconn, o: db.o, ctx: db.ctx},
		closeFn: func(context.Context) error { pconn.Release(); return nil },
		beginFn: pconn.Begin,
	}, nil
}

// Connect opens a single standalone connection (no pool) from explicit
// options. Use it for short-lived programs, scripts, or whenever you explicitly
// want exactly one connection with full open/close control. Pair with Close.
// Connect is construction-time, so it takes the context that is then captured
// once on the returned Conn and propagated internally to every later operation.
func Connect(ctx context.Context, opts Options) (*Conn, error) {
	opts = withDefaults(opts)
	if err := Validate(opts); err != nil {
		return nil, err
	}

	cfg, err := pgx.ParseConfig(opts.dsn())
	if err != nil {
		return nil, fmt.Errorf("database: parse config: %w", err)
	}
	cfg.ConnectTimeout = opts.ConnectionTimeout

	cctx, cancel := timeout(ctx, opts.ConnectionTimeout)
	defer cancel()

	pgxConn, err := pgx.ConnectConfig(cctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("database: connect: %w", err)
	}
	return &Conn{
		conn:    conn{r: pgxConn, o: opts, ctx: ctx},
		closeFn: pgxConn.Close,
		beginFn: pgxConn.Begin,
	}, nil
}

// ConnectFromEnv loads options from HELLNET_DATABASE_* variables (and a .env
// file via LoadFromEnv) and opens a single standalone connection. The env
// loading is fully contained in the library. Like Connect, it captures ctx once.
func ConnectFromEnv(ctx context.Context) (*Conn, error) {
	return Connect(ctx, LoadFromEnv())
}

// Close releases or closes the underlying connection. For an Acquired
// connection it returns to the pool; for a Connect-ed connection it closes the
// physical link. Safe to call more than once; only the first call has effect.
// The context captured once at New/Connect is used internally.
func (c *Conn) Close() error {
	if c.closeFn == nil {
		return nil
	}
	fn := c.closeFn
	c.closeFn = nil
	return fn(c.base())
}

// Begin starts a transaction on this dedicated connection. The returned *Tx is
// committed or rolled back by the caller via Tx.Commit / Tx.Rollback. The Conn
// must remain open until the transaction is finished; call Close afterwards.
// The context captured once at construction is used internally, bounded by the
// configured ConnectionTimeout.
func (c *Conn) Begin() (*Tx, error) {
	cctx, cancel := timeout(c.base(), c.o.ConnectionTimeout)
	defer cancel()

	pgxTx, err := c.beginFn(cctx)
	if err != nil {
		return nil, fmt.Errorf("database: begin on connection: %w", err)
	}
	return &Tx{conn: conn{r: pgxTx, o: c.o, ctx: c.ctx}}, nil
}

// Transactional runs fn atomically on this dedicated connection: fn's
// operations share the connection and commit together, rolling back on error.
// The context captured once at New/Connect is propagated internally to begin,
// fn's statements, commit and rollback. The Conn itself stays open after the
// call — call Close when finished with it.
func (c *Conn) Transactional(fn func(tx *Tx) error) error {
	cctx, cancel := timeout(c.base(), c.o.ConnectionTimeout)
	pgxTx, err := c.beginFn(cctx)
	cancel()
	if err != nil {
		return fmt.Errorf("database: begin transaction: %w", err)
	}

	tx := &Tx{conn: conn{r: pgxTx, o: c.o, ctx: c.ctx}}

	// done tracks whether fn resolved the transaction. A deferred rollback
	// releases the connection if fn panics, since a panic would otherwise
	// unwind past both Commit and the explicit Rollback and leak it.
	done := false
	defer func() {
		if !done {
			_ = pgxTx.Rollback(context.Background())
		}
	}()

	if err := fn(tx); err != nil {
		rctx, rcancel := timeout(c.base(), c.o.CommandTimeout)
		rbErr := pgxTx.Rollback(rctx)
		rcancel()
		if rbErr != nil {
			return fmt.Errorf("database: rollback: %w", rbErr)
		}
		done = true
		return err
	}

	mctx, mcancel := timeout(c.base(), c.o.CommandTimeout)
	comErr := pgxTx.Commit(mctx)
	if comErr != nil {
		_ = pgxTx.Rollback(mctx)
	}
	mcancel()
	if comErr != nil {
		return fmt.Errorf("database: commit: %w", comErr)
	}
	done = true
	return nil
}

// ConnQuery maps every row into T on the dedicated connection. No retry.
func ConnQuery[T any](c *Conn, sql string, args ...any) ([]T, error) {
	return runQuery[T](&c.conn, sql, args...)
}

// ConnQueryRow runs a query expected to return at most one row on the dedicated
// connection. No retry.
func ConnQueryRow[T any](c *Conn, sql string, args ...any) (T, bool, error) {
	return runQueryRow[T](&c.conn, sql, args...)
}

// ConnScalar scans a single value on the dedicated connection. No retry.
func ConnScalar[T any](c *Conn, sql string, args ...any) (T, error) {
	return runScalar[T](&c.conn, sql, args...)
}

var (
	_ runner = (*pgx.Conn)(nil)
	_ runner = (*pgxpool.Conn)(nil)
)
