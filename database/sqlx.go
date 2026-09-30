package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
)

// SQLX is the conventional SQL surface of the library. It uses sqlx with the
// pgx stdlib driver, so callers get database/sql compatibility and sqlx's
// struct mapping while the existing DB type remains available for pgx-native
// features such as COPY, batching and LISTEN/NOTIFY.
//
// SQLX owns its database/sql pool and must be closed by the caller. Use DB for
// the existing pgx pool; the two surfaces are intentionally explicit so a
// service does not accidentally mix transaction types.
type SQLX struct {
	*sqlx.DB
	opts Options
}

// NewSQLX opens a sqlx-backed PostgreSQL pool from explicit options, or from
// DATABASE_* when no options are supplied. It verifies connectivity before
// returning, matching the fail-fast behavior of Connect.
func NewSQLX(ctx context.Context, opts ...Options) (*SQLX, error) { //nolint:contextcheck // TODO(telemetry-fase-D): legacy constructor context.
	if ctx == nil {
		ctx = context.Background()
	}

	o := LoadFromEnv()
	if len(opts) > 0 {
		o = opts[0]
	}
	o = withDefaults(o)
	if err := Validate(o); err != nil {
		return nil, err
	}

	cfg, err := pgx.ParseConfig(o.dsn())
	if err != nil {
		return nil, fmt.Errorf("database: parse sqlx config: %w", err)
	}
	cfg.ConnectTimeout = o.ConnectionTimeout
	inst := o.instrumentation
	if inst == nil {
		inst = instrument.Noop()
	}
	// stdlib.OpenDB uses this pgx config for every connection, so SQLX gets the
	// same driver-level spans and privacy rules as the pgx-native DB surface.
	cfg.Tracer = &pgxTracer{obs: newObservability(inst), options: o}

	sqlDB := stdlib.OpenDB(*cfg)
	xdb := sqlx.NewDb(sqlDB, "pgx")
	xdb.SetMaxOpenConns(o.PoolMaxSize)
	xdb.SetMaxIdleConns(o.PoolMinSize)

	pingCtx, cancel := timeout(ctx, o.ConnectionTimeout)
	err = xdb.PingContext(pingCtx)
	cancel()
	if err != nil {
		if closeErr := xdb.Close(); closeErr != nil {
			return nil, fmt.Errorf("database: ping sqlx pool: %w (close: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("database: ping sqlx pool: %w", err)
	}

	return &SQLX{DB: xdb, opts: o}, nil
}

// OpenSQLXFromEnv opens the sqlx pool using DATABASE_* and .env settings.
func OpenSQLXFromEnv(ctx context.Context) (*SQLX, error) {
	return NewSQLX(ctx)
}

// Options returns the effective SQLX configuration without the password.
func (db *SQLX) Options() Options {
	if db == nil {
		return Options{}
	}
	o := db.opts
	o.Password = ""
	return o
}

// BeginTx starts a sqlx transaction with the caller's context and options.
// It is declared explicitly to make the sqlx transaction boundary visible
// beside DB.Transactional, whose callback uses pgx.Tx.
func (db *SQLX) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) { //nolint:contextcheck // TODO(telemetry-fase-D): legacy context wrapper.
	if ctx == nil {
		ctx = context.Background()
	}
	return db.BeginTxx(ctx, opts)
}
