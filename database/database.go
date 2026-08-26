// Package database provides PostgreSQL-first database infrastructure for
// Hellnet Go services. Configuration is environment-first, modular and
// cloud-native.
//
//	env vars → Options → pgxpool.Pool → *DB / Repository[T]
//
// The package mirrors the Hellnet .NET library Hellnet.Database:
//
//   - Executor-style raw SQL (Execute, Query[T], QueryRow[T], Scalar[T])
//   - Automatic transactions with commit/rollback (Transactional)
//   - A generic Repository[T] with specification-based queries
//   - Typed results (Result[T], PageResult[T])
//   - Transient-failure retry with SQLSTATE discrimination
//
// There is no ORM: structs are mapped by name using `db:"column"` tags.
package database

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// envPrefix is the prefix of every HELLNET_DATABASE_* variable.
const envPrefix = "HELLNET_DATABASE_"

// Options configures the database connection. All fields are populated from
// environment variables (HELLNET_DATABASE_*) via LoadFromEnv, or set
// explicitly.
type Options struct {
	// ── Connection ──────────────────────────────────────────────
	Host     string
	Port     int
	Database string
	Username string
	Password string

	// ── Pool ────────────────────────────────────────────────────
	PoolMinSize int
	PoolMaxSize int

	// ── Timeouts ────────────────────────────────────────────────
	CommandTimeout    time.Duration
	ConnectionTimeout time.Duration

	// ── Resilience ──────────────────────────────────────────────
	RetryEnabled   bool
	RetryMaxCount  int
	RetryBaseDelay time.Duration

	// ── Diagnostics ─────────────────────────────────────────────
	SlowQuery time.Duration
}

// DefaultOptions returns the default configuration.
func DefaultOptions() Options {
	return Options{
		Host:              "localhost",
		Port:              5432,
		Database:          "",
		Username:          "",
		Password:          "",
		PoolMinSize:       10,
		PoolMaxSize:       100,
		CommandTimeout:    30 * time.Second,
		ConnectionTimeout: 15 * time.Second,
		RetryEnabled:      true,
		RetryMaxCount:     3,
		RetryBaseDelay:    100 * time.Millisecond,
		SlowQuery:         500 * time.Millisecond,
	}
}

// LoadFromEnv reads configuration from HELLNET_DATABASE_* environment
// variables, starting from DefaultOptions as the fallback for any unset value.
func LoadFromEnv() Options {
	o := DefaultOptions()

	o.Host = environments.GetString(envPrefix, "", "HOST", o.Host)
	o.Port = environments.GetInt(envPrefix, "", "PORT", o.Port)
	o.Database = environments.GetString(envPrefix, "", "NAME", o.Database)
	o.Username = environments.GetString(envPrefix, "", "USERNAME", o.Username)
	o.Password = environments.GetString(envPrefix, "", "PASSWORD", o.Password)

	o.PoolMinSize = environments.GetInt(envPrefix, "", "POOL_MIN_SIZE", o.PoolMinSize)
	o.PoolMaxSize = environments.GetInt(envPrefix, "", "POOL_MAX_SIZE", o.PoolMaxSize)

	o.CommandTimeout = time.Duration(
		environments.GetInt(envPrefix, "", "COMMAND_TIMEOUT_SECONDS", int(o.CommandTimeout/time.Second))) * time.Second
	o.ConnectionTimeout = time.Duration(
		environments.GetInt(envPrefix, "", "CONNECTION_TIMEOUT_SECONDS", int(o.ConnectionTimeout/time.Second))) * time.Second

	o.RetryEnabled = environments.GetBool(envPrefix, "", "RETRY_ENABLED", o.RetryEnabled)
	o.RetryMaxCount = environments.GetInt(envPrefix, "", "RETRY_MAX_COUNT", o.RetryMaxCount)
	o.RetryBaseDelay = time.Duration(
		environments.GetInt(envPrefix, "", "RETRY_BASE_DELAY_MS", int(o.RetryBaseDelay/time.Millisecond))) * time.Millisecond

	o.SlowQuery = time.Duration(
		environments.GetInt(envPrefix, "", "SLOW_QUERY_MS", int(o.SlowQuery/time.Millisecond))) * time.Millisecond

	return o
}

// Validate reports every missing or invalid option as a single error, mirroring
// the .NET DatabaseEnvBinder.Validate behavior.
func Validate(o Options) error {
	var missing []string

	if strings.TrimSpace(o.Database) == "" {
		missing = append(missing, envPrefix+"NAME")
	}
	if strings.TrimSpace(o.Username) == "" {
		missing = append(missing, envPrefix+"USERNAME")
	}
	if strings.TrimSpace(o.Password) == "" {
		missing = append(missing, envPrefix+"PASSWORD")
	}
	if o.PoolMaxSize <= 0 {
		missing = append(missing, envPrefix+"POOL_MAX_SIZE (must be > 0)")
	}

	if len(missing) > 0 {
		return fmt.Errorf("database: missing or invalid configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

// dsn builds the PostgreSQL connection URL from the individual fields.
// No full connection string is ever required in code.
func (o Options) dsn() string {
	u := url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", o.Host, o.Port),
		Path:   "/" + o.Database,
	}
	if o.Username != "" || o.Password != "" {
		u.User = url.UserPassword(o.Username, o.Password)
	}
	return u.String()
}

// Pool is the minimal surface of pgxpool.Pool used by this package.
// It exists so unit tests can supply a fake implementation.
type Pool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close()
}

// DB is the entry point of the library: a pooled PostgreSQL connection with
// executor methods, transactional support and retry semantics. Create it with
// New, MustNew or OpenFromEnv; it is safe for concurrent use.
//
// DB embeds conn (runner + options) so the typed core is shared with Tx; the
// pool field is kept for lifecycle operations (Begin/Ping/Close) outside the
// runner surface.
type DB struct {
	conn
	pool  Pool
	retry RetryPolicy
}

// withDefaults fills zero-valued fields of opts with DefaultOptions so a caller
// may pass a partial Options (e.g. the documented explicit-options example)
// without hitting validation or ending up with zero-duration timeouts.
func withDefaults(o Options) Options {
	d := DefaultOptions()
	if o.Host == "" {
		o.Host = d.Host
	}
	if o.Port == 0 {
		o.Port = d.Port
	}
	if o.Database == "" {
		o.Database = d.Database
	}
	if o.Username == "" {
		o.Username = d.Username
	}
	if o.Password == "" {
		o.Password = d.Password
	}
	if o.PoolMinSize == 0 {
		o.PoolMinSize = d.PoolMinSize
	}
	if o.PoolMaxSize == 0 {
		o.PoolMaxSize = d.PoolMaxSize
	}
	if o.CommandTimeout == 0 {
		o.CommandTimeout = d.CommandTimeout
	}
	if o.ConnectionTimeout == 0 {
		o.ConnectionTimeout = d.ConnectionTimeout
	}
	if o.RetryMaxCount == 0 {
		o.RetryMaxCount = d.RetryMaxCount
	}
	if o.RetryBaseDelay == 0 {
		o.RetryBaseDelay = d.RetryBaseDelay
	}
	defaultRetryEnabled(&o, d)
	if o.SlowQuery == 0 {
		o.SlowQuery = d.SlowQuery
	}
	return o
}

// defaultRetryEnabled enables retry unless the caller explicitly disabled it
// (signalled by leaving the other retry fields at their defaults too).
func defaultRetryEnabled(o *Options, d Options) {
	if !o.RetryEnabled && o.RetryMaxCount == d.RetryMaxCount && o.RetryBaseDelay == d.RetryBaseDelay {
		o.RetryEnabled = d.RetryEnabled
	}
}

// New creates a DB from explicit options. No connection is established yet;
// call Ping to verify connectivity.
func New(opts Options) (*DB, error) {
	opts = withDefaults(opts)
	if err := Validate(opts); err != nil {
		return nil, err
	}

	cfg, err := pgxpool.ParseConfig(opts.dsn())
	if err != nil {
		return nil, fmt.Errorf("database: parse config: %w", err)
	}
	// Clamp pool sizes to the range accepted by pgxpool (int32).
	minSize := min(max(opts.PoolMinSize, 0), math.MaxInt32)
	maxSize := max(opts.PoolMaxSize, minSize+1)
	cfg.MinConns = int32(minSize)
	cfg.MaxConns = int32(min(maxSize, math.MaxInt32))
	cfg.ConnConfig.ConnectTimeout = opts.ConnectionTimeout

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("database: create pool: %w", err)
	}

	return &DB{
		conn:  conn{r: pool, o: opts},
		pool:  pool,
		retry: NewRetryPolicy(opts.RetryEnabled, opts.RetryMaxCount, opts.RetryBaseDelay),
	}, nil
}

// MustNew is like New but panics on failure. Useful at service startup where a
// misconfiguration should fail fast.
func MustNew(opts Options) *DB {
	db, err := New(opts)
	if err != nil {
		panic(err)
	}
	return db
}

// OpenFromEnv loads options from HELLNET_DATABASE_* variables and connects.
// It is the equivalent of the .NET AddHellnetDatabase() env-first overload.
func OpenFromEnv() (*DB, error) {
	return New(LoadFromEnv())
}

// Close releases the underlying pool.
func (db *DB) Close() {
	db.pool.Close()
}

// Ping verifies database connectivity.
func (db *DB) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, db.o.CommandTimeout)
	defer cancel()
	return db.pool.Ping(ctx)
}

// Options returns the effective configuration (without the password).
func (db *DB) Options() Options {
	o := db.o
	o.Password = ""
	return o
}

// tableOf derives the table name from T, matching the .NET typeof(T).Name
// convention used by PostgresRepository<T> and ByIdSpecification.
func tableOf[T any]() string {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}

// ErrNoRows is re-exported so callers do not need to import pgx directly for
// the most common sentinel check.
var ErrNoRows = pgx.ErrNoRows
