//go:build integration

package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Integration tests run against a REAL PostgreSQL instance.
//
// Usage (cluster namespace tools, via kubectl port-forward):
//
//	export HELLNET_TEST_PG_HOST=localhost
//	export HELLNET_TEST_PG_PORT=5433
//	export HELLNET_TEST_PG_USER=postgres
//	export HELLNET_TEST_PG_PASSWORD=$(kubectl get secret postgres-credentials -n tools -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)
//	go test -tags integration -v -count=1 ./database/
//
// Defaults target localhost:5432/postgres as user postgres.

func integrationOptions(t *testing.T) Options {
	t.Helper()

	host := envOr("HELLNET_TEST_PG_HOST", "localhost")
	port := envOr("HELLNET_TEST_PG_PORT", "5432")
	user := envOr("HELLNET_TEST_PG_USER", "postgres")
	pass := os.Getenv("HELLNET_TEST_PG_PASSWORD")
	if pass == "" {
		t.Skip("HELLNET_TEST_PG_PASSWORD not set")
	}

	var portNum int
	if _, err := fmt.Sscanf(port, "%d", &portNum); err != nil {
		t.Fatalf("invalid port %q", port)
	}

	return Options{
		Host:              host,
		Port:              portNum,
		Database:          envOr("HELLNET_TEST_PG_NAME", "postgres"),
		Username:          user,
		Password:          pass,
		PoolMinSize:       1,
		PoolMaxSize:       5,
		CommandTimeout:    10 * time.Second,
		ConnectionTimeout: 5 * time.Second,
		RetryEnabled:      true,
		RetryMaxCount:     3,
		RetryBaseDelay:    50 * time.Millisecond,
	}
}

func openIntegrationDB(t *testing.T) *DB {
	t.Helper()
	// Context is captured once at construction and propagated internally.
	db, err := New(context.Background(), integrationOptions(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	return db
}

func resetOrdersTable(t *testing.T, db *DB) {
	t.Helper()
	for _, ddl := range []string{
		`DROP TABLE IF EXISTS orders`,
		`CREATE TABLE orders (
			id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			status   TEXT NOT NULL,
			total    NUMERIC(10,2) NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := db.Execute(ddl); err != nil {
			t.Fatalf("ddl %q: %v", ddl, err)
		}
	}
}

type order struct {
	ID        int64     `db:"id"`
	Status    string    `db:"status"`
	Total     float64   `db:"total"`
	CreatedAt time.Time `db:"created_at"`
}

func TestIntegrationCRUDAndTypedQueries(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	n, err := db.Execute(
		"INSERT INTO orders (status, total) VALUES ($1,$2), ($1,$3), ($4,$5)",
		"pending", 10.5, 20.0, "shipped", 99.9)
	if err != nil || n != 3 {
		t.Fatalf("insert: affected=%d err=%v", n, err)
	}

	pending, err := Query[order](db, "SELECT * FROM orders WHERE status = $1 ORDER BY id", "pending")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(pending) != 2 || pending[0].Status != "pending" || pending[0].Total != 10.5 {
		t.Fatalf("Query result wrong: %+v", pending)
	}

	got, found, err := QueryRow[order](db, "SELECT * FROM orders WHERE id = $1", pending[0].ID)
	if err != nil || !found || got.ID != pending[0].ID {
		t.Fatalf("QueryRow: got=%+v found=%v err=%v", got, found, err)
	}

	missing, found, err := QueryRow[order](db, "SELECT * FROM orders WHERE id = $1", -1)
	if err != nil {
		t.Fatalf("QueryRow(not found) returned error: %v", err)
	}
	if found {
		t.Fatal("QueryRow(not found): found = true")
	}
	var wantZero order
	if missing != wantZero {
		t.Fatalf("QueryRow(not found): expected zero value, got %+v", missing)
	}

	count, err := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if err != nil || count != 3 {
		t.Fatalf("Scalar: count=%d err=%v", count, err)
	}

	updated, err := db.Execute("UPDATE orders SET status = $1 WHERE status = $2", "done", "pending")
	if err != nil || updated != 2 {
		t.Fatalf("update: affected=%d err=%v", updated, err)
	}
}

// orderLite deliberately maps only part of the orders table: built-in
// repository queries select an explicit column list, so partial structs work
// (Dapper-like tolerance of unmapped columns).
type orderLite struct {
	ID     int64  `db:"id"`
	Status string `db:"status"`
}

func TestIntegrationRepositoryPartialStruct(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	if _, err := db.Execute(
		"INSERT INTO orders (status, total) VALUES ($1, $2), ($1, $3)",
		"pending", 10.5, 20.0); err != nil {
		t.Fatalf("insert: %v", err)
	}

	repo := NewRepositoryForTable[orderLite](db, "orders")
	if repo.Table() != "orders" {
		t.Fatalf("table = %q", repo.Table())
	}

	all, err := repo.GetAll()
	if err != nil || len(all) != 2 || all[0].Status != "pending" {
		t.Fatalf("GetAll: %+v err=%v", all, err)
	}

	got, found, err := repo.GetByID(all[0].ID)
	if err != nil || !found || got.ID != all[0].ID {
		t.Fatalf("GetByID: got=%+v found=%v err=%v", got, found, err)
	}

	spec := Specification{
		SQL:     "SELECT id, status FROM orders WHERE status = $1",
		Args:    []any{"pending"},
		OrderBy: "id",
	}
	page, err := repo.Paginate(spec, 1, 1)
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	if page.TotalCount != 2 || len(page.Items) != 1 || !page.HasNextPage() {
		t.Fatalf("Paginate result wrong: %+v", page)
	}

	count, err := repo.Count(spec)
	if err != nil || count != 2 {
		t.Fatalf("Count: %d err=%v", count, err)
	}
}

func TestIntegrationTransactionalCommit(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	err := db.Transactional(func(tx *Tx) error {
		for i := 1; i <= 2; i++ {
			if _, err := tx.Execute(
				"INSERT INTO orders (status, total) VALUES ($1, $2)", "tx", float64(i)); err != nil {
				return err
			}
		}
		count, err := TxScalar[int64](tx, "SELECT COUNT(*) FROM orders")
		if err != nil || count != 2 {
			t.Errorf("in-tx count = %d err = %v, want 2", count, err)
		}
		rows, err := TxQuery[order](tx, "SELECT * FROM orders ORDER BY id")
		if err != nil || len(rows) != 2 {
			t.Errorf("TxQuery: rows=%d err=%v", len(rows), err)
		}
		return nil // commit
	})
	if err != nil {
		t.Fatalf("Transactional: %v", err)
	}

	count, _ := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if count != 2 {
		t.Fatalf("post-commit count = %d, want 2", count)
	}
}

func TestIntegrationTransactionalRollback(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	boom := errors.New("boom")
	err := db.Transactional(func(tx *Tx) error {
		if _, err := tx.Execute(
			"INSERT INTO orders (status) VALUES ('rollback-me')"); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Transactional err = %v, want boom", err)
	}

	count, _ := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if count != 0 {
		t.Fatalf("post-rollback count = %d, want 0", count)
	}
}

func TestIntegrationUniqueViolationNotRetried(t *testing.T) {
	db := openIntegrationDB(t)

	// Dedicated table with a plain PK so explicit ids (and their duplicates)
	// are allowed — GENERATED ALWAYS AS IDENTITY would reject them.
	if _, err := db.Execute(
		"CREATE TABLE dup_test (id INT PRIMARY KEY, status TEXT)"); err != nil {
		t.Fatalf("create dup_test: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Execute("DROP TABLE IF EXISTS dup_test") })

	if _, err := db.Execute(
		"INSERT INTO dup_test (id, status) VALUES (7, 'first')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	start := time.Now()
	_, err := db.Execute("INSERT INTO dup_test (id, status) VALUES (7, 'dup')")
	elapsed := time.Since(start)

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("err = %v (%T), want pgconn.PgError 23505", err, err)
	}
	// With retry enabled (max 3, base 50ms) a wrong retry would cost ≥350ms.
	if elapsed > 300*time.Millisecond {
		t.Fatalf("unique violation took %s — it must NOT be retried", elapsed)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── I6: real-transient retry & slow-query logging ────────────────

// TestIntegrationTransientRetryReal proves db.Execute actually retries a REAL
// transient failure raised by PostgreSQL: an uncommitted transaction on a
// second standalone connection holds the row lock, so `FOR UPDATE NOWAIT`
// fails immediately with SQLSTATE 55006 ("could not obtain lock on row",
// class 55 — same lock-family as 55P03) which is NOT on the non-retryable
// list. The holder commits after 300ms; with backoff 50ms and max-count 6 the
// contended attempts fail fast while ~attempt 4 lands after release and
// succeeds, so the slow overall elapsed time is itself evidence that retries
// happened (a single shot would fail in ~10ms).
func TestIntegrationTransientRetryReal(t *testing.T) {
	opts := integrationOptions(t)
	opts.RetryEnabled = true
	opts.RetryMaxCount = 6
	opts.RetryBaseDelay = 50 * time.Millisecond

	ctx := context.Background()
	db, err := New(ctx, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	for _, ddl := range []string{
		`DROP TABLE IF EXISTS lock_retry_test`,
		`CREATE TABLE lock_retry_test (id INT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')`,
		`INSERT INTO lock_retry_test (id) VALUES (1)`,
	} {
		if _, err := db.Execute(ddl); err != nil {
			t.Fatalf("ddl %q: %v", ddl, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Execute("DROP TABLE IF EXISTS lock_retry_test") })

	// Second physical connection: hold the row lock in an open transaction.
	holder, err := Connect(ctx, opts)
	if err != nil {
		t.Fatalf("Connect(holder): %v", err)
	}
	t.Cleanup(func() { _ = holder.Close() })
	htx, err := holder.Begin()
	if err != nil {
		t.Fatalf("holder Begin: %v", err)
	}
	if _, err := htx.Execute("UPDATE lock_retry_test SET status = 'held' WHERE id = 1"); err != nil {
		htx.Rollback() //nolint:errcheck // best-effort cleanup
		t.Fatalf("holder row lock: %v", err)
	}

	released := make(chan struct{})
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = htx.Commit() // frees the row lock for subsequent retry attempts
		close(released)
	}()

	start := time.Now()
	_, err = db.Execute("SELECT status FROM lock_retry_test WHERE id = 1 FOR UPDATE NOWAIT")
	elapsed := time.Since(start)

	<-released // join the commit before cleanup closes the holder conn

	if err != nil {
		var pgErr *pgconn.PgError
		t.Fatalf("db.Execute with retries = %v (asPgError=%v) after %s, want success once the lock was released",
			err, errors.As(err, &pgErr), elapsed)
	}
	// A single non-retried attempt hits the lock error instantly (<~10ms).
	// Only genuine retrying pushes total elapsed beyond the 300ms release.
	if elapsed < 250*time.Millisecond {
		t.Fatalf("execute returned in %s — looks like NO retry happened for the row-lock error", elapsed)
	}
}

// capturingHandler collects warn records so tests can assert logging without
// touching global output configuration beyond slog.SetDefault.
type capturingHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r.Level == slog.LevelWarn {
		h.msgs = append(h.msgs, r.Message)
	}
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) countContaining(sub string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.msgs {
		if strings.Contains(m, sub) {
			n++
		}
	}
	return n
}

// TestIntegrationSlowQueryLogCapture pins the diagnostic contract: statements
// slower than Options.SlowQuery are logged at WARN level with the stable
// "database: slow query" message.
func TestIntegrationSlowQueryLogCapture(t *testing.T) {
	opts := integrationOptions(t)
	opts.SlowQuery = 1 * time.Millisecond

	capture := &capturingHandler{}
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(capture))
	t.Cleanup(func() { slog.SetDefault(prevDefault) })

	db, err := New(context.Background(), opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Execute("SELECT pg_sleep(0.05)"); err != nil {
		t.Fatalf("pg_sleep execute: %v", err)
	}

	if n := capture.countContaining("slow query"); n < 1 {
		t.Fatalf("got %d warn records containing %q (msgs=%v), want >= 1",
			n, "slow query", capture.msgs)
	}
}

// ── Dedicated connections: Acquire / Connect / Begin ─────────────

func TestIntegrationAcquireSingleConn(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	conn, err := db.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Execute(
		"INSERT INTO orders (status, total) VALUES ($1,$2)", "pending", 10.5); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, found, err := ConnQueryRow[order](conn, "SELECT * FROM orders WHERE status = $1", "pending")
	if err != nil || !found || got.Status != "pending" || got.Total != 10.5 {
		t.Fatalf("ConnQueryRow: got=%+v found=%v err=%v", got, found, err)
	}

	count, err := ConnScalar[int64](conn, "SELECT COUNT(*) FROM orders")
	if err != nil || count != 1 {
		t.Fatalf("ConnScalar: count=%d err=%v", count, err)
	}

	// Close must be safe to call more than once (idempotent).
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestIntegrationAcquireMultipleConns(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	const n = 3
	conns := make([]*Conn, n)
	for i := range conns {
		c, err := db.Acquire()
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		defer func() { _ = c.Close() }()
		conns[i] = c
	}

	// Each pinned connection can run independently; together they represent
	// explicit multi-connection usage on top of the pool.
	for i, c := range conns {
		if _, err := c.Execute(
			"INSERT INTO orders (status, total) VALUES ($1,$2)", fmt.Sprintf("c%d", i), float64(i)); err != nil {
			t.Fatalf("insert on conn %d: %v", i, err)
		}
	}
	count, err := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if err != nil || count != n {
		t.Fatalf("count=%d err=%v, want %d", count, err, n)
	}
}

func TestIntegrationConnectSingleConn(t *testing.T) {
	// Connect is construction-time: it captures the context once on the Conn.
	ctx := context.Background()
	conn, err := Connect(ctx, integrationOptions(t))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Execute(
		"INSERT INTO orders (status, total) VALUES ($1,$2)", "standalone", 1.0); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := ConnQuery[order](conn, "SELECT * FROM orders WHERE status = $1", "standalone")
	if err != nil || len(rows) != 1 {
		t.Fatalf("ConnQuery: rows=%v err=%v", rows, err)
	}
}

func TestIntegrationConnBeginCommitRollback(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	conn, err := db.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Commit path.
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Execute("INSERT INTO orders (status) VALUES ('committed')"); err != nil {
		t.Fatalf("tx insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Rollback path.
	tx, err = conn.Begin()
	if err != nil {
		t.Fatalf("Begin(2): %v", err)
	}
	if _, err := tx.Execute("INSERT INTO orders (status) VALUES ('rolledback')"); err != nil {
		t.Fatalf("tx insert(2): %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	count, err := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if err != nil || count != 1 {
		t.Fatalf("after commit/rollback count=%d err=%v, want 1", count, err)
	}
}

func TestIntegrationConnTransactional(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	conn, err := db.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Commits on success.
	if err := conn.Transactional(func(tx *Tx) error {
		if _, err := tx.Execute("INSERT INTO orders (status) VALUES ('ok')"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("Transactional(commit): %v", err)
	}

	// Rolls back on error.
	boom := errors.New("boom")
	if err := conn.Transactional(func(tx *Tx) error {
		if _, err := tx.Execute("INSERT INTO orders (status) VALUES ('bad')"); err != nil {
			return err
		}
		return boom
	}); err == nil {
		t.Fatal("Transactional(rollback): expected error")
	}

	count, err := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if err != nil || count != 1 {
		t.Fatalf("after tx count=%d err=%v, want 1", count, err)
	}
}

// TestIntegrationEnableMetrics é o smoke das métricas nativas contra um pool
// real: EnableMetrics + roundtrip Execute/Query + presença das famílias no
// registro isolado (e limpeza total pelo handle.Close).
//
// Setup padrão (namespace tools, via kubectl port-forward):
//
//	export HELLNET_TEST_PG_PORT=15432   # kubectl port-forward -n tools svc/postgres 15432:5432 &
//	export HELLNET_TEST_PG_PASSWORD=$(kubectl get secret postgres-credentials -n tools -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)
func TestIntegrationEnableMetrics(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	reg := prometheus.NewRegistry()
	handle, err := db.EnableMetrics(reg)
	if err != nil {
		t.Fatalf("EnableMetrics: %v", err)
	}
	t.Cleanup(handle.Close)

	if _, err := db.Execute(
		"INSERT INTO orders (status, total) VALUES ('metric', 42.5)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := Query[order](db, "SELECT id, status FROM orders WHERE status = $1", "metric")
	if err != nil || len(rows) != 1 {
		t.Fatalf("query rows=%d err=%v, want 1", len(rows), err)
	}

	// Transação real: prova que a Tx criada DEPOIS do EnableMetrics herda o
	// registro compartilhado e reporta commit em db_transactions_total.
	if err := db.Transactional(func(tx *Tx) error {
		_, err := tx.Execute("INSERT INTO orders (status) VALUES ('metric-tx')")
		return err
	}); err != nil {
		t.Fatalf("tx: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	families := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		families[mf.GetName()] = true
	}
	for _, want := range []string{
		"db_queries_total",
		"db_query_duration_seconds",
		"db_pool_acquires_total",
		"db_transactions_total",
		"db_rollback_compensations_total",
		"db_pool_in_use", "db_pool_idle", "db_pool_max",
	} {
		if !families[want] {
			t.Errorf("família %s ausente no registro após roundtrip real", want)
		}
	}

	mc := handle.Collector()
	if got := testutil.ToFloat64(mc.queriesTotal.WithLabelValues(OpExec, metricStatusOK)); got < 1 {
		t.Errorf("exec/ok = %v, want >=1 contra Postgres real", got)
	}
	if got := testutil.ToFloat64(mc.txTotal.WithLabelValues(txResultCommit)); got != 1 {
		t.Errorf("tx{commit} = %v, want 1 contra Postgres real", got)
	}
	if max := testutil.ToFloat64(mc.poolMax); max <= 0 {
		t.Errorf("db_pool_max = %v, want >0 amostrado do pool pgx real", max)
	}

	// Close deve desregistrar tudo — smoke do ciclo completo.
	handle.Close()
	mfs, err = reg.Gather()
	if err != nil {
		t.Fatalf("Gather pós-Close: %v", err)
	}
	if len(mfs) != 0 {
		names := make([]string, 0, len(mfs))
		for _, mf := range mfs {
			names = append(names, mf.GetName())
		}
		t.Errorf("pós-handle.Close restaram famílias: %v", names)
	}
}
