//go:build integration

package database

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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
	db, err := New(integrationOptions(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(db.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	return db
}

func resetOrdersTable(t *testing.T, db *DB) {
	t.Helper()
	ctx := context.Background()
	for _, ddl := range []string{
		`DROP TABLE IF EXISTS orders`,
		`CREATE TABLE orders (
			id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			status   TEXT NOT NULL,
			total    NUMERIC(10,2) NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := db.Execute(ctx, ddl); err != nil {
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
	ctx := context.Background()

	n, err := db.Execute(ctx,
		"INSERT INTO orders (status, total) VALUES ($1,$2), ($1,$3), ($4,$5)",
		"pending", 10.5, 20.0, "shipped", 99.9)
	if err != nil || n != 3 {
		t.Fatalf("insert: affected=%d err=%v", n, err)
	}

	pending, err := Query[order](ctx, db, "SELECT * FROM orders WHERE status = $1 ORDER BY id", "pending")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(pending) != 2 || pending[0].Status != "pending" || pending[0].Total != 10.5 {
		t.Fatalf("Query result wrong: %+v", pending)
	}

	got, found, err := QueryRow[order](ctx, db, "SELECT * FROM orders WHERE id = $1", pending[0].ID)
	if err != nil || !found || got.ID != pending[0].ID {
		t.Fatalf("QueryRow: got=%+v found=%v err=%v", got, found, err)
	}

	missing, found, err := QueryRow[order](ctx, db, "SELECT * FROM orders WHERE id = $1", -1)
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

	count, err := Scalar[int64](ctx, db, "SELECT COUNT(*) FROM orders")
	if err != nil || count != 3 {
		t.Fatalf("Scalar: count=%d err=%v", count, err)
	}

	updated, err := db.Execute(ctx, "UPDATE orders SET status = $1 WHERE status = $2", "done", "pending")
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
	ctx := context.Background()

	if _, err := db.Execute(ctx,
		"INSERT INTO orders (status, total) VALUES ($1, $2), ($1, $3)",
		"pending", 10.5, 20.0); err != nil {
		t.Fatalf("insert: %v", err)
	}

	repo := NewRepositoryForTable[orderLite](db, "orders")
	if repo.Table() != "orders" {
		t.Fatalf("table = %q", repo.Table())
	}

	all, err := repo.GetAll(ctx)
	if err != nil || len(all) != 2 || all[0].Status != "pending" {
		t.Fatalf("GetAll: %+v err=%v", all, err)
	}

	got, found, err := repo.GetByID(ctx, all[0].ID)
	if err != nil || !found || got.ID != all[0].ID {
		t.Fatalf("GetByID: got=%+v found=%v err=%v", got, found, err)
	}

	spec := Specification{
		SQL:     "SELECT id, status FROM orders WHERE status = $1",
		Args:    []any{"pending"},
		OrderBy: "id",
	}
	page, err := repo.Paginate(ctx, spec, 1, 1)
	if err != nil {
		t.Fatalf("Paginate: %v", err)
	}
	if page.TotalCount != 2 || len(page.Items) != 1 || !page.HasNextPage() {
		t.Fatalf("Paginate result wrong: %+v", page)
	}

	count, err := repo.Count(ctx, spec)
	if err != nil || count != 2 {
		t.Fatalf("Count: %d err=%v", count, err)
	}
}

func TestIntegrationTransactionalCommit(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)
	ctx := context.Background()

	err := db.Transactional(ctx, func(ctx context.Context, tx *Tx) error {
		for i := 1; i <= 2; i++ {
			if _, err := tx.Execute(ctx,
				"INSERT INTO orders (status, total) VALUES ($1, $2)", "tx", float64(i)); err != nil {
				return err
			}
		}
		count, err := TxScalar[int64](ctx, tx, "SELECT COUNT(*) FROM orders")
		if err != nil || count != 2 {
			t.Errorf("in-tx count = %d err = %v, want 2", count, err)
		}
		rows, err := TxQuery[order](ctx, tx, "SELECT * FROM orders ORDER BY id")
		if err != nil || len(rows) != 2 {
			t.Errorf("TxQuery: rows=%d err=%v", len(rows), err)
		}
		return nil // commit
	})
	if err != nil {
		t.Fatalf("Transactional: %v", err)
	}

	count, _ := Scalar[int64](ctx, db, "SELECT COUNT(*) FROM orders")
	if count != 2 {
		t.Fatalf("post-commit count = %d, want 2", count)
	}
}

func TestIntegrationTransactionalRollback(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)
	ctx := context.Background()

	boom := errors.New("boom")
	err := db.Transactional(ctx, func(ctx context.Context, tx *Tx) error {
		if _, err := tx.Execute(ctx,
			"INSERT INTO orders (status) VALUES ('rollback-me')"); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Transactional err = %v, want boom", err)
	}

	count, _ := Scalar[int64](ctx, db, "SELECT COUNT(*) FROM orders")
	if count != 0 {
		t.Fatalf("post-rollback count = %d, want 0", count)
	}
}

func TestIntegrationUniqueViolationNotRetried(t *testing.T) {
	db := openIntegrationDB(t)
	ctx := context.Background()

	// Dedicated table with a plain PK so explicit ids (and their duplicates)
	// are allowed — GENERATED ALWAYS AS IDENTITY would reject them.
	if _, err := db.Execute(ctx,
		"CREATE TABLE dup_test (id INT PRIMARY KEY, status TEXT)"); err != nil {
		t.Fatalf("create dup_test: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Execute(ctx, "DROP TABLE IF EXISTS dup_test") })

	if _, err := db.Execute(ctx,
		"INSERT INTO dup_test (id, status) VALUES (7, 'first')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	start := time.Now()
	_, err := db.Execute(ctx, "INSERT INTO dup_test (id, status) VALUES (7, 'dup')")
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
