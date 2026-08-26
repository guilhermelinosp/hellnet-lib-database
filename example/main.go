// Command example is a runnable validation of the hellnet-lib-database
// package. It mirrors the integration tests: connect, create a table,
// insert, typed query, repository pagination, and a transaction.
//
// Usage:
//
//	cp .env.example .env   # then fill HELLNET_DATABASE_PASSWORD
//	go run ./example
//
// The .env is read by hellnet-lib-environments (LoadDotEnv) when
// HELLNET_ENVIRONMENT is empty/local/dev.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-database/database"
	"github.com/guilhermelinosp/hellnet-lib-environments/environments"
)

// Order maps the demo table. Tags drive column mapping (created_at is
// intentionally included so a SELECT * would still need an explicit list).
type Order struct {
	ID        int64     `db:"id"`
	Status    string    `db:"status"`
	Total     float64   `db:"total"`
	CreatedAt time.Time `db:"created_at"`
}

func main() {
	_ = environments.LoadDotEnv() // no-op outside dev; logs nothing on failure

	db, err := database.OpenFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}
	fmt.Println("✅ connected")

	// Reset the demo table so the example is repeatable.
	for _, ddl := range []string{
		`DROP TABLE IF EXISTS orders`,
		`CREATE TABLE orders (
			id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			status    TEXT NOT NULL,
			total     NUMERIC(10,2) NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := db.Execute(ctx, ddl); err != nil {
			log.Fatalf("ddl %q: %v", ddl, err)
		}
	}

	// Insert (Execute returns affected rows).
	n, err := db.Execute(ctx,
		"INSERT INTO orders (status, total) VALUES ($1,$2), ($1,$3), ($4,$5)",
		"pending", 10.50, 20.00, "shipped", 99.90)
	if err != nil {
		log.Fatalf("insert: %v", err)
	}
	fmt.Printf("✅ inserted %d rows\n", n)

	// Typed query (Scalar).
	count, err := database.Scalar[int64](ctx, db, "SELECT COUNT(*) FROM orders")
	if err != nil {
		log.Fatalf("scalar: %v", err)
	}
	fmt.Printf("   total rows = %d\n", count)

	// Typed query (Query[T]) — explicit column list is required by the
	// mapping contract for raw queries.
	pending, err := database.Query[Order](ctx, db,
		"SELECT id, status, total, created_at FROM orders WHERE status = $1", "pending")
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	fmt.Printf("✅ pending orders: %d\n", len(pending))
	for _, o := range pending {
		fmt.Printf("   #%d %s $%.2f\n", o.ID, o.Status, o.Total)
	}

	// Repository with explicit table name + specification.
	repo := database.NewRepositoryForTable[Order](db, "orders")
	page, err := repo.Paginate(ctx, database.Specification{
		SQL:     "SELECT id, status, total, created_at FROM orders",
		OrderBy: "id",
	}, 1, 10)
	if err != nil {
		log.Fatalf("paginate: %v", err)
	}
	fmt.Printf("✅ repository page: total=%d items=%d hasNext=%v\n",
		page.TotalCount, len(page.Items), page.HasNextPage())

	// Transaction: commit on success.
	err = db.Transactional(ctx, func(ctx context.Context, tx *database.Tx) error {
		if _, err := tx.Execute(ctx,
			"UPDATE orders SET total = total + 1 WHERE status = $1", "pending"); err != nil {
			return err
		}
		// TxScalar reads the transaction's uncommitted state.
		sum, err := database.TxScalar[float64](ctx, tx,
			"SELECT COALESCE(SUM(total),0) FROM orders WHERE status = $1", "pending")
		if err != nil {
			return err
		}
		fmt.Printf("   in-tx pending sum = %.2f\n", sum)
		return nil
	})
	if err != nil {
		log.Fatalf("transaction: %v", err)
	}
	fmt.Println("✅ transaction committed")

	fmt.Println("🎉 example finished")
}
