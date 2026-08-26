package database

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Order is an example row. The `db` tags map Go fields to PostgreSQL columns.
type Order struct {
	ID        int64     `db:"id"`
	Status    string    `db:"status"`
	Total     float64   `db:"total"`
	CreatedAt time.Time `db:"created_at"`
}

// ExampleOpenFromEnv connects using HELLNET_DATABASE_* variables.
func ExampleOpenFromEnv() {
	// Context is captured once at construction and propagated internally.
	db, err := OpenFromEnv(context.Background())
	if err != nil {
		slog.Error("configuration error", "err", err)
		return
	}
	defer func() { _ = db.Close() }()

	count, err := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if err != nil {
		slog.Error("query failed", "err", err)
		return
	}
	fmt.Println(count)
}

// ExampleQuery runs raw SQL mapped into typed rows.
func ExampleQuery() {
	var _ = func(db *DB) error { //nolint:staticcheck // documentation example
		pending, err := Query[Order](db,
			"SELECT * FROM orders WHERE status = $1", "pending")
		if err != nil {
			return err
		}
		fmt.Println(len(pending))
		return nil
	}
	return
}

// ExampleDB_Transactional moves money between accounts atomically; any error
// rolls the whole unit of work back.
func ExampleDB_Transactional() {
	var _ = func(db *DB) error { //nolint:staticcheck // documentation example
		return db.Transactional(func(tx *Tx) error {
			if _, err := tx.Execute(
				"UPDATE accounts SET balance = balance - 100 WHERE id = $1", 1); err != nil {
				return err
			}
			if _, err := tx.Execute(
				"UPDATE accounts SET balance = balance + 100 WHERE id = $1", 2); err != nil {
				return err
			}
			total, err := TxScalar[int64](tx, "SELECT SUM(balance) FROM accounts")
			if err != nil {
				return err
			}
			fmt.Println(total)
			return nil // commit
		})
	}
	return
}

// ExampleRepository uses the generic repository with a specification.
func ExampleRepository() {
	var _ = func(db *DB) error { //nolint:staticcheck // documentation example
		orders := NewRepository[Order](db)

		order, found, err := orders.GetByID(42)
		if err != nil || !found {
			return err
		}
		fmt.Println(order.Status)

		spec := Specification{
			SQL:     "SELECT * FROM orders WHERE status = $1",
			Args:    []any{"pending"},
			OrderBy: "created_at DESC",
		}
		page, err := orders.Paginate(spec, 1, 20)
		if err != nil {
			return err
		}
		fmt.Println(page.TotalCount, page.HasNextPage())
		return nil
	}
	return
}
