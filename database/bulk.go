package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// bulkCopier is the native PostgreSQL COPY support asserted on the underlying
// runner. Both *pgx.Conn and *pgxpool.Conn implement it natively (pgx.Tx too),
// so the capability check keeps working for pool-acquired and standalone
// connections without widening the shared runner interface used by unit-test
// fakes.
type bulkCopier interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// sliceRows adapts an in-memory [][]any slice into a pgx.CopyFromSource. Each
// element becomes one COPY row; it never errors — row shapes are validated by
// the server during the copy.
type sliceRows struct {
	rows [][]any
	i    int
}

func (s *sliceRows) Next() bool { return s.i < len(s.rows) }

func (s *sliceRows) Values() ([]any, error) {
	row := s.rows[s.i]
	s.i++
	return row, nil
}

func (*sliceRows) Err() error { return nil }

// BulkInsert loads every row into table using PostgreSQL's COPY ... FROM STDIN
// protocol — orders of magnitude faster than INSERT loops for large volumes.
// The table and each column name are validated as plain identifiers first;
// rows must be positional and match the columns order (exactly like raw pgx
// CopyFrom). Returns the number of rows copied. The context captured once at
// New/Connect is used internally, bounded by CommandTimeout.
//
// Only Conn exposes this by design: COPY is pinned to one connection, which is
// exactly what a dedicated Conn guarantees.
func (c *Conn) BulkInsert(table string, columns []string, rows [][]any) (int64, error) {
	if err := validateIdentifier(table); err != nil {
		return 0, err
	}
	if len(columns) == 0 {
		return 0, fmt.Errorf("database: BulkInsert requires at least one column")
	}
	for _, col := range columns {
		if err := validateIdentifier(col); err != nil {
			return 0, err
		}
	}

	copier, ok := c.r.(bulkCopier)
	if !ok {
		return 0, fmt.Errorf("database: this connection does not support COPY")
	}

	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	n, err := copier.CopyFrom(cctx, pgx.Identifier{table}, columns, &sliceRows{rows: rows})
	if err != nil {
		return 0, fmt.Errorf("database: copy into %q: %w", table, err)
	}
	return n, nil
}

// Compile-time capability wiring for the natively-supported handle types.
var (
	_ bulkCopier = (*pgx.Conn)(nil)
	_ bulkCopier = (*pgxpool.Conn)(nil)
)
