package database

import "testing"

func TestStatementName(t *testing.T) {
	cases := []struct {
		sql, op, table, span string
	}{
		{"INSERT INTO outbox_events (id) VALUES ($1)", "INSERT", "outbox_events", "INSERT outbox_events"},
		{"\n  insert into public.orders (id) values ($1)", "INSERT", "public.orders", "INSERT public.orders"},
		{`INSERT INTO "order_status_history" (id) SELECT 1`, "INSERT", "order_status_history", "INSERT order_status_history"},
		{"UPDATE items SET value=$1", "UPDATE", "items", "UPDATE items"},
		{"DELETE FROM items WHERE id=$1", "DELETE", "items", "DELETE items"},
		{"SELECT e.id FROM outbox_events e JOIN x ON 1=1", "SELECT", "outbox_events", "SELECT outbox_events"},
		{"SELECT * FROM users WHERE id=$1", "SELECT", "users", "SELECT users"},
		{"SELECT 1", "SELECT", "", "SELECT"},
		{"begin", "BEGIN", "", "BEGIN"},
		{"commit", "COMMIT", "", "COMMIT"},
		{"-- lead\n/* c */ SELECT * FROM t", "SELECT", "t", "SELECT t"},
		{"WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a", "WITH", "", "WITH"},
		{"", "", "", "db.query"},
		{"  ", "", "", "db.query"},
		{"-- only a comment", "", "", "db.query"},
	}
	for _, c := range cases {
		op, table := statementName(c.sql)
		if op != c.op || table != c.table {
			t.Errorf("statementName(%q) = (%q,%q), want (%q,%q)", c.sql, op, table, c.op, c.table)
		}
		if got := spanNameFor(op, table); got != c.span {
			t.Errorf("span name for %q = %q, want %q", c.sql, got, c.span)
		}
	}
}
