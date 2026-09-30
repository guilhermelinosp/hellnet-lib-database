package database

import (
	"context"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace"
)

func TestStatementName(t *testing.T) {
	cases := []struct {
		sql, op, table, span string
	}{
		{"INSERT INTO outbox_events (id) VALUES ($1)", "INSERT", "outbox_events", "db.insert"},
		{"\n  insert into public.orders (id) values ($1)", "INSERT", "public.orders", "db.insert"},
		{`INSERT INTO "order_status_history" (id) SELECT 1`, "INSERT", "order_status_history", "db.insert"},
		{"UPDATE items SET value=$1", "UPDATE", "items", "db.update"},
		{"DELETE FROM items WHERE id=$1", "DELETE", "items", "db.delete"},
		{"SELECT e.id FROM outbox_events e JOIN x ON 1=1", "SELECT", "outbox_events", "db.select"},
		{"SELECT * FROM users WHERE id=$1", "SELECT", "users", "db.select"},
		{"SELECT 1", "SELECT", "", "db.select"},
		{"begin", "BEGIN", "", "db.transaction"},
		{"commit", "COMMIT", "", "db.transaction"},
		{"rollback", "ROLLBACK", "", "db.transaction"},
		{"-- lead\n/* c */ SELECT * FROM t", "SELECT", "t", "db.select"},
		// A WITH statement is classified by the main statement that follows the CTEs.
		{"WITH a AS (SELECT 1) INSERT INTO t SELECT * FROM a", "INSERT", "t", "db.insert"},
		{"WITH o AS (INSERT INTO orders (id) VALUES ($1)), h AS (INSERT INTO hist (id) VALUES ($2)) INSERT INTO outbox_events (id) VALUES ($3)", "INSERT", "outbox_events", "db.insert"},
		{"WITH g AS (SELECT 1 WHERE EXISTS (SELECT 1 FROM x)) INSERT INTO outbox_events (id) SELECT $1 FROM g", "INSERT", "outbox_events", "db.insert"},
		{"WITH recent AS (SELECT id FROM orders) SELECT * FROM recent", "SELECT", "recent", "db.select"},
		{"WITH x AS (SELECT 'insert into y' AS s) UPDATE t SET a = 1", "UPDATE", "t", "db.update"},
		{"WITH a AS (SELECT 1)", "WITH", "", "db.query"},
		{"EXPLAIN SELECT 1", "EXPLAIN", "", "db.query"},
		{"", "", "", "db.query"},
		{"  ", "", "", "db.query"},
		{"-- only a comment", "", "", "db.query"},
	}
	for _, c := range cases {
		op, table := statementName(c.sql)
		if op != c.op || table != c.table {
			t.Errorf("statementName(%q) = (%q,%q), want (%q,%q)", c.sql, op, table, c.op, c.table)
		}
		if got := spanNameFor(op); got != c.span {
			t.Errorf("span name for %q = %q, want %q", c.sql, got, c.span)
		}
	}
}

func TestPGXTracerSkipsListenSessionControl(t *testing.T) {
	h := telemetry.NewHarness(t)
	tracer := &pgxTracer{obs: newObservability(h), options: Options{}}
	for _, sql := range []string{"LISTEN outbox_events", "UNLISTEN outbox_events", "unlisten *"} {
		ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: sql})
		tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	}
	if got := len(h.Spans()); got != 0 {
		t.Fatalf("LISTEN/UNLISTEN spans = %d, want 0", got)
	}
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	if got := len(h.Spans()); got != 1 {
		t.Fatalf("ordinary statements must still be traced, spans = %d", got)
	}
}

func TestAPISpansAreInternalSoStatementsCountOnceAsClient(t *testing.T) {
	h := telemetry.NewHarness(t)
	db := newTestDB(context.Background(), &fakeRunnerPool{})
	db.obs = newObservability(h)
	if _, err := db.ExecuteContext(context.Background(), "INSERT INTO t (a) VALUES ($1)", 1); err != nil {
		t.Fatal(err)
	}
	spans := h.SpansByName("db.execute")
	if len(spans) != 1 || spans[0].SpanKind() != trace.SpanKindInternal {
		t.Fatalf("db.execute spans = %v, want one Internal span (the driver span is the Client one)", spans)
	}
}
