package database

import (
	"context"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type contextHook struct{ before, after context.Context }

func (h *contextHook) BeforeHook(QueryInfo)                                {}
func (h *contextHook) AfterHook(QueryInfo)                                 {}
func (h *contextHook) BeforeQueryContext(ctx context.Context, _ QueryInfo) { h.before = ctx }
func (h *contextHook) AfterQueryContext(ctx context.Context, _ QueryInfo)  { h.after = ctx }

func TestExecuteContextUsesCallerSpan(t *testing.T) {
	h := telemetry.NewHarness(t)
	db := newTestDB(context.Background(), &fakeRunnerPool{})
	db.obs = newObservability(h)
	ctx, parent := h.TracerProvider().Tracer("caller").Start(context.Background(), "caller")
	parentID := parent.SpanContext().SpanID()
	if _, err := db.ExecuteContext(ctx, "UPDATE items SET value=$1", "secret"); err != nil {
		t.Fatal(err)
	}
	parent.End()
	spans := h.SpansByName("db.execute")
	if len(spans) != 1 || spans[0].Parent().SpanID() != parentID {
		t.Fatalf("spans = %v, want caller-parented db.execute", spans)
	}
}

func TestContextQueryHookReceivesCallerContext(t *testing.T) {
	hook := &contextHook{}
	db := newTestDB(context.Background(), &fakeRunnerPool{})
	db.hooks = newHookRegistry(Options{QueryHooks: []QueryHook{hook}})
	db.obs = newObservability(nil)
	ctx := context.WithValue(context.Background(), "request", "r-1")
	if _, err := db.ExecuteContext(ctx, "UPDATE items SET value=$1", "value"); err != nil {
		t.Fatal(err)
	}
	if hook.before == nil || hook.after == nil || hook.before.Value("request") != "r-1" || hook.after.Value("request") != "r-1" {
		t.Fatal("context hook did not receive the caller context")
	}
}

func TestPGXTracerOmitsArguments(t *testing.T) {
	h := telemetry.NewHarness(t)
	tracer := &pgxTracer{obs: newObservability(h), options: Options{}}
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT * FROM users WHERE id=$1", Args: []any{"secret"}})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	spans := h.SpansByName("SELECT users")
	if len(spans) != 1 {
		t.Fatalf("query spans = %d", len(spans))
	}
	for _, attr := range spans[0].Attributes() {
		if attr.Value.AsString() == "secret" {
			t.Fatal("query arguments leaked to span")
		}
	}
}

func TestPoolMetricsCallbackIsUnregisteredWithDBLifecycle(t *testing.T) {
	h := telemetry.NewHarness(t)
	cfg, err := pgxpool.ParseConfig("postgres://app:secret@postgres.internal:5432/orders")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.MaxConns = 7
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(pool.Close)

	registration := registerPoolMetrics(newObservability(h), pool)
	if registration == nil {
		t.Fatal("registerPoolMetrics() returned nil")
	}
	value, ok := h.GaugeValue(context.Background(), "db.client.connection.max")
	if !ok || value != 7 {
		t.Fatalf("pool callback max = %v (present=%v), want 7", value, ok)
	}
	db := &DB{pool: poolStatsAdapter{Pool: pool}, poolMetrics: registration}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
	if _, ok := h.GaugeValue(context.Background(), "db.client.connection.max"); ok {
		t.Fatal("pool metric callback still produced a datapoint after DB.Close")
	}
}
