package database

import (
	"context"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-database/internal/obstest"
	"github.com/jackc/pgx/v5"
)

type contextHook struct{ before, after context.Context }

func (h *contextHook) BeforeHook(QueryInfo)                                {}
func (h *contextHook) AfterHook(QueryInfo)                                 {}
func (h *contextHook) BeforeQueryContext(ctx context.Context, _ QueryInfo) { h.before = ctx }
func (h *contextHook) AfterQueryContext(ctx context.Context, _ QueryInfo)  { h.after = ctx }

func TestExecuteContextUsesCallerSpan(t *testing.T) {
	h := obstest.New(t)
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
	h := obstest.New(t)
	tracer := &pgxTracer{obs: newObservability(h), options: Options{}}
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT * FROM users WHERE id=$1", Args: []any{"secret"}})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	spans := h.SpansByName("db.query")
	if len(spans) != 1 {
		t.Fatalf("query spans = %d", len(spans))
	}
	for _, attr := range spans[0].Attributes() {
		if attr.Value.AsString() == "secret" {
			t.Fatal("query arguments leaked to span")
		}
	}
}
