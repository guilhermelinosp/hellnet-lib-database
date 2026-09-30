package database

import (
	"context"
	"strings"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/jackc/pgx/v5"
)

func TestNewSQLXRejectsInvalidOptionsBeforeOpening(t *testing.T) {
	_, err := NewSQLX(context.Background(), Options{})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_NAME") {
		t.Fatalf("NewSQLX() error = %v, want validation error mentioning DATABASE_NAME", err)
	}
}

func TestSQLXPGXConfigWiresTracer(t *testing.T) {
	h := telemetry.NewHarness(t)
	cfg, err := sqlxPGXConfig(Options{
		Host:            "postgres.internal",
		Port:            5432,
		Database:        "orders",
		Username:        "app",
		Password:        "secret",
		instrumentation: h,
	})
	if err != nil {
		t.Fatalf("sqlxPGXConfig: %v", err)
	}
	tracer, ok := cfg.Tracer.(*pgxTracer)
	if !ok {
		t.Fatalf("sqlx pgx tracer = %T, want *pgxTracer", cfg.Tracer)
	}

	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
	if spans := h.SpansByName("db.select"); len(spans) != 1 {
		t.Fatalf("SQLX driver spans = %d, want 1", len(spans))
	}
}

func TestSQLXOptionsRedactsPassword(t *testing.T) {
	db := &SQLX{opts: Options{Database: "orders", Password: "secret"}}
	if got := db.Options(); got.Password != "" {
		t.Fatalf("SQLX.Options() exposed password %q", got.Password)
	}
}
