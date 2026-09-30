package database

import (
	"context"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
)

func setOfflineEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_HOST", "127.0.0.1")
	t.Setenv("DATABASE_PORT", "1")
	t.Setenv("DATABASE_NAME", "db")
	t.Setenv("DATABASE_USERNAME", "user")
	t.Setenv("DATABASE_PASSWORD", "secret")
}

func TestNewIsEnvFirstWithInstrumentation(t *testing.T) {
	setOfflineEnv(t)
	h := telemetry.NewHarness(t)
	db, err := New(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if db.conn.obs.inst != h {
		t.Fatal("New must use the supplied instrumentation")
	}
	if db.conn.o.Database != "db" {
		t.Fatalf("Database = %q, want value loaded from the environment", db.conn.o.Database)
	}
}

func TestNewAcceptsNilInstrumentation(t *testing.T) {
	setOfflineEnv(t)
	db, err := New(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
}

func TestNewReportsMissingConfiguration(t *testing.T) {
	for _, k := range []string{"DATABASE_NAME", "DATABASE_USERNAME", "DATABASE_PASSWORD"} {
		t.Setenv(k, "")
	}
	if _, err := New(context.Background(), nil); err == nil {
		t.Fatal("New must fail when required DATABASE_* variables are missing")
	}
}
