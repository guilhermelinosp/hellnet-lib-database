package database

import (
	"testing"
	"time"
)

func TestDefaultOptions(t *testing.T) {
	o := DefaultOptions()

	if o.Host != "localhost" {
		t.Errorf("Host = %q, want localhost", o.Host)
	}
	if o.Port != 5432 {
		t.Errorf("Port = %d, want 5432", o.Port)
	}
	if o.PoolMinSize != 10 || o.PoolMaxSize != 100 {
		t.Errorf("pool sizes = %d/%d, want 10/100", o.PoolMinSize, o.PoolMaxSize)
	}
	if o.CommandTimeout != 30*time.Second {
		t.Errorf("CommandTimeout = %s, want 30s", o.CommandTimeout)
	}
	if o.ConnectionTimeout != 15*time.Second {
		t.Errorf("ConnectionTimeout = %s, want 15s", o.ConnectionTimeout)
	}
	if !o.RetryEnabled || o.RetryMaxCount != 3 || o.RetryBaseDelay != 100*time.Millisecond {
		t.Errorf("retry defaults wrong: %+v", o)
	}
	if o.SlowQuery != 500*time.Millisecond {
		t.Errorf("SlowQuery = %s, want 500ms", o.SlowQuery)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("HELLNET_DATABASE_HOST", "pg.internal")
	t.Setenv("HELLNET_DATABASE_PORT", "6543")
	t.Setenv("HELLNET_DATABASE_NAME", "orders")
	t.Setenv("HELLNET_DATABASE_USERNAME", "app")
	t.Setenv("HELLNET_DATABASE_PASSWORD", "secret")
	t.Setenv("HELLNET_DATABASE_POOL_MAX_SIZE", "42")
	t.Setenv("HELLNET_DATABASE_RETRY_ENABLED", "false")
	t.Setenv("HELLNET_DATABASE_RETRY_BASE_DELAY_MS", "250")
	t.Setenv("HELLNET_DATABASE_COMMAND_TIMEOUT_SECONDS", "7")

	o := LoadFromEnv()

	if o.Host != "pg.internal" || o.Port != 6543 {
		t.Errorf("endpoint = %s:%d, want pg.internal:6543", o.Host, o.Port)
	}
	if o.Database != "orders" || o.Username != "app" || o.Password != "secret" {
		t.Errorf("credentials not loaded: %+v", o)
	}
	if o.PoolMaxSize != 42 {
		t.Errorf("PoolMaxSize = %d, want 42", o.PoolMaxSize)
	}
	if o.RetryEnabled {
		t.Error("RetryEnabled = true, want false")
	}
	if o.RetryBaseDelay != 250*time.Millisecond {
		t.Errorf("RetryBaseDelay = %s, want 250ms", o.RetryBaseDelay)
	}
	if o.CommandTimeout != 7*time.Second {
		t.Errorf("CommandTimeout = %s, want 7s", o.CommandTimeout)
	}
}

func TestValidate(t *testing.T) {
	valid := Options{Database: "db", Username: "u", Password: "p", PoolMaxSize: 10}
	if err := Validate(valid); err != nil {
		t.Errorf("Validate(valid) = %v, want nil", err)
	}

	err := Validate(Options{})
	if err == nil {
		t.Fatal("Validate(empty) = nil, want error")
	}
	for _, want := range []string{"NAME", "USERNAME", "PASSWORD"} {
		if !contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}

	err = Validate(Options{Database: "db", Username: "u", Password: "p", PoolMaxSize: 0})
	if err == nil || !contains(err.Error(), "POOL_MAX_SIZE") {
		t.Errorf("Validate(pool=0) = %v, want POOL_MAX_SIZE error", err)
	}
}

func TestDSN(t *testing.T) {
	got := Options{
		Host: "pg.internal", Port: 5433,
		Database: "orders", Username: "app", Password: "s3c ret",
	}.dsn()

	want := "postgres://app:s3c%20ret@pg.internal:5433/orders"
	if got != want {
		t.Errorf("dsn = %q, want %q", got, want)
	}
}

func TestTableOf(t *testing.T) {
	type Order struct{}
	if got := tableOf[Order](); got != "Order" {
		t.Errorf("tableOf = %q, want Order", got)
	}
	if got := tableOf[*Order](); got != "Order" {
		t.Errorf("tableOf(ptr) = %q, want Order", got)
	}
}

func TestWithDefaults(t *testing.T) {
	o := withDefaults(Options{})
	if o.PoolMaxSize != 100 || o.CommandTimeout != 30*time.Second || o.PoolMinSize != 10 || !o.RetryEnabled {
		t.Errorf("empty Options not defaulted: %+v", o)
	}

	// Explicit values are preserved.
	o = withDefaults(Options{PoolMaxSize: 5, CommandTimeout: time.Second})
	if o.PoolMaxSize != 5 || o.CommandTimeout != time.Second || o.PoolMinSize != 10 {
		t.Errorf("override not preserved: %+v", o)
	}

	// Explicit retry disable with custom counts is respected.
	o = withDefaults(Options{RetryEnabled: false, RetryMaxCount: 5})
	if o.RetryEnabled {
		t.Error("explicit RetryEnabled=false was overridden by defaults")
	}
}

// contains is a tiny helper avoiding strings import noise in tests.
func contains(s, sub string) bool {
	return len(sub) == 0 || len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
