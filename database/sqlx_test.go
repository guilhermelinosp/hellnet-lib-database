package database

import (
	"context"
	"strings"
	"testing"
)

func TestNewSQLXRejectsInvalidOptionsBeforeOpening(t *testing.T) {
	_, err := NewSQLX(context.Background(), Options{})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_NAME") {
		t.Fatalf("NewSQLX() error = %v, want validation error mentioning DATABASE_NAME", err)
	}
}

func TestSQLXOptionsRedactsPassword(t *testing.T) {
	db := &SQLX{opts: Options{Database: "orders", Password: "secret"}}
	if got := db.Options(); got.Password != "" {
		t.Fatalf("SQLX.Options() exposed password %q", got.Password)
	}
}
