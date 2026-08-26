package database

import (
	"errors"
	"testing"
	"time"
)

func TestSQLBuilders(t *testing.T) {
	if got := selectAllSQL("orders", `"id", "status"`, true); got != `SELECT "id", "status" FROM orders WHERE id = $1` {
		t.Errorf("selectAllSQL(byID) = %q", got)
	}
	if got := selectAllSQL("orders", `"id"`, false); got != `SELECT "id" FROM orders` {
		t.Errorf("selectAllSQL(all) = %q", got)
	}

	spec := Specification{SQL: "SELECT * FROM orders WHERE status = $1"}

	if got, want := countSQL(spec), "SELECT COUNT(*) FROM (SELECT * FROM orders WHERE status = $1) AS _count"; got != want {
		t.Errorf("countSQL = %q, want %q", got, want)
	}

	if got, want := pageSQL(spec, 1, 0), spec.SQL; got != want {
		t.Errorf("pageSQL(no paging) = %q, want %q", got, want)
	}

	if got, want := pageSQL(spec, 2, 20), spec.SQL+" LIMIT 20 OFFSET 20"; got != want {
		t.Errorf("pageSQL(paged) = %q, want %q", got, want)
	}

	ordered := Specification{SQL: "SELECT * FROM products", OrderBy: "name DESC"}
	if got, want := pageSQL(ordered, 1, 10),
		"SELECT * FROM products ORDER BY name DESC LIMIT 10 OFFSET 0"; got != want {
		t.Errorf("pageSQL(ordered) = %q, want %q", got, want)
	}
}

type repoRow struct {
	ID        int64  `db:"id"`
	Status    string `db:"status"`
	UserName  string // no tag → snake_case
	Skipped   string `db:"-"`
	hidden    string //nolint:unused // unexported must be ignored
	CreatedAt time.Time
}

func TestColumnsOf(t *testing.T) {
	got := columnsOf[repoRow]()
	want := `"id", "status", "user_name", "created_at"`
	if got != want {
		t.Errorf("columnsOf = %s, want %s", got, want)
	}
}

func TestSnakeCase(t *testing.T) {
	tests := map[string]string{
		"ID":        "id",
		"UserID":    "user_id",
		"CreatedAt": "created_at",
		"status":    "status",
		"HTTPCode":  "http_code",
	}
	for in, want := range tests {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPageResultHasNextPage(t *testing.T) {
	page := PageResult[int]{
		Items:      []int{1, 2},
		TotalCount: 100,
		Page:       1,
		PageSize:   20,
	}
	if !page.HasNextPage() {
		t.Error("page 1 of 5 should have a next page")
	}

	last := PageResult[int]{TotalCount: 40, Page: 2, PageSize: 20}
	if last.HasNextPage() {
		t.Error("last page should not have a next page")
	}

	exactBoundary := PageResult[int]{TotalCount: 60, Page: 3, PageSize: 20}
	if exactBoundary.HasNextPage() {
		t.Error("exact boundary should not have a next page")
	}
}

func TestResult(t *testing.T) {
	ok := Success("data", 1500*time.Millisecond)
	if !ok.IsSuccess() || ok.Data != "data" || ok.Duration != 1500*time.Millisecond {
		t.Errorf("Success wrong: %+v", ok)
	}

	boom := errors.New("boom")
	bad := Failure[string](boom, time.Second)
	if bad.IsSuccess() || !errors.Is(bad.Err, boom) {
		t.Errorf("Failure wrong: %+v", bad)
	}
	var zero string
	if bad.Data != zero {
		t.Errorf("Failure Data should be zero value, got %q", bad.Data)
	}
}
