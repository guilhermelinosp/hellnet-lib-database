package database

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSQLBuilders(t *testing.T) {
	sql, err := selectAllSQL("orders", `"id", "status"`, true)
	if err != nil || sql != `SELECT "id", "status" FROM orders WHERE id = $1` {
		t.Errorf("selectAllSQL(byID) = %q err=%v", sql, err)
	}
	sql, err = selectAllSQL("orders", `"id"`, false)
	if err != nil || sql != `SELECT "id" FROM orders` {
		t.Errorf("selectAllSQL(all) = %q err=%v", sql, err)
	}
	if _, err := selectAllSQL("orders; DROP TABLE x", `"id"`, false); err == nil {
		t.Error("selectAllSQL: expected error for invalid table")
	}
}

func TestValidateOrderBy(t *testing.T) {
	valid := []string{"", "created_at", "created_at DESC", "a.b, c ASC", "status"}
	for _, v := range valid {
		if err := validateOrderBy(v); err != nil {
			t.Errorf("validateOrderBy(%q) unexpected err: %v", v, err)
		}
	}
	invalid := []string{"; DROP TABLE users", "a -- comment", "a; b", "a/*x*/b", "a'b", "a\"b"}
	for _, v := range invalid {
		if err := validateOrderBy(v); err == nil {
			t.Errorf("validateOrderBy(%q) expected error", v)
		}
	}
}

func TestCountAndPageSQL(t *testing.T) {
	spec := Specification{SQL: "SELECT * FROM orders WHERE status = $1"}

	if got, want := countSQL(spec), "SELECT COUNT(*) FROM (SELECT * FROM orders WHERE status = $1) AS _count"; got != want {
		t.Errorf("countSQL = %q, want %q", got, want)
	}

	sql, err := pageSQL(spec, 1, 0)
	if err != nil || sql != spec.SQL {
		t.Errorf("pageSQL(no paging) = %q err=%v, want %q", sql, err, spec.SQL)
	}

	sql, err = pageSQL(spec, 2, 20)
	if err != nil || sql != spec.SQL+" LIMIT 20 OFFSET 20" {
		t.Errorf("pageSQL(paged) = %q err=%v", sql, err)
	}

	ordered := Specification{SQL: "SELECT * FROM products", OrderBy: "name DESC"}
	sql, err = pageSQL(ordered, 1, 10)
	if err != nil || sql != "SELECT * FROM products ORDER BY name DESC LIMIT 10 OFFSET 0" {
		t.Errorf("pageSQL(ordered) = %q err=%v", sql, err)
	}
}

// TestPageSQLRejectsClauseCollision pins the I5 guard: spec SQL that already
// carries ORDER BY / LIMIT / OFFSET / FETCH must fail fast with a descriptive
// error instead of producing doubly-ordered or double-limited SQL. The guard
// is case-insensitive and only fires when the colliding clause would actually
// be appended (specs that fully own their SQL keep working byte-identically).
func TestPageSQLRejectsClauseCollision(t *testing.T) {
	collisionCases := []struct {
		name, sql string
		paged     bool // true → collision surfaces with OrderBy empty too
	}{
		{"upper order by", "SELECT * FROM orders ORDER BY id", false},
		{"lowercase order by", "SELECT * FROM orders order by created_at", false},
		{"newline-separated order by", "SELECT * FROM orders\nORDER\nBY id", false},
		{"lowercase limit", "SELECT * FROM orders WHERE status = $1 limit 10", true},
		{"upper limit", "SELECT * FROM orders LIMIT 25", true},
		{"offset lowercase", "SELECT * FROM events offset 20", true},
		{"fetch first", "SELECT * FROM events FETCH FIRST 10 ROWS ONLY", true},
	}
	for _, tc := range collisionCases {
		spec := Specification{SQL: tc.sql}
		if !tc.paged {
			spec.OrderBy = "id DESC"
		}

		_, err := pageSQL(spec, 1, 10)
		if err == nil {
			t.Errorf("%s: pageSQL accepted %q without error", tc.name, tc.sql)
			continue
		}
		if !strings.Contains(err.Error(), "already contains") ||
			!strings.Contains(err.Error(), "move OrderBy/Pagination into Specification fields") {
			t.Errorf("%s: err = %v, want descriptive clause-collision guidance", tc.name, err)
		}

		// Same spec untouched by ordering/paging goes through unchanged.
		passThrough, err := pageSQL(Specification{SQL: tc.sql}, 1, 0)
		if err != nil || passThrough != tc.sql {
			t.Errorf("%s: pass-through pageSQL = %q err=%v, want the original SQL", tc.name, passThrough, err)
		}
	}

	// No false positives on identifiers merely containing clause words.
	spec := Specification{SQL: "SELECT limit_flag, offset_amount, ordering_hint FROM t"}
	got, err := pageSQL(spec, 1, 5)
	if err != nil || got != spec.SQL+" LIMIT 5 OFFSET 0" {
		t.Errorf("pageSQL(identifier lookalikes) = %q err=%v — word-boundary guard overmatched", got, err)
	}

	// A spec embedding its own ORDER BY but paginating via fields stays valid:
	// only LIMIT/OFFSET/FETCH would be appended to it.
	ownedOrder := Specification{SQL: "SELECT * FROM orders ORDER BY id"}
	got, err = pageSQL(ownedOrder, 2, 3)
	if err != nil || got != ownedOrder.SQL+" LIMIT 3 OFFSET 3" {
		t.Errorf("pageSQL(embedded own order) = %q err=%v", got, err)
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
