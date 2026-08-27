package database

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var fixedTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// newRepoDB builds a repository wired to a recording pool.
func newRepoDB(t *testing.T) (*Repository[repoRow], *recordingPool) {
	t.Helper()
	pool := newRecordingPool()
	db := newTestDB(context.Background(), pool)
	return NewRepositoryForTable[repoRow](db, "users"), pool
}

func TestFieldsOfOrderingAndFieldMap(t *testing.T) {
	fields := fieldsOf[repoRow]()
	want := []colField{
		{name: "id", index: 0},
		{name: "status", index: 1},
		{name: "user_name", index: 2},
		{name: "created_at", index: 5}, // raw struct slot: Skipped+hidden occupy 3–4
	}
	if len(fields) != len(want) {
		t.Fatalf("fieldsOf = %+v, want %+v", fields, want)
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Errorf("fieldsOf[%d] = %+v, want %+v", i, fields[i], want[i])
		}
	}

	m := fieldMapOf[repoRow]()
	// Positions refer to slots in fieldsOf's mapped-field vector — NOT raw
	// struct field indices (unexported/Skipped/hidden shift created_at to
	// struct index 5 while its value slot is 3).
	for name, wantPos := range map[string]int{"id": 0, "status": 1, "user_name": 2, "created_at": 3} {
		if got, ok := m[name]; !ok || got != wantPos {
			t.Errorf("fieldMapOf[%q] = %d,%v; want %d,true", name, got, ok, wantPos)
		}
	}

	// Non-struct types must degrade to "no mapped columns" instead of
	// panicking, so mis-typed repositories fail as ordinary validation
	// errors downstream.
	if fs := fieldsOf[int](); len(fs) != 0 {
		t.Errorf("fieldsOf[int] = %+v, want empty", fs)
	}

	// Refactored columnsOf stays byte-identical to its pre-refactor output.
	if got, want := columnsOf[repoRow](), `"id", "status", "user_name", "created_at"`; got != want {
		t.Errorf("columnsOf = %q, want %q (refactor drifted)", got, want)
	}
}

func TestEntityValuesPointerDeref(t *testing.T) {
	entity := repoRow{ID: 1, Status: "on", UserName: "ana", CreatedAt: fixedTime}

	vals, err := entityValues(entity)
	if err != nil {
		t.Fatalf("entityValues(struct) err=%v", err)
	}
	ptrVals, err := entityValues(&entity)
	if err != nil {
		t.Fatalf("entityValues(pointer) err=%v", err)
	}
	if len(vals) != 4 || len(ptrVals) != 4 {
		t.Fatalf("values lengths: struct=%d ptr=%d, want 4/4", len(vals), len(ptrVals))
	}
	for i := range vals {
		if vals[i] != ptrVals[i] {
			t.Errorf("values[%d] differ between struct and pointer forms: %v vs %v", i, vals[i], ptrVals[i])
		}
	}

	var nilPtr *repoRow
	if _, err := entityValues(nilPtr); err == nil ||
		!strings.Contains(err.Error(), "nil entity") {
		t.Errorf("entityValues(nil pointer) err=%v, want nil-entity validation error", err)
	}

	if _, err := entityValues(7); err == nil || !strings.Contains(err.Error(), "db-tagged") {
		t.Errorf("entityValues(int) err=%v, want db-tagged-columns error", err)
	}
}

func TestValidateColumnListAndResolveColumns(t *testing.T) {
	cases := []struct {
		name, kind string
		cols       []string
		wantErr    string
	}{
		{"empty conflict list", "conflict", nil, "requires at least one column"},
		{"empty update list", "update", []string{}, "requires at least one column"},
		{"bad identifier", "conflict", []string{"id; DROP TABLE x"}, "invalid identifier"},
		{"duplicate column", "set", []string{"status", "status"}, "duplicate"},
		{"unknown column", "set", []string{"wat"}, `not mapped by repoRow`},
		{"snake-cased unknown", "conflict", []string{"user_nm"}, `not mapped by repoRow`},
	}
	for _, tc := range cases {
		_, err := resolveColumns[repoRow](tc.kind, tc.cols)
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: resolveColumns(%v) err=%v, want containing %q", tc.name, tc.cols, err, tc.wantErr)
		}
	}

	idxs, err := resolveColumns[repoRow]("set", []string{"status", "id"})
	if err != nil || len(idxs) != 2 || idxs[0] != 1 || idxs[1] != 0 {
		t.Errorf("resolveColumns(valid) = %v, %v; want [1 0], nil", idxs, err)
	}
}

// expected Upsert SQL shapes derived straight from the builder contract:
// insert placeholders run $1..$n over T's declaration order; DO UPDATE SET
// assignments continue at $n+1 preserving the caller's updateCols order.
func TestUpsertSQLBuilding(t *testing.T) {
	entity := repoRow{ID: 7, Status: "active", UserName: "ana", CreatedAt: fixedTime}

	t.Run("single conflict single update", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.setCommandTag("UPDATE 1")

		n, err := repo.Upsert(entity, []string{"id"}, []string{"status"})
		if err != nil || n != 1 {
			t.Fatalf("Upsert = %d, %v; want 1, nil", n, err)
		}

		rec := pool.execCalls()[0]
		wantSQL := `INSERT INTO users ("id", "status", "user_name", "created_at") VALUES ($1, $2, $3, $4) ` +
			`ON CONFLICT ("id") DO UPDATE SET "status" = $5`
		if rec.SQL != wantSQL {
			t.Errorf("sql = %q\nwant    %q", rec.SQL, wantSQL)
		}
		wantArgs := []any{int64(7), "active", "ana", fixedTime, "active"}
		assertArgs(t, rec.Args, wantArgs)
	})

	t.Run("multi conflict multi update sequential placeholders", func(t *testing.T) {
		sql, args, err := upsertStatement("users", entity,
			[]string{"id", "user_name"}, []string{"status", "created_at"}, false)
		if err != nil {
			t.Fatalf("upsertStatement err=%v", err)
		}
		wantSQL := `INSERT INTO users ("id", "status", "user_name", "created_at") VALUES ($1, $2, $3, $4) ` +
			`ON CONFLICT ("id", "user_name") DO UPDATE SET "status" = $5, "created_at" = $6`
		if sql != wantSQL {
			t.Errorf("sql = %q\nwant    %q", sql, wantSQL)
		}
		wantArgs := []any{int64(7), "active", "ana", fixedTime, "active", fixedTime}
		assertArgs(t, args, wantArgs)
	})

	t.Run("do nothing variant ignores updates entirely", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.setCommandTag("UPDATE 0")

		n, err := repo.UpsertDoNothing(entity, []string{"user_name"})
		if err != nil || n != 0 {
			t.Fatalf("UpsertDoNothing = %d, %v; want 0, nil", n, err)
		}
		rec := pool.execCalls()[0]
		wantSQL := `INSERT INTO users ("id", "status", "user_name", "created_at") VALUES ($1, $2, $3, $4) ` +
			`ON CONFLICT ("user_name") DO NOTHING`
		if rec.SQL != wantSQL {
			t.Errorf("sql = %q\nwant    %q", rec.SQL, wantSQL)
		}
		if len(rec.Args) != 4 {
			t.Errorf("args = %v, want exactly the 4 insert values", rec.Args)
		}
	})

	t.Run("validation failures never reach Exec", func(t *testing.T) {
		repo, pool := newRepoDB(t)

		failCases := []struct {
			name               string
			conflicts, updates []string
			want               string
			useDoNothing       bool
		}{
			{"no conflicts", nil, []string{"status"}, "requires at least one column", false},
			{"injection in conflicts", []string{"id); DROP TABLE x"}, []string{"status"}, "invalid identifier", false},
			{"dup conflicts", []string{"id", "id"}, []string{"status"}, "duplicate", false},
			{"no updates", []string{"id"}, nil, "requires at least one column", false},
			{"unknown update col", []string{"id"}, []string{"wat"}, "not mapped by", false},
			{"unknown conflict col", []string{"zzz"}, []string{"status"}, "not mapped by", true},
			{"do-nothing bad ident", []string{"x y"}, nil, "invalid identifier", true},
		}
		for _, tc := range failCases {
			var err error
			if tc.useDoNothing {
				_, err = repo.UpsertDoNothing(entity, tc.conflicts)
			} else {
				_, err = repo.Upsert(entity, tc.conflicts, tc.updates)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: err=%v, want containing %q", tc.name, err, tc.want)
			}
		}
		if got := pool.execCount(); got != 0 {
			t.Errorf("Exec saw %d calls during validation-failure cases, want 0", got)
		}
	})
}

func assertArgs(t *testing.T, got, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("args = %#v (%d), want %#v (%d)", got, len(got), want, len(want))
	}
	for i := range want {
		if g, w := got[i], want[i]; g != w {
			t.Errorf("args[%d] = %#v, want %#v", i, g, w)
		}
	}
}

func TestUpdateSQLBuilding(t *testing.T) {
	entity := repoRow{ID: 7, Status: "archived", UserName: "skip-me", CreatedAt: fixedTime}
	whereSpec := Specification{SQL: "age > $1 AND status <> $2", Args: []any{18, "pending"}}

	t.Run("statement shape and argument order", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.setCommandTag("UPDATE 3")

		n, err := repo.Update(entity, []string{"status", "created_at"}, whereSpec)
		if err != nil || n != 3 {
			t.Fatalf("Update = %d, %v; want 3, nil", n, err)
		}

		rec := pool.execCalls()[0]
		wantSQL := `UPDATE users SET "status" = $3, "created_at" = $4 WHERE (age > $1 AND status <> $2)`
		if rec.SQL != wantSQL {
			t.Errorf("sql = %q\nwant    %q", rec.SQL, wantSQL)
		}
		// WHERE args first (keeping their $1.. numbering), THEN set values.
		wantArgs := []any{18, "pending", "archived", fixedTime}
		assertArgs(t, rec.Args, wantArgs)
	})

	t.Run("empty spec args shift set placeholders from $1", func(t *testing.T) {
		sql, args, err := updateStatement("users", entity, []string{"status"},
			Specification{SQL: "id = 5"})
		if err != nil {
			t.Fatalf("updateStatement err=%v", err)
		}
		wantSQL := `UPDATE users SET "status" = $1 WHERE (id = 5)`
		if sql != wantSQL {
			t.Errorf("sql = %q, want %q", sql, wantSQL)
		}
		assertArgs(t, args, []any{"archived"})
	})

	t.Run("guards reject misuse before Exec", func(t *testing.T) {
		repo, pool := newRepoDB(t)

		guardCases := []struct {
			name    string
			setCols []string
			spec    Specification
			wantErr string
		}{
			{"empty setCols", nil, Specification{SQL: "true"}, "requires at least one column"},
			{"invalid ident", []string{"a b c"}, Specification{SQL: "true"}, "invalid identifier"},
			{"unknown col", []string{"nope"}, Specification{SQL: "true"}, "not mapped by"},
			{"order-by unsupported", []string{"status"},
				Specification{SQL: "true", OrderBy: "id DESC"}, "does not support ORDER BY"},
			{"pagination unsupported", []string{"status"},
				Specification{SQL: "id = 1 LIMIT 1"}, "remove pagination"},
		}
		for _, tc := range guardCases {
			_, err := repo.Update(entity, tc.setCols, tc.spec)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("%s: err=%v, want containing %q", tc.name, err, tc.wantErr)
			}
		}
		if got := pool.execCount(); got != 0 {
			t.Errorf("Exec saw %d calls, want 0", got)
		}
	})
}

func TestFindOneGuardsAndBehavior(t *testing.T) {
	spec := Specification{SQL: "SELECT * FROM orders WHERE tenant = $1", Args: []any{7}, OrderBy: "created_at DESC"}

	t.Run("found row mapped through typed core", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.enqueueRows([]string{"id", "status"}, []any{int64(9), "ok"})

		row, found, err := repo.FindOne(spec)
		if err != nil || !found {
			t.Fatalf("FindOne = _,%v,%v; want found", found, err)
		}
		if row.ID != 9 || row.Status != "ok" {
			t.Errorf("row = %+v, want {ID:9 Status:ok}", row)
		}

		rec := pool.queries()[0]
		wantSQL := "SELECT * FROM orders WHERE tenant = $1 ORDER BY created_at DESC LIMIT 1"
		if rec.SQL != wantSQL {
			t.Errorf("sql = %q, want %q", rec.SQL, wantSQL)
		}
		assertArgs(t, rec.Args, []any{7})
	})

	t.Run("not found is zero,false,nil", func(t *testing.T) {
		repo, _ := newRepoDB(t)

		row, found, err := repo.FindOne(spec)
		if err != nil || found || row.ID != 0 || row.Status != "" {
			t.Errorf("FindOne = %+v,%v,%v; want zero,false,nil", row, found, err)
		}
	})

	t.Run("pagination embedded in spec SQL rejected", func(t *testing.T) {
		badSpecs := []string{
			"SELECT * FROM orders LIMIT 10",
			"SELECT * FROM orders offset 20",
			"SELECT * FROM orders FETCH FIRST 5 ROWS ONLY",
		}
		for _, s := range badSpecs {
			_, err := findOneSQL(Specification{SQL: s, OrderBy: "id"})
			if err == nil || !strings.Contains(err.Error(), "FindOne adds LIMIT 1 internally") {
				t.Errorf("findOneSQL(%q) err=%v, want pagination rejection", s, err)
			}
		}
	})

	t.Run("double ordering collision rejected", func(t *testing.T) {
		_, err := findOneSQL(Specification{SQL: "SELECT 1 FROM t ORDER BY x", OrderBy: "y DESC"})
		if err == nil || !strings.Contains(err.Error(), "already contains") {
			t.Errorf("findOneSQL double-order err=%v, want clause-collision error", err)
		}
	})

	t.Run("no order by still appends limit", func(t *testing.T) {
		sql, err := findOneSQL(Specification{SQL: "SELECT 1 FROM t"})
		if err != nil || sql != "SELECT 1 FROM t LIMIT 1" {
			t.Errorf("findOneSQL = %q, %v", sql, err)
		}
	})

	t.Run("order by injection validated", func(t *testing.T) {
		_, err := findOneSQL(Specification{SQL: "SELECT 1", OrderBy: "x; DROP TABLE t"})
		if err == nil || !strings.Contains(err.Error(), "invalid ORDER BY") {
			t.Errorf("findOneSQL injection err=%v", err)
		}
	})
}

func TestExistsWrappingAndBehavior(t *testing.T) {
	spec := Specification{SQL: "SELECT 1 FROM users WHERE email = $1", Args: []any{[]byte("a@b.c")}}

	t.Run("wrapped as EXISTS probe", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.enqueueScalar(true)

		ok, err := repo.Exists(spec)
		if err != nil || !ok {
			t.Fatalf("Exists = %v, %v; want true,nil", ok, err)
		}

		rec := pool.queries()[0]
		wantSQL := "SELECT EXISTS(SELECT 1 FROM (SELECT 1 FROM users WHERE email = $1) AS _exists)"
		if rec.SQL != wantSQL {
			t.Errorf("sql = %q\nwant    %q", rec.SQL, wantSQL)
		}
	})

	t.Run("false handled as plain boolean", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.enqueueScalar(false)

		ok, err := repo.Exists(spec)
		if err != nil || ok {
			t.Errorf("Exists = %v, %v; want false,nil", ok, err)
		}
	})

	t.Run("query errors propagate", func(t *testing.T) {
		repo, pool := newRepoDB(t)
		pool.enqueueScalarError(errors.New("connection refused"))

		if _, err := repo.Exists(spec); err == nil || !strings.Contains(err.Error(), "refused") {
			t.Errorf("Exists err=%v, want propagated failure", err)
		}
	})

	t.Run("rejections before execution", func(t *testing.T) {
		if _, err := existsSQL(Specification{SQL: "SELECT 1", OrderBy: "id"}); err == nil ||
			!strings.Contains(err.Error(), "Exists ignores Specification.OrderBy") {
			t.Errorf("OrderBy err=%v, want explicit rejection", err)
		}
		if _, err := existsSQL(Specification{SQL: "SELECT 1 LIMIT 5"}); err == nil ||
			!strings.Contains(err.Error(), "remove pagination") {
			t.Errorf("embedded pagination err=%v, want rejection", err)
		}
	})
}

func TestEnsureTableValidationAndExecution(t *testing.T) {
	pool := newRecordingPool()
	db := newTestDB(context.Background(), pool)

	ddl := "CREATE TABLE IF NOT EXISTS users (id BIGSERIAL PRIMARY KEY)"
	if err := EnsureTable(db, "users", ddl); err != nil {
		t.Fatalf("EnsureTable ok-case err=%v", err)
	}
	recs := pool.execCalls()
	if len(recs) != 1 || recs[0].SQL != ddl {
		t.Errorf("execs = %+v, want single passthrough ddl call", recs)
	}

	errCases := []struct {
		name, dbName, ddl string
		want              string
	}{
		{"nil db", "users", ddl, "non-nil DB"},
		{"injection table", "users; DROP TABLE x", ddl, "invalid identifier"},
		{"wrong prefix", "users", "ALTER TABLE IF NOT EXISTS users ADD c int", "must start with CREATE TABLE"},
		{"missing if-not-exists", "users", "CREATE TABLE users (id int)", "IF NOT EXISTS"},
	}
	for i, tc := range errCases {
		callerDb := db
		if i == 0 { // rotate the nil-db case through the guard directly
			callerDb = nil
		}
		err := EnsureTable(callerDb, tc.dbName, tc.ddl)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err=%v, want containing %q", tc.name, err, tc.want)
		}
	}
	if got := pool.execCount(); got != 1 {
		t.Errorf("failed EnsureTable calls must not execute; exec count = %d", got)
	}
}

func TestQuotedListAndPlaceholderRange(t *testing.T) {
	if got, want := quotedList([]string{"a", "b_c"}), `"a", "b_c"`; got != want {
		t.Errorf("quotedList = %q, want %q", got, want)
	}
	if got, want := placeholderRange(1, 3), "$1, $2, $3"; got != want {
		t.Errorf("placeholderRange(1,3) = %q, want %q", got, want)
	}
	if got, want := placeholderRange(5, 1), "$5"; got != want {
		t.Errorf("placeholderRange(5,1) = %q, want %q", got, want)
	}
	if got := placeholderRange(2, 0); got != "" {
		t.Errorf("placeholderRange(2,0) = %q, want empty", got)
	}
}
