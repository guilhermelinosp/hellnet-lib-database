package database

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// Specification is a lean query filter: raw SQL plus positional arguments,
// optionally ordered. It mirrors the .NET ISpecification<T> — deliberately no
// expression trees.
type Specification struct {
	SQL     string
	Args    []any
	OrderBy string
}

// Repository is the generic data-access helper for one table. It mirrors the
// .NET IRepository<T> (plain data access, not a DDD aggregate). The table name
// is derived from T's type name, matching the .NET typeof(T).Name convention.
//
// Structs should carry `db:"column"` tags so pgx maps columns correctly.
type Repository[T any] struct {
	table string
	db    *DB
}

// NewRepository creates a repository over table name(T), mirroring the
// .NET typeof(T).Name convention. When the Go type name differs from the
// table name (the common case: Order vs orders), use NewRepositoryForTable.
func NewRepository[T any](db *DB) *Repository[T] {
	return &Repository[T]{table: tableOf[T](), db: db}
}

// NewRepositoryForTable creates a repository bound to an explicit table name,
// decoupling the mapping from T's type name.
func NewRepositoryForTable[T any](db *DB, table string) *Repository[T] {
	return &Repository[T]{table: table, db: db}
}

// Table returns the table name used by this repository.
func (r *Repository[T]) Table() string { return r.table }

var structColumnsCache sync.Map // reflect.Type -> string (comma-joined quoted columns)

// columnsOf builds the quoted, comma-joined column list for T from its
// `db:"..."` tags (or snake_cased field names when the tag is absent).
//
// Built-in repository queries select an explicit column list instead of
// `SELECT *`: structs are allowed to cover only part of the underlying table,
// mirroring Dapper's tolerance of unmapped columns without depending on
// pgx-specific scanner semantics.
func columnsOf[T any]() string {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if cached, ok := structColumnsCache.Load(t); ok {
		return cached.(string)
	}

	var cols []string
	for i := range t.NumField() {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		tag, ok := sf.Tag.Lookup("db")
		if ok {
			tag, _, _ = strings.Cut(tag, ",")
			if tag == "-" {
				continue
			}
		}
		name := tag
		if name == "" {
			name = snakeCase(sf.Name)
		}
		cols = append(cols, strconv.Quote(name))
	}
	joined := strings.Join(cols, ", ")
	structColumnsCache.Store(t, joined)
	return joined
}

// snakeCase converts CamelCase identifiers to snake_case: CreatedAt →
// created_at, UserID → user_id.
func snakeCase(s string) string {
	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(runes) + 4)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 && (!unicode.IsUpper(runes[i-1]) || (i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
				b.WriteRune('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// identRe matches a safe SQL identifier (table or column name).
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// orderByRe matches a safe ORDER BY expression: column names, dots,
// commas and whitespace only.
var orderByRe = regexp.MustCompile(`^[A-Za-z0-9_.,\s]+$`)

// validateIdentifier rejects table names that are not plain identifiers,
// preventing SQL injection through NewRepositoryForTable / type-derived names.
func validateIdentifier(name string) error {
	if !identRe.MatchString(name) {
		return fmt.Errorf("database: invalid identifier %q", name)
	}
	return nil
}

// validateOrderBy rejects ORDER BY clauses containing injection tokens or
// characters outside the safe charset.
func validateOrderBy(s string) error {
	if s == "" {
		return nil
	}
	if strings.ContainsAny(s, ";") ||
		strings.Contains(s, "--") ||
		strings.Contains(s, "/*") ||
		strings.Contains(s, "*/") {
		return fmt.Errorf("database: invalid ORDER BY clause %q", s)
	}
	if !orderByRe.MatchString(s) {
		return fmt.Errorf("database: invalid ORDER BY clause %q", s)
	}
	return nil
}

// selectAllSQL builds "SELECT <columns> FROM <table> [WHERE id = $1]".
func selectAllSQL(table, cols string, byID bool) (string, error) {
	if err := validateIdentifier(table); err != nil {
		return "", err
	}
	sql := fmt.Sprintf("SELECT %s FROM %s", cols, table)
	if byID {
		sql += " WHERE id = $1"
	}
	return sql, nil
}

// countSQL wraps spec.SQL into a COUNT subquery.
func countSQL(spec Specification) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS _count", spec.SQL)
}

// orderByInSQLRe detects an ORDER BY clause already written into spec.SQL.
// \s+ between the words (SQL requires whitespace) and word boundaries avoid
// false positives on identifiers like order_by_id or ordering_flag.
var orderByInSQLRe = regexp.MustCompile(`(?i)\bORDER\s+BY\b`)

// pagingInSQLRe detects paging clauses already written into spec.SQL.
var pagingInSQLRe = regexp.MustCompile(`(?i)\b(LIMIT|OFFSET|FETCH)\b`)

// clauseCollision returns the offending SQL fragment when sql already carries
// the given kind of clause (an empty string means no collision).
func clauseCollision(sql string, re *regexp.Regexp) string {
	if loc := re.FindStringIndex(sql); loc != nil {
		return strings.TrimSpace(sql[loc[0]:loc[1]])
	}
	return ""
}

// pageSQL appends ORDER BY / LIMIT / OFFSET to spec.SQL. page is 1-based;
// pageSize <= 0 means no limit. The ORDER BY value is validated to prevent
// SQL injection through Specification.OrderBy. Spec SQL that already embeds
// the clauses this function would append is rejected up front with a
// descriptive error — blindly appending produced doubly-ordered or
// double-limited SQL that only failed later at the server.
func pageSQL(spec Specification, page, pageSize int) (string, error) {
	if err := validateOrderBy(spec.OrderBy); err != nil {
		return "", err
	}

	sql := spec.SQL

	// Guard only what would actually be appended: a spec carrying its own
	// ORDER BY/LIMIT and passing no OrderBy/pageSize keeps working untouched
	// (existing happy-path behavior stays byte-identical).
	if spec.OrderBy != "" {
		if frag := clauseCollision(sql, orderByInSQLRe); frag != "" {
			return "", fmt.Errorf(
				"database: specification SQL already contains %s; move OrderBy/Pagination into Specification fields", frag)
		}
		sql += " ORDER BY " + spec.OrderBy
	}
	if pageSize > 0 {
		if frag := clauseCollision(sql, pagingInSQLRe); frag != "" {
			return "", fmt.Errorf(
				"database: specification SQL already contains %s; move OrderBy/Pagination into Specification fields", frag)
		}
		offset := (page - 1) * pageSize
		if offset < 0 {
			offset = 0
		}
		sql += fmt.Sprintf(" LIMIT %d OFFSET %d", pageSize, offset)
	}
	return sql, nil
}

// GetByID fetches the row with the given primary key, or (zero, false, nil)
// when it does not exist. The context captured once at New is used internally.
func (r *Repository[T]) GetByID(id any) (T, bool, error) {
	var zero T
	sql, err := selectAllSQL(r.table, columnsOf[T](), true)
	if err != nil {
		return zero, false, err
	}
	return QueryRow[T](r.db, sql, id)
}

// GetAll fetches the mapped columns of every row in the table.
func (r *Repository[T]) GetAll() ([]T, error) {
	sql, err := selectAllSQL(r.table, columnsOf[T](), false)
	if err != nil {
		return nil, err
	}
	return Query[T](r.db, sql)
}

// Find executes the specification's query.
func (r *Repository[T]) Find(spec Specification) ([]T, error) {
	sql, err := pageSQL(spec, 1, 0)
	if err != nil {
		return nil, err
	}
	return Query[T](r.db, sql, spec.Args...)
}

// Paginate executes the specification returning one page (1-based) plus the
// total row count.
func (r *Repository[T]) Paginate(spec Specification, page, pageSize int) (PageResult[T], error) {
	total, err := Scalar[int64](r.db, countSQL(spec), spec.Args...)
	if err != nil {
		return PageResult[T]{}, err
	}

	sql, err := pageSQL(spec, page, pageSize)
	if err != nil {
		return PageResult[T]{}, err
	}

	items, err := Query[T](r.db, sql, spec.Args...)
	if err != nil {
		return PageResult[T]{}, err
	}

	return PageResult[T]{
		Items:      items,
		TotalCount: total,
		Page:       page,
		PageSize:   pageSize,
	}, nil
}

// Count counts the rows matching the specification.
func (r *Repository[T]) Count(spec Specification) (int64, error) {
	return Scalar[int64](r.db, countSQL(spec), spec.Args...)
}
