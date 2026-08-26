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

// pageSQL appends ORDER BY / LIMIT / OFFSET to spec.SQL. page is 1-based;
// pageSize <= 0 means no limit. The ORDER BY value is validated to prevent
// SQL injection through Specification.OrderBy.
func pageSQL(spec Specification, page, pageSize int) (string, error) {
	if err := validateOrderBy(spec.OrderBy); err != nil {
		return "", err
	}
	sql := spec.SQL
	if spec.OrderBy != "" {
		sql += " ORDER BY " + spec.OrderBy
	}
	if pageSize > 0 {
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
