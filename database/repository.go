package database

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
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

// colField pairs a mapped column name with the index of the struct field it
// was derived from, in declaration order.
type colField struct {
	name  string
	index int
}

// elemType resolves T to its underlying struct type, dereferencing one
// pointer level when T is a pointer type.
func elemType[T any]() reflect.Type {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

var structFieldsCache sync.Map // reflect.Type -> []colField

// fieldsOf maps T's exported fields to column names (`db:"..."` tags or
// snake_cased field names when the tag is absent), preserving declaration
// order and skipping unexported fields and fields tagged `db:"-"`. It is the
// single reflection source shared by columnsOf (SELECT lists),
// fieldMapOf (validation lookups) and entity value extraction for
// Upsert/Update so every statement sees exactly the same mapping.
func fieldsOf[T any]() []colField {
	t := elemType[T]()
	if cached, ok := structFieldsCache.Load(t); ok {
		return cached.([]colField)
	}
	if t.Kind() != reflect.Struct {
		structFieldsCache.Store(t, []colField(nil))
		return nil
	}

	fields := make([]colField, 0, t.NumField())
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
		fields = append(fields, colField{name: name, index: i})
	}
	structFieldsCache.Store(t, fields)
	return fields
}

// fieldMapOf indexes T's mapped column names to their POSITIONS within
// fieldsOf's ordered output — exactly the indexing entityValues uses when it
// materializes an argument vector. Upsert/Update therefore resolve requested
// columns onto consistent value slots. First-class validations consult this
// to reject unknown columns up front instead of failing at the server.
func fieldMapOf[T any]() map[string]int {
	return positionalFieldMap(fieldsOf[T]())
}

// positionalFieldMap builds a column-name → positional-slot lookup from an
// ordered colField slice.
func positionalFieldMap(fields []colField) map[string]int {
	m := make(map[string]int, len(fields))
	for i, f := range fields {
		m[f.name] = i
	}
	return m
}

// columnsOf builds the quoted, comma-joined column list for T from its
// `db:"..."` tags (or snake_cased field names when the tag is absent).
//
// Built-in repository queries select an explicit column list instead of
// `SELECT *`: structs are allowed to cover only part of the underlying table,
// mirroring Dapper's tolerance of unmapped columns without depending on
// pgx-specific scanner semantics.
func columnsOf[T any]() string {
	t := elemType[T]()
	if cached, ok := structColumnsCache.Load(t); ok {
		return cached.(string)
	}

	cols := make([]string, 0, len(fieldsOf[T]()))
	for _, f := range fieldsOf[T]() {
		cols = append(cols, strconv.Quote(f.name))
	}
	joined := strings.Join(cols, ", ")
	structColumnsCache.Store(t, joined)
	return joined
}

// entityValues extracts the ordered values of T's mapped fields from entity,
// transparently dereferencing a pointer entity. The returned slice aligns
// positionally with fieldsOf[T], so Upsert/Update can pick subsets by index.
func entityValues[T any](entity T) ([]any, error) {
	v := reflect.ValueOf(entity)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("database: nil entity of type %s", v.Type())
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("database: type %T has no db-tagged columns", entity)
	}
	fields := fieldsOf[T]()
	vals := make([]any, len(fields))
	for i, f := range fields {
		vals[i] = v.Field(f.index).Interface()
	}
	return vals, nil
}

// validateColumnList rejects empty / non-identifier / duplicated column
// name lists before any SQL is assembled.
func validateColumnList(kind string, cols []string) error {
	if len(cols) == 0 {
		return fmt.Errorf("database: %s requires at least one column", kind)
	}
	seen := make(map[string]bool, len(cols))
	for _, c := range cols {
		if err := validateIdentifier(c); err != nil {
			return fmt.Errorf("database: invalid %s column: %w", kind, err)
		}
		if seen[c] {
			return fmt.Errorf("database: duplicate %s column %q", kind, c)
		}
		seen[c] = true
	}
	return nil
}

// resolveColumns validates cols against T's field mapping and returns the
// aligned POSITIONAL slots into the value vector produced by entityValues —
// not raw struct field indices.
func resolveColumns[T any](kind string, cols []string) ([]int, error) {
	if err := validateColumnList(kind, cols); err != nil {
		return nil, err
	}
	m := fieldMapOf[T]()
	idxs := make([]int, len(cols))
	for i, c := range cols {
		idx, ok := m[c]
		if !ok {
			return nil, fmt.Errorf(
				"database: %s column %q is not mapped by %s; known columns: %q",
				kind, c, elemType[T]().Name(), fieldNames(m))
		}
		idxs[i] = idx
	}
	return idxs, nil
}

// fieldNames returns a sorted list of a column→index mapping's keys for
// error messages (sorted keeps failures deterministic).
func fieldNames(m map[string]int) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// quotedList joins already validated identifier names into a quoted SQL list.
func quotedList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = strconv.Quote(n)
	}
	return strings.Join(quoted, ", ")
}

// placeholderRange renders "$a, $a+1, …, $b" for sequential placeholders.
func placeholderRange(from, count int) string {
	ph := make([]string, count)
	for i := range ph {
		ph[i] = "$" + strconv.Itoa(from+i)
	}
	return strings.Join(ph, ", ")
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

// findOneSQL validates spec for the single-row path and appends LIMIT 1:
// ORDER BY is honored (validated and collision-guarded like pageSQL), while
// pagination clauses already present in spec.SQL are rejected — FindOne owns
// its own LIMIT and a spec that self-limits would silently truncate the
// intended "first row" semantics in ways the caller cannot see.
func findOneSQL(spec Specification) (string, error) {
	if err := validateOrderBy(spec.OrderBy); err != nil {
		return "", err
	}
	if frag := clauseCollision(spec.SQL, pagingInSQLRe); frag != "" {
		return "", fmt.Errorf(
			"database: specification SQL already contains %s; FindOne adds LIMIT 1 internally", frag)
	}
	sql := spec.SQL
	if spec.OrderBy != "" {
		if frag := clauseCollision(sql, orderByInSQLRe); frag != "" {
			return "", fmt.Errorf(
				"database: specification SQL already contains %s; move OrderBy into Specification fields", frag)
		}
		sql += " ORDER BY " + spec.OrderBy
	}
	return sql + " LIMIT 1", nil
}

// existsSQL wraps spec.SQL as an EXISTS probe over an aliased subquery
// (spec.SQL may itself be a SELECT-returning statement, so plain
// `SELECT EXISTS(<sql>)` is not safe). Ordering is meaningless inside EXISTS:
// Specification.OrderBy is rejected instead of being silently ignored.
func existsSQL(spec Specification) (string, error) {
	if spec.OrderBy != "" {
		return "", fmt.Errorf(
			"database: Exists ignores Specification.OrderBy %q; drop it or embed ordering in the specification SQL", spec.OrderBy)
	}
	if frag := clauseCollision(spec.SQL, pagingInSQLRe); frag != "" {
		// Legal at the server (an inner SELECT may paginate) but almost
		// always a bug here: "does any row match" must not depend on which
		// page happens to be scanned first.
		return "", fmt.Errorf(
			"database: specification SQL contains %s; remove pagination from Exists specifications", frag)
	}
	return fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM (%s) AS _exists)", spec.SQL), nil
}

// FindOne runs the specification expecting at most one row, appending
// LIMIT 1 internally (ORDER BY is honored). Returns (zero, false, nil) when
// nothing matches — mirroring GetByID's not-found contract.
func (r *Repository[T]) FindOne(spec Specification) (T, bool, error) {
	var zero T
	sql, err := findOneSQL(spec)
	if err != nil {
		return zero, false, err
	}
	return QueryRow[T](r.db, sql, spec.Args...)
}

// Exists reports whether any row matches the specification, using a single
// EXISTS(...) round-trip instead of counting.
func (r *Repository[T]) Exists(spec Specification) (bool, error) {
	sql, err := existsSQL(spec)
	if err != nil {
		return false, err
	}
	return Scalar[bool](r.db, sql, spec.Args...)
}

// upsertStatement assembles the INSERT … ON CONFLICT statement plus the full
// positional argument list. Insert values occupy $1..$n across every mapped
// column of T; conflict columns form the ON CONFLICT target; update columns
// become DO UPDATE SET assignments re-using values at $n+1..$n+m (sequential
// placeholders, no renumbering hazards). With doNothing set, updateCols is
// ignored and the statement ends with DO NOTHING.
func upsertStatement[T any](table string, entity T, conflictCols, updateCols []string, doNothing bool) (string, []any, error) {
	if err := validateIdentifier(table); err != nil {
		return "", nil, err
	}
	fields := fieldsOf[T]()
	if len(fields) == 0 {
		return "", nil, fmt.Errorf("database: type %s has no db-tagged columns to insert", elemType[T]().Name())
	}

	conflictIdx, err := resolveColumns[T]("conflict", conflictCols)
	if err != nil {
		return "", nil, err
	}

	vals, err := entityValues(entity)
	if err != nil {
		return "", nil, err
	}

	colNames := make([]string, len(fields))
	for i, f := range fields {
		colNames[i] = f.name
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, quotedList(colNames), placeholderRange(1, len(vals)))

	conflictNames := make([]string, len(conflictIdx))
	for i, idx := range conflictIdx {
		conflictNames[i] = fields[idx].name
	}

	switch {
	case doNothing:
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", quotedList(conflictNames))
	default:
		updateIdx, err := resolveColumns[T]("update", updateCols)
		if err != nil {
			return "", nil, err
		}
		sets := make([]string, len(updateIdx))
		args := make([]any, len(vals), len(vals)+len(updateIdx))
		copy(args, vals)
		for i, idx := range updateIdx {
			args = append(args, vals[idx])
			sets[i] = fmt.Sprintf("%q = $%d", fields[idx].name, len(vals)+i+1)
		}
		sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			quotedList(conflictNames), strings.Join(sets, ", "))
		vals = args
	}

	return sql, vals, nil
}

// Upsert inserts the entity, resolving conflicts on conflictCols with an
// UPDATE of updateCols. Both lists are validated identifiers mapped onto T's
// db-tagged columns. Returns the affected-row count reported by the server:
// 1 on plain insert, 1 on conflict-update, 0 only when the server reports it.
//
//	INSERT INTO tbl(cols…) VALUES($…) ON CONFLICT(conflictCols) DO UPDATE SET col=$n+…
func (r *Repository[T]) Upsert(entity T, conflictCols, updateCols []string) (int64, error) {
	sql, args, err := upsertStatement(r.table, entity, conflictCols, updateCols, false)
	if err != nil {
		return 0, err
	}
	return r.db.Execute(sql, args...)
}

// UpsertDoNothing is the insert-or-ignore variant: rows conflicting on
// conflictCols leave existing data untouched. Returns the affected-row count
// (0 when the row was skipped).
func (r *Repository[T]) UpsertDoNothing(entity T, conflictCols []string) (int64, error) {
	sql, args, err := upsertStatement(r.table, entity, conflictCols, nil, true)
	if err != nil {
		return 0, err
	}
	return r.db.Execute(sql, args...)
}

// updateStatement assembles UPDATE <table> SET <cols> WHERE (<spec>) and the
// aligned argument list. WHERE parameters keep their natural $1.. numbering
// against spec.Args; SET placeholders therefore start after them
// ($len(Args)+1 onwards) so caller-written specification SQL never needs
// renumbering. Pagination clauses in whereSpec are rejected and OrderBy is
// unsupported: an UPDATE matches all qualifying rows by design.
func updateStatement[T any](table string, entity T, setCols []string, whereSpec Specification) (string, []any, error) {
	if err := validateIdentifier(table); err != nil {
		return "", nil, err
	}
	if whereSpec.OrderBy != "" {
		return "", nil, fmt.Errorf(
			"database: Update does not support ORDER BY in the where specification")
	}
	if frag := clauseCollision(whereSpec.SQL, pagingInSQLRe); frag != "" {
		return "", nil, fmt.Errorf(
			"database: where specification SQL contains %s; remove pagination from Update conditions", frag)
	}

	setIdx, err := resolveColumns[T]("set", setCols)
	if err != nil {
		return "", nil, err
	}
	fields := fieldsOf[T]()

	entityVals, err := entityValues(entity)
	if err != nil {
		return "", nil, err
	}

	args := make([]any, 0, len(whereSpec.Args)+len(setIdx))
	args = append(args, whereSpec.Args...)

	sets := make([]string, len(setIdx))
	for i, idx := range setIdx {
		args = append(args, entityVals[idx])
		sets[i] = fmt.Sprintf("%q = $%d", fields[idx].name, len(whereSpec.Args)+i+1)
	}

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE (%s)", table, strings.Join(sets, ", "), whereSpec.SQL)
	return sql, args, nil
}

// Update assigns setCols (a REQUIRED non-empty whitelist validated against
// T's db-tagged mapping) from the entity's values wherever whereSpec
// matches. WHERE placeholders number from $1 across whereSpec.Args; SET
// placeholders continue after them. Returns the affected-row count.
//
//	UPDATE tbl SET col=$N… WHERE(<whereSpec.SQL>, args…)
func (r *Repository[T]) Update(entity T, setCols []string, whereSpec Specification) (int64, error) {
	sql, args, err := updateStatement(r.table, entity, setCols, whereSpec)
	if err != nil {
		return 0, err
	}
	return r.db.Execute(sql, args...)
}

// EnsureTable executes an idempotent CREATE TABLE statement supplied in FULL
// by the caller — ddl must include the leading CREATE TABLE and carry IF NOT
// EXISTS so repeated service startups stay no-ops. The table name is
// validated as a plain identifier first, mirroring NewRepositoryForTable's
// injection guard; when the DDL does not spell out the same name the call is
// still safe (validation is advisory), but passing mismatched inputs is a
// programming slip worth failing loudly on.
func EnsureTable(db *DB, name, ddl string) error {
	if db == nil {
		return errors.New("database: EnsureTable requires a non-nil DB")
	}
	if err := validateIdentifier(name); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(ddl)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "CREATE TABLE") {
		return fmt.Errorf("database: EnsureTable ddl must start with CREATE TABLE (got %q)", trimmed)
	}
	if !strings.Contains(upper, "IF NOT EXISTS") {
		return fmt.Errorf("database: EnsureTable ddl must contain IF NOT EXISTS to stay idempotent (got %q)", trimmed)
	}
	_, err := db.Execute(trimmed)
	return err
}
