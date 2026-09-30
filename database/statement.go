package database

import (
	"regexp"
	"strings"
)

var (
	insertTable = regexp.MustCompile(`(?is)^insert\s+into\s+("?[\w.]+"?)`)
	updateTable = regexp.MustCompile(`(?is)^update\s+(?:only\s+)?("?[\w.]+"?)`)
	deleteTable = regexp.MustCompile(`(?is)^delete\s+from\s+(?:only\s+)?("?[\w.]+"?)`)
	selectTable = regexp.MustCompile(`(?is)\bfrom\s+(?:only\s+)?("?[\w.]+"?)`)
	firstWord   = regexp.MustCompile(`^[A-Za-z]+`)
)

var (
	// mainDML finds the first DML keyword once parenthesised content (CTE bodies,
	// subqueries) and string literals have been blanked out.
	mainDML = regexp.MustCompile(`(?is)\b(insert\s+into|update|delete\s+from|select)\b`)
	literal = regexp.MustCompile(`'(?:[^']|'')*'`)
)

// statementName derives the operation and main table of a SQL statement. The
// operation is INSERT, UPDATE, DELETE, SELECT, BEGIN, COMMIT, ... ; for a WITH
// (CTE) statement it is the operation of the main statement that follows the
// CTEs. It never includes arguments. table is empty when it cannot be
// determined (BEGIN, COMMIT, SELECT 1, ...).
func statementName(sql string) (operation, table string) {
	sql = stripLeadingComments(sql)
	word := firstWord.FindString(sql)
	if word == "" {
		return "", ""
	}
	operation = strings.ToUpper(word)
	if operation == "WITH" {
		flat := blankNested(literal.ReplaceAllStringFunc(sql, func(m string) string { return strings.Repeat(" ", len(m)) }))
		loc := mainDML.FindStringIndex(flat)
		if loc == nil {
			return operation, ""
		}
		sql = sql[loc[0]:]
		operation = strings.ToUpper(firstWord.FindString(sql))
	}
	var match []string
	switch operation {
	case "INSERT":
		match = insertTable.FindStringSubmatch(sql)
	case "UPDATE":
		match = updateTable.FindStringSubmatch(sql)
	case "DELETE":
		match = deleteTable.FindStringSubmatch(sql)
	case "SELECT":
		match = selectTable.FindStringSubmatch(sql)
	}
	if len(match) == 2 {
		table = strings.ReplaceAll(match[1], `"`, "")
	}
	return operation, table
}

// blankNested replaces everything inside parentheses with spaces (keeping the
// length, so indexes still match the original string).
func blankNested(s string) string {
	out := []byte(s)
	depth := 0
	for i, c := range out {
		switch {
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case depth > 0:
			out[i] = ' '
		}
	}
	return string(out)
}

func stripLeadingComments(sql string) string {
	for {
		sql = strings.TrimSpace(sql)
		switch {
		case strings.HasPrefix(sql, "--"):
			i := strings.IndexByte(sql, '\n')
			if i < 0 {
				return ""
			}
			sql = sql[i+1:]
		case strings.HasPrefix(sql, "/*"):
			i := strings.Index(sql, "*/")
			if i < 0 {
				return ""
			}
			sql = sql[i+2:]
		default:
			return sql
		}
	}
}

// spanNameFor returns a low-cardinality span name for a statement: db.insert,
// db.select, db.update, db.delete, db.transaction (BEGIN/COMMIT/ROLLBACK/...),
// or the generic db.query when it cannot be classified. The table travels in
// the db.collection.name attribute, not in the name.
func spanNameFor(operation string) string {
	switch operation {
	case "INSERT":
		return "db.insert"
	case "SELECT":
		return "db.select"
	case "UPDATE":
		return "db.update"
	case "DELETE":
		return "db.delete"
	case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return "db.transaction"
	default:
		return "db.query"
	}
}
