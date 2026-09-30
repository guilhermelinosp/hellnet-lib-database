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

// statementName derives the operation and main table of a SQL statement, for
// span names such as "INSERT outbox_events" (OpenTelemetry database semantic
// conventions). It never includes arguments, only the verb and a table name.
// table is empty when it cannot be determined (BEGIN, COMMIT, SELECT 1, ...).
func statementName(sql string) (operation, table string) {
	sql = stripLeadingComments(sql)
	word := firstWord.FindString(sql)
	if word == "" {
		return "", ""
	}
	operation = strings.ToUpper(word)
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

// spanNameFor returns the span name for a statement: "INSERT outbox_events",
// "BEGIN", or the generic "db.query" when the statement cannot be classified.
func spanNameFor(operation, table string) string {
	switch {
	case operation == "":
		return "db.query"
	case table == "":
		return operation
	default:
		return operation + " " + table
	}
}
