package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/josephjohnjj/sqlce/engine"
)

// stmt implements driver.Stmt.
// Supports only "SELECT * FROM <table>" queries (read-only).
type stmt struct {
	conn  *conn
	query string
}

// Close is a no-op for statements.
func (s *stmt) Close() error {
	return nil
}

// NumInput returns -1 (no parameter support).
func (s *stmt) NumInput() int {
	return -1
}

// Exec is not supported (read-only database).
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("sqlce: Exec not supported (read-only database)")
}

// Query executes a SELECT query and returns rows.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.conn.closed {
		return nil, driver.ErrBadConn
	}

	tableName, objectID, err := parseSelectQuery(s.query)
	if err != nil {
		return nil, err
	}

	tbl, err := s.conn.db.Table(tableName)
	if err != nil {
		return nil, fmt.Errorf("sqlce: %w", err)
	}

	var ri *engine.RowIterator
	if objectID > 0 {
		var err2 error
		ri, err2 = tbl.RowsWithObjectID(objectID)
		if err2 != nil {
			return nil, fmt.Errorf("sqlce: %w", err2)
		}
	} else {
		var err2 error
		ri, err2 = tbl.Rows()
		if err2 != nil {
			return nil, fmt.Errorf("sqlce: %w", err2)
		}
	}

	return &rows{iter: ri}, nil
}

// parseSelectQuery parses a simple "SELECT * FROM <table>" query.
// Also supports "SELECT * FROM <table> WITH OBJECTID <id>" for explicit mapping.
func parseSelectQuery(query string) (tableName string, objectID uint16, err error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	if !strings.HasPrefix(upper, "SELECT") {
		return "", 0, fmt.Errorf("sqlce: only SELECT queries are supported")
	}

	// Find FROM clause
	fromIdx := strings.Index(upper, "FROM")
	if fromIdx < 0 {
		return "", 0, fmt.Errorf("sqlce: missing FROM clause")
	}

	rest := strings.TrimSpace(q[fromIdx+4:])

	// Extract table name (handle quoted names)
	tableName, rest = extractTableName(rest)
	if tableName == "" {
		return "", 0, fmt.Errorf("sqlce: missing table name")
	}

	// Check for WITH OBJECTID clause
	restUpper := strings.ToUpper(strings.TrimSpace(rest))
	if strings.HasPrefix(restUpper, "WITH OBJECTID") {
		remaining := strings.TrimSpace(strings.TrimSpace(rest)[13:])
		var id int
		_, err := fmt.Sscanf(remaining, "%d", &id)
		if err != nil {
			return "", 0, fmt.Errorf("sqlce: invalid OBJECTID value: %w", err)
		}
		objectID = uint16(id)
	}

	return tableName, objectID, nil
}

func extractTableName(s string) (name, rest string) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return "", ""
	}

	// Handle quoted table names: "TableName" or [TableName]
	if s[0] == '"' {
		end := strings.Index(s[1:], "\"")
		if end >= 0 {
			return s[1 : end+1], s[end+2:]
		}
	}
	if s[0] == '[' {
		end := strings.Index(s[1:], "]")
		if end >= 0 {
			return s[1 : end+1], s[end+2:]
		}
	}

	// Unquoted: take until whitespace or end
	end := strings.IndexAny(s, " \t\n\r;")
	if end < 0 {
		return s, ""
	}
	return s[:end], s[end:]
}

var _ driver.Stmt = (*stmt)(nil)
