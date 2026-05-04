package driver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jamestjat/sqlce/engine"
)

// stmt implements driver.Stmt.
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

	pq, err := parseSelectQuery(s.query)
	if err != nil {
		return nil, err
	}

	tbl, err := s.conn.db.Table(pq.Table)
	if err != nil {
		return nil, fmt.Errorf("sqlce: %w", err)
	}

	var ri *engine.RowIterator
	if pq.ObjectID > 0 {
		ri, err = tbl.RowsWithObjectID(pq.ObjectID)
	} else {
		ri, err = tbl.Rows()
	}
	if err != nil {
		return nil, fmt.Errorf("sqlce: %w", err)
	}

	// If specific columns were requested, wrap with column filter
	if len(pq.Columns) > 0 {
		return &filteredRows{iter: ri, wantCols: pq.Columns, allCols: ri.Columns()}, nil
	}
	return &rows{iter: ri}, nil
}

// parsedQuery holds the result of parsing a SELECT statement.
type parsedQuery struct {
	Table    string
	Columns  []string // empty = all columns (SELECT *)
	ObjectID uint16
}

// parseSelectQuery parses SELECT queries.
// Supported forms:
//
//	SELECT * FROM <table>
//	SELECT col1, col2 FROM <table>
//	SELECT * FROM <table> WITH OBJECTID <id>
//	SELECT * FROM [quoted table]
//	SELECT * FROM "quoted table"
func parseSelectQuery(query string) (*parsedQuery, error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	// Reject non-SELECT
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("sqlce: only SELECT queries are supported")
	}

	// Reject unsupported clauses
	for _, clause := range []string{" WHERE ", " JOIN ", " GROUP BY ", " ORDER BY ", " HAVING ", " LIMIT "} {
		if strings.Contains(upper, clause) {
			return nil, fmt.Errorf("sqlce: %s clause is not supported", strings.TrimSpace(clause))
		}
	}

	// Find FROM
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return nil, fmt.Errorf("sqlce: missing FROM clause")
	}

	// Parse column list (between SELECT and FROM)
	colPart := strings.TrimSpace(q[6:fromIdx]) // skip "SELECT"
	var columns []string
	if colPart != "*" {
		for _, c := range strings.Split(colPart, ",") {
			col := strings.TrimSpace(c)
			col = unquoteName(col)
			if col == "" {
				return nil, fmt.Errorf("sqlce: empty column name in SELECT list")
			}
			columns = append(columns, col)
		}
	}

	// Parse table name
	rest := strings.TrimSpace(q[fromIdx+6:]) // skip " FROM "
	tableName, rest := extractTableName(rest)
	if tableName == "" {
		return nil, fmt.Errorf("sqlce: missing table name")
	}

	pq := &parsedQuery{
		Table:   tableName,
		Columns: columns,
	}

	// Check for WITH OBJECTID clause
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return pq, nil
	}

	fields := strings.Fields(rest)
	if len(fields) != 3 || !strings.EqualFold(fields[0], "WITH") || !strings.EqualFold(fields[1], "OBJECTID") {
		return nil, fmt.Errorf("sqlce: unsupported trailing query text after table name: %q", rest)
	}
	id, err := strconv.Atoi(fields[2])
	if err != nil || id < 0 || id > 65535 {
		return nil, fmt.Errorf("sqlce: invalid OBJECTID value: %q", fields[2])
	}
	pq.ObjectID = uint16(id)

	return pq, nil
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

// unquoteName removes surrounding quotes from a name.
func unquoteName(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '[' && s[len(s)-1] == ']') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// filteredRows wraps a RowIterator to return only selected columns.
type filteredRows struct {
	iter     *engine.RowIterator
	wantCols []string
	allCols  []string
	indices  []int // lazily computed column indices
}

func (fr *filteredRows) Columns() []string {
	return fr.wantCols
}

func (fr *filteredRows) Close() error {
	return fr.iter.Close()
}

func (fr *filteredRows) Next(dest []driver.Value) error {
	if fr.indices == nil {
		fr.indices = make([]int, len(fr.wantCols))
		colIndex := make(map[string]int)
		for i, c := range fr.allCols {
			colIndex[c] = i
		}
		for i, wc := range fr.wantCols {
			if idx, ok := colIndex[wc]; ok {
				fr.indices[i] = idx
			} else {
				return fmt.Errorf("sqlce: column %q not found", wc)
			}
		}
	}

	if !fr.iter.Next() {
		if err := fr.iter.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	vals := fr.iter.Values()
	for i, idx := range fr.indices {
		if idx < len(vals) {
			dest[i] = vals[idx]
		}
	}
	return nil
}

var _ driver.Stmt = (*stmt)(nil)
var _ driver.Rows = (*filteredRows)(nil)
