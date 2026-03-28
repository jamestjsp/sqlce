package engine

import (
	"fmt"
	"reflect"
)

// RowIterator provides row-by-row access to table data, modeled after sql.Rows.
type RowIterator struct {
	columns []string
	rows    [][]any
	pos     int
	err     error
	closed  bool
}

// newRowIterator creates a RowIterator from a ScanResult.
func newRowIterator(result *ScanResult) *RowIterator {
	colNames := make([]string, len(result.Columns))
	for i, c := range result.Columns {
		colNames[i] = c.Name
	}
	return &RowIterator{
		columns: colNames,
		rows:    result.Rows,
		pos:     -1,
	}
}

// Next advances to the next row. Returns false when no more rows or on error.
func (ri *RowIterator) Next() bool {
	if ri.closed || ri.err != nil {
		return false
	}
	ri.pos++
	return ri.pos < len(ri.rows)
}

// Values returns the current row's values as a slice of any.
// Must be called after a successful Next().
func (ri *RowIterator) Values() []any {
	if ri.pos < 0 || ri.pos >= len(ri.rows) {
		return nil
	}
	return ri.rows[ri.pos]
}

// Scan copies the current row's values into the provided destination pointers.
// Similar to sql.Rows.Scan(), each dest must be a pointer to a type compatible
// with the corresponding column value.
func (ri *RowIterator) Scan(dest ...any) error {
	if ri.pos < 0 || ri.pos >= len(ri.rows) {
		return fmt.Errorf("Scan called without a valid row")
	}

	row := ri.rows[ri.pos]
	if len(dest) != len(row) {
		return fmt.Errorf("Scan: expected %d destinations, got %d", len(row), len(dest))
	}

	for i, val := range row {
		if dest[i] == nil {
			continue
		}
		dv := reflect.ValueOf(dest[i])
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("Scan: dest[%d] is not a pointer", i)
		}
		if val == nil {
			dv.Elem().Set(reflect.Zero(dv.Elem().Type()))
			continue
		}

		sv := reflect.ValueOf(val)
		target := dv.Elem()

		if sv.Type().AssignableTo(target.Type()) {
			target.Set(sv)
		} else if sv.Type().ConvertibleTo(target.Type()) {
			target.Set(sv.Convert(target.Type()))
		} else {
			// Try string conversion for common cases
			if target.Kind() == reflect.String {
				target.SetString(fmt.Sprintf("%v", val))
			} else {
				return fmt.Errorf("Scan: cannot assign %T to %s at column %d", val, target.Type(), i)
			}
		}
	}

	return nil
}

// Columns returns the column names.
func (ri *RowIterator) Columns() []string {
	return ri.columns
}

// Err returns any error encountered during iteration.
func (ri *RowIterator) Err() error {
	return ri.err
}

// Close releases resources. Safe to call multiple times.
func (ri *RowIterator) Close() error {
	ri.closed = true
	ri.rows = nil
	return nil
}

// RowCount returns the total number of rows (known after loading).
func (ri *RowIterator) RowCount() int {
	if ri.rows == nil {
		return 0
	}
	return len(ri.rows)
}
