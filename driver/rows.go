package driver

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/jamestjat/sqlce/engine"
)

// rows implements driver.Rows.
type rows struct {
	iter *engine.RowIterator
}

// Columns returns the column names.
func (r *rows) Columns() []string {
	return r.iter.Columns()
}

// Close releases the iterator.
func (r *rows) Close() error {
	return r.iter.Close()
}

// Next populates dest with the next row's values.
func (r *rows) Next(dest []driver.Value) error {
	if !r.iter.Next() {
		if err := r.iter.Err(); err != nil {
			return err
		}
		return io.EOF
	}

	vals := r.iter.Values()
	if len(dest) != len(vals) {
		return fmt.Errorf("sqlce: expected %d columns, got %d", len(vals), len(dest))
	}

	for i, v := range vals {
		dest[i] = v
	}

	return nil
}

var _ driver.Rows = (*rows)(nil)
