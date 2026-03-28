package engine

import (
	"reflect"

	"github.com/jamestjat/sqlce/format"
)

// Column provides schema information about a single table column.
type Column struct {
	name      string
	typeName  string
	typeID    uint16
	goType    reflect.Type
	maxLength int
	ordinal   int
	variable  bool
}

// NewColumn creates a Column from a catalog ColumnDef.
func NewColumn(cd format.ColumnDef) Column {
	ti := format.LookupType(cd.TypeID)
	return Column{
		name:      cd.Name,
		typeName:  ti.Name,
		typeID:    cd.TypeID,
		goType:    ti.GoType,
		maxLength: cd.MaxLength,
		ordinal:   cd.Ordinal,
		variable:  ti.IsVariable,
	}
}

// Name returns the column name.
func (c Column) Name() string { return c.name }

// Type returns the SQL CE type name (e.g., "nvarchar", "int", "uniqueidentifier").
func (c Column) Type() string { return c.typeName }

// TypeID returns the raw SQL CE type identifier.
func (c Column) TypeID() uint16 { return c.typeID }

// GoType returns the Go reflect.Type for this column's values.
func (c Column) GoType() reflect.Type { return c.goType }

// MaxLength returns the maximum length in bytes (relevant for variable-length types).
func (c Column) MaxLength() int { return c.maxLength }

// Ordinal returns the 1-based column position in the table.
func (c Column) Ordinal() int { return c.ordinal }

// IsVariable returns true for variable-length types (nvarchar, varbinary, etc.).
func (c Column) IsVariable() bool { return c.variable }

// Schema provides schema introspection for a table.
type Schema struct {
	name    string
	columns []Column
}

// NewSchema creates a Schema from a TableDef.
func NewSchema(td *format.TableDef) *Schema {
	cols := make([]Column, len(td.Columns))
	for i, cd := range td.Columns {
		cols[i] = NewColumn(cd)
	}
	return &Schema{
		name:    td.Name,
		columns: cols,
	}
}

// Name returns the table name.
func (s *Schema) Name() string { return s.name }

// Columns returns all columns in ordinal order.
func (s *Schema) Columns() []Column { return s.columns }

// ColumnCount returns the number of columns.
func (s *Schema) ColumnCount() int { return len(s.columns) }

// ColumnByName returns the column with the given name, or nil.
func (s *Schema) ColumnByName(name string) *Column {
	for i := range s.columns {
		if s.columns[i].name == name {
			return &s.columns[i]
		}
	}
	return nil
}

// Schema returns the table's schema information.
func (t *Table) Schema() *Schema {
	return NewSchema(t.def)
}
