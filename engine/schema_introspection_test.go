package engine_test

import (
	"reflect"
	"testing"

	"github.com/josephjohnjj/sqlce/engine"
)

func TestSchemaIntrospection(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Test Properties table schema
	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	schema := tbl.Schema()
	if schema.Name() != "Properties" {
		t.Errorf("name: expected 'Properties', got %q", schema.Name())
	}
	if schema.ColumnCount() != 2 {
		t.Fatalf("expected 2 columns, got %d", schema.ColumnCount())
	}

	cols := schema.Columns()

	// Name column
	if cols[0].Name() != "Name" {
		t.Errorf("col[0].Name: expected 'Name', got %q", cols[0].Name())
	}
	if cols[0].Type() != "nvarchar" {
		t.Errorf("col[0].Type: expected 'nvarchar', got %q", cols[0].Type())
	}
	if !cols[0].IsVariable() {
		t.Error("col[0] should be variable-length")
	}
	if cols[0].GoType() != reflect.TypeOf("") {
		t.Errorf("col[0].GoType: expected string, got %v", cols[0].GoType())
	}

	// Value column
	if cols[1].Name() != "Value" {
		t.Errorf("col[1].Name: expected 'Value', got %q", cols[1].Name())
	}
	if cols[1].Type() != "nvarchar" {
		t.Errorf("col[1].Type: expected 'nvarchar', got %q", cols[1].Type())
	}

	// Column lookup by name
	c := schema.ColumnByName("Name")
	if c == nil {
		t.Fatal("ColumnByName('Name') returned nil")
	}
	if c.Ordinal() != 1 {
		t.Errorf("Name ordinal: expected 1, got %d", c.Ordinal())
	}

	// Unknown column
	if schema.ColumnByName("NonExistent") != nil {
		t.Error("ColumnByName should return nil for unknown column")
	}
}

func TestSchemaTypes(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Test DataArrayTypes table - mix of GUID, nvarchar, int
	tbl, err := db.Table("DataArrayTypes")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	schema := tbl.Schema()
	cols := schema.Columns()

	expected := []struct {
		name     string
		typeName string
		variable bool
	}{
		{"Identifier", "uniqueidentifier", false},
		{"ArrayType", "nvarchar", true},
		{"Interval", "int", false},
		{"Unit", "nvarchar", true},
	}

	for i, exp := range expected {
		if i >= len(cols) {
			t.Errorf("missing column %d (%s)", i, exp.name)
			continue
		}
		if cols[i].Name() != exp.name {
			t.Errorf("col[%d].Name: expected %q, got %q", i, exp.name, cols[i].Name())
		}
		if cols[i].Type() != exp.typeName {
			t.Errorf("col[%d].Type: expected %q, got %q", i, exp.typeName, cols[i].Type())
		}
		if cols[i].IsVariable() != exp.variable {
			t.Errorf("col[%d].IsVariable: expected %v, got %v", i, exp.variable, cols[i].IsVariable())
		}
		t.Logf("  %s: %s (var=%v, maxLen=%d)", cols[i].Name(), cols[i].Type(), cols[i].IsVariable(), cols[i].MaxLength())
	}
}

func TestSchemaAllTables(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tables := db.Tables()
	readable := 0
	for _, name := range tables {
		tbl, err := db.Table(name)
		if err != nil {
			t.Errorf("Table(%q): %v", name, err)
			continue
		}
		schema := tbl.Schema()
		if schema.ColumnCount() > 0 {
			readable++
			// Check all columns have valid types
			for _, col := range schema.Columns() {
				if col.Type() == "" || col.Type() == "unknown" {
					t.Errorf("%s.%s: unknown type (typeID=%d)", name, col.Name(), col.TypeID())
				}
			}
		}
	}
	t.Logf("All 98 tables have readable schemas (%d with columns)", readable)
	if readable != 98 {
		t.Errorf("expected 98 readable tables, got %d", readable)
	}
}
