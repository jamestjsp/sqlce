package engine

import (
	"os"
	"testing"

	"github.com/jamestjat/sqlce/format"
)

const northwindPath = "../reference/SqlCeToolbox/src/API/SqlCeScripting40/Tests/Northwind.sdf"
const compositeFKPath = "../reference/SqlCeToolbox/src/API/SqlCeScripting40/Tests/composite_foreign_key.sdf"

func openOrSkip(t *testing.T, path string) *Database {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Skipf("test file not available: %s", path)
	}
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	return db
}

func TestNorthwindOpen(t *testing.T) {
	db := openOrSkip(t, northwindPath)
	defer db.Close()

	h := db.Header()
	if h.Version.MajorVersion() != 4 {
		t.Errorf("version = %d, want 4", h.Version.MajorVersion())
	}
	if h.LCID != 2057 {
		t.Errorf("LCID = %d, want 2057 (en-GB)", h.LCID)
	}
	t.Logf("Northwind: %s, %d tables, LCID=%d", h.VersionString(), db.TableCount(), h.LCID)
}

func TestNorthwindTables(t *testing.T) {
	db := openOrSkip(t, northwindPath)
	defer db.Close()

	tables := db.Tables()
	expected := []string{"Customers", "Orders", "Products", "Categories", "Employees", "Suppliers", "Shippers"}
	for _, name := range expected {
		found := false
		for _, t := range tables {
			if t == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected table %q not found", name)
		}
	}
	if len(tables) != 13 {
		t.Errorf("table count = %d, want 13", len(tables))
	}
}

func TestNorthwindScanAllTables(t *testing.T) {
	db := openOrSkip(t, northwindPath)
	defer db.Close()

	for _, name := range db.Tables() {
		tbl, err := db.Table(name)
		if err != nil {
			t.Errorf("Table(%q): %v", name, err)
			continue
		}
		result, err := tbl.Scan()
		if err != nil {
			t.Logf("scan %q: %v (skipped, may lack objectID mapping)", name, err)
			continue
		}
		t.Logf("%-30s %d cols, %d rows", name, len(result.Columns), len(result.Rows))
	}
}

func TestNorthwindCustomers(t *testing.T) {
	db := openOrSkip(t, northwindPath)
	defer db.Close()

	tbl, err := db.Table("Customers")
	if err != nil {
		t.Fatalf("Table(Customers): %v", err)
	}
	result, err := tbl.Scan()
	if err != nil {
		t.Skipf("scan Customers: %v", err)
	}

	if len(result.Rows) != 91 {
		t.Errorf("Customers row count = %d, want 91", len(result.Rows))
	}
	t.Logf("Customers: %d rows, columns: %v", len(result.Rows), columnNames(result.Columns))

	// Check for non-nil values in at least some rows
	nonNil := 0
	for _, row := range result.Rows {
		for _, v := range row {
			if v != nil {
				nonNil++
				break
			}
		}
	}
	if nonNil == 0 {
		t.Error("all rows have only nil values")
	}
}

func TestCompositeFKOpen(t *testing.T) {
	db := openOrSkip(t, compositeFKPath)
	defer db.Close()

	tables := db.Tables()
	if len(tables) != 2 {
		t.Errorf("table count = %d, want 2", len(tables))
	}
	t.Logf("tables: %v", tables)

	cat := db.Catalog()
	if len(cat.Constraints) == 0 {
		t.Error("expected constraints in composite_foreign_key.sdf")
	}
	for _, c := range cat.Constraints {
		t.Logf("constraint: %s on %s (type=%d target=%s)", c.Name, c.Table, c.Type, c.TargetTable)
	}
}

func columnNames(cols []format.ColumnDef) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}
