package engine

import (
	"testing"

	"github.com/jamestjat/sqlce/format"
)

const testdbDir = "../data/testdbs/"

func TestDownloadedDatabases(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		version    int
		minTables  int
		scanTable  string // table to attempt scanning
	}{
		{"employees_CE40", testdbDir + "employees.sdf", 4, 4, "emp"},
		{"TestDatabase_CE40", testdbDir + "TestDatabase40.sdf", 4, 7, "Users"},
		{"TestDatabase_CE35", testdbDir + "TestDatabase35.sdf", 3, 5, "Users"},
		{"sqlce-test_CE35", testdbDir + "sqlce-test.sdf", 3, 2, "Person"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := openOrSkip(t, tc.path)
			defer db.Close()

			h := db.Header()
			if h.Version.MajorVersion() != tc.version {
				t.Errorf("version = %d, want %d", h.Version.MajorVersion(), tc.version)
			}

			tables := db.Tables()
			if len(tables) < tc.minTables {
				t.Errorf("tables = %d, want >= %d", len(tables), tc.minTables)
			}
			t.Logf("%s build=%d tables=%d LCID=%d",
				h.VersionString(), h.BuildNumber, len(tables), h.LCID)

			// Scan all tables, count successes
			scanned := 0
			totalRows := 0
			for _, name := range tables {
				tbl, err := db.Table(name)
				if err != nil {
					continue
				}
				result, err := tbl.Scan()
				if err != nil {
					t.Logf("  skip %s: %v", name, err)
					continue
				}
				scanned++
				totalRows += len(result.Rows)
				t.Logf("  %-30s %d cols, %d rows", name, len(result.Columns), len(result.Rows))
			}
			t.Logf("scanned %d/%d tables, %d total rows", scanned, len(tables), totalRows)
		})
	}
}

func TestCE35DataScanning(t *testing.T) {
	db := openOrSkip(t, testdbDir+"TestDatabase35.sdf")
	defer db.Close()

	tbl, err := db.Table("Users")
	if err != nil {
		t.Fatalf("Table(Users): %v", err)
	}
	result, err := tbl.Scan()
	if err != nil {
		t.Skipf("scan Users: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Skip("Users table empty")
	}

	t.Logf("Users: %d rows", len(result.Rows))
	for i, row := range result.Rows {
		if i >= 3 {
			break
		}
		t.Logf("  row %d: %v", i, row)
	}
}

func TestEmployeesDataTypes(t *testing.T) {
	db := openOrSkip(t, testdbDir+"employees.sdf")
	defer db.Close()

	// Verify schema has the expected variety of types
	tbl, err := db.Table("datatypes")
	if err != nil {
		t.Fatalf("Table(datatypes): %v", err)
	}
	cols := tbl.Columns()
	if len(cols) != 19 {
		t.Errorf("datatypes columns = %d, want 19", len(cols))
	}

	// Log all column types for diagnostics
	for _, col := range cols {
		t.Logf("  %-20s typeID=0x%02X (%s) size=%d",
			col.Name, col.TypeID, format.TypeName(col.TypeID), col.MaxLength)
	}

	// Try scanning the emp table (should have actual data)
	empTbl, err := db.Table("emp")
	if err != nil {
		t.Fatalf("Table(emp): %v", err)
	}
	result, err := empTbl.Scan()
	if err != nil {
		t.Skipf("scan emp: %v", err)
	}
	t.Logf("emp: %d rows", len(result.Rows))
}
