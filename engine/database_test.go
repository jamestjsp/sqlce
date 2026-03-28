package engine_test

import (
	"testing"

	"github.com/jamestjat/sqlce/engine"
)

func TestDatabaseOpen(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	t.Logf("Header: version=%s, pages=%d, LCID=%d",
		db.Header().Version, db.TotalPages(), db.Header().LCID)

	// Should have 98 tables
	tables := db.Tables()
	if len(tables) != 98 {
		t.Errorf("expected 98 tables, got %d", len(tables))
	}
	t.Logf("Tables: %d", len(tables))

	// Tables should be sorted
	for i := 1; i < len(tables); i++ {
		if tables[i] < tables[i-1] {
			t.Errorf("tables not sorted: %q before %q", tables[i-1], tables[i])
			break
		}
	}

	// Can get a specific table
	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table(Properties): %v", err)
	}
	if tbl.Name() != "Properties" {
		t.Errorf("expected name 'Properties', got %q", tbl.Name())
	}
	if tbl.ColumnCount() != 2 {
		t.Errorf("Properties: expected 2 columns, got %d", tbl.ColumnCount())
	}

	// Unknown table returns error
	_, err = db.Table("NoSuchTable")
	if err == nil {
		t.Error("expected error for unknown table")
	}
}

func TestDatabaseOpen_InvalidPath(t *testing.T) {
	_, err := engine.Open("nonexistent.sdf")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestDatabaseOpen_InvalidFile(t *testing.T) {
	_, err := engine.Open("../go.mod")
	if err == nil {
		t.Error("expected error for non-SDF file")
	}
}

func TestDatabaseClose(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// First close should succeed
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Second close should be idempotent
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// Table access after close should fail
	_, err = db.Table("Properties")
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestDatabaseScanWithObjectID(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	// Scan with known objectID
	result, err := tbl.ScanWithObjectID(1305)
	if err != nil {
		t.Fatalf("ScanWithObjectID: %v", err)
	}

	if len(result.Rows) != 6 {
		t.Errorf("expected 6 rows, got %d", len(result.Rows))
	}

	for _, row := range result.Rows {
		t.Logf("  %v = %v", row[0], row[1])
	}
}

func TestDatabaseSetObjectMapping(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Set known mappings
	db.SetObjectMapping(map[string]uint16{
		"Properties":    1305,
		"DataArrayTypes": 1321,
		"BlcModel":      1395,
	})

	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	result, err := tbl.Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(result.Rows) != 6 {
		t.Errorf("expected 6 rows, got %d", len(result.Rows))
	}
	t.Logf("Properties: %d rows via mapping", len(result.Rows))
}
