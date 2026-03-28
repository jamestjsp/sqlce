package driver_test

import (
	"database/sql"
	"testing"

	_ "github.com/jamestjat/sqlce/driver"
)

func TestSQLDriverOpen(t *testing.T) {
	db, err := sql.Open("sqlce", "../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Ping verifies the connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestSQLDriverQuery(t *testing.T) {
	db, err := sql.Open("sqlce", "../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Query Properties table using WITH OBJECTID syntax
	rows, err := db.Query("SELECT * FROM Properties WITH OBJECTID 1305")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	// Check columns
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Columns: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(cols), cols)
	}
	if cols[0] != "Name" || cols[1] != "Value" {
		t.Errorf("columns: %v", cols)
	}

	// Read all rows
	count := 0
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("Scan row %d: %v", count, err)
		}
		t.Logf("  %s = %s", name, value)
		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	if count != 6 {
		t.Errorf("expected 6 rows, got %d", count)
	}
}

func TestSQLDriverQuery_DataArrayTypes(t *testing.T) {
	db, err := sql.Open("sqlce", "../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT * FROM DataArrayTypes WITH OBJECTID 1321")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least 1 row")
	}

	var id, arrayType, unit string
	var interval int32
	if err := rows.Scan(&id, &arrayType, &interval, &unit); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if arrayType != "ContinuousData" {
		t.Errorf("ArrayType: expected 'ContinuousData', got %q", arrayType)
	}
	if interval != 60 {
		t.Errorf("Interval: expected 60, got %d", interval)
	}
	if unit != "Second" {
		t.Errorf("Unit: expected 'Second', got %q", unit)
	}
	t.Logf("GUID=%s ArrayType=%s Interval=%d Unit=%s", id, arrayType, interval, unit)
}

func TestSQLDriverQuery_QuotedTable(t *testing.T) {
	db, err := sql.Open("sqlce", "../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Quoted table name
	rows, err := db.Query(`SELECT * FROM "Properties" WITH OBJECTID 1305`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 6 {
		t.Errorf("expected 6 rows, got %d", count)
	}
}

func TestSQLDriverQuery_InvalidFile(t *testing.T) {
	db, err := sql.Open("sqlce", "nonexistent.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Ping should fail
	if err := db.Ping(); err == nil {
		t.Error("expected error for missing file")
	}
}

func TestSQLDriverQuery_InvalidQuery(t *testing.T) {
	db, err := sql.Open("sqlce", "../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	_, err = db.Query("INSERT INTO Properties VALUES ('a', 'b')")
	if err == nil {
		t.Error("expected error for non-SELECT query")
	}
}
