package engine_test

import (
	"testing"

	"github.com/josephjohnjj/sqlce/engine"
)

func TestRowIterator_Properties(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.SetObjectMapping(map[string]uint16{
		"Properties": 1305,
	})

	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()

	// Check columns
	cols := rows.Columns()
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "Name" || cols[1] != "Value" {
		t.Errorf("columns: got %v", cols)
	}

	// Iterate rows
	count := 0
	for rows.Next() {
		vals := rows.Values()
		if len(vals) != 2 {
			t.Errorf("row %d: expected 2 values, got %d", count, len(vals))
		}
		t.Logf("  %v = %v", vals[0], vals[1])
		count++
	}

	if rows.Err() != nil {
		t.Fatalf("iteration error: %v", rows.Err())
	}

	if count != 6 {
		t.Errorf("expected 6 rows, got %d", count)
	}
}

func TestRowIterator_Scan(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.SetObjectMapping(map[string]uint16{
		"DataArrayTypes": 1321,
	})

	tbl, err := db.Table("DataArrayTypes")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
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
	t.Logf("DataArrayTypes: ID=%s ArrayType=%s Interval=%d Unit=%s", id, arrayType, interval, unit)
}

func TestRowIterator_EmptyTable(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Use a non-existent objectID to simulate an empty table
	db.SetObjectMapping(map[string]uint16{
		"Properties": 9999,
	})

	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Error("expected no rows for non-existent objectID")
	}
	if rows.Err() != nil {
		t.Errorf("unexpected error: %v", rows.Err())
	}
}

func TestRowIterator_CloseIdempotent(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.SetObjectMapping(map[string]uint16{
		"Properties": 1305,
	})

	tbl, err := db.Table("Properties")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}

	if err := rows.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	// Next after close returns false
	if rows.Next() {
		t.Error("Next after Close should return false")
	}
}
