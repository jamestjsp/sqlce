package engine_test

import (
	"os"
	"testing"

	"github.com/jamestjat/sqlce/engine"
	"github.com/jamestjat/sqlce/format"
)

func openSDF(t *testing.T) (*format.PageReader, int) {
	t.Helper()
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening SDF: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	h, err := format.ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := format.NewPageReader(f, h, 128)
	return pr, totalPages
}

func TestTableScan_DataArrayTypes(t *testing.T) {
	pr, totalPages := openSDF(t)

	table := &format.TableDef{
		Name: "DataArrayTypes",
		Columns: []format.ColumnDef{
			{Name: "Identifier", TypeID: format.TypeUniqueIdentifier, Ordinal: 1},
			{Name: "ArrayType", TypeID: format.TypeNVarchar, Ordinal: 2, MaxLength: 100},
			{Name: "Interval", TypeID: format.TypeInt, Ordinal: 3},
			{Name: "Unit", TypeID: format.TypeNVarchar, Ordinal: 4, MaxLength: 100},
		},
	}

	// Known objectID from binary analysis
	scanner := engine.NewTableScanner(pr, totalPages, table, 1321)
	result, err := scanner.Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("DataArrayTypes: %d rows", len(result.Rows))
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if len(result.Rows) > 0 {
		row := result.Rows[0]
		t.Logf("  Row: %v", row)

		// Check ArrayType = "ContinuousData"
		if s, ok := row[1].(string); ok {
			if s != "ContinuousData" {
				t.Errorf("ArrayType: expected 'ContinuousData', got %q", s)
			}
		}

		// Check Interval = 60
		if v, ok := row[2].(int32); ok {
			if v != 60 {
				t.Errorf("Interval: expected 60, got %d", v)
			}
		}

		// Check Unit = "Second"
		if s, ok := row[3].(string); ok {
			if s != "Second" {
				t.Errorf("Unit: expected 'Second', got %q", s)
			}
		}
	}
}

func TestTableScan_Properties(t *testing.T) {
	pr, totalPages := openSDF(t)

	table := &format.TableDef{
		Name: "Properties",
		Columns: []format.ColumnDef{
			{Name: "Name", TypeID: format.TypeNVarchar, Ordinal: 1, MaxLength: 64},
			{Name: "Value", TypeID: format.TypeNVarchar, Ordinal: 2, MaxLength: 512},
		},
	}

	scanner := engine.NewTableScanner(pr, totalPages, table, 1305)
	result, err := scanner.Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("Properties: %d rows", len(result.Rows))
	if len(result.Rows) != 6 {
		t.Errorf("expected 6 rows, got %d", len(result.Rows))
	}

	for i, row := range result.Rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

func TestTableScan_BlcModel(t *testing.T) {
	pr, totalPages := openSDF(t)

	table := &format.TableDef{
		Name: "BlcModel",
		Columns: []format.ColumnDef{
			{Name: "BlcModelIdentifier", TypeID: format.TypeUniqueIdentifier, Ordinal: 1},
			{Name: "RelationIdentifier", TypeID: format.TypeUniqueIdentifier, Ordinal: 2},
			{Name: "LoopIdentifier", TypeID: format.TypeUniqueIdentifier, Ordinal: 3},
			{Name: "ItemSequenceIdentifier", TypeID: format.TypeUniqueIdentifier, Ordinal: 4},
			{Name: "Representation", TypeID: format.TypeNVarchar, Ordinal: 5, MaxLength: 100},
			{Name: "IntendedMV", TypeID: format.TypeNVarchar, Ordinal: 6, MaxLength: 100},
			{Name: "IntendedModelLoopType", TypeID: format.TypeNVarchar, Ordinal: 7, MaxLength: 100},
			{Name: "Status", TypeID: format.TypeNVarchar, Ordinal: 8, MaxLength: 100},
			{Name: "BlcModelBlockId", TypeID: format.TypeUniqueIdentifier, Ordinal: 9},
		},
	}

	scanner := engine.NewTableScanner(pr, totalPages, table, 1395)
	result, err := scanner.Scan()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	t.Logf("BlcModel: %d rows", len(result.Rows))
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}

	for i, row := range result.Rows {
		// Show Representation and IntendedMV
		repr := ""
		if row[4] != nil {
			repr = row[4].(string)
		}
		mv := ""
		if row[5] != nil {
			mv = row[5].(string)
		}
		t.Logf("  Row %d: Repr=%q, MV=%q", i, repr, mv)
	}
}

func TestFindTableObjectIDs(t *testing.T) {
	pr, totalPages := openSDF(t)

	counts, err := engine.FindTableObjectIDs(pr, totalPages)
	if err != nil {
		t.Fatalf("FindTableObjectIDs: %v", err)
	}

	t.Logf("Found %d objectIDs with records", len(counts))

	// Check known objectIDs
	if c, ok := counts[1321]; ok {
		t.Logf("ObjectID 1321 (DataArrayTypes): %d records", c)
	}
	if c, ok := counts[1305]; ok {
		t.Logf("ObjectID 1305 (Properties): %d records", c)
	}
	if c, ok := counts[1395]; ok {
		t.Logf("ObjectID 1395 (BlcModel): %d records", c)
	}
}
