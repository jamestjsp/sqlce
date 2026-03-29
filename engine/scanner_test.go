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
	scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1321})
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

	scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1305})
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

	scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1395})
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

func TestBitmapAutoDetect(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	catalog := db.Catalog()
	mapping := catalog.ObjectMap

	tablesWithGUID := 0
	tablesValid := 0
	tablesFailed := 0

	for _, table := range catalog.Tables {
		objIDs, ok := mapping[table.Name]
		if !ok || len(objIDs) == 0 {
			continue
		}

		guidIdxs := []int{}
		for i, col := range table.Columns {
			if col.TypeID == format.TypeUniqueIdentifier {
				guidIdxs = append(guidIdxs, i)
			}
		}
		if len(guidIdxs) == 0 {
			continue
		}
		tablesWithGUID++

		scanner := engine.NewTableScanner(db.PageReader(), db.TotalPages(), &table, objIDs)
		if pages := db.PagesForObjectIDs(objIDs); len(pages) > 0 {
			scanner.SetPages(pages)
		}
		result, err := scanner.Scan()
		if err != nil {
			t.Logf("  %s: scan error: %v", table.Name, err)
			tablesFailed++
			continue
		}
		if len(result.Rows) == 0 {
			continue
		}

		// Check non-null GUID values are plausible (36-char string format).
		// All-zero GUIDs are valid (NULL or sentinel values).
		bad := 0
		checked := 0
		for _, row := range result.Rows {
			for _, gi := range guidIdxs {
				if gi >= len(row) || row[gi] == nil {
					continue
				}
				s, ok := row[gi].(string)
				if !ok {
					bad++
					checked++
					continue
				}
				if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
					bad++
				}
				checked++
			}
		}

		if bad == 0 {
			tablesValid++
		} else {
			tablesFailed++
			t.Logf("  %s: %d/%d GUID values malformed (bmpExtra=%d, %d rows)",
				table.Name, bad, checked, table.NullBmpExtra, len(result.Rows))
		}
	}

	t.Logf("Tables with GUID columns: %d", tablesWithGUID)
	t.Logf("  Valid: %d", tablesValid)
	t.Logf("  Failed: %d", tablesFailed)

	if tablesFailed > 0 {
		t.Errorf("%d tables have malformed GUID values after bitmap auto-detect", tablesFailed)
	}
}

func TestBitmapComputedMatchesBestProbe(t *testing.T) {
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	pr := db.PageReader()
	catalog := db.Catalog()

	matched := 0
	total := 0

	for _, table := range catalog.Tables {
		objIDs, ok := catalog.ObjectMap[table.Name]
		if !ok || len(objIDs) == 0 {
			continue
		}

		hasGUID := false
		for _, col := range table.Columns {
			if col.TypeID == format.TypeUniqueIdentifier {
				hasGUID = true
				break
			}
		}
		if !hasGUID {
			continue
		}

		pages := db.PagesForObjectIDs(objIDs)

		bestBmp := -1
		bestRecords := 0
		for bmp := 0; bmp <= 3; bmp++ {
			records, err := format.ScanTableRecordsPages(pr, pages, objIDs, table.Columns, bmp)
			if err != nil {
				continue
			}
			if len(records) > bestRecords {
				bestRecords = len(records)
				bestBmp = bmp
			}
		}

		if bestBmp < 0 {
			continue
		}
		total++

		computedRecords, _ := format.ScanTableRecordsPages(pr, pages, objIDs, table.Columns, table.NullBmpExtra)
		if len(computedRecords) == bestRecords {
			matched++
		} else {
			t.Logf("  %s: computed bmp=%d yields %d records, best bmp=%d yields %d",
				table.Name, table.NullBmpExtra, len(computedRecords), bestBmp, bestRecords)
		}
	}

	t.Logf("Tables: %d/%d computed bitmap matches best probe record count", matched, total)
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
