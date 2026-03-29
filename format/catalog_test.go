package format

import (
	"os"
	"sort"
	"testing"
)

func TestScanCatalogFindsTableNames(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize

	pr := NewPageReader(f, h, 128)

	entries, err := ScanCatalogNames(pr, totalPages)
	if err != nil {
		t.Fatalf("ScanCatalogNames: %v", err)
	}

	names := ExtractTableNames(entries)
	sort.Strings(names)

	t.Logf("Found %d table name candidates, %d catalog entries", len(names), len(entries))

	if len(names) < 10 {
		t.Fatalf("expected at least 10 table names, got %d", len(names))
	}

	knownTables := []string{
		"RTOInterface", "ProcessVariables", "TraceLog", "Blocks",
		"Loop", "VariableTransform", "ItemInformation", "Controllers",
		"SisoRelation", "EconomicFunction",
	}

	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}

	for _, kt := range knownTables {
		if !nameSet[kt] {
			t.Errorf("missing expected table: %s", kt)
		}
	}
}

func TestCatalogReadCatalog(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 128)

	cat, err := ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatalf("ReadCatalog: %v", err)
	}

	t.Logf("Catalog: %d tables", len(cat.Tables))

	// Verify known tables exist
	knownTables := []string{
		"RTOInterface", "ProcessVariables", "TraceLog", "Blocks",
		"Loop", "VariableTransform", "ItemInformation", "Controllers",
		"SisoRelation", "EconomicFunction",
	}
	for _, name := range knownTables {
		td := cat.TableByName(name)
		if td == nil {
			t.Errorf("missing table: %s", name)
			continue
		}
		if len(td.Columns) == 0 {
			t.Errorf("table %s has 0 columns", name)
		}
		t.Logf("  %s: %d columns", name, len(td.Columns))
	}
}

func TestCatalogBlocksColumns(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 128)

	cat, err := ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatalf("ReadCatalog: %v", err)
	}

	blocks := cat.TableByName("Blocks")
	if blocks == nil {
		t.Fatal("Blocks table not found")
	}

	t.Logf("Blocks table: %d columns", len(blocks.Columns))
	for _, col := range blocks.Columns {
		t.Logf("  ord=%d name=%-30s typeID=0x%02X maxLen=%d",
			col.Ordinal, col.Name, col.TypeID, col.MaxLength)
	}

	// Verify specific columns
	colByName := make(map[string]ColumnDef)
	for _, c := range blocks.Columns {
		colByName[c.Name] = c
	}

	// BlockIdentifier: uniqueidentifier (typeID=0x65, maxlen=16, ordinal=1)
	if c, ok := colByName["BlockIdentifier"]; ok {
		if c.TypeID != 0x65 {
			t.Errorf("BlockIdentifier typeID = 0x%02X, want 0x65", c.TypeID)
		}
		if c.MaxLength != 16 {
			t.Errorf("BlockIdentifier maxLen = %d, want 16", c.MaxLength)
		}
		if c.Ordinal != 1 {
			t.Errorf("BlockIdentifier ordinal = %d, want 1", c.Ordinal)
		}
	} else {
		t.Error("BlockIdentifier column not found")
	}

	// Name: nvarchar(32) (typeID=0x1F, maxlen=64, ordinal=2)
	if c, ok := colByName["Name"]; ok {
		if c.TypeID != 0x1F {
			t.Errorf("Name typeID = 0x%02X, want 0x1F", c.TypeID)
		}
		if c.MaxLength != 64 {
			t.Errorf("Name maxLen = %d, want 64", c.MaxLength)
		}
	} else {
		t.Error("Name column not found")
	}

	// ModelHorizonInSeconds: INT (typeID=0x03, maxlen=4, ordinal=5)
	if c, ok := colByName["ModelHorizonInSeconds"]; ok {
		if c.TypeID != 0x03 {
			t.Errorf("ModelHorizonInSeconds typeID = 0x%02X, want 0x03", c.TypeID)
		}
		if c.MaxLength != 4 {
			t.Errorf("ModelHorizonInSeconds maxLen = %d, want 4", c.MaxLength)
		}
	} else {
		t.Error("ModelHorizonInSeconds column not found")
	}

	// ResponsePlotType: smallint (typeID=0x02, maxlen=2)
	if c, ok := colByName["ResponsePlotType"]; ok {
		if c.TypeID != 0x02 {
			t.Errorf("ResponsePlotType typeID = 0x%02X, want 0x02", c.TypeID)
		}
	} else {
		t.Error("ResponsePlotType column not found")
	}
}


func TestCatalogColumnPosition(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	pr := NewPageReader(f, h, 128)
	cat, _ := ReadCatalog(pr, int(fi.Size())/h.PageSize)

	blocks := cat.TableByName("Blocks")
	if blocks == nil {
		t.Fatal("Blocks table not found")
	}

	colByName := make(map[string]ColumnDef)
	for _, c := range blocks.Columns {
		colByName[c.Name] = c
	}

	cases := []struct {
		name string
		pos  int
	}{
		{"BlockIdentifier", 0},
		{"Name", 0},
		{"Description", 1},
		{"UseSameHorizonAsModel", 0},
		{"ModelHorizonInSeconds", 16},
		{"PlotIntervalInSeconds", 20},
		{"ResponsePlotType", 32},
		{"FormulaString", 2},
		{"IsFormulaValid", 1},
		{"GainUpdateToleranceMin", 40},
		{"GainUpdateTolerancePercent", 48},
	}
	for _, tc := range cases {
		c, ok := colByName[tc.name]
		if !ok {
			t.Errorf("column %s not found", tc.name)
			continue
		}
		if c.Position != tc.pos {
			t.Errorf("%s Position = %d, want %d", tc.name, c.Position, tc.pos)
		}
	}
}

func TestCatalogRecoveredColumns(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	pr := NewPageReader(f, h, 128)
	cat, _ := ReadCatalog(pr, int(fi.Size())/h.PageSize)

	cases := []struct {
		table, column string
	}{
		{"Blocks", "IsFormulaValid"},
		{"SisoRelation", "SisoRelationIdentifier"},
		{"ControllerVariableReference", "ParentIdentifier"},
		{"ProcessVariables", "NormalMove"},
		{"CodeBlock", "ItemSequenceIdentifier"},
		{"EstimatorVariableSet", "Maintenance"},
		{"FitCharts", "ChartIdentifier"},
		{"Fits", "ClosedLoop"},
	}
	for _, tc := range cases {
		td := cat.TableByName(tc.table)
		if td == nil {
			t.Errorf("table %s not found", tc.table)
			continue
		}
		found := false
		for _, c := range td.Columns {
			if c.Name == tc.column {
				found = true
				t.Logf("  %s.%s: ordinal=%d typeID=0x%02X", tc.table, tc.column, c.Ordinal, c.TypeID)
			}
		}
		if !found {
			t.Errorf("MISSING: %s.%s", tc.table, tc.column)
		}
	}
}

func TestCatalogObjectMap(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 128)

	cat, err := ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("ObjectMap has %d entries", len(cat.ObjectMap))

	knownMappings := map[string]uint16{
		"Properties":                1305,
		"BlcModel":                  1395,
		"DataArrayTypes":            1321,
		"ExternalRuntimeDataSource": 1697,
	}

	for table, expectedObjID := range knownMappings {
		objIDs, ok := cat.ObjectMap[table]
		if !ok {
			t.Errorf("%s: not in ObjectMap", table)
			continue
		}
		if len(objIDs) == 0 || objIDs[0] != expectedObjID {
			t.Errorf("%s: objectIDs=%v, want [%d]", table, objIDs, expectedObjID)
		}
	}

	if len(cat.ObjectMap) < 50 {
		t.Errorf("expected at least 50 ObjectMap entries, got %d", len(cat.ObjectMap))
	}

	for name, objIDs := range cat.ObjectMap {
		t.Logf("  %s -> %v", name, objIDs)
	}
}
