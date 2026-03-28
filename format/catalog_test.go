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

	// Must find at least 10 table names (spike acceptance criteria)
	if len(names) < 10 {
		t.Fatalf("expected at least 10 table names, got %d", len(names))
	}

	// Verify known tables from the SQLite reference DB
	knownTables := []string{
		"RTOInterface",
		"ProcessVariables",
		"TraceLog",
		"Blocks",
		"Loop",
		"VariableTransform",
		"ItemInformation",
		"Controllers",
		"SisoRelation",
		"EconomicFunction",
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

	t.Logf("Table names found:")
	for i, n := range names {
		if i < 30 {
			t.Logf("  %s", n)
		}
	}
	if len(names) > 30 {
		t.Logf("  ... and %d more", len(names)-30)
	}
}
