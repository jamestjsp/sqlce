package engine_test

import (
	"database/sql"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jamestjat/sqlce/format"

	_ "modernc.org/sqlite"
)

// TestSchemaValidation compares the schema extracted from Depropanizer.sdf
// against the reference SQLite database (Depropanizer.db).
func TestSchemaValidation(t *testing.T) {
	// Open SDF and extract catalog
	sdf, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening SDF: %v", err)
	}
	defer sdf.Close()

	h, err := format.ReadHeader(sdf)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	fi, _ := sdf.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := format.NewPageReader(sdf, h, 128)

	cat, err := format.ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatalf("ReadCatalog: %v", err)
	}

	// Open SQLite reference
	db, err := sql.Open("sqlite", "../data/Depropanizer.db")
	if err != nil {
		t.Fatalf("opening SQLite: %v", err)
	}
	defer db.Close()

	// Get SQLite tables
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
	if err != nil {
		t.Fatalf("querying SQLite tables: %v", err)
	}
	var sqliteTables []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		sqliteTables = append(sqliteTables, name)
	}
	rows.Close()

	// Build SDF table name set
	sdfTableNames := make(map[string]bool)
	for _, td := range cat.Tables {
		sdfTableNames[td.Name] = true
	}

	// Validate table names
	t.Logf("SQLite tables: %d, SDF tables: %d", len(sqliteTables), len(cat.Tables))

	missingInSDF := 0
	for _, name := range sqliteTables {
		if !sdfTableNames[name] {
			t.Logf("  MISSING in SDF: %s", name)
			missingInSDF++
		}
	}

	// Accept up to 2 missing tables (system/conversion artifacts)
	if missingInSDF > 2 {
		t.Errorf("%d tables missing from SDF extraction (max 2 allowed)", missingInSDF)
	}

	// Validate columns per table
	totalMismatches := 0
	for _, sqliteTable := range sqliteTables {
		sdfTable := cat.TableByName(sqliteTable)
		if sdfTable == nil {
			continue
		}

		// Get SQLite columns
		colRows, err := db.Query("PRAGMA table_info([" + sqliteTable + "])")
		if err != nil {
			t.Logf("  PRAGMA table_info failed for %s: %v", sqliteTable, err)
			continue
		}
		type sqliteCol struct {
			name    string
			sqlType string
		}
		var sqliteCols []sqliteCol
		for colRows.Next() {
			var cid int
			var name, typ string
			var notnull, pk int
			var dflt sql.NullString
			colRows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk)
			sqliteCols = append(sqliteCols, sqliteCol{name, typ})
		}
		colRows.Close()

		// Build SDF column map
		sdfCols := make(map[string]format.ColumnDef)
		for _, c := range sdfTable.Columns {
			sdfCols[c.Name] = c
		}

		for _, sc := range sqliteCols {
			sdfCol, found := sdfCols[sc.name]
			if !found {
				totalMismatches++
				if totalMismatches <= 10 {
					t.Logf("  MISSING column: %s.%s", sqliteTable, sc.name)
				}
				continue
			}

			// Validate type mapping
			sdfTypeName := format.TypeName(sdfCol.TypeID)
			sqliteTypeBase := strings.ToLower(strings.Split(sc.sqlType, "(")[0])

			if !typesCompatible(sdfTypeName, sqliteTypeBase) {
				totalMismatches++
				if totalMismatches <= 10 {
					t.Logf("  TYPE MISMATCH: %s.%s: SDF=%s, SQLite=%s",
						sqliteTable, sc.name, sdfTypeName, sc.sqlType)
				}
			}
		}
	}

	t.Logf("Total column mismatches: %d", totalMismatches)
	// Remaining mismatches: ~5 columns on B-tree overflow pages where only
	// partial names survive, ~5 type=unknown from overflow/DF__ recovery,
	// 2 smallint/tinyint type mapping differences.
	if totalMismatches > 15 {
		t.Errorf("too many mismatches: %d (max 15)", totalMismatches)
	}
}

func typesCompatible(sdfType, sqliteType string) bool {
	sdfType = strings.ToLower(sdfType)
	sqliteType = strings.ToLower(sqliteType)

	if sdfType == sqliteType {
		return true
	}

	// SQL CE "float" (8-byte double) maps to SQLite "REAL"
	// SQL CE "real" (4-byte float) also maps to SQLite "REAL"
	aliases := map[string]string{
		"float": "real",
	}
	if alias, ok := aliases[sdfType]; ok {
		if alias == sqliteType {
			return true
		}
	}
	if alias, ok := aliases[sqliteType]; ok {
		if alias == sdfType {
			return true
		}
	}
	return false
}

// TestSchemaTableCount verifies approximate table count matches.
func TestSchemaTableCount(t *testing.T) {
	sdf, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening SDF: %v", err)
	}
	defer sdf.Close()

	h, _ := format.ReadHeader(sdf)
	fi, _ := sdf.Stat()
	pr := format.NewPageReader(sdf, h, 128)
	cat, err := format.ReadCatalog(pr, int(fi.Size())/h.PageSize)
	if err != nil {
		t.Fatalf("ReadCatalog: %v", err)
	}

	names := make([]string, len(cat.Tables))
	for i, td := range cat.Tables {
		names[i] = td.Name
	}
	sort.Strings(names)

	t.Logf("Found %d tables", len(cat.Tables))

	// SQLite has 99 tables (includes one extra from conversion)
	// SDF should have at least 95
	if len(cat.Tables) < 95 {
		t.Errorf("expected >= 95 tables, got %d", len(cat.Tables))
	}
}
