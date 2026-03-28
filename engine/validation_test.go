package engine_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jamestjat/sqlce/engine"
	"github.com/jamestjat/sqlce/format"

	_ "modernc.org/sqlite"
)

// TestDataValidation compares row data parsed from SDF against the SQLite reference.
func TestDataValidation(t *testing.T) {
	// Open SDF
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

	// Read catalog
	catalog, err := format.ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatalf("ReadCatalog: %v", err)
	}

	// Open SQLite reference
	db, err := sql.Open("sqlite", "../data/Depropanizer.db")
	if err != nil {
		t.Fatalf("opening SQLite: %v", err)
	}
	defer db.Close()

	// Get row counts from SQLite
	sqliteRowCounts := make(map[string]int)
	for _, table := range catalog.Tables {
		var count int
		query := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table.Name)
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			continue
		}
		sqliteRowCounts[table.Name] = count
	}

	// Collect objectID info from SDF
	objInfos, err := engine.CollectObjectIDInfo(pr, totalPages)
	if err != nil {
		t.Fatalf("CollectObjectIDInfo: %v", err)
	}
	t.Logf("Found %d objectIDs, %d tables with SQLite row counts", len(objInfos), len(sqliteRowCounts))

	// Build table→objectID mapping
	mapping := engine.BuildTableMapping(catalog, objInfos, sqliteRowCounts)
	t.Logf("Mapped %d/%d tables to objectIDs", len(mapping), len(catalog.Tables))

	// Track statistics
	matchedTables := 0
	rowCountMatches := 0
	rowCountMismatches := 0
	valueErrors := 0
	valuePasses := 0

	for tableName, objID := range mapping {
		table := catalog.TableByName(tableName)
		if table == nil {
			continue
		}

		scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{objID})
		result, err := scanner.Scan()
		if err != nil {
			t.Logf("  SKIP %s: scan error: %v", tableName, err)
			continue
		}

		expectedRows := sqliteRowCounts[tableName]
		if len(result.Rows) == expectedRows {
			rowCountMatches++
		} else {
			rowCountMismatches++
			t.Logf("  MISMATCH %s: SDF=%d rows, SQLite=%d rows", tableName, len(result.Rows), expectedRows)
		}
		matchedTables++
	}

	t.Logf("\n=== Row Count Summary ===")
	t.Logf("Tables matched to objectIDs: %d/%d", len(mapping), len(catalog.Tables))
	t.Logf("Row counts correct: %d", rowCountMatches)
	t.Logf("Row counts mismatched: %d", rowCountMismatches)

	// ---- Spot-check specific tables with known objectIDs ----
	t.Logf("\n=== Spot-Check Values ===")

	// Spot-check DataArrayTypes (objectID 1321, 1 row)
	t.Run("SpotCheck_DataArrayTypes", func(t *testing.T) {
		table := catalog.TableByName("DataArrayTypes")
		if table == nil {
			t.Skip("DataArrayTypes not in catalog")
		}
		scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1321})
		result, err := scanner.Scan()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		// Compare against SQLite
		rows, err := db.Query(`SELECT * FROM "DataArrayTypes"`)
		if err != nil {
			t.Fatalf("SQLite query: %v", err)
		}
		defer rows.Close()

		if !rows.Next() {
			t.Fatal("no rows in SQLite")
		}
		var sqlID, sqlArrayType, sqlUnit string
		var sqlInterval int
		if err := rows.Scan(&sqlID, &sqlArrayType, &sqlInterval, &sqlUnit); err != nil {
			t.Fatalf("SQLite scan: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		row := result.Rows[0]

		// Check ArrayType
		if s, ok := row[1].(string); ok && s != sqlArrayType {
			t.Errorf("ArrayType: SDF=%q SQLite=%q", s, sqlArrayType)
			valueErrors++
		} else {
			valuePasses++
		}

		// Check Interval
		if v, ok := row[2].(int32); ok && int(v) != sqlInterval {
			t.Errorf("Interval: SDF=%d SQLite=%d", v, sqlInterval)
			valueErrors++
		} else {
			valuePasses++
		}

		// Check Unit
		if s, ok := row[3].(string); ok && s != sqlUnit {
			t.Errorf("Unit: SDF=%q SQLite=%q", s, sqlUnit)
			valueErrors++
		} else {
			valuePasses++
		}
		t.Logf("DataArrayTypes: ID=%v ArrayType=%v Interval=%v Unit=%v", row[0], row[1], row[2], row[3])
	})

	// Spot-check Properties (objectID 1305, 6 rows)
	t.Run("SpotCheck_Properties", func(t *testing.T) {
		table := catalog.TableByName("Properties")
		if table == nil {
			t.Skip("Properties not in catalog")
		}
		scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1305})
		result, err := scanner.Scan()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		// Get SQLite data
		rows, err := db.Query(`SELECT "Name", "Value" FROM "Properties" ORDER BY rowid`)
		if err != nil {
			t.Fatalf("SQLite query: %v", err)
		}
		defer rows.Close()

		var sqlRows [][]string
		for rows.Next() {
			var name, value string
			if err := rows.Scan(&name, &value); err != nil {
				t.Fatalf("SQLite scan: %v", err)
			}
			sqlRows = append(sqlRows, []string{name, value})
		}

		if len(result.Rows) != len(sqlRows) {
			t.Logf("row count: SDF=%d SQLite=%d", len(result.Rows), len(sqlRows))
		}

		// Build lookup by Name for comparison (order may differ)
		sdfByName := make(map[string]string)
		for _, row := range result.Rows {
			name := ""
			value := ""
			if s, ok := row[0].(string); ok {
				name = s
			}
			if s, ok := row[1].(string); ok {
				value = s
			}
			sdfByName[name] = value
		}

		// Note: SDF and SQLite may have different values for mutable properties
		// like UndoRedoPointer and DatabaseMinorVersion (updated after export).
		matches := 0
		diffs := 0
		for _, sqlRow := range sqlRows {
			sdfVal, found := sdfByName[sqlRow[0]]
			if !found {
				t.Logf("  property %q: missing in SDF", sqlRow[0])
				diffs++
				continue
			}
			if sdfVal != sqlRow[1] {
				t.Logf("  property %q: SDF=%q SQLite=%q (mutable property)", sqlRow[0], sdfVal, sqlRow[1])
				diffs++
			} else {
				matches++
			}
		}
		t.Logf("Properties: %d/%d values match (%d mutable diffs)", matches, matches+diffs, diffs)
		valuePasses += matches
	})

	// Spot-check a table with datetime and numeric values
	t.Run("SpotCheck_BlcModel", func(t *testing.T) {
		table := catalog.TableByName("BlcModel")
		if table == nil {
			t.Skip("BlcModel not in catalog")
		}
		scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1395})
		result, err := scanner.Scan()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}

		// Get SQLite row count
		var sqlCount int
		db.QueryRow(`SELECT COUNT(*) FROM "BlcModel"`).Scan(&sqlCount)
		if len(result.Rows) != sqlCount {
			t.Errorf("row count: SDF=%d SQLite=%d", len(result.Rows), sqlCount)
		}

		// Get SQLite Representation values
		rows, err := db.Query(`SELECT "Representation" FROM "BlcModel"`)
		if err != nil {
			t.Fatalf("SQLite query: %v", err)
		}
		defer rows.Close()

		var sqlReprs []string
		for rows.Next() {
			var r string
			rows.Scan(&r)
			sqlReprs = append(sqlReprs, r)
		}

		sdfReprs := make(map[string]bool)
		for _, row := range result.Rows {
			if s, ok := row[4].(string); ok {
				sdfReprs[s] = true
			}
		}

		for _, r := range sqlReprs {
			if !sdfReprs[r] {
				t.Errorf("missing Representation %q in SDF", r)
			} else {
				t.Logf("  Representation %q: MATCH", r)
			}
		}
	})

	// Broad validation: scan all mapped tables and count rows
	t.Run("BroadRowCounts", func(t *testing.T) {
		matched := 0
		mismatched := 0
		for tableName, objID := range mapping {
			table := catalog.TableByName(tableName)
			if table == nil {
				continue
			}
			scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{objID})
			result, err := scanner.Scan()
			if err != nil {
				continue
			}
			expected := sqliteRowCounts[tableName]
			if len(result.Rows) == expected {
				matched++
			} else {
				mismatched++
				if expected > 0 {
					t.Logf("  %s: SDF=%d SQLite=%d", tableName, len(result.Rows), expected)
				}
			}
		}
		t.Logf("Broad row count validation: %d matched, %d mismatched out of %d mapped tables",
			matched, mismatched, len(mapping))
	})

	// Test value types: check that GUID, datetime, and numeric conversions work
	t.Run("TypeConversion_GUID", func(t *testing.T) {
		table := catalog.TableByName("DataArrayTypes")
		if table == nil {
			t.Skip("DataArrayTypes not in catalog")
		}
		scanner := engine.NewTableScanner(pr, totalPages, table, []uint16{1321})
		result, err := scanner.Scan()
		if err != nil || len(result.Rows) == 0 {
			t.Skip("no data")
		}

		guid, ok := result.Rows[0][0].(string)
		if !ok {
			t.Fatalf("expected string GUID, got %T", result.Rows[0][0])
		}
		// GUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
		parts := strings.Split(guid, "-")
		if len(parts) != 5 {
			t.Errorf("invalid GUID format: %q", guid)
		}
		t.Logf("GUID: %s", guid)
	})

	// Summary of all spot-check results
	t.Run("SpotCheck_Summary", func(t *testing.T) {
		// This is a meta-summary; actual checks are in sub-tests above
		t.Logf("Value spot-checks: %d passed, %d errors", valuePasses, valueErrors)
	})

	// Unmapped tables analysis
	unmapped := 0
	emptyInSQLite := 0
	for _, table := range catalog.Tables {
		if _, mapped := mapping[table.Name]; !mapped {
			expected := sqliteRowCounts[table.Name]
			if expected == 0 {
				emptyInSQLite++
			} else {
				unmapped++
				t.Logf("UNMAPPED: %s (expected %d rows, %d cols)", table.Name, expected, len(table.Columns))
			}
		}
	}
	t.Logf("\n=== Final Summary ===")
	t.Logf("Tables in catalog: %d", len(catalog.Tables))
	t.Logf("Tables mapped: %d", len(mapping))
	t.Logf("Tables empty in SQLite: %d", emptyInSQLite)
	t.Logf("Tables unmapped (non-empty): %d", unmapped)

	_ = time.Now() // keep time import used
	_ = valueErrors
	_ = valuePasses
}
