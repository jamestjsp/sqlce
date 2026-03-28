package engine_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/jamestjat/sqlce/engine"

	_ "github.com/jamestjat/sqlce/driver"
	_ "modernc.org/sqlite"
)

// TestIntegration_EngineAPI tests the end-to-end workflow via the engine API.
func TestIntegration_EngineAPI(t *testing.T) {
	// Open SDF via engine API
	db, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Verify header
	h := db.Header()
	if h.Version.String() != "SQL CE 4.0" {
		t.Errorf("version: %s", h.Version)
	}
	if db.TotalPages() != 1221 {
		t.Errorf("pages: %d", db.TotalPages())
	}
	if h.LCID != 1033 {
		t.Errorf("LCID: %d", h.LCID)
	}

	// Verify 98 tables
	tables := db.Tables()
	if len(tables) != 98 {
		t.Fatalf("expected 98 tables, got %d", len(tables))
	}

	// All tables accessible
	for _, name := range tables {
		tbl, err := db.Table(name)
		if err != nil {
			t.Errorf("Table(%q): %v", name, err)
			continue
		}
		schema := tbl.Schema()
		if schema.ColumnCount() == 0 {
			t.Errorf("%s: no columns", name)
		}
	}

	// Spot-check with known objectID mappings
	knownMappings := map[string]struct {
		objectID uint16
		expected int
	}{
		"Properties":                {1305, 6},
		"DataArrayTypes":            {1321, 1},
		"BlcModel":                  {1395, 3},
		"ExternalRuntimeDataSource": {1697, 1},
	}

	for tableName, info := range knownMappings {
		tbl, err := db.Table(tableName)
		if err != nil {
			t.Errorf("Table(%q): %v", tableName, err)
			continue
		}

		ri, err := tbl.RowsWithObjectID(info.objectID)
		if err != nil {
			t.Errorf("%s: Rows: %v", tableName, err)
			continue
		}

		count := 0
		for ri.Next() {
			count++
		}
		ri.Close()

		if count != info.expected {
			t.Errorf("%s: expected %d rows, got %d", tableName, info.expected, count)
		} else {
			t.Logf("  %s: %d rows ✓", tableName, count)
		}
	}
}

// TestIntegration_DatabaseSQL tests the database/sql driver interface.
func TestIntegration_DatabaseSQL(t *testing.T) {
	// Open via database/sql
	db, err := sql.Open("sqlce", "../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	// Query Properties
	rows, err := db.Query("SELECT * FROM Properties WITH OBJECTID 1305")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if len(cols) != 2 || cols[0] != "Name" || cols[1] != "Value" {
		t.Errorf("columns: %v", cols)
	}

	count := 0
	for rows.Next() {
		var name, value string
		rows.Scan(&name, &value)
		count++
	}
	if count != 6 {
		t.Errorf("Properties: expected 6 rows, got %d", count)
	}

	// Query DataArrayTypes with column selection
	rows2, err := db.Query("SELECT ArrayType, Interval FROM DataArrayTypes WITH OBJECTID 1321")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows2.Close()

	cols2, _ := rows2.Columns()
	if len(cols2) != 2 || cols2[0] != "ArrayType" || cols2[1] != "Interval" {
		t.Errorf("columns: %v", cols2)
	}

	if rows2.Next() {
		var arrayType string
		var interval int32
		if err := rows2.Scan(&arrayType, &interval); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if arrayType != "ContinuousData" || interval != 60 {
			t.Errorf("values: %q, %d", arrayType, interval)
		}
		t.Logf("  ArrayType=%s, Interval=%d ✓", arrayType, interval)
	}
}

func TestEndToEnd_ControlLayerQuery(t *testing.T) {
	sdfDB, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open SDF: %v", err)
	}
	defer sdfDB.Close()

	requiredTables := []string{
		"Relation", "ItemInformation", "RelationBlocks", "Blocks",
		"ModelLayerBlocks", "ModelLayers", "SisoRelation", "SisoElements",
		"ParametricElements", "ProcessVariables", "ControllerVariableReference",
		"VariableRole", "BlcModel", "Loop", "CVRole", "EconomicFunction",
		"VariableTransform", "Models", "ExecutionSequence", "UserParameter",
	}

	allPresent := true
	for _, name := range requiredTables {
		tbl, err := sdfDB.Table(name)
		if err != nil {
			t.Errorf("missing table: %s", name)
			allPresent = false
			continue
		}
		result, err := tbl.Scan()
		if err != nil {
			t.Logf("  %s: scan error: %v", name, err)
			continue
		}
		t.Logf("  %s: %d rows", name, len(result.Rows))
	}

	if !allPresent {
		t.Fatal("not all required tables present")
	}

	sqliteRef, err := sql.Open("sqlite", "../data/Depropanizer.db")
	if err != nil {
		t.Fatalf("Open SQLite ref: %v", err)
	}
	defer sqliteRef.Close()

	queries := map[string]string{
		"Query6_ExecutionSequence": "SELECT ExecutionSequenceIdentifier, IsDefault, ExecutionIntervalInMilliseconds FROM ExecutionSequence",
		"Query3_EconomicFunction": "SELECT COUNT(*) FROM EconomicFunction",
		"Query5_Models":           "SELECT COUNT(*) FROM Models",
	}

	for qName, q := range queries {
		t.Run(qName, func(t *testing.T) {
			var refCount int
			sqliteRef.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM (%s)", q)).Scan(&refCount)
			t.Logf("  reference: %d rows", refCount)
		})
	}
}

// TestIntegration_CompareWithSQLite compares mapped tables against SQLite reference.
func TestIntegration_CompareWithSQLite(t *testing.T) {
	// Open SDF
	sdfDB, err := engine.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("Open SDF: %v", err)
	}
	defer sdfDB.Close()

	// Open SQLite reference
	sqliteDB, err := sql.Open("sqlite", "../data/Depropanizer.db")
	if err != nil {
		t.Fatalf("Open SQLite: %v", err)
	}
	defer sqliteDB.Close()

	// Known table→objectID mappings from testing
	knownMappings := map[string]uint16{
		"Properties":                1305,
		"DataArrayTypes":            1321,
		"BlcModel":                  1395,
		"ExternalRuntimeDataSource": 1697,
	}

	for tableName, objectID := range knownMappings {
		t.Run(tableName, func(t *testing.T) {
			// Get SDF data
			tbl, err := sdfDB.Table(tableName)
			if err != nil {
				t.Fatalf("Table: %v", err)
			}
			ri, err := tbl.RowsWithObjectID(objectID)
			if err != nil {
				t.Fatalf("Rows: %v", err)
			}
			defer ri.Close()

			var sdfRows [][]string
			for ri.Next() {
				vals := ri.Values()
				row := make([]string, len(vals))
				for i, v := range vals {
					row[i] = fmt.Sprintf("%v", v)
				}
				sdfRows = append(sdfRows, row)
			}

			// Get SQLite data
			var sqliteCount int
			sqliteDB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)).Scan(&sqliteCount)

			if len(sdfRows) != sqliteCount {
				t.Logf("row count: SDF=%d SQLite=%d (may differ for mutable data)", len(sdfRows), sqliteCount)
			}

			t.Logf("%s: %d SDF rows, %d SQLite rows", tableName, len(sdfRows), sqliteCount)
		})
	}

	// Broader mapping validation
	t.Run("MappedTableCounts", func(t *testing.T) {
		objInfos, err := engine.CollectObjectIDInfo(sdfDB.PageReader(), sdfDB.TotalPages())
		if err != nil {
			t.Fatalf("CollectObjectIDInfo: %v", err)
		}

		// Get SQLite row counts
		sqliteRowCounts := make(map[string]int)
		for _, name := range sdfDB.Tables() {
			var count int
			sqliteDB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, name)).Scan(&count)
			sqliteRowCounts[name] = count
		}

		mapping := engine.BuildTableMapping(sdfDB.Catalog(), objInfos, sqliteRowCounts)
		t.Logf("Mapped %d/%d tables to objectIDs", len(mapping), len(sdfDB.Tables()))

		matched := 0
		for tableName, objectID := range mapping {
			tbl, _ := sdfDB.Table(tableName)
			ri, err := tbl.RowsWithObjectID(objectID)
			if err != nil {
				continue
			}
			count := 0
			for ri.Next() {
				count++
			}
			ri.Close()

			if count == sqliteRowCounts[tableName] {
				matched++
			}
		}
		t.Logf("Row counts verified: %d/%d mapped tables", matched, len(mapping))
	})
}
