// Temporary bulk confidence test — exercises all available SDF databases.
// Remove once confidence is established.
package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func collectSDFs(dirs []string) []string {
	var files []string
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".sdf" {
				files = append(files, filepath.Join(dir, e.Name()))
			}
		}
	}
	return files
}

func TestBulkConfidence(t *testing.T) {
	files := collectSDFs([]string{
		"../data",
		"../data/testdbs",
		"../reference/SqlCeToolbox/src/API/SqlCeScripting40/Tests",
		"../reference/SqlCeToolbox/src/GUI/SSMSToolbox/Resources",
	})
	t.Logf("found %d SDF files", len(files))

	totalDBs := 0
	totalTables := 0
	totalScanned := 0
	totalRows := 0
	totalFailed := 0

	for _, path := range files {
		name := filepath.Base(path)
		t.Run(name, func(t *testing.T) {
			db, err := Open(path)
			if err != nil {
				t.Skipf("Open: %v", err)
				return
			}
			defer db.Close()

			h := db.Header()
			tables := db.Tables()
			t.Logf("%s  tables=%d  LCID=%d", h.VersionString(), len(tables), h.LCID)

			scanned := 0
			rows := 0
			failed := 0
			for _, tblName := range tables {
				tbl, err := db.Table(tblName)
				if err != nil {
					t.Logf("  ERR open %s: %v", tblName, err)
					failed++
					continue
				}
				result, err := tbl.Scan()
				if err != nil {
					t.Logf("  SKIP %s: %v", tblName, err)
					continue
				}
				scanned++
				rows += len(result.Rows)

				// Verify no panic on row access
				for _, row := range result.Rows {
					for range row {
					}
				}

				// Check for scan warnings
				if len(result.Warnings) > 0 {
					t.Logf("  WARN %s: %d warnings", tblName, len(result.Warnings))
				}
			}
			t.Logf("  scanned %d/%d tables, %d rows, %d errors", scanned, len(tables), rows, failed)

			totalDBs++
			totalTables += len(tables)
			totalScanned += scanned
			totalRows += rows
			totalFailed += failed
		})
	}

	t.Logf("\n=== BULK CONFIDENCE SUMMARY ===")
	t.Logf("Databases: %d", totalDBs)
	t.Logf("Tables found: %d", totalTables)
	t.Logf("Tables scanned: %d", totalScanned)
	t.Logf("Total rows: %d", totalRows)
	t.Logf("Errors: %d", totalFailed)
}
