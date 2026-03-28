package format

// Catalog Format Notes (Spike Findings)
//
// SQL CE stores metadata in system tables (__SysObjects, __SysColumns, etc.)
// that are themselves stored in Leaf pages (type 0x40).
//
// Binary Layout Observations:
//
//   Page Header (bytes 0x00-0x1F):
//     0x00-0x03: Page checksum/hash (4 bytes, varies per page)
//     0x04-0x05: Object ID (u16 LE) — identifies which table owns this page
//     0x06:      Page type byte (0x40 for Leaf, 0x30 for Data, etc.)
//     0x07:      Always 0x00
//     0x08-0x0B: Unknown (u32 LE) — possibly next page pointer
//     0x0C-0x0F: Reserved (zeros)
//     0x10-0x13: Unknown (u32 LE) — e.g. 0x0403 on catalog pages
//     0x14-0x17: Flags/pointers
//     0x18-0x1B: Padding or reserved
//     0x1C-0x1F: Record count or variable (e.g. "Value" string on pages 1-2)
//
//   Record Area (bytes 0x20 onward):
//     Records are variable-length, containing:
//     - Fixed header (~85 bytes) starting with marker bytes (often 80 1F 40 FF FF)
//     - Table name as null-terminated ASCII string
//     - Column/object name as null-terminated ASCII string
//     - Padding to alignment boundary
//
//   Page Tail:
//     Last ~64 bytes may contain hash/checksum data (not a simple slot array)
//
//   Key Pages:
//     Pages 1-2: Root catalog pages (contain "Value" at offset 0x1C)
//     Pages 3-5+: Catalog data pages with table definitions
//     System object IDs: 0x0001 (AllocationMap), 0x0002 (SpaceMap), 0x0403+ (user objects)
//
//   System Tables:
//     __SysObjects — table definitions (found extensively in Leaf pages)
//     __SysColumns — column definitions
//     __SysIndexes — index definitions
//     __SysConstraints — constraint definitions

import (
	"bytes"
)

// CatalogEntry represents a table/column name pair found in catalog pages.
type CatalogEntry struct {
	TableName  string
	ColumnName string
	PageNum    int
	Offset     int
}

// ScanCatalogNames scans all Leaf pages in the database for table and column
// name pairs. This is a brute-force approach based on spike analysis; it finds
// null-terminated ASCII string pairs in Leaf (0x40) page record areas.
//
// Returns deduplicated entries keyed by (TableName, ColumnName).
func ScanCatalogNames(pr *PageReader, totalPages int) ([]CatalogEntry, error) {
	seen := make(map[[2]string]bool)
	var entries []CatalogEntry

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}

		// Scan for ASCII string pairs: tableName\0columnName\0
		found := findNamePairs(page, pg)
		for _, e := range found {
			key := [2]string{e.TableName, e.ColumnName}
			if !seen[key] {
				seen[key] = true
				entries = append(entries, e)
			}
		}
	}
	return entries, nil
}

// ExtractTableNames returns a sorted deduplicated list of table names found
// in catalog pages. Filters out constraint names (PK__, FK__, DF__, UQ__)
// and internal markers.
func ExtractTableNames(entries []CatalogEntry) []string {
	seen := make(map[string]bool)
	var names []string

	for _, e := range entries {
		name := e.TableName
		if seen[name] {
			continue
		}
		// Filter out constraint/index names
		if len(name) > 4 && (name[:4] == "PK__" || name[:4] == "FK__" || name[:4] == "DF__" || name[:4] == "UQ__") {
			continue
		}
		// Filter out names with slashes (data values, not table names)
		if bytes.ContainsRune([]byte(name), '/') {
			continue
		}
		// Filter out very short names or all-hex names
		if len(name) < 2 {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names
}

func findNamePairs(page []byte, pageNum int) []CatalogEntry {
	var results []CatalogEntry
	n := len(page) - 64 // skip tail area

	i := 0x20 // skip page header
	for i < n {
		// Look for the start of a table name (uppercase letter or _)
		b := page[i]
		if (b >= 'A' && b <= 'Z') || b == '_' {
			// Read first string (table name candidate)
			start1 := i
			for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
				i++
			}
			if i >= n || page[i] != 0 {
				i = start1 + 1
				continue
			}
			name1 := string(page[start1:i])
			i++ // skip null

			// Read second string (column name candidate)
			if i < n && page[i] >= 32 && page[i] < 127 {
				start2 := i
				for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
					i++
				}
				if i < n && page[i] == 0 {
					name2 := string(page[start2:i])
					if len(name1) >= 2 && len(name2) >= 2 {
						results = append(results, CatalogEntry{
							TableName:  name1,
							ColumnName: name2,
							PageNum:    pageNum,
							Offset:     start1,
						})
					}
				}
			}
			i++
		} else {
			i++
		}
	}
	return results
}
