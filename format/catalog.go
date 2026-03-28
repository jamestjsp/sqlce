package format

// Catalog Format Notes (Spike Findings)
//
// SQL CE stores metadata in system tables (__SysObjects, __SysColumns, etc.)
// within Leaf pages (type 0x40).
//
// Binary layout of a column catalog record:
//
//   Each record has a ~85-byte fixed header followed by two null-terminated
//   ASCII strings (table name, column/object name). Key fields are at fixed
//   offsets relative to the start of the name string:
//
//     name_offset - 66: u16 LE = Type ID (SQL CE internal type number)
//     name_offset - 64: u16 LE = Ordinal position (1-based column index)
//     name_offset - 46: u16 LE = Max length in bytes
//
//   Type ID mapping (discovered from sample data):
//     0x02 = smallint (2 bytes)
//     0x03 = int (4 bytes)
//     0x05 = float/real (4 bytes)
//     0x06 = float/double (8 bytes)
//     0x07 = datetime
//     0x0B = bit
//     0x1F = nvarchar (variable length, UTF-16)
//     0x65 = uniqueidentifier (16-byte GUID)

import (
	"encoding/binary"
	"sort"
	"strings"
)

// Catalog holds the parsed schema metadata for all tables in the database.
type Catalog struct {
	Tables    []TableDef
	ObjectMap map[string][]uint16
}

// TableDef describes a single table.
type TableDef struct {
	Name    string
	Columns []ColumnDef
}

// ColumnDef describes a single column within a table.
type ColumnDef struct {
	Name      string
	TypeID    uint16
	Ordinal   int // 1-based position
	MaxLength int // in bytes
}

// TableByName returns the table definition for the given name, or nil.
func (c *Catalog) TableByName(name string) *TableDef {
	for i := range c.Tables {
		if c.Tables[i].Name == name {
			return &c.Tables[i]
		}
	}
	return nil
}

// ReadCatalog scans the database and builds a Catalog of table/column definitions.
func ReadCatalog(pr *PageReader, totalPages int) (*Catalog, error) {
	colMap := make(map[struct{ table, column string }]*ColumnDef)
	tableSet := make(map[string]bool)

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		extractColumnRecords(page, colMap, tableSet)
	}

	// Group columns by table
	tableCols := make(map[string][]ColumnDef)
	for k, col := range colMap {
		tableCols[k.table] = append(tableCols[k.table], *col)
	}

	// Build sorted table list
	var tables []TableDef
	for name := range tableSet {
		cols := tableCols[name]
		sort.Slice(cols, func(i, j int) bool {
			return cols[i].Ordinal < cols[j].Ordinal
		})
		tables = append(tables, TableDef{Name: name, Columns: cols})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})

	objectMap := extractObjectMap(pr, totalPages)

	return &Catalog{Tables: tables, ObjectMap: objectMap}, nil
}

// Minimum bytes before a name string needed to read metadata fields.
const metadataPrefix = 66

func extractColumnRecords(page []byte, colMap map[struct{ table, column string }]*ColumnDef, tableSet map[string]bool) {
	n := len(page) - 64 // skip tail checksums

	i := 0x20 // skip page header
	for i < n {
		b := page[i]
		if !((b >= 'A' && b <= 'Z') || b == '_') {
			i++
			continue
		}

		// Read first null-terminated string (table name)
		start1 := i
		for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
			i++
		}
		if i >= n || page[i] != 0 {
			i = start1 + 1
			continue
		}
		tableName := string(page[start1:i])
		i++ // skip null

		// Read second null-terminated string (column name)
		if i >= n || page[i] < 32 || page[i] >= 127 {
			continue
		}
		start2 := i
		for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
			i++
		}
		if i >= n || page[i] != 0 {
			i = start2 + 1
			continue
		}
		colName := string(page[start2:i])
		i++

		if len(tableName) < 2 || len(colName) < 2 {
			continue
		}

		// Filter out non-table entries
		if isFilteredName(tableName) {
			continue
		}

		// Try to read metadata from fixed offsets before the name string
		le := binary.LittleEndian
		typeID := uint16(0)
		ordinal := 0
		maxLen := 0

		if start1 >= metadataPrefix {
			typeID = le.Uint16(page[start1-66:])
			ordinal = int(le.Uint16(page[start1-64:]))
			maxLen = int(le.Uint16(page[start1-46:]))
		}

		// Only accept column records with valid ordinals
		if ordinal < 1 || ordinal > 500 {
			continue
		}

		// Record table
		tableSet[tableName] = true

		key := struct{ table, column string }{tableName, colName}
		if _, exists := colMap[key]; !exists {
			colMap[key] = &ColumnDef{
				Name:      colName,
				TypeID:    typeID,
				Ordinal:   ordinal,
				MaxLength: maxLen,
			}
		}
	}
}

func isFilteredName(name string) bool {
	if len(name) > 4 {
		prefix := name[:4]
		if prefix == "PK__" || prefix == "FK__" || prefix == "DF__" || prefix == "UQ__" {
			return true
		}
	}
	if strings.ContainsRune(name, '/') {
		return true
	}
	if name == "Value" || name == "Case__000000000000092C" {
		return true
	}
	return false
}

// ScanCatalogNames scans all Leaf pages for table and column name pairs.
// Kept for backward compatibility; prefer ReadCatalog for structured data.
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

// CatalogEntry represents a table/column name pair found in catalog pages.
type CatalogEntry struct {
	TableName  string
	ColumnName string
	PageNum    int
	Offset     int
}

// ExtractTableNames returns deduplicated table names from catalog entries.
func ExtractTableNames(entries []CatalogEntry) []string {
	seen := make(map[string]bool)
	var names []string
	for _, e := range entries {
		if seen[e.TableName] || isFilteredName(e.TableName) || len(e.TableName) < 2 {
			continue
		}
		seen[e.TableName] = true
		names = append(names, e.TableName)
	}
	return names
}

// ScanDataPageTargets scans all Data (0x30) pages and returns a map from
// Data page objectID to the target Leaf objectID stored at offset 0x18.
// Entries with target 0 (empty tables) are excluded.
func ScanDataPageTargets(pr *PageReader, totalPages int) map[uint16]uint16 {
	targets := make(map[uint16]uint16)
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageData {
			continue
		}
		target := ParseDataPageTarget(page)
		if target != 0 {
			objID := PageObjectID(page)
			targets[objID] = target
		}
	}
	return targets
}

func findNamePairs(page []byte, pageNum int) []CatalogEntry {
	var results []CatalogEntry
	n := len(page) - 64

	i := 0x20
	for i < n {
		b := page[i]
		if (b >= 'A' && b <= 'Z') || b == '_' {
			start1 := i
			for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
				i++
			}
			if i >= n || page[i] != 0 {
				i = start1 + 1
				continue
			}
			name1 := string(page[start1:i])
			i++

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

const sysObjectsMarker = "__SysObjects\x00"
const sysObjectsObjIDOffset = 62

func extractObjectMap(pr *PageReader, totalPages int) map[string][]uint16 {
	dataTargets := ScanDataPageTargets(pr, totalPages)
	leafObjIDs := scanLeafObjectIDs(pr, totalPages)

	seen := make(map[string]bool)
	objectMap := make(map[string][]uint16)

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		extractSysObjectRecords(page, dataTargets, leafObjIDs, seen, objectMap)
	}

	return objectMap
}

func scanLeafObjectIDs(pr *PageReader, totalPages int) map[uint16]bool {
	ids := make(map[uint16]bool)
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		if page[0x14] > 0 {
			ids[PageObjectID(page)] = true
		}
	}
	return ids
}


func extractSysObjectRecords(page []byte, dataTargets map[uint16]uint16, leafObjIDs map[uint16]bool, seen map[string]bool, objectMap map[string][]uint16) {
	n := len(page) - 16

	for i := 0x20; i < n-len(sysObjectsMarker)-2; i++ {
		if string(page[i:i+len(sysObjectsMarker)]) != sysObjectsMarker {
			continue
		}

		nameStart := i + len(sysObjectsMarker)
		nameEnd := nameStart
		for nameEnd < n && page[nameEnd] != 0 && page[nameEnd] >= 32 && page[nameEnd] < 127 {
			nameEnd++
		}
		tableName := string(page[nameStart:nameEnd])
		if len(tableName) < 2 || seen[tableName] {
			continue
		}

		off := i - sysObjectsObjIDOffset
		if off < 0 || off+2 > len(page) {
			continue
		}

		dataObjID := binary.LittleEndian.Uint16(page[off : off+2])

		if leafObjID, hasLeaf := dataTargets[dataObjID]; hasLeaf {
			seen[tableName] = true
			objectMap[tableName] = []uint16{leafObjID}
		} else if leafObjIDs[dataObjID] {
			seen[tableName] = true
			objectMap[tableName] = []uint16{dataObjID}
		}
	}
}
