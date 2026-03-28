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
	Name         string
	Columns      []ColumnDef
	NullBmpExtra int // extra null bitmap bytes beyond the header byte (0, 1, or 2)
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
	// Overflow records: name pairs found at the start of B-tree pages
	// where the metadata (at name_offset - 66) is on the preceding page.
	var overflow []struct{ table, column string }
	// DF__ columns: extracted from default constraint records, added only if
	// the column wasn't already found by the regular heuristic.
	var dfColumns []struct{ table, column string }

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		extractColumnRecords(page, colMap, tableSet, &dfColumns)
		extractOverflowRecords(page, colMap, tableSet, &overflow)
	}

	// Build a map of column name → known (typeID, maxLen) from successfully
	// parsed records, used to recover type info for overflow/DF__ records.
	colTypeLookup := make(map[string]*ColumnDef)
	for _, col := range colMap {
		if col.TypeID != 0 {
			if existing, ok := colTypeLookup[col.Name]; !ok || existing.TypeID == 0 {
				colTypeLookup[col.Name] = col
			}
		}
	}

	// Recover overflow records (page-start name pairs without metadata)
	recoverOverflowRecords(colMap, tableSet, overflow, colTypeLookup)

	// Add DF__-extracted columns only for those not already found
	for _, dc := range dfColumns {
		key := struct{ table, column string }{dc.table, dc.column}
		if _, exists := colMap[key]; !exists {
			tableSet[dc.table] = true
			var typeID uint16
			var maxLen int
			if ref, ok := colTypeLookup[dc.column]; ok {
				typeID = ref.TypeID
				maxLen = ref.MaxLength
			}
			colMap[key] = &ColumnDef{
				Name:      dc.column,
				TypeID:    typeID,
				Ordinal:   0,
				MaxLength: maxLen,
			}
		}
	}

	// Assign ordinals to any columns still missing them
	recoverMissingOrdinals(colMap)

	objectMap := extractObjectMap(pr, totalPages)

	// Infer types for columns with typeID=0 using data page record structure
	inferMissingTypes(pr, totalPages, colMap, objectMap)

	// Apply reference schema to fix remaining typeID=0 columns and add missing columns
	applyReferenceSchema(colMap, tableSet)

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
		bmpExtra := computeNullBmpExtra(cols)
		tables = append(tables, TableDef{Name: name, Columns: cols, NullBmpExtra: bmpExtra})
	}
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})

	return &Catalog{Tables: tables, ObjectMap: objectMap}, nil
}

// computeNullBmpExtra calculates the extra null bitmap bytes from column definitions.
// Bitmap layout: ceil(colCount/8) null-flag bytes + ceil(numBitCols/8) bit-value bytes.
// The first byte is always present (header), so extra = total - 1.
func computeNullBmpExtra(cols []ColumnDef) int {
	numBits := 0
	for _, c := range cols {
		if c.TypeID == TypeBit {
			numBits++
		}
	}
	total := (len(cols) + 7) / 8
	if numBits > 0 {
		total += (numBits + 7) / 8
	}
	if total < 1 {
		return 0
	}
	return total - 1
}

// extractOverflowRecords collects name pairs at the start of B-tree leaf pages
// where the record's metadata overflows from the preceding page. When a catalog
// B-tree record spans two pages, the variable data (table name + column name)
// may be split. We scan for readable ASCII strings near offset 0x18.
func extractOverflowRecords(page []byte, colMap map[struct{ table, column string }]*ColumnDef, _ map[string]bool, overflow *[]struct{ table, column string }) {
	n := len(page) - 64
	if n < 0x20 {
		return
	}

	// Scan for the first two null-terminated ASCII strings in the first ~100 bytes
	// of the data area. These may be [tableName\x00colName\x00] or
	// [partialTableTail\x00colName\x00] for overflow records.
	var strs []struct {
		start int
		value string
	}
	i := 0x1C
	limit := 0x18 + metadataPrefix + 40
	if limit > n {
		limit = n
	}
	for i < limit && len(strs) < 2 {
		b := page[i]
		if b >= 32 && b < 127 {
			start := i
			for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
				i++
			}
			if i < n && page[i] == 0 {
				s := string(page[start:i])
				if len(s) >= 2 {
					strs = append(strs, struct {
						start int
						value string
					}{start, s})
				}
				i++
				continue
			}
		}
		i++
	}

	if len(strs) < 2 {
		return
	}

	tableName := strs[0].value
	colName := strs[1].value

	// Filter: skip constraints (FK__, PK__, DF__) and system objects
	if isFilteredName(tableName) || isFilteredName(colName) {
		return
	}
	if strings.HasPrefix(tableName, "__Sys") || strings.HasPrefix(colName, "__Sys") {
		return
	}

	// Only collect if within the overflow zone (first ~66 bytes of data area)
	if strs[0].start >= 0x18+metadataPrefix {
		return
	}

	key := struct{ table, column string }{tableName, colName}
	if _, exists := colMap[key]; !exists {
		*overflow = append(*overflow, key)
	}
}

// recoverOverflowRecords fills in metadata for overflow name pairs by:
// 1. Resolving partial table names (overflow may split mid-name)
// 2. Matching the column name against known columns in other tables (for typeID/maxLen)
// 3. Finding the ordinal gap in the table's existing column sequence
func recoverOverflowRecords(colMap map[struct{ table, column string }]*ColumnDef, tableSet map[string]bool, overflow []struct{ table, column string }, colTypeLookup map[string]*ColumnDef) {
	// Build per-table ordinal sets from already-parsed columns
	tableOrdinals := make(map[string]map[int]bool)
	tableMaxOrdinal := make(map[string]int)
	for k, col := range colMap {
		if tableOrdinals[k.table] == nil {
			tableOrdinals[k.table] = make(map[int]bool)
		}
		tableOrdinals[k.table][col.Ordinal] = true
		if col.Ordinal > tableMaxOrdinal[k.table] {
			tableMaxOrdinal[k.table] = col.Ordinal
		}
	}

	// Collect known table names for partial-name resolution
	var knownTables []string
	for name := range tableSet {
		knownTables = append(knownTables, name)
	}

	for _, ov := range overflow {
		tableName := ov.table
		colName := ov.column

		// Resolve partial table name: if "lements" is a suffix of "FitParametricElements",
		// use the full name. Also handle the case where ov.table is actually the tail of
		// a table name and ov.column is the column name.
		if !tableSet[tableName] {
			resolved := resolvePartialTableName(tableName, knownTables, tableOrdinals, tableMaxOrdinal)
			if resolved != "" {
				tableName = resolved
			}
		}

		key := struct{ table, column string }{tableName, colName}
		if _, exists := colMap[key]; exists {
			continue
		}

		// Only recover columns for tables already known from the heuristic
		if !tableSet[tableName] {
			continue
		}

		// Determine typeID and maxLen from a column with the same name in another table
		var typeID uint16
		var maxLen int
		if ref, ok := colTypeLookup[colName]; ok {
			typeID = ref.TypeID
			maxLen = ref.MaxLength
		}

		// Find first missing ordinal for this table
		ordinal := 0
		ordinals := tableOrdinals[tableName]
		maxOrd := tableMaxOrdinal[tableName]
		for o := 1; o <= maxOrd+1; o++ {
			if !ordinals[o] {
				ordinal = o
				break
			}
		}
		if ordinal == 0 {
			ordinal = maxOrd + 1
		}

		colMap[key] = &ColumnDef{
			Name:      colName,
			TypeID:    typeID,
			Ordinal:   ordinal,
			MaxLength: maxLen,
		}

		if tableOrdinals[tableName] == nil {
			tableOrdinals[tableName] = make(map[int]bool)
		}
		tableOrdinals[tableName][ordinal] = true
		if ordinal > tableMaxOrdinal[tableName] {
			tableMaxOrdinal[tableName] = ordinal
		}
	}
}

// inferMissingTypes determines the typeID for columns with typeID=0 by examining
// the first record on the table's data pages. For each table with exactly one
// unknown-type column, it calculates total fixed bytes from the first data record
// and subtracts known fixed bytes to determine the missing column's size.
func inferMissingTypes(pr *PageReader, totalPages int, colMap map[struct{ table, column string }]*ColumnDef, objectMap map[string][]uint16) {
	tablesWithUnknown := make(map[string]bool)
	for k, col := range colMap {
		if col.TypeID == 0 {
			tablesWithUnknown[k.table] = true
		}
	}

	for tableName := range tablesWithUnknown {
		objIDs, ok := objectMap[tableName]
		if !ok || len(objIDs) == 0 {
			continue
		}

		var cols []ColumnDef
		var unknownCount int
		for k, col := range colMap {
			if k.table == tableName {
				cols = append(cols, *col)
				if col.TypeID == 0 {
					unknownCount++
				}
			}
		}
		if unknownCount != 1 {
			continue
		}

		// Sum known fixed column sizes
		knownFixed := 0
		var knownVarCount int
		for _, c := range cols {
			if c.TypeID == 0 {
				continue
			}
			ti := LookupType(c.TypeID)
			if ti.IsVariable {
				knownVarCount++
			} else {
				knownFixed += ti.FixedSize
			}
		}

		// Read the first record to determine total fixed size.
		// Parse with known-only columns, then see where the variable
		// section actually starts vs where it would start without the unknown column.
		totalFixed := measureFixedSize(pr, totalPages, objIDs, cols)
		if totalFixed <= knownFixed {
			continue
		}

		missingBytes := totalFixed - knownFixed
		typeID := sizeToTypeID(missingBytes)
		if typeID == 0 {
			continue
		}

		for k, col := range colMap {
			if k.table == tableName && col.TypeID == 0 {
				col.TypeID = typeID
				col.MaxLength = missingBytes
			}
		}
	}
}

// measureFixedSize finds the total fixed column bytes in the first record
// of a table by scanning for the variable section header pattern.
func measureFixedSize(pr *PageReader, totalPages int, objectIDs []uint16, cols []ColumnDef) int {
	idSet := make(map[uint16]bool, len(objectIDs))
	for _, id := range objectIDs {
		idSet[id] = true
	}

	totalCols := len(cols)
	var knownVarCount int
	for _, c := range cols {
		if c.TypeID == 0 {
			continue
		}
		ti := LookupType(c.TypeID)
		if ti.IsVariable {
			knownVarCount++
		}
	}
	if knownVarCount == 0 {
		return 0
	}
	// Try assuming the unknown column is fixed (most likely for recovered columns)
	varHeaderSize := 2*knownVarCount - 1

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		pt := ClassifyPage(page)
		if pt != PageLeaf && pt != PageData {
			continue
		}
		if !idSet[PageObjectID(page)] || page[0x14] == 0 {
			continue
		}

		offset := 0x18
		if offset+9 > len(page) {
			continue
		}
		offset += 4 // status
		offset += 4 // colCount
		header := page[offset]
		offset++

		if header != 0xF0 {
			nullBmpSize := (totalCols + 7) / 8
			offset += nullBmpSize
		}

		fixedStart := offset

		// The variable section starts after all fixed columns.
		// Its header has the pattern: [flag][endOff][flag][endOff]...[flag]
		// where flag is 0x80 (data) or 0x00 (null).
		// Try each possible fixed size and check if the var header pattern matches.
		for trySize := fixedStart; trySize < fixedStart+200 && trySize+varHeaderSize < len(page); trySize++ {
			pos := trySize
			valid := true
			for vi := 0; vi < knownVarCount; vi++ {
				flag := page[pos]
				if flag != 0x80 && flag != 0x00 {
					valid = false
					break
				}
				pos++
				if vi < knownVarCount-1 {
					pos++ // skip cumEnd byte
				}
			}
			if valid && pos-trySize == varHeaderSize {
				return trySize - fixedStart
			}
		}
	}
	return 0
}

// sizeToTypeID maps a fixed column byte size to the most likely SQL CE type.
func sizeToTypeID(size int) uint16 {
	switch size {
	case 1:
		return TypeBit
	case 2:
		return TypeSmallInt
	case 4:
		return TypeInt
	case 8:
		return TypeFloat // could also be bigint/datetime; float is most common
	case 16:
		return TypeUniqueIdentifier
	default:
		return 0
	}
}

// recoverMissingOrdinals assigns ordinals to columns with Ordinal=0 (from DF__
// constraint extraction or overflow recovery) by finding gaps in the table's
// ordinal sequence.
func recoverMissingOrdinals(colMap map[struct{ table, column string }]*ColumnDef) {
	// Build per-table ordinal sets
	tableOrdinals := make(map[string]map[int]bool)
	tableMaxOrdinal := make(map[string]int)
	var needsOrdinal []struct{ table, column string }

	for k, col := range colMap {
		if col.Ordinal == 0 {
			needsOrdinal = append(needsOrdinal, k)
			continue
		}
		if tableOrdinals[k.table] == nil {
			tableOrdinals[k.table] = make(map[int]bool)
		}
		tableOrdinals[k.table][col.Ordinal] = true
		if col.Ordinal > tableMaxOrdinal[k.table] {
			tableMaxOrdinal[k.table] = col.Ordinal
		}
	}

	for _, key := range needsOrdinal {
		ordinals := tableOrdinals[key.table]
		maxOrd := tableMaxOrdinal[key.table]
		ordinal := 0
		for o := 1; o <= maxOrd+1; o++ {
			if !ordinals[o] {
				ordinal = o
				break
			}
		}
		if ordinal == 0 {
			ordinal = maxOrd + 1
		}
		colMap[key].Ordinal = ordinal
		if tableOrdinals[key.table] == nil {
			tableOrdinals[key.table] = make(map[int]bool)
		}
		tableOrdinals[key.table][ordinal] = true
		if ordinal > tableMaxOrdinal[key.table] {
			tableMaxOrdinal[key.table] = ordinal
		}
	}
}

// resolvePartialTableName tries to match a partial table name suffix against
// known table names. When multiple tables match the suffix, disambiguates
// by checking which table has an ordinal gap (indicating a missing column).
func resolvePartialTableName(partial string, knownTables []string, tableOrdinals map[string]map[int]bool, tableMaxOrd map[string]int) string {
	if len(partial) < 3 {
		return ""
	}
	var matches []string
	for _, t := range knownTables {
		if strings.HasSuffix(t, partial) && len(partial) < len(t) {
			matches = append(matches, t)
		}
	}
	if len(matches) == 1 {
		return matches[0]
	}
	// Disambiguate: prefer the table with an ordinal gap
	if len(matches) > 1 {
		for _, t := range matches {
			ords := tableOrdinals[t]
			maxOrd := tableMaxOrd[t]
			for o := 1; o <= maxOrd; o++ {
				if !ords[o] {
					return t // has a gap — likely the one missing a column
				}
			}
		}
	}
	return ""
}

// Minimum bytes before a name string needed to read metadata fields.
const metadataPrefix = 66

func extractColumnRecords(page []byte, colMap map[struct{ table, column string }]*ColumnDef, tableSet map[string]bool, dfColumns *[]struct{ table, column string }) {
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

		// DF__ constraint records have 3 strings:
		//   tableName\x00DF__name\x00columnName\x00
		// The heuristic read (tableName, DF__name) as a pair. Read the third
		// string to get the actual column name this constraint applies to.
		isDefault := false
		if len(colName) > 4 && colName[:4] == "DF__" {
			if i < n && page[i] >= 32 && page[i] < 127 {
				start3 := i
				for i < n && page[i] != 0 && page[i] >= 32 && page[i] < 127 {
					i++
				}
				if i < n && page[i] == 0 {
					colName = string(page[start3:i])
					i++
					isDefault = true
				}
			}
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

		// For DF__ records: we read the third string (actual column name).
		// Collect for later processing (after the regular heuristic finishes).
		if isDefault {
			if len(colName) >= 2 && !isFilteredName(colName) {
				*dfColumns = append(*dfColumns, struct{ table, column string }{tableName, colName})
			}
			continue
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
	parentToLeafIDs := scanLeafPagesByParent(pr, totalPages)

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
		extractSysObjectRecords(page, parentToLeafIDs, seen, objectMap)
	}

	return objectMap
}

func scanLeafPagesByParent(pr *PageReader, totalPages int) map[uint16][]uint16 {
	groups := make(map[uint16]map[uint16]bool)
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf || page[0x14] == 0 {
			continue
		}
		parentID := binary.LittleEndian.Uint16(page[0x10:0x12])
		objID := PageObjectID(page)
		if groups[parentID] == nil {
			groups[parentID] = make(map[uint16]bool)
		}
		groups[parentID][objID] = true
	}
	result := make(map[uint16][]uint16, len(groups))
	for parentID, set := range groups {
		ids := make([]uint16, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		result[parentID] = ids
	}
	return result
}

func extractSysObjectRecords(page []byte, parentToLeafIDs map[uint16][]uint16, seen map[string]bool, objectMap map[string][]uint16) {
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

		if leafIDs, ok := parentToLeafIDs[dataObjID]; ok && len(leafIDs) > 0 {
			seen[tableName] = true
			objectMap[tableName] = leafIDs
		}
	}
}
