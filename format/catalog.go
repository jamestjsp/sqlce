package format

// __SysObjects is the single master catalog table in SQL CE 4.0.
// It stores tables, columns, indexes, and constraints as rows differentiated
// by ObjectType (field at fixed[0:2]):
//   ObjectType=1: Table definition (ObjectName=tableName, TablePageId at fixed[16:20])
//   ObjectType=4: Column definition (ObjectOwner=tableName, ObjectName=colName)
//   ObjectType=8: Constraint definition
//
// All catalog records have colCount=38 and share this fixed-data layout
// (offsets relative to start of fixed section, after 7-byte bitmap):
//   fixed[0:2]   ObjectType   (uint16 LE)
//   fixed[12:14]  ColumnType   (uint16 LE): SQL CE type ID
//   fixed[14:16]  ObjectOrdinal(uint16 LE): 1-based column ordinal
//   fixed[16:20]  TablePageId  (uint32 LE): data page ID for tables
//   fixed[32:34]  ColumnSize   (uint16 LE): max length in bytes
//   fixed[38:40]  ColumnPosition(uint16 LE): physical position within storage area
//
// Variable section (2 null-terminated ASCII strings starting 85 bytes after
// the bitmap, i.e. 93 bytes from record start):
//   ObjectOwner: parent name ("__SysObjects" for tables, tableName for columns)
//   ObjectName:  object name (tableName for tables, columnName for columns)

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
	Position  int
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

// sysObjColCount is the column count for __SysObjects records (38 columns).
const sysObjColCount = 38

// Fixed-section field offsets within __SysObjects records (from start of fixed data).
const (
	sysObjOffObjectType    = 0  // uint16: 1=Table, 4=Column
	sysObjOffColumnType    = 12 // uint16: SQL CE type ID
	sysObjOffObjectOrdinal = 14 // uint16: column ordinal (1-based)
	sysObjOffTablePageId   = 16 // uint32: data page ID for table records
	sysObjOffColumnSize     = 32 // uint16: max length in bytes
	sysObjOffColumnPosition = 38 // uint16: physical position within storage area
)

// Byte offset from bitmap start to variable data (strings).
// = 7 (bitmap) + 59 (fixed) + 19 (var header) = 85
const sysObjVarDataOffset = 85

// ReadCatalog parses the __SysObjects system catalog to build table/column definitions.
//
// DATA pages (type 0x40) use a slotted page format: a slot array at the page end
// gives each entry's offset and size, so records are extracted precisely without
// pattern-matching. This eliminates the page-boundary overflow problem.
func ReadCatalog(pr *PageReader, totalPages int) (*Catalog, error) {
	le := binary.LittleEndian

	type tableEntry struct {
		name   string
		pageID uint32
	}
	type columnEntry struct {
		table    string
		column   string
		typeID   uint16
		ordinal  int
		maxLen   int
		position int
	}

	var tables []tableEntry
	var columns []columnEntry
	seenTables := make(map[string]bool)
	seenCols := make(map[struct{ t, c string }]bool)

	// Build objectID → file page number mapping for Leaf pages.
	// nextChunk pointers use logical page IDs (= objectID at page[4:6]).
	objIDToFilePage := make(map[uint16]int)
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		objID := PageObjectID(page)
		if _, exists := objIDToFilePage[objID]; !exists {
			objIDToFilePage[objID] = pg
		}
	}

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}

		slots := readDataPageSlots(page)
		for _, slot := range slots {
			if slot.flags&1 != 0 {
				continue // free/empty
			}
			if slot.flags&2 == 0 {
				continue // continuation entry, handled via nextChunk
			}

			entry := slot.data
			if len(entry) < 4 {
				continue
			}
			// Follow nextChunk chain for multi-page records
			nextChunk := le.Uint32(entry[:4])
			if nextChunk != 0 {
				entry = followChunks(pr, entry, nextChunk, objIDToFilePage)
			}

			if len(entry) < 93+4 {
				continue
			}

			colCount := le.Uint32(entry[4:8])
			if colCount != sysObjColCount {
				continue
			}

			afterCC := 8
			fixedStart := afterCC + 7
			varStart := afterCC + sysObjVarDataOffset

			if varStart+4 > len(entry) {
				continue
			}

			objectType := le.Uint16(entry[fixedStart+sysObjOffObjectType:])
			owner, name := readTwoStrings(entry, varStart)
			if name == "" {
				continue
			}

			switch objectType {
			case 1: // Table
				if !seenTables[name] && !strings.HasPrefix(name, "__Sys") {
					seenTables[name] = true
					pageID := le.Uint32(entry[fixedStart+sysObjOffTablePageId:])
					tables = append(tables, tableEntry{name: name, pageID: pageID})
				}
			case 4: // Column
				if strings.HasPrefix(owner, "__Sys") {
					continue
				}
				key := struct{ t, c string }{owner, name}
				if !seenCols[key] {
					seenCols[key] = true
					columns = append(columns, columnEntry{
						table:    owner,
						column:   name,
						typeID:   le.Uint16(entry[fixedStart+sysObjOffColumnType:]),
						ordinal:  int(le.Uint16(entry[fixedStart+sysObjOffObjectOrdinal:])),
						maxLen:   int(le.Uint16(entry[fixedStart+sysObjOffColumnSize:])),
						position: int(le.Uint16(entry[fixedStart+sysObjOffColumnPosition:])),
					})
				}
			}
		}
	}

	// Build objectMap: tableName → leaf page objectIDs
	// Primary: read TABLE pages via PageMapping for deterministic resolution
	pm, pmErr := BuildPageMapping(pr)
	objectMap := make(map[string][]uint16, len(tables))
	if pmErr == nil {
		for _, t := range tables {
			if t.pageID == 0 {
				continue
			}
			ids := readTablePageLeafIDs(pr, pm, int(t.pageID))
			if len(ids) > 0 {
				objectMap[t.name] = ids
			}
		}
	}
	if len(objectMap) < len(tables) {
		parentToLeafIDs := scanLeafPagesByParent(pr, totalPages)
		for _, t := range tables {
			if _, ok := objectMap[t.name]; ok {
				continue
			}
			if leafIDs, ok := parentToLeafIDs[uint16(t.pageID)]; ok {
				objectMap[t.name] = leafIDs
			}
		}
	}

	// Group columns by table
	tableCols := make(map[string][]ColumnDef)
	for _, c := range columns {
		tableCols[c.table] = append(tableCols[c.table], ColumnDef{
			Name:      c.column,
			TypeID:    c.typeID,
			Ordinal:   c.ordinal,
			MaxLength: c.maxLen,
			Position:  c.position,
		})
	}

	// Build sorted table list
	var tableDefs []TableDef
	for _, t := range tables {
		cols := tableCols[t.name]
		sort.Slice(cols, func(i, j int) bool {
			return cols[i].Ordinal < cols[j].Ordinal
		})
		bmpExtra := computeNullBmpExtra(cols)
		tableDefs = append(tableDefs, TableDef{Name: t.name, Columns: cols, NullBmpExtra: bmpExtra})
	}
	sort.Slice(tableDefs, func(i, j int) bool {
		return tableDefs[i].Name < tableDefs[j].Name
	})

	return &Catalog{Tables: tableDefs, ObjectMap: objectMap}, nil
}

type dataSlot struct {
	data  []byte
	flags uint32
}

// readDataPageSlots extracts entries from a DATA/Leaf page using its slot array.
//
// DATA page layout:
//   [page header 16B][data header 8B][entries...][...slot array]
//
// The slot array grows backwards from the page end (4 bytes per slot).
// Each slot dword: offset[11:0], size[23:12], flags[31:24]
// Flag bit 0 = empty/free, flag bit 1 = start of new record.
func readDataPageSlots(page []byte) []dataSlot {
	if len(page) < 32 {
		return nil
	}
	le := binary.LittleEndian

	dword := le.Uint32(page[20:24])
	entriesCount := int(dword & 0xFFF)

	if entriesCount == 0 || entriesCount > 500 {
		return nil
	}

	var slots []dataSlot
	pageLen := len(page)

	for i := 0; i < entriesCount; i++ {
		slotPos := pageLen - 4 - 4*i
		if slotPos < 24 {
			break
		}
		sd := le.Uint32(page[slotPos:])
		entryOffset := int(sd & 0xFFF)
		entrySize := int((sd >> 12) & 0xFFF)
		flags := sd >> 24

		start := entryOffset + 24
		end := start + entrySize
		if start >= pageLen || end > pageLen || end <= start {
			continue
		}

		slots = append(slots, dataSlot{data: page[start:end], flags: flags})
	}
	return slots
}

// readDataPageEntries returns just entry data (for backward compat with record.go).
func readDataPageEntries(page []byte) [][]byte {
	slots := readDataPageSlots(page)
	entries := make([][]byte, 0, len(slots))
	for _, s := range slots {
		if s.flags&1 == 0 {
			entries = append(entries, s.data)
		}
	}
	return entries
}

// followChunks reassembles a multi-page record by following nextChunk pointers.
// nextChunk format: logicalPageId[31:12], entryIndex[11:0].
// objIDToFilePage maps logical page IDs (= objectIDs) to file page numbers.
func followChunks(pr *PageReader, firstEntry []byte, nextChunk uint32, objIDToFilePage map[uint16]int) []byte {
	buf := make([]byte, len(firstEntry))
	copy(buf, firstEntry)

	const maxRecordSize = 64 * 1024 // 64KB reasonable limit for multi-slot records
	visited := make(map[uint32]bool)

	for i := 0; i < 20 && nextChunk != 0; i++ {
		// Detect cycles in chunk chain
		if visited[nextChunk] {
			break
		}
		visited[nextChunk] = true

		logicalPageID := uint16(nextChunk >> 12)
		entryIdx := int(nextChunk & 0xFFF)

		filePage, ok := objIDToFilePage[logicalPageID]
		if !ok {
			break
		}

		page, err := pr.ReadPage(filePage)
		if err != nil {
			break
		}

		slots := readDataPageSlots(page)
		if entryIdx >= len(slots) {
			break
		}

		contData := slots[entryIdx].data
		if len(contData) < 4 {
			break
		}

		nextChunk = binary.LittleEndian.Uint32(contData[:4])
		
		// Prevent unbounded memory allocation
		if len(buf)+len(contData[4:]) > maxRecordSize {
			break
		}
		
		buf = append(buf, contData[4:]...)
	}
	return buf
}

// readTwoStrings reads two consecutive null-terminated ASCII strings from data at offset.
func readTwoStrings(data []byte, offset int) (string, string) {
	n := len(data)
	if offset >= n {
		return "", ""
	}

	end1 := offset
	for end1 < n && data[end1] != 0 {
		if data[end1] < 32 || data[end1] >= 127 {
			return "", ""
		}
		end1++
	}
	if end1 >= n || end1 == offset {
		return "", ""
	}
	s1 := string(data[offset:end1])
	end1++

	if end1 >= n || data[end1] < 32 || data[end1] >= 127 {
		return s1, ""
	}
	end2 := end1
	for end2 < n && data[end2] != 0 {
		if data[end2] < 32 || data[end2] >= 127 {
			return s1, ""
		}
		end2++
	}
	if end2 >= n || end2 == end1 {
		return s1, ""
	}
	return s1, string(data[end1:end2])
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

func isFilteredName(name string) bool {
	if len(name) > 4 {
		p := name[:4]
		if p == "PK__" || p == "FK__" || p == "DF__" || p == "UQ__" {
			return true
		}
	}
	return strings.ContainsRune(name, '/')
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

func readTablePageLeafIDs(pr *PageReader, pm *PageMapping, tablePageLogID int) []uint16 {
	fp, ok := pm.FilePageNum(tablePageLogID)
	if !ok {
		return nil
	}
	page, err := pr.ReadPage(fp)
	if err != nil || ClassifyPage(page) != PageData {
		return nil
	}
	if len(page) < 24 {
		return nil
	}

	le := binary.LittleEndian
	dataPageCount := int(le.Uint32(page[16:20]))
	flags := le.Uint32(page[20:24])

	listOffset := 24
	if flags == 1 {
		listOffset = 0x70
		if len(page) < listOffset {
			return nil
		}
		dataPageCount = int(le.Uint32(page[listOffset-8:]))
		flags = le.Uint32(page[listOffset-4:])
	}
	if flags != 0 || dataPageCount <= 0 || dataPageCount > 10000 {
		return nil
	}

	seen := make(map[uint16]bool)
	var ids []uint16
	for i := 0; i < dataPageCount; i++ {
		off := listOffset + (i/3)*8
		if off+8 > len(page) {
			break
		}
		qw := le.Uint64(page[off:])
		logID := int((qw >> (uint(i%3) * 20)) & 0xFFFFF)
		if logID == 0 {
			continue
		}

		leafFP, ok := pm.FilePageNum(logID)
		if !ok {
			continue
		}
		leafPage, err := pr.ReadPage(leafFP)
		if err != nil {
			continue
		}
		if ClassifyPage(leafPage) != PageLeaf {
			continue
		}
		objID := PageObjectID(leafPage)
		if !seen[objID] {
			seen[objID] = true
			ids = append(ids, objID)
		}
	}
	return ids
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

