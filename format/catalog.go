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
	Tables      []TableDef
	Indexes     []IndexDef
	Constraints []ConstraintDef
	ObjectMap   map[string][]uint16
}

// IndexDef describes an index on a table.
type IndexDef struct {
	Table      string
	Name       string
	Root       uint32 // page ID of index B-tree root
	Unique     bool
	NullOption uint16
}

// ConstraintDef describes a constraint on a table.
type ConstraintDef struct {
	Table       string
	Name        string
	Type        uint32 // constraint type code
	OnDelete    uint32 // referential action on delete
	OnUpdate    uint32 // referential action on update
	IndexName   string // associated index name
	TargetTable string // target table for foreign keys
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
	Precision uint8
	Scale     uint8
	AutoType  uint16 // non-zero for IDENTITY columns
	Nullable  bool
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
	sysObjOffObjectType      = 0  // uint16: 1=Table, 4=Column
	sysObjOffColumnType      = 12 // uint16: SQL CE type ID (lower 16 bits of ObjectCedbInfo)
	sysObjOffObjectOrdinal   = 14 // uint16: column ordinal (1-based)
	sysObjOffTablePageId     = 16 // uint32: data page ID for table records
	sysObjOffColumnSize      = 32 // uint16: max length in bytes
	sysObjOffColumnPrecision = 34 // uint8: numeric precision
	sysObjOffColumnScale     = 35 // uint8: numeric scale
	sysObjOffColumnAutoType  = 36 // uint16: auto-increment type (non-zero = IDENTITY)
	sysObjOffColumnPosition  = 38 // uint16: physical position within storage area
	sysObjOffIndexRoot       = 40 // uint32: index B-tree root page ID
	sysObjOffIndexNullOption = 44 // uint16: null handling option
	sysObjOffConstraintType  = 46 // uint32: constraint type code
	sysObjOffConstraintOnDel = 50 // uint32: referential action on delete
	sysObjOffConstraintOnUpd = 54 // uint32: referential action on update
)

// Byte offset from bitmap start to variable data (strings).
// = 7 (bitmap) + 59 (fixed) + 19 (var header) = 85
const sysObjVarDataOffset = 85

const (
	maxDataPageSlots         = 4095
	maxRecordChunkHops       = 20
	maxReassembledRecordSize = 64 * 1024
)

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
		table     string
		column    string
		typeID    uint16
		ordinal   int
		maxLen    int
		position  int
		precision uint8
		scale     uint8
		autoType  uint16
		nullable  bool
	}

	var tables []tableEntry
	var columns []columnEntry
	var indexes []IndexDef
	var constraints []ConstraintDef
	seenTables := make(map[string]bool)
	seenCols := make(map[struct{ t, c string }]bool)
	seenConstraints := make(map[struct{ t, c string }]bool)

	// Resolve nextChunk pointers to their continuation pages via the page
	// map. System catalog records occasionally span multiple slots on the
	// same page; multi-page user tables always need the page map to find
	// the right continuation.
	pmChunks, _ := BuildPageMapping(pr)

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
			// Follow nextChunk chain for multi-slot records
			nextChunk := le.Uint32(entry[:4])
			if nextChunk != 0 {
				entry = followChunks(pr, entry, nextChunk, pmChunks)
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
					// Bit values section: entry[afterCC+5..afterCC+7]
					// ColumnNullable is bit 4 (5th bit field in __SysObjects schema)
					nullable := len(entry) > afterCC+5 && entry[afterCC+5]&0x10 != 0
					columns = append(columns, columnEntry{
						table:     owner,
						column:    name,
						typeID:    le.Uint16(entry[fixedStart+sysObjOffColumnType:]),
						ordinal:   int(le.Uint16(entry[fixedStart+sysObjOffObjectOrdinal:])),
						maxLen:    int(le.Uint16(entry[fixedStart+sysObjOffColumnSize:])),
						position:  int(le.Uint16(entry[fixedStart+sysObjOffColumnPosition:])),
						precision: entry[fixedStart+sysObjOffColumnPrecision],
						scale:     entry[fixedStart+sysObjOffColumnScale],
						autoType:  le.Uint16(entry[fixedStart+sysObjOffColumnAutoType:]),
						nullable:  nullable,
					})
				}
			case 8: // Constraint/Index
				if strings.HasPrefix(owner, "__Sys") {
					continue
				}
				key := struct{ t, c string }{owner, name}
				if seenConstraints[key] {
					continue
				}
				seenConstraints[key] = true

				if fixedStart+sysObjOffConstraintOnUpd+4 > len(entry) {
					continue
				}

				cType := le.Uint32(entry[fixedStart+sysObjOffConstraintType:])
				indexRoot := le.Uint32(entry[fixedStart+sysObjOffIndexRoot:])
				indexUnique := len(entry) > afterCC+5 && entry[afterCC+5]&0x80 != 0

				if indexRoot != 0 {
					indexes = append(indexes, IndexDef{
						Table:      owner,
						Name:       name,
						Root:       indexRoot,
						Unique:     indexUnique,
						NullOption: le.Uint16(entry[fixedStart+sysObjOffIndexNullOption:]),
					})
				}

				if cType != 0 {
					cd := ConstraintDef{
						Table:    owner,
						Name:     name,
						Type:     cType,
						OnDelete: le.Uint32(entry[fixedStart+sysObjOffConstraintOnDel:]),
						OnUpdate: le.Uint32(entry[fixedStart+sysObjOffConstraintOnUpd:]),
					}
					strs := readStrings(entry, varStart, 8)
					if len(strs) >= 4 {
						cd.IndexName = strs[2]
					}
					if len(strs) >= 6 {
						cd.TargetTable = strs[5]
					}
					constraints = append(constraints, cd)
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
			Precision: c.precision,
			Scale:     c.scale,
			AutoType:  c.autoType,
			Nullable:  c.nullable,
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

	return &Catalog{Tables: tableDefs, Indexes: indexes, Constraints: constraints, ObjectMap: objectMap}, nil
}

type dataSlot struct {
	data  []byte
	flags uint32
}

// readDataPageSlots extracts entries from a DATA/Leaf page using its slot array.
//
// DATA page layout:
//
//	[page header 16B][data header 8B][entries...][...slot array]
//
// The slot array grows backwards from the page end (4 bytes per slot).
// Each slot dword: offset[11:0], size[23:12], flags[31:24]
// Flag bit 0 = empty/free, flag bit 1 = start of new record.
//
// Empty/invalid slots are dropped from the returned slice. Callers that
// need to look up a slot by its raw array index (e.g. nextChunk
// pointers) must use readDataPageSlotAt so that filtering does not
// renumber the surviving slots.
func readDataPageSlots(page []byte) []dataSlot {
	all := readDataPageSlotsRaw(page)
	if all == nil {
		return nil
	}
	slots := make([]dataSlot, 0, len(all))
	for _, s := range all {
		if s.data != nil {
			slots = append(slots, s)
		}
	}
	return slots
}

// readDataPageSlotsRaw returns every slot in the slot array positionally,
// with data set to nil for slots that are empty or have an invalid
// offset/size. Callers that resolve slot indices from on-disk pointers
// (nextChunk, overflow pointers) must use this function — collapsing
// empty slots would shift real slot indices down and break lookups.
func readDataPageSlotsRaw(page []byte) []dataSlot {
	if len(page) < 32 {
		return nil
	}
	le := binary.LittleEndian

	dword := le.Uint32(page[20:24])
	entriesCount := int(dword & 0xFFF)

	if entriesCount == 0 || entriesCount > maxDataPageSlots {
		return nil
	}

	slots := make([]dataSlot, 0, entriesCount)
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
			slots = append(slots, dataSlot{flags: flags})
			continue
		}

		slots = append(slots, dataSlot{data: page[start:end], flags: flags})
	}
	return slots
}

// readDataPageSlotAt returns the slot at the given raw array index on a
// data/leaf page, or a zero-valued dataSlot (with data=nil) if the slot
// is empty, invalid, or out of range.
func readDataPageSlotAt(page []byte, rawIdx int) dataSlot {
	all := readDataPageSlotsRaw(page)
	if rawIdx < 0 || rawIdx >= len(all) {
		return dataSlot{}
	}
	return all[rawIdx]
}

// followChunks reassembles a multi-slot record by following nextChunk
// pointers. The pointer format is:
//
//	bits 0..11  entry index within the target page's slot array
//	bits 12..31 logical page ID (not an objectID; resolved via PageMapping)
//
// Historically this used an objectID→file-page map, which worked only when
// a table fit on a single page. User tables that span multiple physical
// pages share an objectID across pages but each has its own logical page
// ID, so the objectID-based lookup could not disambiguate the continuation
// page. The PageMapping resolves the logical ID to the correct file page.
//
// pm may be nil; in that case the function returns the first chunk only.
func followChunks(pr *PageReader, firstEntry []byte, nextChunk uint32, pm *PageMapping) []byte {
	buf := make([]byte, len(firstEntry))
	copy(buf, firstEntry)

	if pm == nil {
		return buf
	}

	visited := make(map[uint32]bool)

	for i := 0; i < maxRecordChunkHops && nextChunk != 0; i++ {
		if visited[nextChunk] {
			break
		}
		visited[nextChunk] = true

		logicalPageID := int(nextChunk >> 12)
		entryIdx := int(nextChunk & 0xFFF)

		filePage, ok := pm.FilePageNum(logicalPageID)
		if !ok {
			break
		}

		page, err := pr.ReadPage(filePage)
		if err != nil {
			break
		}

		slot := readDataPageSlotAt(page, entryIdx)
		contData := slot.data
		if len(contData) < 4 {
			break
		}

		nextChunk = binary.LittleEndian.Uint32(contData[:4])

		if len(buf)+len(contData[4:]) > maxReassembledRecordSize {
			break
		}

		buf = append(buf, contData[4:]...)
	}
	return buf
}

// readStrings reads up to max consecutive null-terminated ASCII strings from data at offset.
func readStrings(data []byte, offset, max int) []string {
	n := len(data)
	var result []string
	pos := offset
	for len(result) < max && pos < n {
		start := pos
		for pos < n && data[pos] != 0 {
			if data[pos] < 32 || data[pos] >= 127 {
				return result
			}
			pos++
		}
		if pos >= n || pos == start {
			return result
		}
		result = append(result, string(data[start:pos]))
		pos++ // skip null terminator
	}
	return result
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
