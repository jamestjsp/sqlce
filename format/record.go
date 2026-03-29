package format

import (
	"encoding/binary"
	"fmt"
	"sort"
)

type Record struct {
	Values [][]byte
}

type PageRecords struct {
	ObjectID    uint16
	ColumnCount int
	Records     []Record
}

func ParsePageRecords(page []byte, columns []ColumnDef, nullBmpExtra ...int) (*PageRecords, error) {
	if len(page) < 32 {
		return nil, nil
	}
	pt := ClassifyPage(page)
	if pt != PageLeaf && pt != PageData {
		return nil, nil
	}

	objID := PageObjectID(page)

	colCount := len(columns)
	if colCount == 0 {
		return nil, nil
	}

	var fixedCols []colLayout
	var varCols []colLayout
	var bitCols []colLayout
	for i, c := range columns {
		ti := LookupType(c.TypeID)
		if c.TypeID == TypeBit {
			bitCols = append(bitCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID, position: c.Position})
		} else if ti.IsVariable {
			varCols = append(varCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID, position: c.Position})
		} else {
			fixedCols = append(fixedCols, colLayout{schemaIdx: i, size: ti.FixedSize, typeID: c.TypeID, position: c.Position})
		}
	}
	sort.SliceStable(fixedCols, func(i, j int) bool { return fixedCols[i].position < fixedCols[j].position })
	sort.SliceStable(varCols, func(i, j int) bool { return varCols[i].position < varCols[j].position })
	sort.SliceStable(bitCols, func(i, j int) bool { return bitCols[i].position < bitCols[j].position })

	pr := &PageRecords{
		ObjectID:    objID,
		ColumnCount: colCount,
	}

	bmpExtra := 0
	if len(nullBmpExtra) > 0 {
		bmpExtra = nullBmpExtra[0]
	}

	slots := readDataPageSlots(page)
	for _, slot := range slots {
		if slot.flags&1 != 0 {
			continue
		}
		entry := slot.data
		if len(entry) < 8 {
			continue
		}
		entryColCount := int(binary.LittleEndian.Uint32(entry[4:8]))
		if entryColCount != colCount {
			continue
		}
		r, _, err := parseOneRecord(entry, 0, fixedCols, varCols, bitCols, len(columns), bmpExtra)
		if err != nil {
			continue
		}
		if r != nil {
			pr.Records = append(pr.Records, *r)
		}
	}

	if len(pr.Records) == 0 {
		return nil, nil
	}

	return pr, nil
}

type colLayout struct {
	schemaIdx int
	size      int
	typeID    uint16
	position  int
}

func parseOneRecord(entry []byte, offset int, fixedCols, varCols, bitCols []colLayout, totalCols int, nullBmpExtra int) (*Record, int, error) {
	if offset+9 > len(entry) {
		return nil, len(entry), fmt.Errorf("offset %d past entry end", offset)
	}

	offset += 4 // nextChunk pointer (zero for single-slot records)

	_ = binary.LittleEndian.Uint32(entry[offset:])
	offset += 4 // colCount

	// Bitmap layout: [null flags: ceil(colCount/8) bytes][bit values: ceil(numBitCols/8) bytes]
	bitmapSize := 1 + nullBmpExtra
	var bitmapBytes []byte
	if offset+bitmapSize <= len(entry) {
		bitmapBytes = make([]byte, bitmapSize)
		copy(bitmapBytes, entry[offset:offset+bitmapSize])
	}
	offset += bitmapSize

	values := make([][]byte, totalCols)

	// Extract bit column values from the bit-value section of the bitmap
	if len(bitCols) > 0 && len(bitmapBytes) > 0 {
		nullFlagBytes := (totalCols + 7) / 8
		for i, bc := range bitCols {
			byteIdx := nullFlagBytes + i/8
			bitIdx := uint(i % 8)
			if byteIdx < len(bitmapBytes) {
				val := (bitmapBytes[byteIdx] >> bitIdx) & 1
				values[bc.schemaIdx] = []byte{val}
			}
		}
	}

	// SQL CE records have no "fixed data length" field (unlike full SQL Server's Fsize
	// at record header bytes 2-3). We compute the upper bound from column sizes, then
	// scan backward within the entry for the 0x00 0x80 separator that marks the
	// fixed/variable boundary. This handles tables where catalog Position values overlap
	// (e.g., ParametricElements: sum=124 but actual on-disk extent=116).
	fixedDataSize := 0
	for _, fc := range fixedCols {
		fixedDataSize += fc.size
	}
	if len(varCols) > 0 {
		for pos := len(entry) - 1; pos > offset; pos-- {
			if entry[pos] == 0x80 && pos > offset && entry[pos-1] == 0x00 {
				actualFixed := (pos - 1) - offset
				if actualFixed > 0 && actualFixed < fixedDataSize {
					fixedDataSize = actualFixed
				}
				break
			}
		}
	}

	if offset+fixedDataSize > len(entry) {
		return nil, len(entry), fmt.Errorf("fixed data overflows entry at %d", offset)
	}

	fixedAreaEnd := offset + fixedDataSize
	for _, fc := range fixedCols {
		if offset+fc.size > fixedAreaEnd {
			break
		}
		data := make([]byte, fc.size)
		copy(data, entry[offset:offset+fc.size])
		values[fc.schemaIdx] = data
		offset += fc.size
	}
	offset = fixedAreaEnd

	if len(varCols) > 0 && offset+2 <= len(entry) {
		offset++ // skip 1-byte separator between fixed data and variable section
		offset = parseVariableColumns(entry, offset, varCols, values)
	}

	return &Record{Values: values}, offset, nil
}

// Variable section format:
// [flag0][cumEnd0][flag1][cumEnd1]...[flagN-1] (2*N-1 bytes total)
// flag=0x80: has data, flag=0x00: NULL
// cumEnd values are cumulative end offsets into the data area
// Last column has no cumEnd -- terminated by scanning to 0x00 byte
func parseVariableColumns(entry []byte, offset int, varCols []colLayout, values [][]byte) int {
	nVar := len(varCols)
	headerSize := 2*nVar - 1

	if offset+headerSize > len(entry) {
		return offset
	}

	varHeader := entry[offset : offset+headerSize]
	offset += headerSize

	type varColInfo struct {
		hasData bool
		endOff  int
	}
	infos := make([]varColInfo, nVar)

	for i := 0; i < nVar; i++ {
		flagIdx := i * 2
		if flagIdx < len(varHeader) {
			infos[i].hasData = varHeader[flagIdx] == 0x80
		}
		if i < nVar-1 {
			offIdx := i*2 + 1
			if offIdx < len(varHeader) {
				infos[i].endOff = int(varHeader[offIdx])
			}
		}
	}

	varDataStart := offset
	prevEnd := 0
	for i, info := range infos {
		if !info.hasData {
			values[varCols[i].schemaIdx] = nil
			continue
		}

		start := prevEnd
		var end int
		if i < nVar-1 {
			end = info.endOff
		} else {
			end = start
			absPos := varDataStart + end
			for absPos < len(entry) && entry[absPos] != 0x00 {
				end++
				absPos++
			}
		}

		if end < start {
			end = start
		}

		absStart := varDataStart + start
		absEnd := varDataStart + end
		if absEnd > len(entry) {
			absEnd = len(entry)
		}
		if absStart > len(entry) {
			absStart = len(entry)
		}

		if absEnd > absStart {
			data := make([]byte, absEnd-absStart)
			copy(data, entry[absStart:absEnd])
			values[varCols[i].schemaIdx] = data
		} else {
			values[varCols[i].schemaIdx] = []byte{}
		}
		prevEnd = end
	}

	if prevEnd > 0 {
		offset = varDataStart + prevEnd
	}

	return offset
}

func ScanTableRecords(pr *PageReader, totalPages int, objectID uint16, columns []ColumnDef, nullBmpExtra ...int) ([]Record, error) {
	return ScanTableRecordsMulti(pr, totalPages, []uint16{objectID}, columns, nullBmpExtra...)
}

func ScanTableRecordsMulti(pr *PageReader, totalPages int, objectIDs []uint16, columns []ColumnDef, nullBmpExtra ...int) ([]Record, error) {
	idSet := make(map[uint16]bool, len(objectIDs))
	for _, id := range objectIDs {
		idSet[id] = true
	}

	// Build objectID→filePage map for chunk following
	objIDToFilePage := buildObjIDToFilePage(pr, totalPages)

	var records []Record
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		pt := ClassifyPage(page)
		if pt != PageLeaf && pt != PageData {
			continue
		}
		if !idSet[PageObjectID(page)] {
			continue
		}

		parsed, err := parsePageRecordsFollow(page, columns, pr, objIDToFilePage, nullBmpExtra...)
		if err != nil {
			continue
		}
		if parsed != nil {
			records = append(records, parsed.Records...)
		}
	}

	return records, nil
}

func ScanTableRecordsPages(pr *PageReader, pages []int, objectIDs []uint16, columns []ColumnDef, nullBmpExtra ...int) ([]Record, error) {
	idSet := make(map[uint16]bool, len(objectIDs))
	for _, id := range objectIDs {
		idSet[id] = true
	}

	// Build objectID→filePage map for chunk following
	objIDToFilePage := make(map[uint16]int)
	for _, pg := range pages {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		pt := ClassifyPage(page)
		if pt == PageLeaf || pt == PageData {
			objID := PageObjectID(page)
			if _, exists := objIDToFilePage[objID]; !exists {
				objIDToFilePage[objID] = pg
			}
		}
	}

	var records []Record
	for _, pg := range pages {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		pt := ClassifyPage(page)
		if pt != PageLeaf && pt != PageData {
			continue
		}
		if !idSet[PageObjectID(page)] {
			continue
		}

		parsed, err := parsePageRecordsFollow(page, columns, pr, objIDToFilePage, nullBmpExtra...)
		if err != nil {
			continue
		}
		if parsed != nil {
			records = append(records, parsed.Records...)
		}
	}

	return records, nil
}

// buildObjIDToFilePage builds an objectID→file page number map for all Leaf/Data pages.
func buildObjIDToFilePage(pr *PageReader, totalPages int) map[uint16]int {
	m := make(map[uint16]int)
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		pt := ClassifyPage(page)
		if pt != PageLeaf && pt != PageData {
			continue
		}
		objID := PageObjectID(page)
		if _, exists := m[objID]; !exists {
			m[objID] = pg
		}
	}
	return m
}

// parsePageRecordsFollow is like ParsePageRecords but follows nextChunk pointers
// for multi-slot records that span beyond a single slot entry.
func parsePageRecordsFollow(page []byte, columns []ColumnDef, pr *PageReader, objIDToFilePage map[uint16]int, nullBmpExtra ...int) (*PageRecords, error) {
	if len(page) < 32 {
		return nil, nil
	}
	pt := ClassifyPage(page)
	if pt != PageLeaf && pt != PageData {
		return nil, nil
	}

	objID := PageObjectID(page)
	colCount := len(columns)
	if colCount == 0 {
		return nil, nil
	}

	var fixedCols []colLayout
	var varCols []colLayout
	var bitCols []colLayout
	for i, c := range columns {
		ti := LookupType(c.TypeID)
		if c.TypeID == TypeBit {
			bitCols = append(bitCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID, position: c.Position})
		} else if ti.IsVariable {
			varCols = append(varCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID, position: c.Position})
		} else {
			fixedCols = append(fixedCols, colLayout{schemaIdx: i, size: ti.FixedSize, typeID: c.TypeID, position: c.Position})
		}
	}
	sort.SliceStable(fixedCols, func(i, j int) bool { return fixedCols[i].position < fixedCols[j].position })
	sort.SliceStable(varCols, func(i, j int) bool { return varCols[i].position < varCols[j].position })
	sort.SliceStable(bitCols, func(i, j int) bool { return bitCols[i].position < bitCols[j].position })

	result := &PageRecords{
		ObjectID:    objID,
		ColumnCount: colCount,
	}

	bmpExtra := 0
	if len(nullBmpExtra) > 0 {
		bmpExtra = nullBmpExtra[0]
	}

	le := binary.LittleEndian
	slots := readDataPageSlots(page)
	for _, slot := range slots {
		if slot.flags&1 != 0 {
			continue
		}
		entry := slot.data
		if len(entry) < 8 {
			continue
		}
		entryColCount := int(le.Uint32(entry[4:8]))
		if entryColCount != colCount {
			continue
		}

		// Follow nextChunk chain for multi-slot records
		nextChunk := le.Uint32(entry[0:4])
		if nextChunk != 0 && pr != nil && objIDToFilePage != nil {
			entry = followChunks(pr, entry, nextChunk, objIDToFilePage)
		}

		r, _, err := parseOneRecord(entry, 0, fixedCols, varCols, bitCols, len(columns), bmpExtra)
		if err != nil {
			continue
		}
		if r != nil {
			result.Records = append(result.Records, *r)
		}
	}

	if len(result.Records) == 0 {
		return nil, nil
	}

	return result, nil
}

func FindTableObjectID(pr *PageReader, totalPages int, tableName string, columns []ColumnDef) (uint16, error) {
	return 0, fmt.Errorf("use ScanTableRecords with known objectID")
}
