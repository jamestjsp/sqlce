package format

import (
	"encoding/binary"
	"fmt"
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
			bitCols = append(bitCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID})
		} else if ti.IsVariable {
			varCols = append(varCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID})
		} else {
			fixedCols = append(fixedCols, colLayout{schemaIdx: i, size: ti.FixedSize, typeID: c.TypeID})
		}
	}

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
}

func parseOneRecord(page []byte, offset int, fixedCols, varCols, bitCols []colLayout, totalCols int, nullBmpExtra int) (*Record, int, error) {
	if offset+9 > len(page) {
		return nil, len(page), fmt.Errorf("offset %d past page end", offset)
	}

	offset += 4 // status

	_ = binary.LittleEndian.Uint32(page[offset:])
	offset += 4 // colCount

	// Bitmap layout: [null flags: ceil(colCount/8) bytes][bit values: ceil(numBitCols/8) bytes]
	bitmapSize := 1 + nullBmpExtra
	var bitmapBytes []byte
	if offset+bitmapSize <= len(page) {
		bitmapBytes = make([]byte, bitmapSize)
		copy(bitmapBytes, page[offset:offset+bitmapSize])
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

	fixedDataSize := 0
	for _, fc := range fixedCols {
		fixedDataSize += fc.size
	}
	if offset+fixedDataSize > len(page) {
		return nil, len(page), fmt.Errorf("fixed data overflows page at %d", offset)
	}

	for _, fc := range fixedCols {
		data := make([]byte, fc.size)
		copy(data, page[offset:offset+fc.size])
		values[fc.schemaIdx] = data
		offset += fc.size
	}

	if len(varCols) > 0 && offset < len(page)-4 {
		// Scan forward to find the 0x80 variable section flag
		for offset < len(page)-4 && page[offset] != 0x80 {
			offset++
		}
		offset = parseVariableColumns(page, offset, varCols, values)
	}

	return &Record{Values: values}, offset, nil
}

// Variable section format:
// [flag0][cumEnd0][flag1][cumEnd1]...[flagN-1] (2*N-1 bytes total)
// flag=0x80: has data, flag=0x00: NULL
// cumEnd values are cumulative end offsets into the data area
// Last column has no cumEnd -- terminated by scanning to 0x00 byte
func parseVariableColumns(page []byte, offset int, varCols []colLayout, values [][]byte) int {
	nVar := len(varCols)
	headerSize := 2*nVar - 1

	if offset+headerSize > len(page) {
		return offset
	}

	varHeader := page[offset : offset+headerSize]
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
			for absPos < len(page) && page[absPos] != 0x00 {
				end++
				absPos++
			}
		}

		if end < start {
			end = start
		}

		absStart := varDataStart + start
		absEnd := varDataStart + end
		if absEnd > len(page) {
			absEnd = len(page)
		}
		if absStart > len(page) {
			absStart = len(page)
		}

		if absEnd > absStart {
			data := make([]byte, absEnd-absStart)
			copy(data, page[absStart:absEnd])
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

		parsed, err := ParsePageRecords(page, columns, nullBmpExtra...)
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

		parsed, err := ParsePageRecords(page, columns, nullBmpExtra...)
		if err != nil {
			continue
		}
		if parsed != nil {
			records = append(records, parsed.Records...)
		}
	}

	return records, nil
}

func FindTableObjectID(pr *PageReader, totalPages int, tableName string, columns []ColumnDef) (uint16, error) {
	return 0, fmt.Errorf("use ScanTableRecords with known objectID")
}
