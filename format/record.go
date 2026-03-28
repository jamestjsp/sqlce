package format

import (
	"encoding/binary"
	"fmt"
)

// Record represents a single parsed row from a Leaf page.
type Record struct {
	// Values contains the column values in schema order.
	// Variable-length string columns are returned as Go strings (ASCII decoded).
	// Fixed-size columns are returned as raw byte slices for later conversion.
	// Null columns are nil.
	Values [][]byte
}

// PageRecords holds all current records parsed from a single Leaf page.
type PageRecords struct {
	ObjectID    uint16
	ColumnCount int
	Records     []Record
}

// ParsePageRecords extracts row records from a Leaf (0x40) page.
// The columns slice must describe the table's columns in schema order.
// Returns nil if the page contains no parseable records.
func ParsePageRecords(page []byte, columns []ColumnDef) (*PageRecords, error) {
	if len(page) < 32 || ClassifyPage(page) != PageLeaf {
		return nil, nil
	}

	objID := PageObjectID(page)
	recordCount := int(page[0x14])
	if recordCount == 0 {
		return nil, nil
	}

	colCount := int(binary.LittleEndian.Uint16(page[0x1C:]))
	if colCount == 0 || colCount > 500 {
		return nil, nil
	}

	// Separate columns into fixed and variable by physical layout order:
	// SQL CE stores fixed-size columns first, then variable-size columns.
	var fixedCols []colLayout
	var varCols []colLayout
	for i, c := range columns {
		ti := LookupType(c.TypeID)
		if ti.IsVariable {
			varCols = append(varCols, colLayout{schemaIdx: i, size: 0, typeID: c.TypeID})
		} else {
			fixedCols = append(fixedCols, colLayout{schemaIdx: i, size: ti.FixedSize, typeID: c.TypeID})
		}
	}

	pr := &PageRecords{
		ObjectID:    objID,
		ColumnCount: colCount,
	}

	offset := 0x18 // records start after 24-byte page header
	for rec := 0; rec < recordCount && offset < len(page)-16; rec++ {
		r, nextOff, err := parseOneRecord(page, offset, fixedCols, varCols, len(columns))
		if err != nil {
			// Skip malformed record, try to continue
			break
		}
		if r != nil {
			pr.Records = append(pr.Records, *r)
		}
		offset = nextOff
	}

	return pr, nil
}

type colLayout struct {
	schemaIdx int
	size      int    // 0 for variable
	typeID    uint16 // SQL CE type ID
}

func parseOneRecord(page []byte, offset int, fixedCols, varCols []colLayout, totalCols int) (*Record, int, error) {
	if offset+8 >= len(page) {
		return nil, len(page), fmt.Errorf("offset %d past page end", offset)
	}

	// Skip 4-byte status prefix
	offset += 4

	// Read 4-byte column count
	if offset+4 > len(page) {
		return nil, len(page), fmt.Errorf("no column count at %d", offset)
	}
	_ = int(binary.LittleEndian.Uint32(page[offset:]))
	offset += 4

	// Read 1-byte record header
	if offset >= len(page) {
		return nil, len(page), fmt.Errorf("no header at %d", offset)
	}
	header := page[offset]
	offset++

	// Null bitmap: present for non-0xF0 headers. Size = 1 byte.
	if header != 0xF0 && offset < len(page) {
		_ = page[offset] // null bitmap byte
		offset++
	}

	values := make([][]byte, totalCols)

	// Read fixed-size columns
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

	// Parse variable-length columns
	if len(varCols) > 0 && offset < len(page)-4 {
		// Skip padding/alignment bytes between fixed data and variable section.
		// The variable section always starts with 0x80.
		for offset < len(page)-4 && page[offset] != 0x80 {
			offset++
		}
		offset = parseVariableColumns(page, offset, varCols, values)
	}

	return &Record{Values: values}, offset, nil
}

// parseVariableColumns reads the variable-length column section.
// Format: alternating flag (0x80=has data, 0x00=empty) and cumulative end offsets.
// Pattern: [flag1] [end1] [flag2] [end2] ... [flagN]
// Total bytes: 2*N - 1 where N = number of variable columns.
func parseVariableColumns(page []byte, offset int, varCols []colLayout, values [][]byte) int {
	nVar := len(varCols)
	headerSize := 2*nVar - 1

	if offset+headerSize > len(page) {
		return offset
	}

	// Read the variable section header
	varHeader := page[offset : offset+headerSize]
	offset += headerSize

	// Parse flags and cumulative end offsets
	type varColInfo struct {
		hasData bool
		endOff  int // cumulative end offset in variable data area
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

	// Calculate variable data sizes and extract
	varDataStart := offset
	prevEnd := 0
	for i, info := range infos {
		if !info.hasData {
			values[varCols[i].schemaIdx] = nil
			continue
		}

		var start, end int
		start = prevEnd
		if i < nVar-1 {
			end = info.endOff
		} else {
			// Last variable column: scan to find end
			// Look for the next record boundary (00 00 00 00 followed by col count)
			end = findVarDataEnd(page, varDataStart, prevEnd)
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

	// Advance offset past variable data
	if prevEnd > 0 {
		offset = varDataStart + prevEnd
	}

	return offset
}

// findVarDataEnd scans forward from the variable data area to find where
// the last variable column's data ends. It looks for the record boundary
// pattern (zero padding followed by a new record's column count).
func findVarDataEnd(page []byte, varDataStart, currentOffset int) int {
	pos := varDataStart + currentOffset
	// Scan for null byte that marks end of variable data
	for pos < len(page)-8 {
		if page[pos] == 0x00 {
			return pos - varDataStart
		}
		pos++
	}
	return pos - varDataStart
}

// ScanTableRecords reads all Leaf pages for a given objectID and returns
// parsed records. This requires knowing which objectID maps to which table.
func ScanTableRecords(pr *PageReader, totalPages int, objectID uint16, columns []ColumnDef) ([]Record, error) {
	var records []Record

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		if PageObjectID(page) != objectID {
			continue
		}

		parsed, err := ParsePageRecords(page, columns)
		if err != nil {
			continue
		}
		if parsed != nil {
			records = append(records, parsed.Records...)
		}
	}

	return records, nil
}

// FindTableObjectID scans Leaf pages to find the objectID for a given table
// by looking for records that contain recognizable data patterns.
// This is a heuristic approach since the SDF catalog doesn't directly expose
// the mapping between table names and data page objectIDs.
func FindTableObjectID(pr *PageReader, totalPages int, tableName string, columns []ColumnDef) (uint16, error) {
	// We need to try each objectID that has Leaf pages and see which one
	// produces valid records matching the expected column layout.
	// This is done by the caller using ScanTableRecords with candidate objectIDs.
	return 0, fmt.Errorf("use ScanTableRecords with known objectID")
}
