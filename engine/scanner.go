package engine

import (
	"encoding/binary"
	"fmt"

	"github.com/jamestjat/sqlce/format"
)

// TableScanner iterates all rows of a table by scanning Leaf pages
// with matching objectIDs.
type TableScanner struct {
	reader     *format.PageReader
	totalPages int
	table      *format.TableDef
	objectIDs  []uint16
	pages      []int
}

// ScanResult holds the scanned rows from a table.
type ScanResult struct {
	Columns []format.ColumnDef
	Rows    [][]any
}

// NewTableScanner creates a scanner for the given table.
// objectIDs identifies which Leaf pages belong to this table.
func NewTableScanner(pr *format.PageReader, totalPages int, table *format.TableDef, objectIDs []uint16) *TableScanner {
	return &TableScanner{
		reader:     pr,
		totalPages: totalPages,
		table:      table,
		objectIDs:  objectIDs,
	}
}

func (ts *TableScanner) SetPages(pages []int) {
	ts.pages = pages
}

func (ts *TableScanner) Scan() (*ScanResult, error) {
	bmpExtra := ts.table.NullBmpExtra

	if hasGUIDColumns(ts.table.Columns) {
		validated := ts.validateBitmapSize(bmpExtra)
		if validated != bmpExtra {
			bmpExtra = validated
		}
	}

	var records []format.Record
	var err error
	if len(ts.pages) > 0 {
		records, err = format.ScanTableRecordsPages(ts.reader, ts.pages, ts.objectIDs, ts.table.Columns, bmpExtra)
	} else {
		records, err = format.ScanTableRecordsMulti(ts.reader, ts.totalPages, ts.objectIDs, ts.table.Columns, bmpExtra)
	}
	if err != nil {
		return nil, fmt.Errorf("scanning table %s: %w", ts.table.Name, err)
	}

	// Build page mapping for LOB resolution if table has ntext/image columns
	var pm *format.PageMapping
	if hasLOBColumns(ts.table.Columns) {
		pm, _ = format.BuildPageMapping(ts.reader)
	}

	result := &ScanResult{
		Columns: ts.table.Columns,
	}

	for _, rec := range records {
		row, err := convertRecord(rec, ts.table.Columns, ts.reader, pm)
		if err != nil {
			continue
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

func hasGUIDColumns(cols []format.ColumnDef) bool {
	for _, c := range cols {
		if c.TypeID == format.TypeUniqueIdentifier {
			return true
		}
	}
	return false
}

func hasLOBColumns(cols []format.ColumnDef) bool {
	for _, c := range cols {
		if c.TypeID == format.TypeNText || c.TypeID == format.TypeImage {
			return true
		}
	}
	return false
}

func (ts *TableScanner) scanWithBmp(bmpExtra int) []format.Record {
	var records []format.Record
	var err error
	if len(ts.pages) > 0 {
		records, err = format.ScanTableRecordsPages(ts.reader, ts.pages, ts.objectIDs, ts.table.Columns, bmpExtra)
	} else {
		records, err = format.ScanTableRecordsMulti(ts.reader, ts.totalPages, ts.objectIDs, ts.table.Columns, bmpExtra)
	}
	if err != nil {
		return nil
	}
	return records
}

// isAllZero returns true if every byte in b is 0.
func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// scoreGUIDs returns a plausibility score for records parsed with a given bitmap size.
// Each non-null GUID with a valid 16-byte value and non-zero Data1 scores +1.
// Each non-null GUID with wrong length or suspicious pattern scores -1.
// All-zero GUIDs (NULL) are neutral.
func scoreGUIDs(records []format.Record, cols []format.ColumnDef) int {
	score := 0
	for _, rec := range records {
		for i, col := range cols {
			if col.TypeID != format.TypeUniqueIdentifier {
				continue
			}
			if i >= len(rec.Values) || rec.Values[i] == nil {
				continue
			}
			data := rec.Values[i]
			if len(data) != 16 {
				score--
				continue
			}
			if isAllZero(data) {
				continue
			}
			d1 := binary.LittleEndian.Uint32(data[0:4])
			if d1 != 0 {
				score++
			} else {
				score--
			}
		}
	}
	return score
}

// validateBitmapSize checks if the computed NullBmpExtra produces valid GUIDs.
// If not, probes sizes 0-3 and returns the best one.
func (ts *TableScanner) validateBitmapSize(computed int) int {
	records := ts.scanWithBmp(computed)
	if len(records) == 0 {
		return computed
	}

	bestScore := scoreGUIDs(records, ts.table.Columns)
	if bestScore >= len(records) {
		return computed
	}

	bestSize := computed
	for probe := 0; probe <= 3; probe++ {
		if probe == computed {
			continue
		}
		recs := ts.scanWithBmp(probe)
		if len(recs) == 0 {
			continue
		}
		s := scoreGUIDs(recs, ts.table.Columns)
		if s > bestScore {
			bestScore = s
			bestSize = probe
		}
	}
	return bestSize
}

// convertRecord converts raw record bytes to Go-typed values.
// If pr and pm are non-nil, LOB columns (ntext/image) are resolved from LV pages.
func convertRecord(rec format.Record, columns []format.ColumnDef, pr *format.PageReader, pm *format.PageMapping) ([]any, error) {
	row := make([]any, len(columns))
	for i, col := range columns {
		if i >= len(rec.Values) || rec.Values[i] == nil {
			row[i] = nil
			continue
		}
		data := rec.Values[i]
		if len(data) == 0 {
			row[i] = ""
			continue
		}

		// Resolve LOB pointers for ntext/image columns
		if pr != nil && pm != nil && len(data) == 16 &&
			(col.TypeID == format.TypeNText || col.TypeID == format.TypeImage) {
			resolved, err := format.ResolveLOB(pr, pm, data)
			if err == nil && len(resolved) > 16 {
				data = resolved
			}
		}

		val, err := ConvertValue(data, col.TypeID)
		if err != nil {
			row[i] = data
			continue
		}
		row[i] = val
	}
	return row, nil
}

// FindTableObjectIDs scans all Leaf and Data pages and returns a map of
// objectID to the number of records found.
func FindTableObjectIDs(pr *format.PageReader, totalPages int) (map[uint16]int, error) {
	counts := make(map[uint16]int)

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			return nil, err
		}
		pt := format.ClassifyPage(page)
		if pt != format.PageLeaf && pt != format.PageData {
			continue
		}
		objID := format.PageObjectID(page)
		recordCount := int(page[0x14])
		if recordCount > 0 {
			counts[objID] += recordCount
		}
	}

	return counts, nil
}

// MatchTableToObjectID attempts to find the objectID for a given table
// by comparing record counts with the SQLite reference (if available)
// or by testing column parsing against candidate objectIDs.
func MatchTableToObjectID(pr *format.PageReader, totalPages int, table *format.TableDef, candidates map[uint16]int) (uint16, error) {
	for objID := range candidates {
		records, err := format.ScanTableRecords(pr, totalPages, objID, table.Columns, table.NullBmpExtra)
		if err != nil || len(records) == 0 {
			continue
		}

		// Validate: check if the first record has reasonable values
		rec := records[0]
		valid := true
		for i, col := range table.Columns {
			if i >= len(rec.Values) {
				valid = false
				break
			}
			ti := format.LookupType(col.TypeID)
			if !ti.IsVariable && rec.Values[i] != nil && len(rec.Values[i]) != ti.FixedSize {
				valid = false
				break
			}
		}
		if valid {
			return objID, nil
		}
	}
	return 0, fmt.Errorf("no matching objectID found for table %s", table.Name)
}
