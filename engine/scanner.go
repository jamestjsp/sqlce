package engine

import (
	"fmt"

	"github.com/jamestjat/sqlce/format"
)

// TableScanner iterates all rows of a table by scanning Leaf pages
// with matching objectIDs.
type TableScanner struct {
	reader     *format.PageReader
	totalPages int
	table      *format.TableDef
	objectID   uint16
}

// ScanResult holds the scanned rows from a table.
type ScanResult struct {
	Columns []format.ColumnDef
	Rows    [][]any
}

// NewTableScanner creates a scanner for the given table.
// objectID identifies which Leaf pages belong to this table.
func NewTableScanner(pr *format.PageReader, totalPages int, table *format.TableDef, objectID uint16) *TableScanner {
	return &TableScanner{
		reader:     pr,
		totalPages: totalPages,
		table:      table,
		objectID:   objectID,
	}
}

// Scan reads all rows from the table's Leaf pages and returns typed values.
func (ts *TableScanner) Scan() (*ScanResult, error) {
	records, err := format.ScanTableRecords(ts.reader, ts.totalPages, ts.objectID, ts.table.Columns)
	if err != nil {
		return nil, fmt.Errorf("scanning table %s: %w", ts.table.Name, err)
	}

	result := &ScanResult{
		Columns: ts.table.Columns,
	}

	for _, rec := range records {
		row, err := convertRecord(rec, ts.table.Columns)
		if err != nil {
			// Skip malformed rows
			continue
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// convertRecord converts raw record bytes to Go-typed values.
func convertRecord(rec format.Record, columns []format.ColumnDef) ([]any, error) {
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

		val, err := ConvertValue(data, col.TypeID)
		if err != nil {
			row[i] = data // fall back to raw bytes
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
		records, err := format.ScanTableRecords(pr, totalPages, objID, table.Columns)
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
