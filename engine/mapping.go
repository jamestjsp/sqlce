package engine

import (
	"encoding/binary"

	"github.com/jamestjat/sqlce/format"
)

// ObjectIDInfo holds metadata about a Leaf-page objectID.
type ObjectIDInfo struct {
	ObjectID    uint16
	ColumnCount int
	RecordCount int
}

// CollectObjectIDInfo scans all Leaf and Data pages and collects metadata per objectID.
// This is fast -- only reads page headers, no record parsing.
// System/catalog pages where byte 0x1C contains record data (not a column count)
// are filtered out by checking for unreasonable column count values.
func CollectObjectIDInfo(pr *format.PageReader, totalPages int) (map[uint16]*ObjectIDInfo, error) {
	infos := make(map[uint16]*ObjectIDInfo)

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
		recCount := int(page[0x14])
		colCount := int(binary.LittleEndian.Uint16(page[0x1C:]))

		if _, ok := infos[objID]; !ok {
			infos[objID] = &ObjectIDInfo{
				ObjectID:    objID,
				ColumnCount: colCount,
			}
		}
		infos[objID].RecordCount += recCount
	}

	// Filter out entries with unreasonable column counts (system/catalog pages
	// where 0x1C contains record data rather than column count).
	filtered := make(map[uint16]*ObjectIDInfo)
	for objID, info := range infos {
		if info.ColumnCount > 0 && info.ColumnCount <= 100 {
			filtered[objID] = info
		}
	}

	return filtered, nil
}

// BuildTableMapping maps catalog tables to objectIDs by matching
// (column_count, record_count) against expected row counts from SQLite.
// Returns map[tableName]objectID for matched tables.
func BuildTableMapping(catalog *format.Catalog, objInfos map[uint16]*ObjectIDInfo, expectedRowCounts map[string]int) map[string]uint16 {
	mapping := make(map[string]uint16)
	usedObjIDs := make(map[uint16]bool)

	// Build reverse index: (colCount, recCount) → []objectID
	type key struct{ cols, rows int }
	objByKey := make(map[key][]uint16)
	for objID, info := range objInfos {
		k := key{info.ColumnCount, info.RecordCount}
		objByKey[k] = append(objByKey[k], objID)
	}

	// Pass 1: exact match on (column_count, record_count)
	for _, table := range catalog.Tables {
		expectedRows, hasExpected := expectedRowCounts[table.Name]
		if !hasExpected {
			continue
		}
		k := key{len(table.Columns), expectedRows}
		candidates := objByKey[k]
		if len(candidates) == 1 && !usedObjIDs[candidates[0]] {
			mapping[table.Name] = candidates[0]
			usedObjIDs[candidates[0]] = true
		}
	}

	// Pass 2: match by record_count only (for tables with missing catalog columns)
	// Group remaining objectIDs by record count
	objByCount := make(map[int][]uint16)
	for objID, info := range objInfos {
		if usedObjIDs[objID] {
			continue
		}
		objByCount[info.RecordCount] = append(objByCount[info.RecordCount], objID)
	}

	for _, table := range catalog.Tables {
		if _, mapped := mapping[table.Name]; mapped {
			continue
		}
		expectedRows, hasExpected := expectedRowCounts[table.Name]
		if !hasExpected || expectedRows == 0 {
			continue
		}
		candidates := objByCount[expectedRows]
		if len(candidates) == 1 && !usedObjIDs[candidates[0]] {
			mapping[table.Name] = candidates[0]
			usedObjIDs[candidates[0]] = true
		}
	}

	// Pass 3: fuzzy column count match (±3 cols tolerance for page boundary issues)
	for _, table := range catalog.Tables {
		if _, mapped := mapping[table.Name]; mapped {
			continue
		}
		expectedRows, hasExpected := expectedRowCounts[table.Name]
		if !hasExpected || expectedRows == 0 {
			continue
		}
		catalogCols := len(table.Columns)
		var candidates []uint16
		for objID, info := range objInfos {
			if usedObjIDs[objID] {
				continue
			}
			if info.RecordCount != expectedRows {
				continue
			}
			colDiff := info.ColumnCount - catalogCols
			if colDiff < 0 {
				colDiff = -colDiff
			}
			if colDiff <= 3 {
				candidates = append(candidates, objID)
			}
		}
		if len(candidates) == 1 && !usedObjIDs[candidates[0]] {
			mapping[table.Name] = candidates[0]
			usedObjIDs[candidates[0]] = true
		}
	}

	return mapping
}
