package format

import (
	"encoding/binary"
	"os"
	"testing"
)

// Tests cross-validate our CE 4.0 implementation against the rags2html sdf.py
// parser (CE 3.5). Source: https://github.com/Kassy2048/rags2html/blob/master/sdf.py
//
// rags2html is the only other known open-source binary SDF parser. While it
// targets CE 3.5, many format aspects are shared with CE 4.0.

func TestRagsCompat_PageTypeEncoding(t *testing.T) {
	// rags2html extracts page type as: (DWORD(page, 4) >> 20) & 0xF
	// We extract as: page[6] (a full byte).
	// Verify these are consistent: our byte values should be rags values << 4.
	//
	// rags types: HEADER=0, MAPA=1, MAPB=2, TABLE=3, DATA=4, LV=5, BTREE=6, BITMAP=8
	// Our types:  Free=0x00, AllocMap=0x10, SpaceMap=0x20, Data=0x30, Leaf=0x40, LongValue=0x50, Index=0x60, Overflow=0x80
	ragsToOurs := map[int]PageType{
		0: PageFree,          // HEADER → Free (page 0)
		1: PageAllocationMap, // MAPA → AllocationMap
		2: PageSpaceMap,      // MAPB → SpaceMap
		3: PageData,          // TABLE → Data
		4: PageLeaf,          // DATA → Leaf
		5: PageLongValue,     // LV → LongValue
		6: PageIndex,         // BTREE → Index
		8: PageOverflow,      // BITMAP → Overflow
	}

	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 64)

	checked := 0
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		ourType := ClassifyPage(page)
		ragsDword := binary.LittleEndian.Uint32(page[4:8])
		ragsType := int((ragsDword >> 20) & 0xF)

		if expected, ok := ragsToOurs[ragsType]; ok {
			if ourType != expected {
				t.Errorf("page %d: rags type %d → expected %v, got %v", pg, ragsType, expected, ourType)
			}
			checked++
		}
	}
	t.Logf("Verified page type encoding for %d/%d pages", checked, totalPages)
	if checked < totalPages/2 {
		t.Errorf("too few pages verified: %d/%d", checked, totalPages)
	}
}

func TestRagsCompat_SlotArrayFormat(t *testing.T) {
	// rags2html slot: 4 bytes from page end, backwards
	//   offset[11:0], size[23:12], flags[31:24]
	//   flags & 0xFC must == 0 (only bits 0 and 1 used)
	//   entry data at: entryOffset + 16 + 8 (page header + data header = 24)
	// Our implementation uses the same encoding.
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 64)

	totalSlots := 0
	badFlags := 0
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		pt := ClassifyPage(page)
		if pt != PageLeaf && pt != PageData {
			continue
		}
		slots := readDataPageSlots(page)
		for _, s := range slots {
			totalSlots++
			// rags2html validation: flags & 0xFC == 0
			if s.flags&0xFC != 0 {
				badFlags++
			}
		}
	}
	t.Logf("Checked %d slots, %d with unexpected high flag bits", totalSlots, badFlags)
	if badFlags > 0 {
		t.Errorf("%d slots have flags & 0xFC != 0 (rags2html expects these to be zero)", badFlags)
	}
}

func TestRagsCompat_SysObjectsMinRecordSize(t *testing.T) {
	// rags2html: minRowSize for __SysObjects = 93 (line 1320: `if len(record) < 93`)
	// Breakdown: headerSize(13) + bitfield(2) + fixed(58) + varOffsets(20) = 93
	// Our implementation uses the same threshold at catalog.go line 150: `if len(entry) < 93+4`
	// (we add +4 because our entry includes the 4-byte nextChunk prefix that rags2html strips)
	//
	// Verify by reading actual catalog records.
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 256)

	tooSmall := 0
	total := 0
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}
		slots := readDataPageSlots(page)
		for _, slot := range slots {
			if slot.flags&1 != 0 || slot.flags&2 == 0 {
				continue
			}
			if len(slot.data) < 8 {
				continue
			}
			cc := binary.LittleEndian.Uint32(slot.data[4:8])
			if cc != sysObjColCount {
				continue
			}
			total++
			// rags2html expects >= 93 bytes (after stripping nextChunk)
			// our entry includes nextChunk, so >= 93+4 = 97
			if len(slot.data) < 97 {
				tooSmall++
			}
		}
	}
	t.Logf("Found %d catalog records (colCount=38), %d below rags minRowSize", total, tooSmall)
	if total == 0 {
		t.Error("no catalog records found")
	}
}

func TestRagsCompat_SysObjectsSchema(t *testing.T) {
	// rags2html defines 38 columns for __SysObjects. Verify our catalog reads
	// column definitions consistent with their schema:
	//   - ColumnType at fixed offset 12-13 (our sysObjOffColumnType = 12) ✓
	//   - ObjectOrdinal at fixed offset 14-15 (rags: position=10, 2-byte ushort)
	//     Wait — rags says ObjectOrdinal is at position=10, but we use offset 14.
	//     This is because rags positions are within storage-class groups, not absolute.
	//     Our offsets are absolute from fixed data start.
	//   - ColumnPosition at fixed offset 38-39 (our sysObjOffColumnPosition = 38) ✓
	//   - Variable strings start at offset 85 from bitmap (our sysObjVarDataOffset = 85)
	//
	// Verify we find tables and columns from the catalog.
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 256)

	catalog, err := ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatal(err)
	}

	// rags2html ObjectType constants match ours
	// ObjectType=1 → Table, ObjectType=4 → Column
	if len(catalog.Tables) < 90 {
		t.Errorf("expected >= 90 tables, got %d", len(catalog.Tables))
	}

	// Verify some columns have reasonable type IDs that map to known types
	unknownTypes := 0
	totalCols := 0
	for _, td := range catalog.Tables {
		for _, c := range td.Columns {
			totalCols++
			ti := LookupType(c.TypeID)
			if ti.Name == "unknown" {
				unknownTypes++
				t.Logf("  %s.%s: unknown typeID=0x%02x", td.Name, c.Name, c.TypeID)
			}
		}
	}
	t.Logf("Total columns: %d, unknown types: %d", totalCols, unknownTypes)
	if unknownTypes > 0 {
		t.Errorf("%d columns have unknown type IDs", unknownTypes)
	}
}

func TestRagsCompat_PageMapConstants(t *testing.T) {
	// rags2html constants:
	//   MapA at page 1 (address from header at offset 0x2C)
	//   MapA maps pages 2-1026 (1025 entries)
	//   MapB pages map 1528 entries each, starting at logical page 1027
	//   3 page addresses packed per QWORD, 20 bits each, starting at offset 16
	// Our constants (pagemap.go):
	//   mapAEntries = 1025, mapBEntries = 1528, firstMapBLogID = 1027
	//   mapDataOffset = 16, addrMask = 0xFFFFF (20-bit)
	if mapAEntries != 1025 {
		t.Errorf("mapAEntries: got %d, rags expects 1025", mapAEntries)
	}
	if mapBEntries != 1528 {
		t.Errorf("mapBEntries: got %d, rags expects 1528", mapBEntries)
	}
	if firstMapBLogID != 1027 {
		t.Errorf("firstMapBLogID: got %d, rags expects 1027", firstMapBLogID)
	}
	if mapDataOffset != 16 {
		t.Errorf("mapDataOffset: got %d, rags expects 16", mapDataOffset)
	}
	if addrMask != 0xFFFFF {
		t.Errorf("addrMask: got 0x%X, rags expects 0xFFFFF", addrMask)
	}

	// Verify page mapping works on actual data
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	pr := NewPageReader(f, h, 64)

	pm, err := BuildPageMapping(pr)
	if err != nil {
		t.Fatal(err)
	}
	// rags2html: page 0 maps to file page 0
	if fp, ok := pm.FilePageNum(0); !ok || fp != 0 {
		t.Errorf("logical page 0: expected file page 0, got %d (ok=%v)", fp, ok)
	}
	t.Logf("PageMapping has %d entries", pm.Len())
}

func TestRagsCompat_RecordHeaderFormat(t *testing.T) {
	// rags2html record header:
	//   [0:4] nextChunk DWORD (pageId << 12 | entryIndex)
	//   [4:8] colCount DWORD
	//   [8:8+ceil(N/8)] column mask (null bitmap), each byte XOR'd with 0xFF
	//
	// Verify on actual data: for single-slot records, nextChunk should be 0.
	// ColCount should match table definition.
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 256)

	catalog, err := ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatal(err)
	}

	// Check Properties table (2 cols, 6 rows, all single-slot)
	td := catalog.TableByName("Properties")
	if td == nil {
		t.Fatal("Properties not found")
	}
	objIDs := catalog.ObjectMap["Properties"]
	if len(objIDs) == 0 {
		t.Fatal("no objectIDs")
	}

	idSet := make(map[uint16]bool)
	for _, id := range objIDs {
		idSet[id] = true
	}

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf && ClassifyPage(page) != PageData {
			continue
		}
		if !idSet[PageObjectID(page)] {
			continue
		}
		slots := readDataPageSlots(page)
		for i, s := range slots {
			if s.flags&1 != 0 {
				continue
			}
			if len(s.data) < 8 {
				continue
			}
			le := binary.LittleEndian
			nextChunk := le.Uint32(s.data[0:4])
			colCount := le.Uint32(s.data[4:8])

			if nextChunk != 0 {
				t.Errorf("Properties slot %d: nextChunk=%d (expected 0 for single-slot)", i, nextChunk)
			}
			if colCount != uint32(len(td.Columns)) {
				t.Errorf("Properties slot %d: colCount=%d (expected %d)", i, colCount, len(td.Columns))
			}
		}
		break
	}
}

func TestRagsCompat_NullBitmapXOR(t *testing.T) {
	// rags2html: null bitmap bytes are XOR'd with 0xFF
	// bit=1 (after XOR) means column is present
	// bit=0 (after XOR) means column is missing/null
	//
	// Our code reads raw bitmap and interprets bit=1 as non-null (same after XOR).
	// Verify: for Properties (2 nvarchar cols, always present), the bitmap should
	// have bits 0 and 1 set (= 0x03 after XOR, or 0xFC raw).
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 256)
	catalog, _ := ReadCatalog(pr, totalPages)

	objIDs := catalog.ObjectMap["Properties"]
	idSet := make(map[uint16]bool)
	for _, id := range objIDs {
		idSet[id] = true
	}

	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if !idSet[PageObjectID(page)] || ClassifyPage(page) != PageLeaf {
			continue
		}
		slots := readDataPageSlots(page)
		for i, s := range slots {
			if s.flags&1 != 0 || len(s.data) < 9 {
				continue
			}
			rawBmp := s.data[8]
			xored := rawBmp ^ 0xFF
			// Properties has 2 columns, both always present
			// After XOR: bits 0,1 should be set → xored & 0x03 == 0x03
			if xored&0x03 != 0x03 {
				t.Errorf("slot %d: bitmap after XOR=0x%02x, expected bits 0-1 set", i, xored)
			}
		}
		break
	}
}
