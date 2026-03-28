package format

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

func TestDiagnosticRecordFormat(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatal(err)
	}
	pr := NewPageReader(f, h, 64)

	t.Run("SlotArray", func(t *testing.T) {
		valid := 0
		invalid := 0
		for pg := 0; pg < int(h.PageCount); pg++ {
			page, err := pr.ReadPage(pg)
			if err != nil {
				continue
			}
			if ClassifyPage(page) != PageLeaf {
				continue
			}
			rc := int(page[0x14])
			if rc == 0 || rc > 100 {
				continue
			}
			ss := len(page) - rc*2
			ok := true
			for s := 0; s < rc; s++ {
				off := binary.LittleEndian.Uint16(page[ss+s*2:])
				if int(off) < 0x18 || int(off) >= ss {
					ok = false
					break
				}
			}
			if ok {
				valid++
			} else {
				invalid++
			}
		}
		t.Logf("FINDING: NO SLOT ARRAYS. %d valid (coincidence), %d invalid. Records are sequential from 0x18.", valid, invalid)
	})

	t.Run("RecordFormat", func(t *testing.T) {
		t.Logf("FINDING: Record format = [4B zero/status][4B colCount][1B headerByte][ceil(colCount/8) nullBitmap][fixed data][variable section]")
		t.Logf("FINDING: headerByte values seen: 0xF0, 0xFC, 0x80 - all have null bitmap, headerByte does NOT control null bitmap presence")
		t.Logf("FINDING: null bitmap always present, size = ceil(colCount/8)")
	})

	t.Run("VariableSectionFormat", func(t *testing.T) {
		t.Logf("=== Verifying variable section format ===")

		t.Logf("\n--- Page 393: DataArrayTypes, 2 var cols ---")
		page393, _ := pr.ReadPage(393)
		pos := 0x18
		pos += 4 // status
		pos += 4 // colCount=4
		pos++    // header=0xF0

		nullBmpSize := (4 + 7) / 8 // 1 byte
		nullBmp := page393[pos : pos+nullBmpSize]
		t.Logf("  null bitmap: %X", nullBmp)
		pos += nullBmpSize

		pos += 16 // GUID
		pos += 4  // INT
		t.Logf("  After fixed data, pos=%d (0x%X)", pos, pos)

		nVar := 2
		varHeaderSize := 2*nVar - 1 // 3 bytes
		varHeader := page393[pos : pos+varHeaderSize]
		t.Logf("  var header (%d bytes): %X", varHeaderSize, varHeader)

		flags := make([]byte, nVar)
		cumEndOffsets := make([]int, nVar)
		for i := 0; i < nVar; i++ {
			flags[i] = varHeader[i*2]
			if i < nVar-1 {
				cumEndOffsets[i] = int(varHeader[i*2+1])
			}
		}
		t.Logf("  flags=%X, cumEndOffsets=%v", flags, cumEndOffsets)

		pos += varHeaderSize
		dataStart := pos

		prevEnd := 0
		for i := 0; i < nVar; i++ {
			if flags[i] != 0x80 {
				t.Logf("  varCol[%d]: NULL (flag=0x%02X)", i, flags[i])
				continue
			}
			start := dataStart + prevEnd
			var end int
			if i < nVar-1 {
				end = dataStart + cumEndOffsets[i]
			} else {
				end = start
				for end < len(page393) && page393[end] != 0 {
					end++
				}
			}
			data := string(page393[start:end])
			t.Logf("  varCol[%d]: [%d..%d) = %q", i, start, end, data)
			prevEnd = end - dataStart
		}

		t.Logf("\n--- Page 467: BlcModel, 4 var cols (row 0) ---")
		page467, _ := pr.ReadPage(467)
		pos = 0x18
		pos += 4 // status
		pos += 4 // colCount=9
		pos++    // header=0x80
		nullBmpSize = (9 + 7) / 8 // 2 bytes
		t.Logf("  null bitmap: %X", page467[pos:pos+nullBmpSize])
		pos += nullBmpSize
		pos += 5 * 16 // 5 GUIDs

		nVar = 4
		varHeaderSize = 2*nVar - 1 // 7 bytes
		varHeader = page467[pos : pos+varHeaderSize]
		t.Logf("  var header (%d bytes): %X", varHeaderSize, varHeader)

		flags = make([]byte, nVar)
		cumEndOffsets = make([]int, nVar)
		for i := 0; i < nVar; i++ {
			flags[i] = varHeader[i*2]
			if i < nVar-1 {
				cumEndOffsets[i] = int(varHeader[i*2+1])
			}
		}
		t.Logf("  flags=%X, cumEndOffsets=%v", flags, cumEndOffsets)

		pos += varHeaderSize
		dataStart = pos

		prevEnd = 0
		for i := 0; i < nVar; i++ {
			if flags[i] != 0x80 {
				t.Logf("  varCol[%d]: NULL (flag=0x%02X)", i, flags[i])
				continue
			}
			start := dataStart + prevEnd
			var end int
			if i < nVar-1 {
				end = dataStart + cumEndOffsets[i]
			} else {
				end = start
				for end < len(page467) && page467[end] != 0 {
					end++
				}
			}
			if start < end && end <= len(page467) {
				data := string(page467[start:end])
				t.Logf("  varCol[%d]: [%d..%d) = %q", i, start, end, data)
			}
			prevEnd = end - dataStart
		}

		t.Logf("\nFINDING: Variable section = [flag0][cumEnd0][flag1][cumEnd1]...[flagN-1 (no cumEnd)]")
		t.Logf("FINDING: flag=0x80 means has data, flag=0x00 means NULL")
		t.Logf("FINDING: cumEndN are CUMULATIVE end offsets into the data area")
		t.Logf("FINDING: Last var col has no cumEnd, terminated by 0x00 byte scanning")
		t.Logf("FINDING: Data is ASCII, NOT UTF-16LE")
	})

	t.Run("MultiRecordParsing", func(t *testing.T) {
		t.Logf("=== Parsing all 6 records on page 240 (Properties, 2 nvarchar cols, 0 fixed) ===")
		page, _ := pr.ReadPage(240)

		pos := 0x18
		for rec := 0; rec < 6; rec++ {
			if pos+9 > len(page) {
				t.Logf("  Record %d: out of bounds at pos=%d", rec, pos)
				break
			}

			recStart := pos
			status := binary.LittleEndian.Uint32(page[pos : pos+4])
			pos += 4
			colCount := binary.LittleEndian.Uint32(page[pos : pos+4])
			pos += 4
			hdr := page[pos]
			pos++

			if colCount != 2 || (hdr != 0xFC && hdr != 0xF0 && hdr != 0x80 && hdr != 0xFE) {
				t.Logf("  Record %d @ 0x%X: INVALID colCount=%d header=0x%02X, stopping", rec, recStart, colCount, hdr)
				break
			}

			nullBmpSize := (int(colCount) + 7) / 8
			pos += nullBmpSize

			nVar := 2
			varHeaderSize := 2*nVar - 1 // 3
			if pos+varHeaderSize > len(page) {
				break
			}
			varHeader := page[pos : pos+varHeaderSize]
			pos += varHeaderSize

			flags := []byte{varHeader[0], varHeader[2]}
			cumEnd0 := int(varHeader[1])

			dataStart := pos

			var name, value string
			if flags[0] == 0x80 {
				end0 := dataStart + cumEnd0
				if end0 <= len(page) {
					name = string(page[dataStart:end0])
				}
			}

			if flags[1] == 0x80 {
				start1 := dataStart + cumEnd0
				end1 := start1
				for end1 < len(page) && page[end1] != 0 {
					end1++
				}
				value = string(page[start1:end1])
				pos = end1
			} else {
				pos = dataStart + cumEnd0
			}

			for pos < len(page) && page[pos] == 0 {
				pos++
			}

			t.Logf("  Record %d @ 0x%X: status=0x%08X hdr=0x%02X Name=%q Value=%q",
				rec, recStart, status, hdr, name, value)
		}
	})

	t.Run("RecordEndDetection", func(t *testing.T) {
		t.Logf("=== How to detect record end / next record start ===")
		page, _ := pr.ReadPage(240)

		t.Logf("freeSpace@0x16 = %d", binary.LittleEndian.Uint16(page[0x16:0x18]))
		t.Logf("This might be wrong or represent something else")

		t.Logf("\nScanning for record boundaries (status=0x00000000 + valid colCount):")
		for i := 0x18; i < 0x300; i++ {
			if i+8 <= len(page) &&
				page[i] == 0 && page[i+1] == 0 && page[i+2] == 0 && page[i+3] == 0 {
				cc := binary.LittleEndian.Uint32(page[i+4 : i+8])
				if cc == 2 {
					t.Logf("  Record boundary at 0x%X (%d)", i, i)
				}
			}
		}

		t.Logf("\nRecord sizes (distances between boundaries):")
		boundaries := []int{0x18}
		for i := 0x19; i < 0x300; i++ {
			if i+8 <= len(page) &&
				page[i] == 0 && page[i+1] == 0 && page[i+2] == 0 && page[i+3] == 0 {
				cc := binary.LittleEndian.Uint32(page[i+4 : i+8])
				if cc == 2 {
					boundaries = append(boundaries, i)
				}
			}
		}
		for i := 1; i < len(boundaries); i++ {
			t.Logf("  Record %d: start=0x%X, size=%d bytes", i-1, boundaries[i-1], boundaries[i]-boundaries[i-1])
		}
	})

	t.Run("NullBitmapInterpretation", func(t *testing.T) {
		t.Logf("=== Null bitmap: is bit=1 non-null or null? ===")

		page467, _ := pr.ReadPage(467)
		pos := 0x18 + 8 + 1 // skip status+colCount+header
		nullBmp := page467[pos : pos+2]
		t.Logf("Page 467 (BlcModel, 9 cols): null bitmap = %X = %08b %08b", nullBmp, nullBmp[0], nullBmp[1])

		for col := 0; col < 9; col++ {
			byteIdx := col / 8
			bitIdx := uint(col % 8)
			isSet := nullBmp[byteIdx]&(1<<bitIdx) != 0
			t.Logf("  col %d: bit=%v", col, isSet)
		}

		t.Logf("GUID[4] is all zeros (null GUID), corresponds to col 4")
		t.Logf("Col 4 bit = %v", nullBmp[0]&(1<<4) != 0)
		t.Logf("If bit=1 means non-null: col 4 should be 0")
		t.Logf("If bit=1 means null: col 4 should be 1")

		t.Logf("\nStatus col (col 7, var) should be null based on 0x00 flag in var section")
		t.Logf("Col 7 bit = %v", nullBmp[0]&(1<<7) != 0)

		t.Logf("\nBits for GUID cols (0-3, should be non-null): %v %v %v %v",
			nullBmp[0]&1 != 0, nullBmp[0]&2 != 0, nullBmp[0]&4 != 0, nullBmp[0]&8 != 0)
		t.Logf("Bit for GUID[4] (col 4, null GUID all zeros): %v", nullBmp[0]&16 != 0)
		t.Logf("Bits for var cols (5-7, Repr+MV+LoopType non-null, Status null): %v %v %v %v",
			nullBmp[0]&32 != 0, nullBmp[0]&64 != 0, nullBmp[0]&128 != 0, nullBmp[1]&1 != 0)

		t.Logf("BlcModelBlockId (col 8) bit = %v", nullBmp[1]&1 != 0)
	})

	t.Run("DataPages", func(t *testing.T) {
		t.Logf("=== Data (0x30) pages for target objectIDs ===")
		targetObjs := map[uint16]string{1321: "DataArrayTypes", 1305: "Properties", 1395: "BlcModel"}
		found := false
		for pg := 0; pg < int(h.PageCount); pg++ {
			page, err := pr.ReadPage(pg)
			if err != nil {
				continue
			}
			if ClassifyPage(page) != PageData {
				continue
			}
			objID := PageObjectID(page)
			if name, ok := targetObjs[objID]; ok {
				found = true
				t.Logf("  Data page %d: obj=%d (%s), records=%d, flags=0x%02X",
					pg, objID, name, page[0x14], page[0x15])
			}
		}
		if !found {
			t.Logf("FINDING: No Data (0x30) pages found for these objectIDs")
			t.Logf("FINDING: All user table data is on Leaf (0x40) pages")
		}

		t.Logf("\nData page objectID distribution:")
		dataObjs := make(map[uint16]int)
		for pg := 0; pg < int(h.PageCount); pg++ {
			page, err := pr.ReadPage(pg)
			if err != nil {
				continue
			}
			if ClassifyPage(page) == PageData {
				dataObjs[PageObjectID(page)]++
			}
		}
		if len(dataObjs) == 0 {
			t.Logf("  NO Data pages in this database at all")
		}
		for obj, cnt := range dataObjs {
			t.Logf("  obj %d: %d Data pages", obj, cnt)
		}
	})

	t.Run("HeaderByteDistribution", func(t *testing.T) {
		headerDist := make(map[byte]int)
		for pg := 0; pg < int(h.PageCount); pg++ {
			page, err := pr.ReadPage(pg)
			if err != nil {
				continue
			}
			if ClassifyPage(page) != PageLeaf {
				continue
			}
			rc := int(page[0x14])
			if rc == 0 {
				continue
			}
			pos := 0x18
			if pos+9 < len(page) {
				cc := binary.LittleEndian.Uint32(page[pos+4 : pos+8])
				if cc > 0 && cc < 500 {
					headerDist[page[pos+8]]++
				}
			}
		}
		t.Logf("Header byte distribution (first record per page):")
		for b, c := range headerDist {
			t.Logf("  0x%02X (%08b): %d pages", b, b, c)
		}
	})

	t.Run("FreeSpaceOffset", func(t *testing.T) {
		t.Logf("=== Page header bytes 0x16-0x17 analysis ===")
		for _, pgNum := range []int{393, 240, 467} {
			page, _ := pr.ReadPage(pgNum)
			val := binary.LittleEndian.Uint16(page[0x16:0x18])
			t.Logf("  Page %d: bytes[0x16:0x18] = %d (0x%X), recCount=%d",
				pgNum, val, val, page[0x14])
		}
	})

	t.Run("RecordBoundaryPrecise", func(t *testing.T) {
		t.Logf("=== Precise record boundary analysis on page 240 ===")
		page, _ := pr.ReadPage(240)

		boundaries := []int{0x18}
		for i := 0x19; i < 0x300; i++ {
			if i+8 <= len(page) &&
				page[i] == 0 && page[i+1] == 0 && page[i+2] == 0 && page[i+3] == 0 {
				cc := binary.LittleEndian.Uint32(page[i+4 : i+8])
				if cc == 2 {
					boundaries = append(boundaries, i)
				}
			}
		}

		t.Logf("Found %d boundaries (expected 6 for 6 records, plus extras):", len(boundaries))
		for i, b := range boundaries {
			if i >= 15 {
				break
			}
			t.Logf("  Boundary %d @ 0x%X (%d)", i, b, b)
			if i > 0 {
				prevEnd := boundaries[i-1]
				gap := b - prevEnd
				t.Logf("    gap from prev = %d bytes", gap)

				recHdrSize := 9  // 4+4+1
				nullBmpSize := 1 // ceil(2/8)
				fixedSize := 0   // no fixed cols
				overhead := recHdrSize + nullBmpSize + fixedSize
				dataSize := gap - overhead

				t.Logf("    overhead=%d, data region=%d bytes", overhead, dataSize)

				dataStart := prevEnd + overhead
				varHeaderSize := 2*2 - 1 // 3 bytes for 2 var cols
				if dataStart+varHeaderSize <= len(page) {
					varHeader := page[dataStart : dataStart+varHeaderSize]
					t.Logf("    var header: %X", varHeader)

					cumEnd0 := int(varHeader[1])
					totalVarData := gap - overhead - varHeaderSize
					t.Logf("    cumEnd0=%d, totalVarData=%d", cumEnd0, totalVarData)

					col0Start := dataStart + varHeaderSize
					col0End := col0Start + cumEnd0
					col1Start := col0End
					col1End := col0Start + totalVarData
					if col0End <= len(page) && col1End <= len(page) {
						t.Logf("    col0: %q", string(page[col0Start:col0End]))
						t.Logf("    col1: %q", string(page[col1Start:col1End]))
					}
				}
			}
		}
	})

	t.Run("RecordSizeAlignment", func(t *testing.T) {
		t.Logf("=== Are records aligned to any boundary? ===")
		page, _ := pr.ReadPage(240)

		boundaries := []int{0x18}
		for i := 0x19; i < 0x300; i++ {
			if i+8 <= len(page) &&
				page[i] == 0 && page[i+1] == 0 && page[i+2] == 0 && page[i+3] == 0 {
				cc := binary.LittleEndian.Uint32(page[i+4 : i+8])
				if cc == 2 {
					boundaries = append(boundaries, i)
				}
			}
		}

		for i, b := range boundaries {
			if i >= 10 {
				break
			}
			t.Logf("  Boundary %d @ %d: mod2=%d mod4=%d mod8=%d", i, b, b%2, b%4, b%8)
		}

		t.Logf("\nAre records 2-byte aligned? All mod2==0: %v",
			func() bool {
				for _, b := range boundaries {
					if b%2 != 0 {
						return false
					}
				}
				return true
			}())
	})

	t.Run("PreciseVarColParsing", func(t *testing.T) {
		t.Logf("=== Parsing all records on page 240 with precise sizing ===")
		page, _ := pr.ReadPage(240)
		recCount := int(page[0x14])

		pos := 0x18
		for rec := 0; rec < recCount; rec++ {
			if pos+9 > len(page) {
				break
			}

			_ = binary.LittleEndian.Uint32(page[pos : pos+4]) // status
			colCount := int(binary.LittleEndian.Uint32(page[pos+4 : pos+8]))
			hdr := page[pos+8]

			if colCount < 1 || colCount > 500 {
				t.Logf("  Record %d @ 0x%X: bad colCount=%d, stopping", rec, pos, colCount)
				break
			}

			pos += 9

			nullBmpSize := (colCount + 7) / 8
			pos += nullBmpSize

			nVar := 2
			varHeaderSize := 2*nVar - 1
			if pos+varHeaderSize > len(page) {
				break
			}
			varHeader := page[pos : pos+varHeaderSize]
			pos += varHeaderSize

			flags := []byte{varHeader[0], varHeader[2]}
			cumEnd0 := int(varHeader[1])

			dataStart := pos
			var name, value string

			if flags[0] == 0x80 {
				name = string(page[dataStart : dataStart+cumEnd0])
			}

			if flags[1] == 0x80 {
				valStart := dataStart + cumEnd0
				valEnd := valStart
				for valEnd < len(page) && page[valEnd] != 0 {
					valEnd++
				}
				value = string(page[valStart:valEnd])
				pos = valEnd
			} else {
				pos = dataStart + cumEnd0
			}

			t.Logf("  Record %d: hdr=0x%02X Name=%q Value=%q (nextPos=0x%X)",
				rec, hdr, name, value, pos)
		}
	})

	t.Run("ParsePage467AllRecords", func(t *testing.T) {
		t.Logf("=== Parsing all 3 records on page 467 (BlcModel) ===")
		page, _ := pr.ReadPage(467)
		recCount := int(page[0x14])

		pos := 0x18
		for rec := 0; rec < recCount; rec++ {
			if pos+9 > len(page) {
				break
			}

			status := binary.LittleEndian.Uint32(page[pos : pos+4])
			colCount := int(binary.LittleEndian.Uint32(page[pos+4 : pos+8]))
			hdr := page[pos+8]
			pos += 9

			if colCount != 9 {
				t.Logf("  Record %d: bad colCount=%d, stopping", rec, colCount)
				break
			}

			nullBmpSize := (colCount + 7) / 8
			pos += nullBmpSize

			for g := 0; g < 5; g++ {
				pos += 16
			}

			nVar := 4
			varHeaderSize := 2*nVar - 1
			varHeader := page[pos : pos+varHeaderSize]
			pos += varHeaderSize

			flags := make([]byte, nVar)
			cumEnds := make([]int, nVar)
			for i := 0; i < nVar; i++ {
				flags[i] = varHeader[i*2]
				if i < nVar-1 {
					cumEnds[i] = int(varHeader[i*2+1])
				}
			}

			dataStart := pos
			var vals [4]string
			prevEnd := 0
			for i := 0; i < nVar; i++ {
				if flags[i] != 0x80 {
					continue
				}
				start := dataStart + prevEnd
				var end int
				if i < nVar-1 {
					end = dataStart + cumEnds[i]
				} else {
					end = start
					for end < len(page) && page[end] != 0 {
						end++
					}
				}
				if start < end && end <= len(page) {
					vals[i] = string(page[start:end])
				}
				prevEnd = end - dataStart
			}

			pos = dataStart + prevEnd

			t.Logf("  Record %d @ status=0x%X hdr=0x%02X: Repr=%q MV=%q LoopType=%q Status=%q (nextPos=0x%X)",
				rec, status, hdr, vals[0], vals[1], vals[2], vals[3], pos)
		}
	})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func decodeUTF16LE(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	runes := make([]rune, len(b)/2)
	for i := 0; i < len(b); i += 2 {
		runes[i/2] = rune(binary.LittleEndian.Uint16(b[i:]))
	}
	return string(runes)
}

func TestDiagnosticPageHeaderFields(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	pr := NewPageReader(f, h, 64)

	t.Logf("=== Page header field analysis ===")

	for pg := 0; pg < int(h.PageCount) && pg < 50; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		pt := ClassifyPage(page)
		if pt != PageLeaf && pt != PageData {
			continue
		}
		rc := page[0x14]
		if rc == 0 {
			continue
		}

		freeOff := binary.LittleEndian.Uint16(page[0x16:0x18])
		colCount := binary.LittleEndian.Uint16(page[0x1C:0x1E])
		t.Logf("  Page %d: type=%s obj=%d rec=%d flags=0x%02X val@0x16=%d colCount=%d",
			pg, pt, PageObjectID(page), rc, page[0x15], freeOff, colCount)
	}

	_ = fmt.Sprintf("done")
}
