package format

import (
	"os"
	"testing"
)

// TestFollowChunks_RawSlotIndex guards against an earlier bug in
// readDataPageSlots. When a page contained empty/freed slots the helper
// collapsed them out of the returned slice, but followChunks dereferenced
// entryIdx against the filtered list. Multi-slot records whose
// continuation lived past an empty slot were silently truncated — the
// decoded nvarchar values appeared to drop or duplicate one character
// relative to the SDF contents.
//
// This test scans the full Depropanizer sample and fails if a split
// record's continuation is unreachable, which is the symptom that
// previously manifested as corrupt TransferFunction strings.
func TestFollowChunks_RawSlotIndex(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 256)

	pm, err := BuildPageMapping(pr)
	if err != nil {
		t.Fatalf("BuildPageMapping: %v", err)
	}

	var unreachable, chased int
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		if ClassifyPage(page) != PageLeaf {
			continue
		}

		raw := readDataPageSlotsRaw(page)
		for _, slot := range raw {
			if slot.data == nil || len(slot.data) < 4 {
				continue
			}
			// Only starts-of-record contain meaningful nextChunk pointers
			// for the main record stream; flag bit 1 marks them.
			if slot.flags&2 == 0 {
				continue
			}
			nc := uint32(slot.data[0]) | uint32(slot.data[1])<<8 |
				uint32(slot.data[2])<<16 | uint32(slot.data[3])<<24
			if nc == 0 {
				continue
			}
			chased++
			logID := int(nc >> 12)
			idx := int(nc & 0xFFF)
			fp, ok := pm.FilePageNum(logID)
			if !ok {
				unreachable++
				continue
			}
			cont, err := pr.ReadPage(fp)
			if err != nil {
				unreachable++
				continue
			}
			contSlot := readDataPageSlotAt(cont, idx)
			if contSlot.data == nil {
				unreachable++
			}
		}
	}
	t.Logf("chased=%d unreachable=%d", chased, unreachable)
	if unreachable != 0 {
		t.Errorf("expected all chunk continuations to be reachable, got %d unreachable of %d", unreachable, chased)
	}
}
