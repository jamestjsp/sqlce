package format

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestResolveLOBRejectsOversizedValue(t *testing.T) {
	ptr := make([]byte, 16)
	binary.LittleEndian.PutUint32(ptr[0:4], uint32(maxLOBSize+1))
	binary.LittleEndian.PutUint32(ptr[8:12], 1)

	pr := NewPageReader(bytes.NewReader(make([]byte, DefaultPageSize)), &FileHeader{PageSize: DefaultPageSize}, 1)
	pm := &PageMapping{mapping: map[int]int{1: 0}}
	_, err := ResolveLOB(pr, pm, ptr)
	if err == nil {
		t.Fatal("expected oversized LOB error")
	}
	if !strings.Contains(err.Error(), "LOB too large") {
		t.Fatalf("error = %q, want LOB too large", err)
	}
}

func TestReadDataPageSlotsRejectsInvalidSlotOffset(t *testing.T) {
	page := make([]byte, DefaultPageSize)
	page[pageTypeOffset] = byte(PageLeaf)
	binary.LittleEndian.PutUint32(page[20:24], 1)

	slot := uint32(0xFFF) | uint32(16)<<12 | uint32(2)<<24
	binary.LittleEndian.PutUint32(page[len(page)-4:], slot)

	raw := readDataPageSlotsRaw(page)
	if len(raw) != 1 {
		t.Fatalf("raw slot count = %d, want 1", len(raw))
	}
	if raw[0].data != nil {
		t.Fatalf("invalid slot data len = %d, want nil", len(raw[0].data))
	}
	if got := readDataPageSlots(page); len(got) != 0 {
		t.Fatalf("filtered slot count = %d, want 0", len(got))
	}
}

func TestBuildPageMappingHandlesShortHeaderPage(t *testing.T) {
	pr := NewPageReader(bytes.NewReader([]byte{1, 2, 3}), &FileHeader{PageSize: DefaultPageSize}, 1)
	pm, err := BuildPageMapping(pr)
	if err != nil {
		t.Fatalf("BuildPageMapping: %v", err)
	}
	if pm.Len() != 1 {
		t.Fatalf("mapping len = %d, want only logical page 0", pm.Len())
	}
	if fp, ok := pm.FilePageNum(0); !ok || fp != 0 {
		t.Fatalf("logical page 0 = (%d, %v), want (0, true)", fp, ok)
	}
}

func TestFollowChunksStopsAtTraversalLimit(t *testing.T) {
	pageCount := maxRecordChunkHops + 3
	data := make([]byte, pageCount*DefaultPageSize)
	mapping := make(map[int]int, pageCount)
	for i := 1; i < pageCount; i++ {
		logicalID := i
		filePage := i
		mapping[logicalID] = filePage

		var next uint32
		if i+1 < pageCount {
			next = uint32(i+1) << 12
		}
		page := data[filePage*DefaultPageSize : (filePage+1)*DefaultPageSize]
		putContinuationSlot(page, next, byte(i))
	}

	pr := NewPageReader(bytes.NewReader(data), &FileHeader{PageSize: DefaultPageSize}, 1)
	pm := &PageMapping{mapping: mapping}
	got := followChunks(pr, []byte{0xAA}, uint32(1)<<12, pm)

	wantLen := 1 + maxRecordChunkHops
	if len(got) != wantLen {
		t.Fatalf("reassembled len = %d, want %d", len(got), wantLen)
	}
}

func TestFollowChunksStopsAtRecordSizeLimit(t *testing.T) {
	page := make([]byte, DefaultPageSize)
	putContinuationSlot(page, 0, 0xBB, 0xBB)

	pr := NewPageReader(bytes.NewReader(page), &FileHeader{PageSize: DefaultPageSize}, 1)
	pm := &PageMapping{mapping: map[int]int{1: 0}}
	first := bytes.Repeat([]byte{0xAA}, maxReassembledRecordSize-1)
	got := followChunks(pr, first, uint32(1)<<12, pm)

	if len(got) != len(first) {
		t.Fatalf("reassembled len = %d, want unchanged first chunk len %d", len(got), len(first))
	}
}

func putContinuationSlot(page []byte, next uint32, payload ...byte) {
	binary.LittleEndian.PutUint32(page[20:24], 1)
	entry := make([]byte, 4+len(payload))
	binary.LittleEndian.PutUint32(entry[0:4], next)
	copy(entry[4:], payload)
	copy(page[24:], entry)
	slot := uint32(0) | uint32(len(entry))<<12
	binary.LittleEndian.PutUint32(page[len(page)-4:], slot)
}
