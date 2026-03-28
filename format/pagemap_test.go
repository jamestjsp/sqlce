package format

import (
	"encoding/binary"
	"testing"
)

func TestBuildPageMapping(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 128)
	pm, err := BuildPageMapping(pr)
	if err != nil {
		t.Fatalf("BuildPageMapping: %v", err)
	}

	if fp, ok := pm.FilePageNum(0); !ok || fp != 0 {
		t.Errorf("logical 0: got (%d, %v), want (0, true)", fp, ok)
	}

	fp1, ok := pm.FilePageNum(1)
	if !ok {
		t.Fatal("logical 1 not mapped")
	}

	header, _ := pr.ReadPage(0)
	expectedFP1 := int(binary.LittleEndian.Uint32(header[offsetPage1Addr:]) & addrMask)
	if fp1 != expectedFP1 {
		t.Errorf("logical 1: got %d, want %d", fp1, expectedFP1)
	}

	t.Logf("page mapping has %d entries, page1 at file page %d", pm.Len(), fp1)
}

func TestPageMappingCatalogPages(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 128)
	pm, err := BuildPageMapping(pr)
	if err != nil {
		t.Fatalf("BuildPageMapping: %v", err)
	}

	// Logical page 1028 = SysObjects BTree root per reference
	fp, ok := pm.FilePageNum(1028)
	if !ok {
		t.Fatal("logical 1028 (SysObjects BTree) not mapped")
	}

	page, err := pr.ReadPage(fp)
	if err != nil {
		t.Fatalf("ReadPage(%d): %v", fp, err)
	}

	pt := ClassifyPage(page)
	t.Logf("logical 1028 -> file page %d, type=%s", fp, pt)
}

func TestPageMappingAllPagesReadable(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 128)
	pm, err := BuildPageMapping(pr)
	if err != nil {
		t.Fatalf("BuildPageMapping: %v", err)
	}

	fi, _ := f.Stat()
	maxFilePage := int(fi.Size()) / h.PageSize

	var bad int
	for logID, fp := range pm.mapping {
		if fp >= maxFilePage {
			t.Errorf("logical %d -> file page %d exceeds file size (%d pages)", logID, fp, maxFilePage)
			bad++
			if bad > 10 {
				t.Fatal("too many out-of-range pages")
			}
		}
	}
	t.Logf("all %d mapped pages within file bounds", pm.Len())
}

func TestUnpackAddr(t *testing.T) {
	page := make([]byte, 32)
	binary.LittleEndian.PutUint64(page[16:], 0x00000_00005_00003) // addr0=3, addr1=5, addr2=0

	if got := unpackAddr(page, 0); got != 3 {
		t.Errorf("unpackAddr(0) = %d, want 3", got)
	}
	if got := unpackAddr(page, 1); got != 5 {
		t.Errorf("unpackAddr(1) = %d, want 5", got)
	}
	if got := unpackAddr(page, 2); got != 0 {
		t.Errorf("unpackAddr(2) = %d, want 0", got)
	}
}
