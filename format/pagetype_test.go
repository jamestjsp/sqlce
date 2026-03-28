package format

import (
	"os"
	"testing"
)

func TestPageClassify(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize

	pr := NewPageReader(f, h, 128)

	counts := make(map[PageType]int)
	unknownPages := []int{}

	for i := 0; i < totalPages; i++ {
		page, err := pr.ReadPage(i)
		if err != nil {
			t.Fatalf("ReadPage(%d): %v", i, err)
		}
		pt := ClassifyPage(page)
		counts[pt]++
		if !pt.IsKnown() {
			unknownPages = append(unknownPages, i)
		}
	}

	t.Logf("Page type distribution across %d pages:", totalPages)
	for _, pt := range []PageType{PageFree, PageAllocationMap, PageSpaceMap, PageData, PageLeaf, PageLongValue, PageIndex, PageOverflow} {
		t.Logf("  %-16s: %d", pt, counts[pt])
	}

	if len(unknownPages) > 0 {
		t.Errorf("found %d pages with unknown type: %v", len(unknownPages), unknownPages)
	}

	// Page 0 should be free/header
	page0, _ := pr.ReadPage(0)
	if pt := ClassifyPage(page0); pt != PageFree {
		t.Errorf("page 0 type = %s, want Free", pt)
	}
}

func TestPageClassifyDistinguishesDataFromCatalog(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 64)

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize

	dataPages := 0
	catalogPages := 0

	for i := 0; i < totalPages; i++ {
		page, err := pr.ReadPage(i)
		if err != nil {
			t.Fatalf("ReadPage(%d): %v", i, err)
		}
		pt := ClassifyPage(page)
		switch pt {
		case PageData:
			dataPages++
		case PageLeaf, PageLongValue:
			catalogPages++
		}
	}

	if dataPages == 0 {
		t.Error("no data pages found")
	}
	if catalogPages == 0 {
		t.Error("no catalog/leaf pages found")
	}
	t.Logf("data pages: %d, catalog/leaf pages: %d", dataPages, catalogPages)
}

func TestPageTypeString(t *testing.T) {
	tests := []struct {
		pt   PageType
		want string
	}{
		{PageFree, "Free"},
		{PageData, "Data"},
		{PageIndex, "Index"},
		{PageType(0xFF), "Unknown"},
	}
	for _, tc := range tests {
		if got := tc.pt.String(); got != tc.want {
			t.Errorf("PageType(0x%02X).String() = %q, want %q", byte(tc.pt), got, tc.want)
		}
	}
}

func TestPageObjectID(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	pr := NewPageReader(f, h, 16)

	// Check that data pages have non-zero object IDs
	page, _ := pr.ReadPage(9) // type 0x30 (Data) from analysis
	pt := ClassifyPage(page)
	oid := PageObjectID(page)
	t.Logf("page 9: type=%s, objectID=%d (0x%04X)", pt, oid, oid)
	if oid == 0 {
		t.Error("expected non-zero object ID for data page 9")
	}
}
