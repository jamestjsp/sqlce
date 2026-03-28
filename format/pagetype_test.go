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

func TestParseDataPageTarget(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	pr := NewPageReader(f, h, 64)

	page9, _ := pr.ReadPage(9)
	target := ParseDataPageTarget(page9)
	if target == 0 {
		t.Error("expected non-zero target for Data page 9 (obj 1087)")
	}
	t.Logf("Data page 9 (obj %d): target=%d", PageObjectID(page9), target)

	leafPage, _ := pr.ReadPage(1)
	if ParseDataPageTarget(leafPage) != 0 {
		t.Error("expected 0 target for Leaf page")
	}

	page24, _ := pr.ReadPage(24)
	emptyTarget := ParseDataPageTarget(page24)
	t.Logf("Data page 24 (obj %d): target=%d (empty table)", PageObjectID(page24), emptyTarget)
}

func TestScanDataPageTargets(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h, _ := ReadHeader(f)
	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 128)

	targets := ScanDataPageTargets(pr, totalPages)
	t.Logf("Found %d Data page -> Leaf objectID mappings", len(targets))

	if len(targets) == 0 {
		t.Error("expected some Data page targets")
	}

	for dataObj, leafObj := range targets {
		t.Logf("  Data obj %d -> Leaf obj %d", dataObj, leafObj)
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
