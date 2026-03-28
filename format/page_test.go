package format

import (
	"encoding/binary"
	"os"
	"testing"
)

func openTestDB(t *testing.T) (*os.File, *FileHeader) {
	t.Helper()
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	h, err := ReadHeader(f)
	if err != nil {
		f.Close()
		t.Fatalf("ReadHeader: %v", err)
	}
	return f, h
}

func TestPageReaderReadPage0(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 0)
	page, err := pr.ReadPage(0)
	if err != nil {
		t.Fatalf("ReadPage(0): %v", err)
	}
	if len(page) != h.PageSize {
		t.Fatalf("page 0 size = %d, want %d", len(page), h.PageSize)
	}

	magic := binary.LittleEndian.Uint32(page[:4])
	if magic != Magic {
		t.Errorf("page 0 magic = 0x%08X, want 0x%08X", magic, Magic)
	}
}

func TestPageReaderAllPages(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize

	pr := NewPageReader(f, h, 16)
	for i := 0; i < totalPages; i++ {
		page, err := pr.ReadPage(i)
		if err != nil {
			t.Fatalf("ReadPage(%d): %v", i, err)
		}
		if len(page) != h.PageSize {
			t.Fatalf("page %d size = %d, want %d", i, len(page), h.PageSize)
		}
	}
	t.Logf("successfully read all %d pages (0-%d)", totalPages, totalPages-1)
}

func TestPageReaderCache(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 4)

	// First read — cache miss
	p1, err := pr.ReadPage(5)
	if err != nil {
		t.Fatalf("first ReadPage(5): %v", err)
	}

	// Second read — cache hit (should return identical data)
	p2, err := pr.ReadPage(5)
	if err != nil {
		t.Fatalf("second ReadPage(5): %v", err)
	}

	if len(p1) != len(p2) {
		t.Fatalf("cached page length mismatch: %d vs %d", len(p1), len(p2))
	}
	for i := range p1 {
		if p1[i] != p2[i] {
			t.Fatalf("cached page differs at byte %d", i)
		}
	}

	// Verify returned slices are independent copies
	p1[0] = 0xFF
	if p2[0] == 0xFF {
		t.Error("cache returned aliased slice, not a copy")
	}
}

func TestPageReaderCacheEviction(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	cacheSize := 4
	pr := NewPageReader(f, h, cacheSize)

	// Fill cache beyond capacity
	for i := 0; i < cacheSize+2; i++ {
		_, err := pr.ReadPage(i)
		if err != nil {
			t.Fatalf("ReadPage(%d): %v", i, err)
		}
	}

	pr.mu.Lock()
	if pr.lru.Len() > cacheSize {
		t.Errorf("cache size = %d, want <= %d", pr.lru.Len(), cacheSize)
	}
	pr.mu.Unlock()
}

func TestPageReaderNegativePage(t *testing.T) {
	f, h := openTestDB(t)
	defer f.Close()

	pr := NewPageReader(f, h, 0)
	_, err := pr.ReadPage(-1)
	if err == nil {
		t.Error("expected error for negative page number")
	}
}
