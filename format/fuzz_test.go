package format

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func FuzzReadHeader(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not an sdf file"))
	f.Add(validHeaderSeed())

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadHeader(bytes.NewReader(data))
	})
}

func FuzzParsePageRecords(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3, 4, 5, byte(PageLeaf)})
	f.Add(minimalDataPageSeed())

	columns := []ColumnDef{
		{Name: "ID", TypeID: TypeInt, Ordinal: 1, Position: 0},
		{Name: "Name", TypeID: TypeNVarchar, Ordinal: 2, MaxLength: 64, Position: 4},
		{Name: "Enabled", TypeID: TypeBit, Ordinal: 3, Position: 0},
	}
	bmpExtra := computeNullBmpExtra(columns)

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParsePageRecords(data, columns, bmpExtra)
	})
}

func FuzzReadCatalog(f *testing.F) {
	f.Add([]byte{})
	f.Add(validHeaderSeed())
	f.Add(minimalDataPageSeed())

	f.Fuzz(func(t *testing.T, data []byte) {
		pageData := append([]byte(nil), data...)
		if len(pageData) < DefaultPageSize {
			pageData = append(pageData, make([]byte, DefaultPageSize-len(pageData))...)
		}
		// Keep catalog fuzzing focused on catalog page contents instead of
		// spending the fuzz budget chasing arbitrary page-map pointers.
		if len(pageData) >= offsetPage1Addr+4 {
			clear(pageData[offsetPage1Addr : offsetPage1Addr+4])
		}
		pr := NewPageReader(bytes.NewReader(pageData), &FileHeader{PageSize: DefaultPageSize}, 4)
		_, _ = ReadCatalog(pr, 1)
	})
}

func FuzzResolveLOB(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 16))
	f.Add(validLongValueLOBSeed())

	f.Fuzz(func(t *testing.T, data []byte) {
		ptr := make([]byte, 16)
		copy(ptr, data)
		totalLen := int(binary.LittleEndian.Uint32(ptr[0:4]))
		// Oversized LOB rejection has a regression test; this fuzz target keeps
		// generated reads small enough to exercise malformed pointer handling.
		if totalLen > 8192 {
			return
		}

		pageData := data
		if len(pageData) < DefaultPageSize {
			pageData = append([]byte(nil), pageData...)
			pageData = append(pageData, make([]byte, DefaultPageSize-len(pageData))...)
		}
		pr := NewPageReader(bytes.NewReader(pageData), &FileHeader{PageSize: DefaultPageSize}, 4)
		pm := &PageMapping{mapping: map[int]int{0: 0, 1: 0}}
		_, _ = ResolveLOB(pr, pm, ptr)
	})
}

func validHeaderSeed() []byte {
	buf := make([]byte, headerSize)
	le := binary.LittleEndian
	le.PutUint32(buf[offsetDatabaseID:], 0x12345678)
	le.PutUint32(buf[offsetVersion:], uint32(VersionCE40))
	le.PutUint32(buf[offsetBuildNumber:], 74412)
	le.PutUint32(buf[offsetPageCount:], 1)
	le.PutUint32(buf[offsetLCID:], 1033)
	return buf
}

func minimalDataPageSeed() []byte {
	page := make([]byte, DefaultPageSize)
	page[pageTypeOffset] = byte(PageLeaf)
	binary.LittleEndian.PutUint32(page[20:24], 1)

	entry := []byte{
		0, 0, 0, 0,
		3, 0, 0, 0,
		0, 0,
		1, 0, 0, 0,
		0x80, 1, 0,
		'A', 0,
	}
	copy(page[24:], entry)
	slot := uint32(0) | uint32(len(entry))<<12 | uint32(2)<<24
	binary.LittleEndian.PutUint32(page[len(page)-4:], slot)
	return page
}

func validLongValueLOBSeed() []byte {
	buf := make([]byte, DefaultPageSize)
	binary.LittleEndian.PutUint32(buf[0:4], 4)
	binary.LittleEndian.PutUint32(buf[8:12], 1)
	buf[pageTypeOffset] = byte(PageLongValue)
	copy(buf[lvPageDataOffset:], []byte("data"))
	return buf
}
