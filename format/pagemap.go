package format

import (
	"encoding/binary"
	"fmt"
)

const (
	offsetPage1Addr = 0x2C
	addrMask        = 0xFFFFF // 20-bit page address
	mapDataOffset   = 16      // packed entries start at byte 16 in map pages
	mapAEntries     = 1025    // MapA maps logical pages 2..1026
	mapBEntries     = 1528    // each MapB maps 1528 logical pages
	mapBStride      = 1527    // logical page range per MapB slot (mapBEntries - 1 for the MapB page itself)
	firstMapBLogID  = 1027    // first logical page mapped by MapB[0]
)

type PageMapping struct {
	mapping  map[int]int
	Warnings []error
}

func BuildPageMapping(pr *PageReader) (*PageMapping, error) {
	pm := &PageMapping{mapping: map[int]int{0: 0}}

	header, err := pr.ReadPage(0)
	if err != nil {
		return nil, err
	}
	if len(header) < offsetPage1Addr+4 {
		return pm, nil
	}

	page1Addr := int(binary.LittleEndian.Uint32(header[offsetPage1Addr:]) & addrMask)
	if page1Addr == 0 {
		return pm, nil
	}
	pm.mapping[1] = page1Addr

	mapA, err := pr.ReadPage(page1Addr)
	if err != nil {
		return nil, err
	}

	for i := 0; i < mapAEntries; i++ {
		addr := unpackAddr(mapA, i)
		if addr == 0 {
			continue
		}
		logicalID := i + 2
		pm.mapping[logicalID] = addr

		mapB, err := pr.ReadPage(addr)
		if err != nil {
			pm.Warnings = append(pm.Warnings, fmt.Errorf("MapB slot %d (page %d): %w", i, addr, err))
			continue
		}

		baseID := firstMapBLogID + i*mapBStride
		for j := 0; j < mapBEntries; j++ {
			bAddr := unpackAddr(mapB, j)
			if bAddr == 0 {
				continue
			}
			pm.mapping[baseID+j] = bAddr
		}
	}

	return pm, nil
}

func (pm *PageMapping) FilePageNum(logicalID int) (int, bool) {
	fp, ok := pm.mapping[logicalID]
	return fp, ok
}

func (pm *PageMapping) Len() int {
	return len(pm.mapping)
}

// unpackAddr extracts the i-th 20-bit packed page address from a map page.
// Three addresses are packed per 8-byte QWORD starting at mapDataOffset.
func unpackAddr(page []byte, i int) int {
	off := mapDataOffset + (i/3)*8
	if off+8 > len(page) {
		return 0
	}
	qw := binary.LittleEndian.Uint64(page[off:])
	return int((qw >> (uint(i%3) * 20)) & addrMask)
}
