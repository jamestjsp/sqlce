package format

import "encoding/binary"

const lvPageDataOffset = 16
const lvPageDataSize = 4096 - lvPageDataOffset // 4080 bytes per LV page

// ResolveLOB reads overflow data from LongValue pages referenced by a 16-byte
// inline pointer stored in ntext/image columns.
//
// Pointer layout:
//
//	bytes 2-3:   total data length (uint16 LE)
//	bytes 10-11: logical page ID of first LV page (uint16 LE)
//
// Data spans consecutive logical page IDs, 4080 bytes per page.
func ResolveLOB(pr *PageReader, pm *PageMapping, ptr []byte) ([]byte, error) {
	if len(ptr) < 14 {
		return ptr, nil
	}

	le := binary.LittleEndian
	totalLen := int(le.Uint16(ptr[2:4]))
	firstLogID := int(le.Uint16(ptr[10:12]))

	if totalLen == 0 || firstLogID == 0 {
		return ptr, nil
	}

	buf := make([]byte, 0, totalLen)
	remaining := totalLen

	for logID := firstLogID; remaining > 0; logID++ {
		fp, ok := pm.FilePageNum(logID)
		if !ok {
			break
		}

		page, err := pr.ReadPage(fp)
		if err != nil {
			break
		}

		if ClassifyPage(page) != PageLongValue {
			break
		}

		chunk := lvPageDataSize
		if chunk > remaining {
			chunk = remaining
		}
		if lvPageDataOffset+chunk > len(page) {
			chunk = len(page) - lvPageDataOffset
		}
		if chunk <= 0 {
			break
		}

		buf = append(buf, page[lvPageDataOffset:lvPageDataOffset+chunk]...)
		remaining -= chunk
	}

	if len(buf) == 0 {
		return ptr, nil
	}
	return buf, nil
}
