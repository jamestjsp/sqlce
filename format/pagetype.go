package format

// PageType identifies the kind of page based on the type byte at page offset 6.
type PageType byte

const (
	// PageFree indicates a free/unused page or the file header page (page 0).
	PageFree PageType = 0x00

	// PageAllocationMap is a page allocation bitmap.
	PageAllocationMap PageType = 0x10

	// PageSpaceMap tracks space usage across pages.
	PageSpaceMap PageType = 0x20

	// PageData holds table row data.
	PageData PageType = 0x30

	// PageLeaf is a B-tree leaf page (catalog metadata or index leaf).
	PageLeaf PageType = 0x40

	// PageLongValue stores long/overflow values and catalog text.
	PageLongValue PageType = 0x50

	// PageIndex is a B-tree interior index page.
	PageIndex PageType = 0x60

	// PageOverflow stores large object data.
	PageOverflow PageType = 0x80
)

// pageTypeOffset is the byte position within each page that holds the type indicator.
const pageTypeOffset = 6

func (pt PageType) String() string {
	switch pt {
	case PageFree:
		return "Free"
	case PageAllocationMap:
		return "AllocationMap"
	case PageSpaceMap:
		return "SpaceMap"
	case PageData:
		return "Data"
	case PageLeaf:
		return "Leaf"
	case PageLongValue:
		return "LongValue"
	case PageIndex:
		return "Index"
	case PageOverflow:
		return "Overflow"
	default:
		return "Unknown"
	}
}

// IsKnown reports whether the page type is a recognized value.
func (pt PageType) IsKnown() bool {
	switch pt {
	case PageFree, PageAllocationMap, PageSpaceMap, PageData,
		PageLeaf, PageLongValue, PageIndex, PageOverflow:
		return true
	default:
		return false
	}
}

// ClassifyPage returns the type of an SDF page by examining the type byte
// at offset 6. The page slice must be at least 7 bytes long.
func ClassifyPage(page []byte) PageType {
	if len(page) < pageTypeOffset+1 {
		return PageFree
	}
	return PageType(page[pageTypeOffset])
}

// PageObjectID returns the table/object ID stored at page offset 4-5 (u16 LE).
// System objects have low IDs (1, 2); user tables start at ~0x0403.
func PageObjectID(page []byte) uint16 {
	if len(page) < 6 {
		return 0
	}
	return uint16(page[4]) | uint16(page[5])<<8
}
