package format

import (
	"container/list"
	"fmt"
	"io"
	"sync"
)

const defaultCacheSize = 64 // number of pages to cache

// PageReader provides random access to fixed-size pages in an SDF file.
type PageReader struct {
	r        io.ReaderAt
	header   *FileHeader
	mu       sync.Mutex
	cache    map[int]*list.Element
	lru      *list.List
	maxCache int
}

type cachedPage struct {
	pageNum int
	data    []byte
}

// NewPageReader creates a PageReader that reads pages from r using the given header.
// cacheSize controls how many pages are kept in the LRU cache (0 uses the default of 64).
func NewPageReader(r io.ReaderAt, header *FileHeader, cacheSize int) *PageReader {
	if cacheSize <= 0 {
		cacheSize = defaultCacheSize
	}
	return &PageReader{
		r:        r,
		header:   header,
		cache:    make(map[int]*list.Element),
		lru:      list.New(),
		maxCache: cacheSize,
	}
}

// ReadPage returns the raw bytes of the page at the given page number.
// Pages are 0-indexed: page 0 is the file header page.
func (pr *PageReader) ReadPage(pageNum int) ([]byte, error) {
	if pageNum < 0 {
		return nil, fmt.Errorf("invalid page number: %d", pageNum)
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Check cache
	if elem, ok := pr.cache[pageNum]; ok {
		pr.lru.MoveToFront(elem)
		cp := elem.Value.(*cachedPage)
		out := make([]byte, len(cp.data))
		copy(out, cp.data)
		return out, nil
	}

	// Read from disk
	offset := int64(pageNum) * int64(pr.header.PageSize)
	buf := make([]byte, pr.header.PageSize)
	n, err := pr.r.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading page %d at offset %d: %w", pageNum, offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("page %d: read 0 bytes at offset %d", pageNum, offset)
	}
	buf = buf[:n]

	// Store in cache
	cp := &cachedPage{pageNum: pageNum, data: make([]byte, len(buf))}
	copy(cp.data, buf)
	elem := pr.lru.PushFront(cp)
	pr.cache[pageNum] = elem

	// Evict if over capacity
	for pr.lru.Len() > pr.maxCache {
		back := pr.lru.Back()
		if back == nil {
			break
		}
		evicted := pr.lru.Remove(back).(*cachedPage)
		delete(pr.cache, evicted.pageNum)
	}

	out := make([]byte, len(buf))
	copy(out, buf)
	return out, nil
}

// PageSize returns the page size in bytes.
func (pr *PageReader) PageSize() int {
	return pr.header.PageSize
}
