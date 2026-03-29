package engine

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/jamestjat/sqlce/format"
)

// Database represents an opened SQL CE database file.
type Database struct {
	mu         sync.RWMutex
	file       *os.File
	header     *format.FileHeader
	reader     *format.PageReader
	catalog    *format.Catalog
	totalPages int
	objMapping map[string][]uint16 // table name → objectIDs (best effort)
	pageIndex  map[uint16][]int    // objectID → file page numbers (Leaf/Data only)
	closed     bool
}

// Table provides access to a single table's metadata and data.
type Table struct {
	db     *Database
	def    *format.TableDef
	objIDs []uint16
}

// Open opens a SQL CE database file (.sdf) for reading.
func Open(path string) (*Database, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	return openFromFile(f)
}

func openFromFile(f *os.File) (*Database, error) {
	h, err := format.ReadHeader(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("reading header: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}

	totalPages := int(fi.Size()) / h.PageSize
	pr := format.NewPageReader(f, h, 256)

	catalog, err := format.ReadCatalog(pr, totalPages)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("reading catalog: %w", err)
	}

	pageIndex, err := buildPageIndex(pr, totalPages)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("building page index: %w", err)
	}

	db := &Database{
		file:       f,
		header:     h,
		reader:     pr,
		catalog:    catalog,
		totalPages: totalPages,
		objMapping: catalog.ObjectMap,
		pageIndex:  pageIndex,
	}

	return db, nil
}

// Close releases all resources held by the database.
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true
	return db.file.Close()
}

func buildPageIndex(pr *format.PageReader, totalPages int) (map[uint16][]int, error) {
	idx := make(map[uint16][]int)
	for pg := 0; pg < totalPages; pg++ {
		page, err := pr.ReadPage(pg)
		if err != nil {
			continue
		}
		pt := format.ClassifyPage(page)
		if pt != format.PageLeaf && pt != format.PageData {
			continue
		}
		objID := format.PageObjectID(page)
		idx[objID] = append(idx[objID], pg)
	}
	return idx, nil
}

func (db *Database) PagesForObjectIDs(objectIDs []uint16) []int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.pageIndex == nil {
		return nil
	}
	seen := make(map[int]struct{})
	var pages []int
	for _, id := range objectIDs {
		for _, pg := range db.pageIndex[id] {
			if _, ok := seen[pg]; !ok {
				seen[pg] = struct{}{}
				pages = append(pages, pg)
			}
		}
	}
	sort.Ints(pages)
	return pages
}

// Tables returns the names of all tables in the database, sorted alphabetically.
func (db *Database) Tables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, len(db.catalog.Tables))
	for i, t := range db.catalog.Tables {
		names[i] = t.Name
	}
	sort.Strings(names)
	return names
}

// TableCount returns the number of tables in the database.
func (db *Database) TableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return len(db.catalog.Tables)
}

// Table returns a handle for the named table.
func (db *Database) Table(name string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.closed {
		return nil, fmt.Errorf("database is closed")
	}

	td := db.catalog.TableByName(name)
	if td == nil {
		return nil, fmt.Errorf("table %q not found", name)
	}

	var objIDs []uint16
	if db.objMapping != nil {
		objIDs = db.objMapping[name]
	}

	return &Table{
		db:     db,
		def:    td,
		objIDs: objIDs,
	}, nil
}

// Header returns the parsed file header.
func (db *Database) Header() *format.FileHeader {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.header
}

// Catalog returns the parsed catalog.
func (db *Database) Catalog() *format.Catalog {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.catalog
}

// PageReader returns the underlying page reader.
func (db *Database) PageReader() *format.PageReader {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.reader
}

// TotalPages returns the total number of pages in the database file.
func (db *Database) TotalPages() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.totalPages
}

// BuildObjectMapping attempts to map all tables to their Leaf page objectIDs.
// This requires expected row counts (e.g., from a reference SQLite database).
func (db *Database) BuildObjectMapping(expectedRowCounts map[string]int) error {
	objInfos, err := CollectObjectIDInfo(db.reader, db.totalPages)
	if err != nil {
		return fmt.Errorf("collecting objectID info: %w", err)
	}

	single := BuildTableMapping(db.catalog, objInfos, expectedRowCounts)
	multi := make(map[string][]uint16, len(single))
	for name, id := range single {
		multi[name] = []uint16{id}
	}

	db.mu.Lock()
	db.objMapping = multi
	db.mu.Unlock()

	return nil
}

// SetObjectMapping directly sets the table-to-objectID mapping.
func (db *Database) SetObjectMapping(m map[string]uint16) {
	multi := make(map[string][]uint16, len(m))
	for name, id := range m {
		multi[name] = []uint16{id}
	}
	db.mu.Lock()
	db.objMapping = multi
	db.mu.Unlock()
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.def.Name
}

// Columns returns the table's column definitions.
func (t *Table) Columns() []format.ColumnDef {
	return t.def.Columns
}

// ColumnCount returns the number of columns.
func (t *Table) ColumnCount() int {
	return len(t.def.Columns)
}

// Scan reads all rows from the table. Requires the table's objectID to be
// known (via BuildObjectMapping or SetObjectMapping on the database).
func (t *Table) Scan() (*ScanResult, error) {
	if len(t.objIDs) == 0 {
		return nil, fmt.Errorf("objectID unknown for table %q; use ScanWithObjectID or WITH OBJECTID in SQL", t.def.Name)
	}

	scanner := NewTableScanner(t.db.reader, t.db.totalPages, t.def, t.objIDs)
	if pages := t.db.PagesForObjectIDs(t.objIDs); len(pages) > 0 {
		scanner.SetPages(pages)
	}
	return scanner.Scan()
}

// ScanWithObjectID reads all rows using the specified objectID.
func (t *Table) ScanWithObjectID(objectID uint16) (*ScanResult, error) {
	scanner := NewTableScanner(t.db.reader, t.db.totalPages, t.def, []uint16{objectID})
	if pages := t.db.PagesForObjectIDs([]uint16{objectID}); len(pages) > 0 {
		scanner.SetPages(pages)
	}
	return scanner.Scan()
}

// Rows returns a RowIterator for the table. Requires the table's objectID to be known.
func (t *Table) Rows() (*RowIterator, error) {
	result, err := t.Scan()
	if err != nil {
		return nil, err
	}
	return newRowIterator(result), nil
}

// RowsWithObjectID returns a RowIterator using the specified objectID.
func (t *Table) RowsWithObjectID(objectID uint16) (*RowIterator, error) {
	result, err := t.ScanWithObjectID(objectID)
	if err != nil {
		return nil, err
	}
	return newRowIterator(result), nil
}
