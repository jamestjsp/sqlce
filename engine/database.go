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
	mu         sync.Mutex
	file       *os.File
	header     *format.FileHeader
	reader     *format.PageReader
	catalog    *format.Catalog
	totalPages int
	objMapping map[string]uint16 // table name → objectID (best effort)
	closed     bool
}

// Table provides access to a single table's metadata and data.
type Table struct {
	db    *Database
	def   *format.TableDef
	objID uint16
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

	db := &Database{
		file:       f,
		header:     h,
		reader:     pr,
		catalog:    catalog,
		totalPages: totalPages,
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

// Tables returns the names of all tables in the database, sorted alphabetically.
func (db *Database) Tables() []string {
	names := make([]string, len(db.catalog.Tables))
	for i, t := range db.catalog.Tables {
		names[i] = t.Name
	}
	sort.Strings(names)
	return names
}

// TableCount returns the number of tables in the database.
func (db *Database) TableCount() int {
	return len(db.catalog.Tables)
}

// Table returns a handle for the named table.
func (db *Database) Table(name string) (*Table, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, fmt.Errorf("database is closed")
	}

	td := db.catalog.TableByName(name)
	if td == nil {
		return nil, fmt.Errorf("table %q not found", name)
	}

	objID := uint16(0)
	if db.objMapping != nil {
		objID = db.objMapping[name]
	}

	return &Table{
		db:    db,
		def:   td,
		objID: objID,
	}, nil
}

// Header returns the parsed file header.
func (db *Database) Header() *format.FileHeader {
	return db.header
}

// Catalog returns the parsed catalog.
func (db *Database) Catalog() *format.Catalog {
	return db.catalog
}

// PageReader returns the underlying page reader.
func (db *Database) PageReader() *format.PageReader {
	return db.reader
}

// TotalPages returns the total number of pages in the database file.
func (db *Database) TotalPages() int {
	return db.totalPages
}

// BuildObjectMapping attempts to map all tables to their Leaf page objectIDs.
// This requires expected row counts (e.g., from a reference SQLite database).
func (db *Database) BuildObjectMapping(expectedRowCounts map[string]int) error {
	objInfos, err := CollectObjectIDInfo(db.reader, db.totalPages)
	if err != nil {
		return fmt.Errorf("collecting objectID info: %w", err)
	}

	db.mu.Lock()
	db.objMapping = BuildTableMapping(db.catalog, objInfos, expectedRowCounts)
	db.mu.Unlock()

	return nil
}

// SetObjectMapping directly sets the table-to-objectID mapping.
func (db *Database) SetObjectMapping(m map[string]uint16) {
	db.mu.Lock()
	db.objMapping = m
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
	if t.objID == 0 {
		return nil, fmt.Errorf("objectID unknown for table %q; call BuildObjectMapping first", t.def.Name)
	}

	scanner := NewTableScanner(t.db.reader, t.db.totalPages, t.def, t.objID)
	return scanner.Scan()
}

// ScanWithObjectID reads all rows using the specified objectID.
func (t *Table) ScanWithObjectID(objectID uint16) (*ScanResult, error) {
	scanner := NewTableScanner(t.db.reader, t.db.totalPages, t.def, objectID)
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
