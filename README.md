# sqlce

A pure Go library for reading SQL Server Compact Edition (SQL CE) `.sdf` database files without requiring Windows, COM, or ODBC.

## Features

- **Pure Go**: zero CGO dependencies, runs on any OS
- **`database/sql` driver**: use standard Go database interfaces
- **Engine API**: direct access to tables, schemas, and row iteration
- **CLI tool** (`sdfutil`): inspect and export SDF files from the command line
- **Type-safe**: SQL CE types mapped to native Go types (int32, float64, time.Time, etc.)

## Installation

```bash
go get github.com/josephjohnjj/sqlce

```

## Quick Start

### Using `database/sql`

```go
import (
    "database/sql"
    "fmt"
    _ "github.com/josephjohnjj/sqlce/driver"
)

db, _ := sql.Open("sqlce", "path/to/database.sdf")
defer db.Close()

rows, _ := db.Query("SELECT * FROM Properties WITH OBJECTID 1305")
defer rows.Close()

for rows.Next() {
    var name, value string
    rows.Scan(&name, &value)
    fmt.Printf("%s = %s\n", name, value)
}
```

### Using the Engine API

```go
import "github.com/josephjohnjj/sqlce/engine"

db, _ := engine.Open("path/to/database.sdf")
defer db.Close()

// List tables
for _, name := range db.Tables() {
    fmt.Println(name)
}

// Read table schema
tbl, _ := db.Table("Properties")
for _, col := range tbl.Schema().Columns() {
    fmt.Printf("  %s: %s\n", col.Name(), col.Type())
}

// Iterate rows (automatic objectID mapping)
ri, _ := tbl.Scan()
defer ri.Close()
for ri.Next() {
    fmt.Println(ri.Values())
}

// Or specify objectID manually if needed
// ri, _ := tbl.RowsWithObjectID(1305)
```

### Using the CLI

```bash
# Build
go build ./cmd/sdfutil/

# Database info
sdfutil info database.sdf

# List tables
sdfutil tables database.sdf

# Show table schema
sdfutil schema database.sdf TableName

# Dump rows (tab-separated)
sdfutil dump database.sdf TableName 1305

# Export as CSV or JSON
sdfutil export database.sdf TableName 1305 --format csv
sdfutil export database.sdf TableName 1305 --format json
```

## SQL Support

The `database/sql` driver supports a minimal SQL subset:

```sql
SELECT * FROM TableName
SELECT col1, col2 FROM TableName
SELECT * FROM "Quoted Table"
SELECT * FROM [Bracketed Table]

-- Optional: specify objectID if automatic mapping fails
SELECT * FROM TableName WITH OBJECTID 1305
```

The library automatically maps table names to internal objectIDs via page mapping and TABLE page analysis. For tables that can't be auto-mapped, use the `WITH OBJECTID` clause. See [ObjectID Mapping](#objectid-mapping) below.

## Supported Types

| SQL CE Type         | Go Type        | Size     |
|---------------------|----------------|----------|
| tinyint             | uint8          | 1 byte   |
| smallint            | int16          | 2 bytes  |
| int                 | int32          | 4 bytes  |
| bigint              | int64          | 8 bytes  |
| float               | float64        | 8 bytes  |
| real                | float32        | 4 bytes  |
| money               | float64        | 8 bytes  |
| bit                 | bool           | 1 byte   |
| datetime            | time.Time      | 8 bytes  |
| uniqueidentifier    | string (GUID)  | 16 bytes |
| nvarchar            | string         | variable |
| ntext               | string         | variable |
| binary              | []byte         | variable |
| varbinary           | []byte         | variable |
| image               | []byte         | variable |
| numeric             | float64        | variable |
| rowversion          | uint64         | 8 bytes  |

## ObjectID Mapping

SQL CE stores table data across Leaf pages identified by internal objectIDs. The library automatically maps table names to objectIDs using:

1. **Automatic mapping**: deterministic mapping via TABLE pages and page mapping (MapA/MapB) discovers 60-70% of tables automatically
2. **Manual override**: set directly via `db.SetObjectMapping(map[string]uint16{...})` for tables that can't be auto-mapped
3. **Row count matching**: `db.BuildObjectMapping(expectedRowCounts)` matches tables to objectIDs by comparing column counts and row counts against a reference

For most databases, automatic mapping is sufficient. Use manual methods only when encountering unmapped tables.

## Packages

| Package | Description |
|---------|-------------|
| `format` | Low-level binary format: header, pages, catalog, records, types |
| `engine` | High-level API: Database, Table, Schema, RowIterator, type conversion |
| `driver` | `database/sql/driver` implementation |
| `cmd/sdfutil` | Command-line utility |

## Limitations

- **Read-only**: SQL CE files are opened for reading only; no INSERT/UPDATE/DELETE
- **No WHERE/JOIN**: the SQL parser supports only SELECT with optional column lists (in-memory JOIN engine available via `ExtractControlLayer()`)
- **No encryption**: encrypted `.sdf` files are detected but not yet decryptable
- **No LongValue**: large values stored in LongValue (0x50) pages are not yet parsed
- **Partial auto-mapping**: automatic objectID mapping discovers 60-70% of tables; remaining tables may need manual `WITH OBJECTID` clause or manual mapping

## SQL CE Version Support

| Version | Supported |
|---------|-----------|
| SQL CE 4.0 | ✅ Tested |
| SQL CE 3.5 | ⚠️ Untested (same page size, may work) |
| SQL CE 3.0 | ❌ Not supported |
| SQL CE 2.0 | ❌ Not supported |

## License

MIT
