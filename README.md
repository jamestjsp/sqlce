# sqlce

Pure Go library for reading SQL Server Compact Edition (SQL CE) `.sdf` database files. No Windows, COM, or ODBC required.

## Features

- **Pure Go**, zero CGO, runs on any platform
- **`database/sql` driver** for standard Go database interfaces
- **Engine API** for direct table, schema, and row access
- **CLI tool** (`sdfutil`) for inspecting and exporting SDF files
- **Encryption support** for RC4 (CE 3.x) and AES-128 (CE 4.0)
- **SQLite export** for migrating entire databases in one step

## Installation

```bash
go get github.com/jamestjat/sqlce
```

## Quick Start

### Using `database/sql`

```go
import (
    "database/sql"
    "fmt"
    _ "github.com/jamestjat/sqlce/driver"
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

For encrypted databases, append a password to the DSN:

```go
db, _ := sql.Open("sqlce", "path/to/database.sdf?password=secret")
```

### Using the Engine API

```go
import "github.com/jamestjat/sqlce/engine"

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

For encrypted databases:

```go
db, _ := engine.OpenWithPassword("path/to/database.sdf", "secret")
```

### Using the CLI

```bash
# Build
go build ./cmd/sdfutil/

# Database info
sdfutil info database.sdf

# List tables
sdfutil tables database.sdf

# Show table schema with indexes and constraints
sdfutil schema database.sdf TableName

# Dump rows (tab-separated)
sdfutil dump database.sdf TableName 1305

# Export single table as CSV or JSON
sdfutil export database.sdf TableName 1305 --format csv
sdfutil export database.sdf TableName 1305 --format json

# Export all tables to a SQLite database
sdfutil export --format sqlite database.sdf output.db
```

## SQL Support

The `database/sql` driver supports a minimal SQL subset:

```sql
SELECT * FROM TableName
SELECT col1, col2 FROM TableName
SELECT * FROM "Quoted Table"
SELECT * FROM [Bracketed Table]

-- Specify objectID when automatic mapping fails
SELECT * FROM TableName WITH OBJECTID 1305
```

Table names are mapped to internal objectIDs automatically via page mapping and TABLE page analysis. For tables that can't be auto-mapped, use `WITH OBJECTID`.

## Supported Types

| SQL CE Type         | Go Type        | Size     |
|---------------------|----------------|----------|
| tinyint             | uint8          | 1 byte   |
| smallint            | int16          | 2 bytes  |
| int                 | int32          | 4 bytes  |
| bigint              | int64          | 8 bytes  |
| float               | float64        | 8 bytes  |
| real                | float32        | 4 bytes  |
| money               | int64          | 8 bytes  |
| bit                 | bool           | 1 byte   |
| datetime            | time.Time      | 8 bytes  |
| uniqueidentifier    | string (GUID)  | 16 bytes |
| nvarchar            | string         | variable |
| nchar               | string         | variable |
| ntext               | string (LOB)   | variable |
| binary              | []byte         | variable |
| varbinary           | []byte         | variable |
| image               | []byte (LOB)   | variable |
| numeric             | string         | 19 bytes |
| rowversion          | []byte         | 8 bytes  |

LOB columns (ntext, image) store 16-byte inline pointers. The library resolves these automatically by reading the referenced LongValue pages.

## ObjectID Mapping

SQL CE stores table data across Leaf pages identified by internal objectIDs. The library maps table names to objectIDs through three strategies:

1. **Automatic**: deterministic mapping via TABLE pages and page mapping (MapA/MapB). Covers 60-70% of tables.
2. **Manual override**: set directly via `db.SetObjectMapping(map[string]uint16{...})`
3. **Row count matching**: `db.BuildObjectMapping(expectedRowCounts)` matches tables to objectIDs by comparing column counts and row counts against a reference

For most databases, automatic mapping is sufficient. Manual methods are only needed for unmapped tables.

## Encryption

The library supports opening password-protected databases:

| Algorithm | CE Version | Status |
|-----------|------------|--------|
| RC4       | CE 3.x     | Supported |
| AES-128   | CE 4.0     | Supported |
| AES-256   | CE 4.0     | Not yet supported |

Key derivation: password is encoded as UTF-16LE, hashed with SHA-256, and truncated to 16 bytes.

## Packages

| Package | Description |
|---------|-------------|
| `format` | Low-level binary format: header, pages, catalog, records, types, crypto |
| `engine` | High-level API: Database, Table, Schema, RowIterator, type conversion, SQLite export |
| `driver` | `database/sql/driver` implementation |
| `cmd/sdfutil` | Command-line utility |

## Limitations

- **Read-only**: no INSERT, UPDATE, or DELETE
- **No WHERE/JOIN in SQL**: the SQL parser supports SELECT with optional column lists only
- **Partial auto-mapping**: automatic objectID mapping covers 60-70% of tables; the rest need manual `WITH OBJECTID` or `SetObjectMapping`

## SQL CE Version Support

Tested against 20 databases from 10 different sources (11,602 rows, zero errors).

| Version | Status |
|---------|--------|
| SQL CE 4.0 | Tested (builds 73799, 74018, 74390, 74412) |
| SQL CE 3.5 | Tested (builds 5357, 5386, 8080, 8081) |
| SQL CE 3.0 | Not supported |
| SQL CE 2.0 | Not supported |

## License

MIT
