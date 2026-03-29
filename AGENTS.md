# sqlce

A pure Go library for reading SQL Server Compact Edition (.sdf) database files.

## Project Context

- **Task management**: We use `ergo` for backlog and task management. Use the ergo skill when appropriate.
- **Reference data**: Sample `.sdf` files, SQL scripts, and converted databases are available in the `data/` folder.
- **Testing**: Run `go test ./...` for full test suite, `go vet ./...` for static analysis.

## Key Packages

- `format/` - Low-level binary format parsing (pages, records, catalog)
- `engine/` - High-level API (Database, Table, Schema, RowIterator)
- `driver/` - `database/sql` driver implementation
- `cmd/sdfutil/` - CLI tool for inspecting and exporting SDF files