# SQL Server Compact SDF File Format

## Overview

Microsoft SQL Server Compact Edition (SQL CE) stores each database in a single `.sdf` file.
It is an in-process relational database engine designed for low-footprint desktop and mobile use.

This document summarizes:

- Version history and compatibility
- Engine architecture and native components
- Physical and logical storage model
- Forensic identification and recovery considerations
- Deployment and migration practices

## Historical Evolution

SQL CE evolved across several product names and versions:

| Version | Core target platform | Significant features |
| --- | --- | --- |
| SQL Server CE 1.0/2.0 | Windows CE | Mobile-first, ActiveSync-era data sync |
| SQL Server CE 3.0/3.1 | Mobile and desktop | Visual Studio 2005 era desktop support |
| SQL Server Compact 3.5 | Desktop and mobile | LINQ support, 64-bit support |
| SQL Server Compact 4.0 | ASP.NET/WebMatrix | Private deployment, modernized web usage |

Deprecation was announced in 2013, and extended support for 4.0 ended in 2021, but `.sdf` files remain common in legacy systems.

## Engine Architecture

### In-process model

Unlike full SQL Server editions, SQL CE runs inside the host process. It does not run as a background service.

Benefits:

- Small binary/runtime footprint
- Simple app-local deployment

Trade-offs:

- Windows-only runtime behavior
- Careful handling of local file and locking semantics

### Core native components

| DLL | Role |
| --- | --- |
| `sqlceme40.dll` | Core engine API entry point |
| `sqlceca40.dll` | COM objects (engine/replication/error) |
| `sqlceoledb40.dll` | OLE DB provider |
| `sqlceqp40.dll` | Query processor |
| `sqlcese40.dll` | Storage engine |
| `sqlcecompact40.dll` | Compact/repair operations |
| `sqlceer40en.dll` | Error strings/resources |

## SDF Physical Layout

### Page and extent model

The database is page-based. Pages are grouped into extents (8 contiguous pages).

- Page header contains metadata such as page number/type and object ownership.
- Row data is stored in the page data region.
- A slot array tracks row offsets for variable-length row organization.

### Allocation maps

Allocation is tracked by special map pages:

- `GAM` (Global Allocation Map): tracks extent usage
- `SGAM` (Shared Global Allocation Map): tracks mixed extents with available pages

### Size limits

The practical maximum database size is about 4 GB, with common `Max Database Size` values configured up to `4091` MB.

## Binary Identification and Versioning

Forensic and parser workflows often rely on fixed-offset header checks.

| Offset | Meaning | Example |
| --- | --- | --- |
| `0x00` | Format/version signature | Legacy 2.0 commonly includes `saba` |
| `0x10` | Build/version metadata | E.g. `3505053` for a 3.5 SP1-era file |
| `0x20` | Extended metadata | Version-specific flags |

Opening files with mismatched provider/runtime versions commonly yields incompatible version errors.

## Security Characteristics

SQL CE supports file-level encryption.

- 3.5 era: legacy encryption support
- 4.0 era: stronger SHA2-related security updates

Access requires the correct password in the provider connection string.

## SQL Surface and Behavior

SQL CE supports a compact T-SQL subset focused on core CRUD and DDL use cases.

Notable limits versus full SQL Server:

- No stored procedures
- No triggers
- No views

4.0 added useful query functionality such as paging with `OFFSET/FETCH`.

## Transaction and Durability Notes

SQL CE is ACID-oriented, but flush behavior is important in failure scenarios.

- Buffered commit behavior can optimize performance.
- Immediate durability may require explicit immediate flush modes depending on API usage.

## Deployment Patterns

### Connection string example

```text
Provider=Microsoft.SQLSERVER.CE.OLEDB.4.0;Data Source=|DataDirectory|\MyData.sdf;ssce:max database size=4091;Persist Security Info=False;
```

### Common parameters

| Parameter | Purpose |
| --- | --- |
| `Data Source` | Path to `.sdf` file |
| `Max Database Size` | Maximum database size in MB |
| `Password` | Encryption password |
| `Flush Interval` | Buffered flush behavior |
| `Case Sensitive` | String comparison behavior |

### Private deployment

SQL CE 4.0 supports app-local deployment by shipping required managed/native DLLs with the application.

## Tooling and Migration

### Common management tools

- SQL Server Compact Toolbox (Visual Studio + standalone)
- LINQPad
- CompactView

### Migration targets

Most legacy migrations move to one of these:

- SQL Server/Azure SQL (feature-rich server model)
- SQLite (cross-platform embedded model)

| Feature | SQL CE | SQLite |
| --- | --- | --- |
| Platform | Windows-centric | Cross-platform |
| Packaging | Multiple native DLLs | Single native library |
| DB size profile | ~4 GB practical cap | Much larger practical limits |
| Built-in encryption | Included in SQL CE model | Usually extension-based |

## Forensic and Recovery Notes

Because `.sdf` is page-based, deleted rows can remain recoverable until pages are reused or compacted.

- Deleted slot entries may leave row remnants in page slack
- Compaction can permanently remove recoverable remnants
- Corruption analysis often includes checksum verification and page-level reconstruction

## Programmatic Example

```csharp
using System.Data.SqlServerCe;

string connStr = "Data Source=MyApp.sdf; Password='MySecurePassword'; Max Database Size=4091;";
SqlCeEngine engine = new SqlCeEngine(connStr);
engine.CreateDatabase();
```

## Summary

SQL CE `.sdf` files are compact, page-based relational containers built for embedded Windows applications.
Even after deprecation, they remain important for:

- Legacy system maintenance
- Data extraction and conversion
- Digital forensic workflows