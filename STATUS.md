# SQL CE SDF Reader — Status Overview

## What Works

### Core Infrastructure (solid)
- Binary format parsing: header, page types, page caching (LRU)
- Type system: 18 SQL CE types mapped to Go types
- Record parser: handles 0xF0 (no null bitmap) and other header formats
- `database/sql` driver with `SELECT * FROM table [WITH OBJECTID n]`
- CLI tool (`sdfutil`): info, tables, schema, dump, export (CSV/JSON/SQLite)
- Row iterator API with Next/Values/Scan/Close

### Multi-Page Table Discovery (fixed in this session)
- Leaf page bytes `[0x10:0x12]` contain parent objectID matching `__SysObjects.dataObjID`
- All overflow pages discovered regardless of objectID distance
- 62 tables mapped via `extractObjectMap` (was ~50 before)

### Control Layer Tables — Row Counts
All 20 tables have correct row counts:

| Table | Rows | Status |
|-------|------|--------|
| Relation | 2 | OK |
| ItemInformation | 204 (ref: 203) | 1 extra row |
| RelationBlocks | 25 | OK |
| Blocks | 25 | OK |
| ModelLayerBlocks | 40 | OK |
| ModelLayers | 3 | OK |
| SisoRelation | 25 | OK |
| SisoElements | 27 | OK |
| ParametricElements | 27 | OK |
| ProcessVariables | 31 | OK |
| ControllerVariableReference | 21 | OK |
| VariableRole | 25 | OK |
| BlcModel | 3 | OK |
| Loop | 3 | OK |
| CVRole | 5 | OK |
| EconomicFunction | 2 | OK |
| VariableTransform | 1 | OK |
| Models | 2 | OK |
| ExecutionSequence | 2 | OK |
| UserParameter | 3 | OK |

### get_control_layer.sql Queries via SQLite Export
| Query | Description | Ref Rows | Exported Result |
|-------|------------|----------|-----------------|
| Q1 | Control layer matrix | 44 | FAILS (missing column) |
| Q2 | CV role constraints | 5 | 0 (broken GUIDs) |
| Q3 | Economic functions | 2 | 2 OK |
| Q4 | Variable transforms | 1 | 0 (broken GUIDs) |
| Q5 | Model metadata | 2 | 0 (broken GUIDs) |
| Q6 | Execution sequence | 2 | 2 OK |
| Q7 | User parameters | 3 | 1 (broken GUIDs) |
| Q8 | Loop details | 3 | 3 OK |

Q3, Q6, Q8 work because they don't JOIN through the 4 broken tables.

---

## What Does NOT Work

### Root Cause: Catalog Parser Misses Columns

`extractColumnRecords` in `format/catalog.go` uses a heuristic name-pair scanner
that misses columns whose catalog records fall near Leaf page boundaries.

**4 control-layer columns missing:**

| Table | Missing Column | Type | Impact |
|-------|---------------|------|--------|
| ProcessVariables | NormalMove | float (8 bytes) | GUIDs garbled — all JOINs through PV fail |
| SisoRelation | SisoRelationIdentifier | uniqueidentifier (16 bytes) | GUIDs garbled — Q1 SISO joins fail |
| ControllerVariableReference | ParentIdentifier | uniqueidentifier (16 bytes) | GUIDs garbled — Q1/Q2 CVRef joins fail |
| Blocks | IsFormulaValid | bit (1 byte) | GUIDs still correct (1-byte error self-corrects) |

**18 total columns missing across all 98 tables** (schema test allows up to 25).

### Why Missing Columns Break GUIDs

The record parser reads fixed-size columns sequentially. A missing N-byte column
means the parser reads N bytes too few for fixed data, enters the variable section
too early, and produces a wrong `nextOff`. The first record on a page parses
correctly; all subsequent records are garbled.

- Missing 8-byte float (ProcessVariables): 30/31 records garbled
- Missing 16-byte GUID (SisoRelation): 24/25 records garbled
- Missing 1-byte bit (Blocks): self-corrects, all 25 records OK

### ItemInformation: 204 vs 203 Rows

One extra row — likely a logically deleted record still physically on a page.
The status bytes at the start of each record (4 bytes) probably indicate deletion,
but the parser doesn't check them.

---

## Areas to Focus

### Priority 1: Fix the Catalog Parser

This is the single fix that unblocks everything. Two approaches:

#### Option A: Parse `__SysColumns` (recommended)
Parse the `__SysColumns` system table the same way `__SysObjects` is parsed.
`__SysColumns` stores authoritative column definitions (name, type, ordinal, table ID).

- Marker to search for: `"__SysColumns\x00"` in Leaf pages
- Similar pattern to `extractSysObjectRecords` in `format/catalog.go`
- Would replace the heuristic `extractColumnRecords` entirely
- Eliminates all 18 missing column issues at once

#### Option B: Fix Page-Boundary Handling in `extractColumnRecords`
The heuristic scanner processes each page independently. Records near the end of
a page may have their metadata (at `name_offset - 66`) on the previous page.

- Buffer the last ~100 bytes of each page and prepend to the next
- More surgical fix but doesn't address other heuristic fragilities

### Priority 2: Record Status Byte Parsing

The 4-byte status field at the start of each record is currently ignored.
Parsing it would:
- Filter deleted records (fixes ItemInformation 204→203)
- Potentially improve record boundary detection

### Priority 3: Fixed Column Physical Ordering

Empirically verified that SQL CE stores fixed columns in **size-descending** order
(not ordinal order). This was confirmed on ProcessVariables where float64 1.0 sits
at offset 48 (after 3 GUIDs), not at offset 49 (after 3 GUIDs + 1 bit).

The current parser works because the catalog schemas happen to have same-size
columns (all GUIDs) or the error is small enough to self-correct. Once the
catalog is fixed (Priority 1), the size-ordering may need explicit handling for
tables with mixed-size fixed columns.

Additional finding: null fixed columns may NOT be stored on disk (their bytes are
absent, not zeroed). The null bitmap (MSB-first per byte, bit=1 = non-null) likely
controls this. Behavior may vary by header byte value. This needs more research.

### Priority 4: Control Layer Extractor

Once Priorities 1-2 are done, implement `engine/control_layer.go`:
- Load 20 tables into memory with correct schemas
- In-memory GUID-indexed JOIN logic for the 8 queries
- `sdfutil control-layer <file.sdf>` command outputting JSON

---

## Key Files

| File | Purpose |
|------|---------|
| `format/catalog.go` | Catalog parser — **fix here for Priority 1** |
| `format/record.go` | Record parser (parseOneRecord, variable section) |
| `format/pagetype.go` | Page type classification, objectID extraction |
| `format/types.go` | SQL CE type registry (18 types) |
| `engine/mapping.go` | ObjectID mapping (BuildTableMapping) |
| `engine/scanner.go` | Table scanning, record conversion |
| `engine/database.go` | High-level Database/Table API |
| `data/Depropanizer.sdf` | Test database (98 tables, 1221 pages) |
| `data/Depropanizer.db` | SQLite reference (ground truth) |
| `data/dc3.sql` | SQL export with correct CREATE TABLE schemas |

## Hard-Won Rules (Don't Break These)

- Record header `0xF0` = no null bitmap; all others = bitmap of `ceil(cols/8)` bytes
- Don't add a `0x00` stop condition to variable section scan — breaks 0xF0 tables
- Don't trust the page header's `colCount` at `0x1C` — it's often garbage for catalog pages
- Leaf page `[0x10:0x12]` = parent objectID (the multi-page discovery mechanism)
- Variable section data is ASCII, NOT UTF-16LE
- The `sort` import was added to `record.go` during investigation but reverted — don't re-add without the full size-sorting implementation
