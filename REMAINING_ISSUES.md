# Remaining Issues — SQL CE SDF Reader

## Current State

All queries produce correct row counts.

```
Q1: 44 ✓                 Q5: 2 ✓
Q2: 5 ✓                  Q6: 2 ✓
Q3: 2 ✓                  Q7: 3 ✓
Q4: 1 ✓                  Q8: 3 ✓
```

---

## Issue 1: Bit Column Values — FIXED

Bit columns are correctly extracted from the bitmap section of each record.
The bitmap layout is `[column-count header bytes][bit value bytes]` where
the header bytes use high bits as padding (1s for unused positions).
Bit extraction reads from `byte[nullFlagBytes + i/8]`, bit `i%8`.

Additionally, the fixed data area size was incorrect for tables with
overlapping column positions (e.g., ParametricElements where int4 followed
by float8 columns caused position overlap). The fix scans backward from
the entry end to locate the variable section `0x00 0x80` marker, correctly
determining the fixed/variable boundary. This fixed TransferFunction
extraction for ParametricElements and unblocked Q1.

---

## Issue 2: Record Boundary Scanner False Positives (MEDIUM)

**What**: `ParsePageRecords` (`format/record.go:62-74`) finds records by scanning for `[00000000][colCount LE32]`. This 8-byte pattern can match within record data.

**Example**: For a table with `colCount=6`, the pattern is `00 00 00 00 06 00 00 00`. Any 4 zero bytes in GUID data followed by `06 00 00 00` in variable data would match.

**Mitigation already in place**: `len(recOffsets) < recordCount` stops after finding the expected number of records. This prevents unbounded false positives.

**Remaining risk**: If a false positive occurs BEFORE a real record, it takes the real record's slot. The real record is then missed, and parsing from the false positive position garbles that "record."

**Observable effect**: Some nvarchar values are garbled (e.g., VariableRole.RoleType distribution differs from reference: our CV=6/DV=10 vs ref CV=5/DV=8). This contributes to Q1 returning 19 instead of 44.

**Fix approach**: Validate found record positions — e.g., check that `page[offset+8]` (header byte) has reasonable values, or verify the first GUID column against known valid GUIDs.

---

## Issue 3: Variable Section 0x80 Scan Unbounded (MEDIUM)

**What**: After fixed data, the parser scans forward for `0x80` to find the variable section start (`format/record.go:130-134`):
```go
for offset < len(page)-4 && page[offset] != 0x80 {
    offset++
}
```

**Risk**: If the byte after fixed data isn't 0x80, the scan continues through the entire page until it finds one in unrelated data. This can:
- Skip past the actual variable section
- Land on 0x80 in nvarchar text or GUID bytes of a different record
- Produce garbage variable column values

**Why it works now**: For most tables, the 0x80 flag IS immediately after fixed data (or within 1-2 bytes). The scan succeeds quickly.

**When it fails**: When there's significant padding between fixed and variable data, or when the variable section doesn't start with 0x80 (e.g., first variable column is NULL → flag = 0x00).

**Fix approach**: Bound the scan to `min(fixedDataEnd + 8, nextRecordStart)` to prevent overshooting.

---

## Issue 4: Column Physical Ordering (MEDIUM — latent)

**What**: STATUS.md documents that SQL CE stores fixed columns in size-descending order. But implementation testing showed ordinal order works for GUID joins (all GUIDs are same size), and the sort was reverted.

**Current state**: Parser reads in ordinal order (`format/record.go:41-50`). This is correct for:
- Tables with all same-size fixed columns (e.g., all GUIDs)
- Tables where the first column is the largest (GUID at ordinal 1)

**When it breaks**: Tables where smaller fixed columns (int, smallint) appear before larger ones (GUID) in ordinal order. The parser reads wrong bytes for each column.

**Evidence**: The earlier sort attempt broke SisoRelation (6 cols: GUID, GUID, bit, bit, GUID, GUID). With ordinal order, the two middle bits "shift" columns 5-6. With bit FixedSize=0, the bits don't occupy space, so ordinal order now works correctly for SisoRelation.

**Current theory**: With bit FixedSize=0, the remaining non-bit fixed columns may actually be in ordinal order on disk. The original "size-descending" finding was for ProcessVariables where the bit column's phantom byte was misinterpreted as part of the float.

**Risk**: Low now. If a table has int(4) before GUID(16) in ordinal order, the parser would read the int from the first 4 bytes of the GUID, garbling both. No such case has been observed in control-layer tables.

---

## Issue 5: Deleted Record Filtering (LOW)

**What**: `parseOneRecord` reads 4 status bytes but discards them (`format/record.go:103`). ItemInformation has 204 rows instead of 203 — one deleted record is included.

**Impact**: The extra row's GUID doesn't match any foreign key, so it never appears in JOIN results. Functionally harmless for the control layer extractor.

**Fix approach**: Check if `status != 0x00000000` and skip the record. The record boundary scanner already requires `status == 0`, so deleted records (with non-zero status) are naturally excluded. This issue may already be fixed by the scanner.

---

## Issue 6: Null Bitmap Extra Bytes Hardcoded (LOW — fragility)

**What**: `referenceNullBmpExtra` in `format/reference.go:10-32` is a hardcoded map of 57 table names to their extra null bitmap byte counts, empirically determined from Depropanizer.sdf.

**Risk**: If the reader is used with a DIFFERENT .sdf file, the bitmap sizes would be wrong for tables not in this map (defaults to 0).

**Fix approach**: Auto-detect bitmap size at scan time by trying different sizes and checking if the first record produces a valid GUID (non-zero D1 for the first GUID column). Cache per table.

---

## Priority Order

1. ~~**Bit extraction from bitmap**~~ — DONE. Q1 now produces 44 rows.
2. **Record boundary validation** → reduces false positive garbling
3. **Bounded 0x80 scan** → prevents variable section overshoot
4. **Auto-detect bitmap size** → removes hardcoded dependency on Depropanizer.sdf
5. **Status byte check** → cosmetic (already mitigated by scanner)
6. **Column ordering** → monitor but likely not needed with bit FixedSize=0
