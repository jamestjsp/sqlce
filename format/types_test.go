package format

import (
	"os"
	"reflect"
	"testing"
	"time"
)

func TestTypeMappingKnownTypes(t *testing.T) {
	tests := []struct {
		id       uint16
		name     string
		fixed    int
		variable bool
	}{
		{TypeTinyInt, "tinyint", 1, false},
		{TypeSmallInt, "smallint", 2, false},
		{TypeInt, "int", 4, false},
		{TypeBigInt, "bigint", 8, false},
		{TypeFloat, "float", 8, false},
		{TypeReal, "real", 4, false},
		{TypeDatetime, "datetime", 8, false},
		{TypeDatetime, "datetime", 8, false},
		{TypeMoney, "money", 8, false},
		{TypeBit, "bit", 1, false},
		{TypeNChar, "nchar", 0, true},
		{TypeNVarchar, "nvarchar", 0, true},
		{TypeVarBinary, "varbinary", 0, true},
		{TypeBinary, "binary", 0, true},
		{TypeImage, "image", 16, false},
		{TypeNText, "ntext", 16, false},
		{TypeNumeric, "numeric", 19, false},
		{TypeUniqueIdentifier, "uniqueidentifier", 16, false},
	}

	for _, tc := range tests {
		info := LookupType(tc.id)
		if info.Name != tc.name {
			t.Errorf("type 0x%02X: name = %q, want %q", tc.id, info.Name, tc.name)
		}
		if info.FixedSize != tc.fixed {
			t.Errorf("type 0x%02X (%s): fixedSize = %d, want %d", tc.id, tc.name, info.FixedSize, tc.fixed)
		}
		if info.IsVariable != tc.variable {
			t.Errorf("type 0x%02X (%s): isVariable = %v, want %v", tc.id, tc.name, info.IsVariable, tc.variable)
		}
		if info.GoType == nil {
			t.Errorf("type 0x%02X (%s): GoType is nil", tc.id, tc.name)
		}
	}
}

func TestDatetimeGoType(t *testing.T) {
	info := LookupType(TypeDatetime)
	want := reflect.TypeOf(time.Time{})
	if info.GoType != want {
		t.Errorf("datetime GoType = %v, want %v", info.GoType, want)
	}
}

func TestNCharLookup(t *testing.T) {
	info := LookupType(TypeNChar)
	if info.Name != "nchar" {
		t.Errorf("NChar name = %q, want %q", info.Name, "nchar")
	}
	if !info.IsVariable {
		t.Error("NChar should be variable")
	}
}

func TestTypeMappingUnknownType(t *testing.T) {
	info := LookupType(0xFF)
	if info.Name != "unknown" {
		t.Errorf("unknown type name = %q, want %q", info.Name, "unknown")
	}
}

func TestTypeMappingCatalogCoverage(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	fi, _ := f.Stat()
	totalPages := int(fi.Size()) / h.PageSize
	pr := NewPageReader(f, h, 128)

	cat, err := ReadCatalog(pr, totalPages)
	if err != nil {
		t.Fatalf("ReadCatalog: %v", err)
	}

	// Collect all type IDs used in the database
	typeIDs := make(map[uint16]int)
	for _, table := range cat.Tables {
		for _, col := range table.Columns {
			typeIDs[col.TypeID]++
		}
	}

	unmapped := 0
	for id, count := range typeIDs {
		info := LookupType(id)
		if info.Name == "unknown" {
			t.Errorf("unmapped type ID 0x%02X used by %d columns", id, count)
			unmapped++
		} else {
			t.Logf("  type 0x%02X %-20s: %d columns", id, info.Name, count)
		}
	}

	if unmapped > 0 {
		t.Errorf("%d unmapped type IDs found", unmapped)
	}
}
