package engine

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestGUIDParse(t *testing.T) {
	// Example: {6F9619FF-8B86-D011-B42D-00C04FC964FF}
	// SQL Server byte order (mixed endian):
	b := []byte{
		0xFF, 0x19, 0x96, 0x6F, // Data1 LE: 6F9619FF
		0x86, 0x8B,             // Data2 LE: 8B86
		0x11, 0xD0,             // Data3 LE: D011
		0xB4, 0x2D,             // Data4[0-1]
		0x00, 0xC0, 0x4F, 0xC9, 0x64, 0xFF, // Data4[2-7]
	}

	got, err := ParseGUID(b)
	if err != nil {
		t.Fatalf("ParseGUID: %v", err)
	}
	want := "6f9619ff-8b86-d011-b42d-00c04fc964ff"
	if got != want {
		t.Errorf("ParseGUID = %q, want %q", got, want)
	}
}

func TestGUIDZero(t *testing.T) {
	b := make([]byte, 16)
	got, err := ParseGUID(b)
	if err != nil {
		t.Fatalf("ParseGUID: %v", err)
	}
	want := "00000000-0000-0000-0000-000000000000"
	if got != want {
		t.Errorf("zero GUID = %q, want %q", got, want)
	}
}

func TestGUIDTooShort(t *testing.T) {
	_, err := ParseGUID(make([]byte, 8))
	if err == nil {
		t.Error("expected error for short input")
	}
}

func TestOLEDateTimeParse(t *testing.T) {
	tests := []struct {
		name   string
		days   float64
		expect time.Time
	}{
		{
			name:   "OLE epoch",
			days:   0,
			expect: time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "Jan 1 1900",
			days:   2,
			expect: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "Jan 1 2000",
			days:   36526,
			expect: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:   "Jan 1 2000 12:00:00",
			days:   36526.5,
			expect: time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, math.Float64bits(tc.days))

			got, err := ParseOLEDateTime(b)
			if err != nil {
				t.Fatalf("ParseOLEDateTime: %v", err)
			}

			diff := got.Sub(tc.expect)
			if diff < -time.Second || diff > time.Second {
				t.Errorf("got %v, want %v (diff %v)", got, tc.expect, diff)
			}
		})
	}
}

func TestOLEDateTimeTooShort(t *testing.T) {
	_, err := ParseOLEDateTime(make([]byte, 4))
	if err == nil {
		t.Error("expected error for short input")
	}
}

func TestConvertValueIntegers(t *testing.T) {
	tests := []struct {
		name   string
		typeID uint16
		data   []byte
		want   any
	}{
		{"bit-true", 0x0B, []byte{1}, true},
		{"bit-false", 0x0B, []byte{0}, false},
		{"tinyint", 0x01, []byte{42}, uint8(42)},
		{"smallint", 0x02, []byte{0xE8, 0x03}, int16(1000)},
		{"int", 0x03, []byte{0x39, 0x05, 0x00, 0x00}, int32(1337)},
		{"bigint", 0x04, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}, int64(math.MaxInt64)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertValue(tc.data, tc.typeID)
			if err != nil {
				t.Fatalf("ConvertValue: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestConvertValueFloats(t *testing.T) {
	// float64
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(3.14))
	got, err := ConvertValue(b, 0x05)
	if err != nil {
		t.Fatalf("ConvertValue float: %v", err)
	}
	if v, ok := got.(float64); !ok || math.Abs(v-3.14) > 1e-10 {
		t.Errorf("got %v, want ~3.14", got)
	}

	// float32 (real)
	b32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b32, math.Float32bits(2.5))
	got32, err := ConvertValue(b32, 0x06)
	if err != nil {
		t.Fatalf("ConvertValue real: %v", err)
	}
	if v, ok := got32.(float32); !ok || math.Abs(float64(v)-2.5) > 1e-5 {
		t.Errorf("got %v, want ~2.5", got32)
	}
}

func TestConvertValueNVarchar(t *testing.T) {
	// "Hello" in UTF-16LE
	data := []byte{0x48, 0x00, 0x65, 0x00, 0x6C, 0x00, 0x6C, 0x00, 0x6F, 0x00}
	got, err := ConvertValue(data, 0x1F)
	if err != nil {
		t.Fatalf("ConvertValue nvarchar: %v", err)
	}
	if got != "Hello" {
		t.Errorf("got %q, want %q", got, "Hello")
	}
}

func TestConvertValueGUID(t *testing.T) {
	b := make([]byte, 16)
	b[3] = 0x01 // Data1 = 0x01000000
	got, err := ConvertValue(b, 0x65)
	if err != nil {
		t.Fatalf("ConvertValue guid: %v", err)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T", got)
	}
	if len(s) != 36 {
		t.Errorf("GUID length = %d, want 36", len(s))
	}
}

func TestConvertValueBinary(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	got, err := ConvertValue(data, 0x3F) // varbinary
	if err != nil {
		t.Fatalf("ConvertValue varbinary: %v", err)
	}
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if len(b) != 4 || b[0] != 0xDE {
		t.Errorf("unexpected binary: %x", b)
	}
	// Verify it's a copy
	data[0] = 0xFF
	if b[0] == 0xFF {
		t.Error("returned slice is not a copy")
	}
}
