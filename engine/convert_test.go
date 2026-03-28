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
