package format

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func TestReadHeader(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	if h.Magic != Magic {
		t.Errorf("Magic = 0x%08X, want 0x%08X", h.Magic, Magic)
	}
	if h.Version != VersionCE40 {
		t.Errorf("Version = 0x%08X, want 0x%08X (CE 4.0)", uint32(h.Version), uint32(VersionCE40))
	}
	if h.Version.MajorVersion() != 4 {
		t.Errorf("MajorVersion = %d, want 4", h.Version.MajorVersion())
	}
	if h.LCID != 1033 {
		t.Errorf("LCID = %d, want 1033", h.LCID)
	}
	if h.PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", h.PageSize)
	}
	if h.PageCount == 0 {
		t.Error("PageCount should be non-zero")
	}

	t.Logf("Header: version=%s, LCID=%d, pages=%d, encrypted=%v (type=%d)",
		h.Version, h.LCID, h.PageCount, h.Encrypted, h.EncryptionType)
}

func TestReadHeaderRejectsNonSDF(t *testing.T) {
	garbage := bytes.NewReader(make([]byte, 256))
	_, err := ReadHeader(garbage)
	if err == nil {
		t.Fatal("expected error for non-SDF data, got nil")
	}
	t.Logf("correctly rejected non-SDF: %v", err)
}

func TestReadHeaderRejectsTooSmall(t *testing.T) {
	tiny := bytes.NewReader(make([]byte, 8))
	_, err := ReadHeader(tiny)
	if err == nil {
		t.Fatal("expected error for undersized data, got nil")
	}
	t.Logf("correctly rejected too-small input: %v", err)
}

func TestReadHeaderRejectsUnknownVersion(t *testing.T) {
	buf := make([]byte, headerSize)
	le := binary.LittleEndian

	// Valid magic, but bogus version
	le.PutUint32(buf[offsetMagic:], Magic)
	le.PutUint32(buf[offsetVersion:], 0xDEADBEEF)

	_, err := ReadHeader(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected error for unknown version, got nil")
	}
	t.Logf("correctly rejected unknown version: %v", err)
}

func TestSQLCEVersionString(t *testing.T) {
	tests := []struct {
		v    SQLCEVersion
		want string
	}{
		{VersionCE20, "SQL CE 2.0"},
		{VersionCE30, "SQL CE 3.0"},
		{VersionCE35, "SQL CE 3.5"},
		{VersionCE35b, "SQL CE 3.5"},
		{VersionCE40, "SQL CE 4.0"},
		{SQLCEVersion(0xFFFF), "Unknown"},
	}
	for _, tc := range tests {
		if got := tc.v.String(); got != tc.want {
			t.Errorf("%#v.String() = %q, want %q", tc.v, got, tc.want)
		}
	}
}
