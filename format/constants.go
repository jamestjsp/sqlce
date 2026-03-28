// Package format provides low-level binary parsing of SQL Server Compact Edition (.sdf) files.
package format

// SDF file magic number (little-endian u32 of bytes EB 6D 87 AE).
const Magic uint32 = 0xAE876DEB

// Default page size for SQL CE databases (4 KB).
const DefaultPageSize = 4096

// Header field offsets within page 0.
const (
	offsetMagic      = 0x00
	offsetVersion    = 0x10
	offsetPageCount  = 0x18
	offsetLCID       = 0x28
	offsetEncryption = 0x30
)

// Minimum header size required to parse all known fields.
const headerSize = 0x44

// SQLCEVersion represents the database engine version encoded in the file header.
type SQLCEVersion uint32

const (
	VersionCE20 SQLCEVersion = 0x73616261
	VersionCE30 SQLCEVersion = 0x002DD714
	VersionCE35 SQLCEVersion = 0x00357B9D
	VersionCE35b SQLCEVersion = 0x00357DD9
	VersionCE40 SQLCEVersion = 0x003D0900
)

// MajorVersion returns the human-readable major version number (2, 3, or 4).
func (v SQLCEVersion) MajorVersion() int {
	switch v {
	case VersionCE20:
		return 2
	case VersionCE30:
		return 3
	case VersionCE35, VersionCE35b:
		return 3
	case VersionCE40:
		return 4
	default:
		return 0
	}
}

func (v SQLCEVersion) String() string {
	switch v {
	case VersionCE20:
		return "SQL CE 2.0"
	case VersionCE30:
		return "SQL CE 3.0"
	case VersionCE35:
		return "SQL CE 3.5"
	case VersionCE35b:
		return "SQL CE 3.5"
	case VersionCE40:
		return "SQL CE 4.0"
	default:
		return "Unknown"
	}
}
