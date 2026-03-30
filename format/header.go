package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FileHeader holds the parsed metadata from page 0 of an SDF file.
type FileHeader struct {
	// DatabaseID is a per-database identifier at offset 0x00 (NOT a fixed magic number).
	DatabaseID uint32

	// Version is the raw database engine version identifier at offset 0x10.
	Version SQLCEVersion

	// BuildNumber is the engine build number at offset 0x34.
	BuildNumber uint32

	// PageSize is the size of each database page in bytes (always 4096 for CE 4.0).
	PageSize int

	// PageCount is the internal page count stored in the header.
	PageCount uint32

	// LCID is the Windows locale identifier (e.g. 1033 for en-US).
	LCID uint32

	// Encrypted indicates whether the database file is encrypted.
	Encrypted bool

	// EncryptionType is the raw encryption field value from the header.
	EncryptionType uint32
}

// Magic returns DatabaseID for backward compatibility.
func (h *FileHeader) Magic() uint32 { return h.DatabaseID }

// VersionString returns version with build number (e.g. "SQL CE 4.0 (build 74412)").
func (h *FileHeader) VersionString() string {
	s := h.Version.String()
	if h.BuildNumber > 0 {
		return fmt.Sprintf("%s (build %d)", s, h.BuildNumber)
	}
	return s
}

var (
	ErrNotSDF         = errors.New("not an SDF file")
	ErrUnknownVersion = errors.New("unknown SQL CE version")
	ErrHeaderTooSmall = errors.New("file too small to contain a valid SDF header")
)

// ReadHeader reads and validates the SDF file header from r.
// Identification uses the version field at offset 0x10 (not offset 0x00,
// which is a per-database ID). Bytes 4-7 must be zero as a secondary check.
func ReadHeader(r io.ReaderAt) (*FileHeader, error) {
	buf := make([]byte, headerSize)
	n, err := r.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("reading SDF header: %w", err)
	}
	if n < headerSize {
		return nil, ErrHeaderTooSmall
	}

	le := binary.LittleEndian
	h := &FileHeader{}

	h.DatabaseID = le.Uint32(buf[offsetDatabaseID:])

	// Bytes 4-7 are always zero in valid SDF files
	reserved := le.Uint32(buf[offsetReserved:])
	if reserved != 0 {
		return nil, fmt.Errorf("%w: non-zero reserved bytes at offset 0x04", ErrNotSDF)
	}

	h.Version = SQLCEVersion(le.Uint32(buf[offsetVersion:]))
	if h.Version.MajorVersion() == 0 {
		return nil, fmt.Errorf("%w: unrecognized version 0x%08X at offset 0x10", ErrNotSDF, uint32(h.Version))
	}

	h.BuildNumber = le.Uint32(buf[offsetBuildNumber:])
	h.PageCount = le.Uint32(buf[offsetPageCount:])
	h.LCID = le.Uint32(buf[offsetLCID:])
	h.EncryptionType = le.Uint32(buf[offsetEncryption:])
	h.Encrypted = h.EncryptionType != 0
	h.PageSize = DefaultPageSize

	return h, nil
}
