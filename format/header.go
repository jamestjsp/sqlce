package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// FileHeader holds the parsed metadata from page 0 of an SDF file.
type FileHeader struct {
	// Magic is the 4-byte signature (must equal 0xAE876DEB).
	Magic uint32

	// Version is the raw database engine version identifier.
	Version SQLCEVersion

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

var (
	ErrNotSDF          = errors.New("not an SDF file: invalid magic bytes")
	ErrUnknownVersion  = errors.New("unknown SQL CE version")
	ErrHeaderTooSmall  = errors.New("file too small to contain a valid SDF header")
)

// ReadHeader reads and validates the SDF file header from r.
// The reader must support random access (io.ReaderAt) so that pages
// can later be read at arbitrary offsets.
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

	h.Magic = le.Uint32(buf[offsetMagic:])
	if h.Magic != Magic {
		return nil, fmt.Errorf("%w: got 0x%08X, want 0x%08X", ErrNotSDF, h.Magic, Magic)
	}

	h.Version = SQLCEVersion(le.Uint32(buf[offsetVersion:]))
	if h.Version.MajorVersion() == 0 {
		return nil, fmt.Errorf("%w: 0x%08X", ErrUnknownVersion, uint32(h.Version))
	}

	h.PageCount = le.Uint32(buf[offsetPageCount:])
	h.LCID = le.Uint32(buf[offsetLCID:])
	h.EncryptionType = le.Uint32(buf[offsetEncryption:])
	h.Encrypted = h.EncryptionType != 0

	// Page size is fixed at 4096 for all known SQL CE versions.
	h.PageSize = DefaultPageSize

	return h, nil
}
