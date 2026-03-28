package format

import (
	"encoding/binary"
	"io"
)

// EncryptionAlgorithm identifies the encryption method used on an SDF file.
type EncryptionAlgorithm int

const (
	EncryptionNone   EncryptionAlgorithm = iota
	EncryptionRC4                        // Legacy 128-bit RC4
	EncryptionAES128                     // AES-128 (CE 4.0 "platform default")
	EncryptionAES256                     // AES-256
)

func (a EncryptionAlgorithm) String() string {
	switch a {
	case EncryptionNone:
		return "None"
	case EncryptionRC4:
		return "RC4"
	case EncryptionAES128:
		return "AES-128"
	case EncryptionAES256:
		return "AES-256"
	default:
		return "Unknown"
	}
}

// EncryptionInfo describes the encryption state of an SDF file.
type EncryptionInfo struct {
	// Encrypted is true if the database uses any form of encryption.
	Encrypted bool

	// Algorithm identifies the encryption method.
	Algorithm EncryptionAlgorithm

	// RawFlag is the raw value from the header encryption field.
	RawFlag uint32
}

// Encryption-related header offsets.
// The encryption flag occupies 4 bytes starting at page offset 0x20.
// For unencrypted databases this region is typically 0x00000001 or similar
// small values. Encrypted databases have distinct signatures in a different
// header region.
const (
	offsetEncFlag = 0x20 // u32 LE — general flags (not a direct encryption indicator)
)

// DetectEncryption reads the SDF header and determines encryption state.
// An unencrypted SQL CE 4.0 file can be read directly; an encrypted file
// requires a password to derive the decryption key.
func DetectEncryption(r io.ReaderAt) (*EncryptionInfo, error) {
	h, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}
	return detectEncryptionFromHeader(r, h)
}

func detectEncryptionFromHeader(r io.ReaderAt, h *FileHeader) (*EncryptionInfo, error) {
	info := &EncryptionInfo{RawFlag: h.EncryptionType}

	// Heuristic: if we can read and classify page 1 as a valid Leaf page,
	// the file is not encrypted (encrypted pages would be garbled).
	buf := make([]byte, h.PageSize)
	n, err := r.ReadAt(buf, int64(h.PageSize)) // page 1
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n < 8 {
		info.Encrypted = true
		info.Algorithm = EncryptionAES128
		return info, nil
	}

	// Page 1 of an unencrypted DB has a valid page type at offset 6
	// and recognizable structure (e.g., "Value" string at offset 0x1C).
	pageType := PageType(buf[6])
	if pageType.IsKnown() && pageType != PageFree {
		// Additional check: look for "Value" string in page 1 header area
		hasValue := len(buf) > 0x21 &&
			buf[0x1C] == 'V' && buf[0x1D] == 'a' && buf[0x1E] == 'l'

		// Check that the object ID field has reasonable values
		objID := binary.LittleEndian.Uint16(buf[4:6])
		if hasValue || objID > 0 {
			info.Encrypted = false
			info.Algorithm = EncryptionNone
			return info, nil
		}
	}

	// If page 1 doesn't look valid, assume encrypted
	info.Encrypted = true
	// Default to AES-128 for CE 4.0
	if h.Version == VersionCE40 {
		info.Algorithm = EncryptionAES128
	} else {
		info.Algorithm = EncryptionRC4
	}
	return info, nil
}
