package format

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rc4"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type EncryptionAlgorithm int

const (
	EncryptionNone   EncryptionAlgorithm = iota
	EncryptionRC4
	EncryptionAES128
	EncryptionAES256
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

type EncryptionInfo struct {
	Encrypted bool
	Algorithm EncryptionAlgorithm
	RawFlag   uint32
}

const (
	offsetEncFlag = 0x20
)

var (
	ErrWrongPassword = errors.New("wrong password or corrupted database")
	ErrEncrypted     = errors.New("database is encrypted; password required")
)

// Decryptor decrypts a single database page.
// pageNum is the 0-based page index; data is the raw ciphertext.
// Returns decrypted page bytes. Page 0 (header) is never encrypted.
type Decryptor interface {
	DecryptPage(pageNum int, data []byte) ([]byte, error)
}

func DetectEncryption(r io.ReaderAt) (*EncryptionInfo, error) {
	h, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}
	return detectEncryptionFromHeader(r, h)
}

func detectEncryptionFromHeader(r io.ReaderAt, h *FileHeader) (*EncryptionInfo, error) {
	info := &EncryptionInfo{RawFlag: h.EncryptionType}

	buf := make([]byte, h.PageSize)
	n, err := r.ReadAt(buf, int64(h.PageSize))
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n < 8 {
		info.Encrypted = true
		info.Algorithm = EncryptionAES128
		return info, nil
	}

	pageType := PageType(buf[6])
	if pageType.IsKnown() && pageType != PageFree {
		hasValue := len(buf) > 0x21 &&
			buf[0x1C] == 'V' && buf[0x1D] == 'a' && buf[0x1E] == 'l'
		objID := binary.LittleEndian.Uint16(buf[4:6])
		if hasValue || objID > 0 {
			info.Encrypted = false
			info.Algorithm = EncryptionNone
			return info, nil
		}
	}

	info.Encrypted = true
	if h.Version == VersionCE40 {
		info.Algorithm = EncryptionAES128
	} else {
		info.Algorithm = EncryptionRC4
	}
	return info, nil
}

// DeriveKey produces a 128-bit encryption key from the user password.
// SQL CE 4.0 uses SHA-256 of the UTF-16LE password, truncated to 16 bytes.
// Older versions use SHA-256 truncated to 16 bytes as well (RC4 key size).
func DeriveKey(password string) []byte {
	utf16 := encodeUTF16LE(password)
	hash := sha256.Sum256(utf16)
	key := make([]byte, 16)
	copy(key, hash[:16])
	return key
}

func encodeUTF16LE(s string) []byte {
	buf := make([]byte, len(s)*2)
	for i, r := range s {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(r))
	}
	return buf
}

type rc4Decryptor struct {
	key []byte
}

// NewRC4Decryptor creates a Decryptor using RC4 with the given key.
func NewRC4Decryptor(key []byte) Decryptor {
	k := make([]byte, len(key))
	copy(k, key)
	return &rc4Decryptor{key: k}
}

func (d *rc4Decryptor) DecryptPage(pageNum int, data []byte) ([]byte, error) {
	if pageNum == 0 || len(data) == 0 {
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}

	// Per-page key: append 4-byte LE page number to base key
	perPageKey := make([]byte, len(d.key)+4)
	copy(perPageKey, d.key)
	binary.LittleEndian.PutUint32(perPageKey[len(d.key):], uint32(pageNum))

	c, err := rc4.NewCipher(perPageKey)
	if err != nil {
		return nil, fmt.Errorf("rc4 decrypt page %d: %w", pageNum, err)
	}

	out := make([]byte, len(data))
	c.XORKeyStream(out, data)
	return out, nil
}

type aes128Decryptor struct {
	key []byte
}

// NewAES128Decryptor creates a Decryptor using AES-128-CBC with the given key.
func NewAES128Decryptor(key []byte) (Decryptor, error) {
	if len(key) != 16 {
		return nil, fmt.Errorf("AES-128 requires 16-byte key, got %d", len(key))
	}
	k := make([]byte, 16)
	copy(k, key)
	return &aes128Decryptor{key: k}, nil
}

func (d *aes128Decryptor) DecryptPage(pageNum int, data []byte) ([]byte, error) {
	if pageNum == 0 || len(data) == 0 {
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}

	block, err := aes.NewCipher(d.key)
	if err != nil {
		return nil, fmt.Errorf("aes decrypt page %d: %w", pageNum, err)
	}

	// IV derived from page number: 16-byte LE page number (zero-padded)
	iv := make([]byte, aes.BlockSize)
	binary.LittleEndian.PutUint32(iv, uint32(pageNum))

	if len(data)%aes.BlockSize != 0 {
		// Data not block-aligned: decrypt only the aligned portion
		aligned := (len(data) / aes.BlockSize) * aes.BlockSize
		if aligned == 0 {
			out := make([]byte, len(data))
			copy(out, data)
			return out, nil
		}
		out := make([]byte, len(data))
		mode := cipher.NewCBCDecrypter(block, iv)
		mode.CryptBlocks(out[:aligned], data[:aligned])
		copy(out[aligned:], data[aligned:])
		return out, nil
	}

	out := make([]byte, len(data))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(out, data)
	return out, nil
}

// NewDecryptor creates the appropriate Decryptor for the given algorithm and password.
func NewDecryptor(alg EncryptionAlgorithm, password string) (Decryptor, error) {
	key := DeriveKey(password)
	switch alg {
	case EncryptionRC4:
		return NewRC4Decryptor(key), nil
	case EncryptionAES128:
		return NewAES128Decryptor(key)
	case EncryptionAES256:
		return nil, fmt.Errorf("AES-256 decryption not yet supported")
	default:
		return nil, fmt.Errorf("unknown encryption algorithm: %v", alg)
	}
}

// ValidateDecryptor checks whether a decryptor can correctly decrypt page 1.
// A correct password should produce a page with a recognized page type.
func ValidateDecryptor(r io.ReaderAt, h *FileHeader, dec Decryptor) error {
	buf := make([]byte, h.PageSize)
	n, err := r.ReadAt(buf, int64(h.PageSize))
	if err != nil && err != io.EOF {
		return err
	}
	buf = buf[:n]

	plain, err := dec.DecryptPage(1, buf)
	if err != nil {
		return err
	}

	if len(plain) < 8 {
		return ErrWrongPassword
	}

	pt := PageType(plain[6])
	if pt.IsKnown() && pt != PageFree {
		return nil
	}
	return ErrWrongPassword
}

type nullDecryptor struct{}

func (nullDecryptor) DecryptPage(_ int, data []byte) ([]byte, error) {
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

// NullDecryptor returns a Decryptor that passes data through unchanged.
func NullDecryptor() Decryptor {
	return nullDecryptor{}
}
