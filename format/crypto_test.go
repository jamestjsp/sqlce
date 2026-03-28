package format

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rc4"
	"encoding/binary"
	"os"
	"testing"
)

func TestEncryptionDetectUnencrypted(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Fatalf("opening sample SDF: %v", err)
	}
	defer f.Close()

	info, err := DetectEncryption(f)
	if err != nil {
		t.Fatalf("DetectEncryption: %v", err)
	}

	if info.Encrypted {
		t.Errorf("Depropanizer.sdf detected as encrypted, want unencrypted")
	}
	if info.Algorithm != EncryptionNone {
		t.Errorf("algorithm = %s, want None", info.Algorithm)
	}

	t.Logf("Encryption: encrypted=%v, algorithm=%s, rawFlag=%d",
		info.Encrypted, info.Algorithm, info.RawFlag)
}

func TestEncryptionAlgorithmString(t *testing.T) {
	tests := []struct {
		alg  EncryptionAlgorithm
		want string
	}{
		{EncryptionNone, "None"},
		{EncryptionRC4, "RC4"},
		{EncryptionAES128, "AES-128"},
		{EncryptionAES256, "AES-256"},
	}
	for _, tc := range tests {
		if got := tc.alg.String(); got != tc.want {
			t.Errorf("EncryptionAlgorithm(%d).String() = %q, want %q", tc.alg, got, tc.want)
		}
	}
}

func TestDecryptionNullDecryptor(t *testing.T) {
	dec := NullDecryptor()
	data := []byte{0x01, 0x02, 0x03, 0x04}
	out, err := dec.DecryptPage(1, data)
	if err != nil {
		t.Fatalf("DecryptPage: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Errorf("NullDecryptor changed data: got %x, want %x", out, data)
	}
	out[0] = 0xFF
	if data[0] == 0xFF {
		t.Error("NullDecryptor returned aliased slice")
	}
}

func TestDecryptionPage0Passthrough(t *testing.T) {
	key := DeriveKey("test")
	decryptors := []struct {
		name string
		dec  Decryptor
	}{
		{"RC4", NewRC4Decryptor(key)},
	}
	aesDec, err := NewAES128Decryptor(key)
	if err != nil {
		t.Fatalf("NewAES128Decryptor: %v", err)
	}
	decryptors = append(decryptors, struct {
		name string
		dec  Decryptor
	}{"AES128", aesDec})

	original := []byte{0xEB, 0x6D, 0x87, 0xAE, 0x00, 0x01, 0x02, 0x03}
	for _, tc := range decryptors {
		out, err := tc.dec.DecryptPage(0, original)
		if err != nil {
			t.Fatalf("%s DecryptPage(0): %v", tc.name, err)
		}
		if !bytes.Equal(out, original) {
			t.Errorf("%s: page 0 was modified, want passthrough", tc.name)
		}
	}
}

func TestDecryptionRC4RoundTrip(t *testing.T) {
	key := DeriveKey("mypassword")
	pageNum := 5

	plaintext := make([]byte, DefaultPageSize)
	plaintext[6] = byte(PageLeaf)
	binary.LittleEndian.PutUint16(plaintext[4:], 42)
	copy(plaintext[0x1C:], []byte("Value"))

	perPageKey := make([]byte, len(key)+4)
	copy(perPageKey, key)
	binary.LittleEndian.PutUint32(perPageKey[len(key):], uint32(pageNum))
	c, err := rc4.NewCipher(perPageKey)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	c.XORKeyStream(ciphertext, plaintext)

	dec := NewRC4Decryptor(key)
	out, err := dec.DecryptPage(pageNum, ciphertext)
	if err != nil {
		t.Fatalf("DecryptPage: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Error("RC4 round-trip failed: decrypted != plaintext")
	}
}

func TestDecryptionAES128RoundTrip(t *testing.T) {
	key := DeriveKey("secretpass")
	pageNum := 3

	plaintext := make([]byte, DefaultPageSize)
	plaintext[6] = byte(PageLeaf)
	binary.LittleEndian.PutUint16(plaintext[4:], 7)
	copy(plaintext[0x1C:], []byte("Value"))

	iv := make([]byte, aes.BlockSize)
	binary.LittleEndian.PutUint32(iv, uint32(pageNum))
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(ciphertext, plaintext)

	dec, err := NewAES128Decryptor(key)
	if err != nil {
		t.Fatal(err)
	}
	out, err := dec.DecryptPage(pageNum, ciphertext)
	if err != nil {
		t.Fatalf("DecryptPage: %v", err)
	}
	if !bytes.Equal(out, plaintext) {
		t.Error("AES-128 round-trip failed: decrypted != plaintext")
	}
}

func TestDecryptionDeriveKeyDeterministic(t *testing.T) {
	k1 := DeriveKey("password")
	k2 := DeriveKey("password")
	if !bytes.Equal(k1, k2) {
		t.Error("DeriveKey not deterministic")
	}
	if len(k1) != 16 {
		t.Errorf("key length = %d, want 16", len(k1))
	}

	k3 := DeriveKey("different")
	if bytes.Equal(k1, k3) {
		t.Error("different passwords produced same key")
	}
}

func TestDecryptionNewDecryptor(t *testing.T) {
	_, err := NewDecryptor(EncryptionRC4, "pass")
	if err != nil {
		t.Errorf("NewDecryptor(RC4): %v", err)
	}

	_, err = NewDecryptor(EncryptionAES128, "pass")
	if err != nil {
		t.Errorf("NewDecryptor(AES128): %v", err)
	}

	_, err = NewDecryptor(EncryptionAES256, "pass")
	if err == nil {
		t.Error("NewDecryptor(AES256) should fail")
	}
}

func TestDecryptionWrongPassword(t *testing.T) {
	key := DeriveKey("correct")
	pageNum := 1

	plaintext := make([]byte, DefaultPageSize)
	plaintext[6] = byte(PageLeaf)
	binary.LittleEndian.PutUint16(plaintext[4:], 1)

	perPageKey := make([]byte, len(key)+4)
	copy(perPageKey, key)
	binary.LittleEndian.PutUint32(perPageKey[len(key):], uint32(pageNum))
	c, _ := rc4.NewCipher(perPageKey)
	ciphertext := make([]byte, len(plaintext))
	c.XORKeyStream(ciphertext, plaintext)

	wrongKey := DeriveKey("wrong")
	wrongDec := NewRC4Decryptor(wrongKey)
	out, err := wrongDec.DecryptPage(pageNum, ciphertext)
	if err != nil {
		t.Fatalf("DecryptPage: %v", err)
	}
	if bytes.Equal(out, plaintext) {
		t.Error("wrong password produced correct plaintext")
	}
}

func TestDecryptionValidateDecryptor(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Skipf("no test SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	err = ValidateDecryptor(f, h, NullDecryptor())
	if err != nil {
		t.Errorf("ValidateDecryptor(null) on unencrypted DB: %v", err)
	}
}

func TestDecryptionPageReaderIntegration(t *testing.T) {
	f, err := os.Open("../data/Depropanizer.sdf")
	if err != nil {
		t.Skipf("no test SDF: %v", err)
	}
	defer f.Close()

	h, err := ReadHeader(f)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	pr := NewPageReaderWithDecryptor(f, h, 16, NullDecryptor())
	page, err := pr.ReadPage(1)
	if err != nil {
		t.Fatalf("ReadPage(1): %v", err)
	}
	if len(page) != h.PageSize {
		t.Errorf("page size = %d, want %d", len(page), h.PageSize)
	}

	pt := PageType(page[6])
	if !pt.IsKnown() {
		t.Errorf("page 1 type %d not recognized", pt)
	}
}

func TestDecryptionEmptyData(t *testing.T) {
	key := DeriveKey("test")
	dec := NewRC4Decryptor(key)
	out, err := dec.DecryptPage(1, nil)
	if err != nil {
		t.Fatalf("DecryptPage(nil): %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(out))
	}

	out, err = dec.DecryptPage(1, []byte{})
	if err != nil {
		t.Fatalf("DecryptPage(empty): %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(out))
	}
}

func TestDecryptionAES128BadKeyLength(t *testing.T) {
	_, err := NewAES128Decryptor([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for wrong key length")
	}
}
