package format

import (
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
