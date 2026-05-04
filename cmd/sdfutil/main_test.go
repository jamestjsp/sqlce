package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareSQLiteOutputRejectsExistingFileWithoutForce(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "out.db")
	if err := os.WriteFile(outputPath, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := prepareSQLiteOutput(outputPath, false)
	if err == nil {
		t.Fatal("expected existing output file to be rejected")
	}
	if !strings.Contains(err.Error(), "use --force to overwrite") {
		t.Fatalf("expected force guidance, got %q", err)
	}
	got, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != "existing" {
		t.Fatalf("existing file was modified: %q", got)
	}
}

func TestPrepareSQLiteOutputRemovesExistingFileWithForce(t *testing.T) {
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "out.db")
	if err := os.WriteFile(outputPath, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := prepareSQLiteOutput(outputPath, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("expected existing file to be removed, stat err=%v", err)
	}
}

func TestPrepareSQLiteOutputRejectsDirectory(t *testing.T) {
	outputPath := t.TempDir()

	err := prepareSQLiteOutput(outputPath, true)
	if err == nil {
		t.Fatal("expected directory output path to be rejected")
	}
	if !strings.Contains(err.Error(), "is a directory") {
		t.Fatalf("expected directory error, got %q", err)
	}
}

func TestPrepareSQLiteOutputRejectsMissingParent(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "missing", "out.db")

	err := prepareSQLiteOutput(outputPath, false)
	if err == nil {
		t.Fatal("expected missing parent directory to be rejected")
	}
	if !strings.Contains(err.Error(), "check SQLite output directory") {
		t.Fatalf("expected parent directory error, got %q", err)
	}
}
