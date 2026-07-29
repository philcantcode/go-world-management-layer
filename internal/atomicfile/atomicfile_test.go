package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAtomicallyReplacesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.json")
	if err := Write(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Write(path, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, path, "new")
}

func TestWriteExclusiveNeverReplacesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "content.json")
	if err := WriteExclusive(path, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteExclusive(path, []byte("second"), 0o600); err == nil {
		t.Fatal("exclusive publication replaced an existing file")
	}
	assertFileContent(t, path, "first")
}

func TestPublishExclusiveMovesOnlyCompletedSameDirectoryFile(t *testing.T) {
	directory := t.TempDir()
	staged := filepath.Join(directory, "payload.creating")
	path := filepath.Join(directory, "payload")
	if err := os.WriteFile(staged, []byte("complete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PublishExclusive(staged, path); err != nil {
		t.Fatal(err)
	}
	assertFileContent(t, path, "complete")
	if _, err := os.Lstat(staged); !os.IsNotExist(err) {
		t.Fatalf("published staging file remains: %v", err)
	}

	second := filepath.Join(directory, "second.creating")
	if err := os.WriteFile(second, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PublishExclusive(second, path); err == nil {
		t.Fatal("exclusive publication replaced an existing destination")
	}
	assertFileContent(t, path, "complete")
	assertFileContent(t, second, "replacement")
}

func assertFileContent(t *testing.T, path, expected string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != expected {
		t.Fatalf("content = %q, want %q", content, expected)
	}
}
