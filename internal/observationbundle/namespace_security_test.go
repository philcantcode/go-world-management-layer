package observationbundle_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
)

func TestFinalizerRejectsRedirectedDurableNamespaces(t *testing.T) {
	for _, logicalDirectory := range []string{"objects", "runs"} {
		t.Run(logicalDirectory, func(t *testing.T) {
			root := t.TempDir()
			if _, err := observationbundle.New(root); err != nil {
				t.Fatal(err)
			}
			namespacePath := filepath.Join(root, logicalDirectory)
			if err := os.Remove(namespacePath); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			want := []byte("outside remains untouched")
			if err := os.WriteFile(sentinel, want, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, namespacePath); err != nil {
				t.Skipf("directory links unavailable: %v", err)
			}
			if _, err := observationbundle.New(root); err == nil {
				t.Fatal("redirected finalizer namespace was accepted")
			}
			assertFinalizerFileContent(t, sentinel, want)
		})
	}
}

func TestFinalizerRetryRejectsHardLinkedSealedMetadata(t *testing.T) {
	request := completedRequest(t)
	finalizer, err := observationbundle.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(result.Path)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "sealed-alias.json")
	if err := os.Link(result.Path, outside); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if _, err := finalizer.Finalize(ctx, request); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("hard-linked sealed metadata error = %v, want integrity violation", err)
	}
	assertFinalizerFileContent(t, outside, want)
}

func TestSealedContentOpenRejectsRedirectedRunDirectory(t *testing.T) {
	request := completedRequest(t)
	root := t.TempDir()
	finalizer, err := observationbundle.New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := finalizer.Finalize(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	runDirectory := filepath.Dir(result.Path)
	displaced := runDirectory + "-displaced"
	if err := os.Rename(runDirectory, displaced); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "sealed.json"), []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, runDirectory); err != nil {
		t.Skipf("directory links unavailable: %v", err)
	}
	reader, err := result.Content.Open(ctx)
	if reader != nil {
		_ = reader.Close()
	}
	if err == nil {
		t.Fatal("sealed content followed a redirected run directory")
	}
	assertFinalizerFileContent(t, filepath.Join(outside, "sealed.json"), []byte("foreign"))
}

func assertFinalizerFileContent(t *testing.T, path string, want []byte) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read %s: %v", path, readErr)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s changed: got %q, want %q", path, got, want)
	}
}
