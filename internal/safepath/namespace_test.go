package safepath

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func requireSupportedNamespace(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	namespace, err := OpenNamespace(root, "probe")
	if errors.Is(err, ErrUnsupported) {
		t.Skip("safe namespace is unsupported on this platform")
	}
	if err != nil {
		t.Fatal(err)
	}
	_ = namespace.Close()
}

func TestNamespacePublishesReadsReplacesListsAndRemoves(t *testing.T) {
	requireSupportedNamespace(t)
	root := t.TempDir()
	namespace, err := OpenNamespace(root, "evidence")
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Close()
	if err := namespace.EnsureRegularAtomically("one.json", []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := namespace.EnsureRegularAtomically("one.json", []byte("one"), 0o600); err != nil {
		t.Fatalf("exact ensure replay: %v", err)
	}
	if err := namespace.EnsureRegularAtomically("one.json", []byte("different"), 0o600); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting ensure error = %v, want ErrConflict", err)
	}
	if err := namespace.ReplaceRegularAtomically("marker.json", []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := namespace.ReplaceRegularAtomically("marker.json", []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	content, err := namespace.ReadRegularBounded("marker.json", 3)
	if err != nil || !bytes.Equal(content, []byte("new")) {
		t.Fatalf("read marker = %q, %v", content, err)
	}
	if _, err := namespace.ReadRegularBounded("marker.json", 2); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("bounded read error = %v, want ErrTooLarge", err)
	}
	names, err := namespace.ListNames()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"marker.json", "one.json"}) {
		t.Fatalf("names = %#v", names)
	}
	if err := namespace.RemoveRegular("one.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := namespace.ReadRegularBounded("one.json", 3); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed read error = %v, want not exist", err)
	}
}

func TestNamespaceRejectsHardLinkedFiles(t *testing.T) {
	requireSupportedNamespace(t)
	root := t.TempDir()
	namespace, err := OpenNamespace(root, "evidence")
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Close()
	outside := filepath.Join(root, "outside.json")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(root, "evidence", "linked.json")
	if err := os.Link(outside, linked); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if _, err := namespace.ReadRegularBounded("linked.json", 32); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("hard-link read error = %v, want ErrUnsafe", err)
	}
	if err := namespace.RemoveRegular("linked.json"); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("hard-link remove error = %v, want ErrUnsafe", err)
	}
	content, err := os.ReadFile(outside)
	if err != nil || string(content) != "outside" {
		t.Fatalf("outside hard-link target changed: %q, %v", content, err)
	}
}

func TestNamespaceRejectsReparseOrSymlinkDirectory(t *testing.T) {
	requireSupportedNamespace(t)
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "evidence")); err != nil {
		t.Skipf("directory links unavailable: %v", err)
	}
	if _, err := OpenNamespace(root, "evidence"); err == nil {
		t.Fatal("linked namespace directory was accepted")
	}
}

func TestNamespaceCleanupPrefixRejectsNonRegularEntry(t *testing.T) {
	requireSupportedNamespace(t)
	root := t.TempDir()
	namespace, err := OpenNamespace(root, "evidence")
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Close()
	if err := os.Mkdir(filepath.Join(root, "evidence", ".staging-directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := namespace.CleanupPrefix(".staging-"); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("cleanup error = %v, want ErrUnsafe", err)
	}
}
