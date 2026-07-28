//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package processlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParentNamespaceLockDefeatsLockPathReplacementForConformingAcquirer(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "control.db")
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := owner.LockPath()
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lockPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	runLockHelper(t, controlPath, "held")
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	runLockHelper(t, controlPath, "acquired")
	reacquired, err := Acquire(controlPath)
	if err != nil {
		t.Fatalf("local claim leaked after replacement test: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestParentNamespaceSerializesDifferentControlFilesAndReleasesLast(t *testing.T) {
	root := t.TempDir()
	first, err := Acquire(filepath.Join(root, "first.db"))
	if err != nil {
		t.Fatal(err)
	}
	if second, err := Acquire(filepath.Join(root, "second.db")); second != nil || !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("same-directory acquisition owner=%v error=%v", second, err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(filepath.Join(root, "second.db"))
	if err != nil {
		t.Fatalf("same-directory acquisition after release: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenedPathIdentityRejectsReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "lock")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := os.Rename(path, filepath.Join(root, "moved")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := requireSameOpenedPath(path, file, false); err == nil {
		t.Fatal("replacement path matched the original opened handle")
	}
}
