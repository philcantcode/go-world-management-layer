//go:build !aix

package processlock

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcquireIsExclusiveAndReacquirableAfterRelease(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "nested", "..", "nested", "control.db")
	first, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Release() })
	if !filepath.IsAbs(first.ControlPath()) || filepath.Dir(first.ControlPath()) != filepath.Dir(first.LockPath()) {
		t.Fatalf("control path %q and lock path %q are not canonical absolute siblings", first.ControlPath(), first.LockPath())
	}
	if !strings.HasSuffix(first.LockPath(), lockSuffix) {
		t.Fatalf("lock path = %q", first.LockPath())
	}

	second, err := Acquire(first.ControlPath())
	if second != nil || !errors.Is(err, ErrAlreadyHeld) || !strings.Contains(err.Error(), first.LockPath()) {
		t.Fatalf("second acquisition owner=%v error=%v", second, err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("second release: %v", err)
	}

	third, err := Acquire(controlPath)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := third.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRejectsInMemoryControlState(t *testing.T) {
	if owner, err := Acquire(":memory:"); owner != nil || err == nil || !strings.Contains(err.Error(), "in-memory") {
		t.Fatalf("in-memory acquisition owner=%v error=%v", owner, err)
	}
}

func TestAcquireIsExclusiveAcrossProcessesAndReleasedOnClose(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "control.db")
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	runLockHelper(t, controlPath, "held")
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	runLockHelper(t, controlPath, "acquired")
}

func TestProcessLockHelper(t *testing.T) {
	controlPath := os.Getenv("WORLD_PROCESSLOCK_HELPER_PATH")
	if controlPath == "" {
		return
	}
	expectation := os.Getenv("WORLD_PROCESSLOCK_HELPER_EXPECT")
	owner, err := Acquire(controlPath)
	switch expectation {
	case "held":
		if owner != nil || !errors.Is(err, ErrAlreadyHeld) {
			t.Fatalf("expected held lock, owner=%v error=%v", owner, err)
		}
	case "acquired":
		if err != nil {
			t.Fatalf("expected acquisition after release: %v", err)
		}
		if err := owner.Release(); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown helper expectation %q", expectation)
	}
}

func runLockHelper(t *testing.T, controlPath, expectation string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestProcessLockHelper$")
	command.Env = append(os.Environ(),
		"WORLD_PROCESSLOCK_HELPER_PATH="+controlPath,
		"WORLD_PROCESSLOCK_HELPER_EXPECT="+expectation,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("lock helper %s: %v\n%s", expectation, err, output)
	}
}

func TestAcquireRejectsNonRegularLockPath(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "control.db")
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := owner.LockPath()
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(lockPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if owner, err := Acquire(controlPath); owner != nil || err == nil || errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("non-regular acquisition owner=%v error=%v", owner, err)
	}
}

func TestAcquireCanonicalizesDirectorySymlinkAliases(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(realDirectory, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	owner, err := Acquire(filepath.Join(alias, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })
	if second, err := Acquire(filepath.Join(realDirectory, "control.db")); second != nil || !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("canonical alias acquisition owner=%v error=%v", second, err)
	}
}

func TestAcquireRejectsExistingControlSymlink(t *testing.T) {
	root := t.TempDir()
	targetDirectory := filepath.Join(root, "target")
	aliasDirectory := filepath.Join(root, "alias")
	if err := os.MkdirAll(targetDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(aliasDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(targetDirectory, "control.db")
	if err := os.WriteFile(target, []byte("control"), 0o600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(aliasDirectory, "control-link.db")
	if err := os.Symlink(target, alias); err != nil {
		t.Skipf("file symlinks unavailable: %v", err)
	}
	if owner, err := Acquire(alias); owner != nil || err == nil || errors.Is(err, ErrAlreadyHeld) || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("existing control symlink owner=%v error=%v", owner, err)
	}
	owner, err := Acquire(target)
	if err != nil {
		t.Fatalf("symlink rejection leaked an ownership claim: %v", err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRejectsNonRegularControlPath(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "control.db")
	if err := os.Mkdir(controlPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if owner, err := Acquire(controlPath); owner != nil || err == nil || errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("non-regular control acquisition owner=%v error=%v", owner, err)
	}
}

func TestAcquireRejectsSymlinkLockPath(t *testing.T) {
	root := t.TempDir()
	controlPath := filepath.Join(root, "control.db")
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := owner.LockPath()
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "unrelated")
	if err := os.WriteFile(target, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Skipf("file symlinks unavailable: %v", err)
	}
	if owner, err := Acquire(controlPath); owner != nil || err == nil || errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("symlink acquisition owner=%v error=%v", owner, err)
	}
	contents, err := os.ReadFile(target)
	if err != nil || string(contents) != "untouched" {
		t.Fatalf("symlink target changed: contents=%q error=%v", contents, err)
	}
}

func TestAcquireRejectsAlreadyHardLinkedControlFile(t *testing.T) {
	root := t.TempDir()
	controlPath := filepath.Join(root, "control.db")
	aliasPath := filepath.Join(root, "control-alias.db")
	if err := os.WriteFile(controlPath, []byte("existing control state"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(controlPath, aliasPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if owner, err := Acquire(controlPath); owner != nil || err == nil || errors.Is(err, ErrAlreadyHeld) || !strings.Contains(err.Error(), "exactly one link") {
		t.Fatalf("hard-linked control acquisition owner=%v error=%v", owner, err)
	}
	if err := os.Remove(aliasPath); err != nil {
		t.Fatal(err)
	}
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatalf("acquire after removing control hard link: %v", err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestSecondAcquireRejectsControlHardLinkAlias(t *testing.T) {
	root := t.TempDir()
	firstDirectory := filepath.Join(root, "first")
	secondDirectory := filepath.Join(root, "second")
	if err := os.MkdirAll(firstDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(secondDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	controlPath := filepath.Join(firstDirectory, "control.db")
	aliasPath := filepath.Join(secondDirectory, "control-alias.db")
	if err := os.WriteFile(controlPath, []byte("existing control state"), 0o600); err != nil {
		t.Fatal(err)
	}
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })
	if err := os.Link(controlPath, aliasPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if second, err := Acquire(aliasPath); second != nil || err == nil || errors.Is(err, ErrAlreadyHeld) || !strings.Contains(err.Error(), "exactly one link") {
		t.Fatalf("control hard-link alias acquisition owner=%v error=%v", second, err)
	}
	if err := os.Remove(aliasPath); err != nil {
		t.Fatal(err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	reacquired, err := Acquire(controlPath)
	if err != nil {
		t.Fatalf("acquire after hard-link rejection and release: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRejectsHardLinkedLockFile(t *testing.T) {
	root := t.TempDir()
	controlPath := filepath.Join(root, "control.db")
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := owner.LockPath()
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	aliasPath := filepath.Join(root, "lock-alias")
	if err := os.Link(lockPath, aliasPath); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if owner, err := Acquire(controlPath); owner != nil || err == nil || errors.Is(err, ErrAlreadyHeld) || !strings.Contains(err.Error(), "exactly one link") {
		t.Fatalf("hard-linked lock acquisition owner=%v error=%v", owner, err)
	}
	if err := os.Remove(aliasPath); err != nil {
		t.Fatal(err)
	}
	reacquired, err := Acquire(controlPath)
	if err != nil {
		t.Fatalf("acquire after removing lock hard link: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatal(err)
	}
}
