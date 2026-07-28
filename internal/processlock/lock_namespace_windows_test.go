//go:build windows

package processlock

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHeldWindowsLockFileCannotBeRemovedOrReplaced(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "control.db")
	owner, err := Acquire(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(owner.LockPath()); err == nil {
		t.Fatal("held lock path was removable despite delete sharing being disabled")
	}
	runLockHelper(t, controlPath, "held")
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	runLockHelper(t, controlPath, "acquired")
}
