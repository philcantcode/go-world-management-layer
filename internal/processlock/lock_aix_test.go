//go:build aix

package processlock

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAcquireFailsClosedWithoutStableNamespaceOnAIX(t *testing.T) {
	controlPath := filepath.Join(t.TempDir(), "control.db")
	for attempt := 0; attempt < 2; attempt++ {
		owner, err := Acquire(controlPath)
		if owner != nil || !errors.Is(err, errAIXStableNamespaceUnsupported) || errors.Is(err, ErrAlreadyHeld) {
			t.Fatalf("attempt %d owner=%v error=%v", attempt+1, owner, err)
		}
	}
}
