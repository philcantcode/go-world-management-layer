//go:build windows

package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestWriteWaitsForTransientWindowsReplacementDenial(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "control.json")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	handle := openWithoutDeleteSharing(t, path)
	released := make(chan struct{})
	go func() {
		time.Sleep(25 * time.Millisecond)
		_ = windows.CloseHandle(handle)
		close(released)
	}()
	t.Cleanup(func() {
		select {
		case <-released:
		default:
			_ = windows.CloseHandle(handle)
		}
	})

	if err := Write(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("replace after transient Windows denial: %v", err)
	}
	<-released
	assertFileContent(t, path, "new")
}

func TestWriteBoundsPermanentWindowsReplacementDenial(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "control.json")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	handle := openWithoutDeleteSharing(t, path)
	t.Cleanup(func() { _ = windows.CloseHandle(handle) })

	started := time.Now()
	if err := Write(path, []byte("new"), 0o600); err == nil {
		t.Fatal("permanent Windows replacement denial unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > 2*windowsReplaceRetryLimit {
		t.Fatalf("permanent Windows replacement denial was not bounded: %s", elapsed)
	}
	assertFileContent(t, path, "old")
}

func openWithoutDeleteSharing(t *testing.T, path string) windows.Handle {
	t.Helper()
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(
		pathPointer,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("open test destination without delete sharing: %v", err)
	}
	return handle
}
