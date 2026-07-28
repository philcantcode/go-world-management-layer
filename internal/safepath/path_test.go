package safepath

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	t.Parallel()
	valid := map[string]string{
		"result.json":      "result.json",
		"notes/output.txt": "notes/output.txt",
		"unicode/λ.txt":    "unicode/λ.txt",
	}
	for input, want := range valid {
		input, want := input, want
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := Normalize(input)
			if err != nil || got != want {
				t.Fatalf("Normalize(%q) = %q, %v; want %q", input, got, err, want)
			}
		})
	}
	invalid := []string{"", ".", "..", "../escape", "a/../b", "/absolute", "//server/share", `C:\escape`, `a\b`, "a//b", "a/./b", "name:stream", "nul\x00byte"}
	for _, input := range invalid {
		input := input
		t.Run("reject_"+input, func(t *testing.T) {
			t.Parallel()
			if _, err := Normalize(input); err == nil {
				t.Fatalf("Normalize(%q) unexpectedly succeeded", input)
			}
		})
	}
}

func FuzzNormalize(f *testing.F) {
	for _, seed := range []string{
		"result.json", "nested/output.bin", "", ".", "..", "../escape",
		"a/../b", "/absolute", "//server/share", `C:\escape`, `a\b`,
		"a//b", "name:stream", "nul\x00byte", "unicode/λ.txt",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		normalized, err := Normalize(value)
		if err != nil {
			return
		}
		if normalized != value {
			t.Fatalf("Normalize accepted non-canonical path %q as %q", value, normalized)
		}
		if path.IsAbs(normalized) || strings.ContainsAny(normalized, "\\:\x00") {
			t.Fatalf("Normalize accepted unsafe path %q", normalized)
		}
		for _, component := range strings.Split(normalized, "/") {
			if component == "" || component == "." || component == ".." {
				t.Fatalf("Normalize accepted unsafe component in %q", normalized)
			}
		}
		if repeated, repeatErr := Normalize(normalized); repeatErr != nil || repeated != normalized {
			t.Fatalf("Normalize is not idempotent for %q: %q, %v", normalized, repeated, repeatErr)
		}
	})
}

func TestOpenRegularAndRejectSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "safe"), 0o700); err != nil {
		t.Fatal(err)
	}
	want := []byte("evidence")
	if err := os.WriteFile(filepath.Join(root, "safe", "result.bin"), want, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := OpenRegular(root, "safe/result.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var got bytes.Buffer
	if _, err := CopyBounded(&got, file, int64(len(want))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("copied %q, want %q", got.Bytes(), want)
	}

	link := filepath.Join(root, "link.bin")
	if err := os.Symlink(filepath.Join(root, "safe", "result.bin"), link); err == nil {
		if _, err := OpenRegular(root, "link.bin"); err == nil {
			t.Fatal("symlink unexpectedly opened")
		}
	}
}

func TestCopyBounded(t *testing.T) {
	t.Parallel()
	var exact bytes.Buffer
	if n, err := CopyBounded(&exact, bytes.NewReader([]byte("1234")), 4); err != nil || n != 4 {
		t.Fatalf("exact copy = %d, %v", n, err)
	}
	var oversized bytes.Buffer
	if _, err := CopyBounded(&oversized, bytes.NewReader([]byte("12345")), 4); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized error = %v, want ErrTooLarge", err)
	}
}

func FuzzCopyBounded(f *testing.F) {
	f.Add([]byte("evidence"), uint16(8))
	f.Add([]byte("oversized"), uint16(4))
	f.Add([]byte{}, uint16(0))
	f.Fuzz(func(t *testing.T, content []byte, maximum uint16) {
		var destination bytes.Buffer
		n, err := CopyBounded(&destination, bytes.NewReader(content), int64(maximum))
		if len(content) > int(maximum) {
			if !errors.Is(err, ErrTooLarge) || n != int64(maximum)+1 || destination.Len() != int(maximum)+1 {
				t.Fatalf("oversized copy = %d bytes/%d output, %v", n, destination.Len(), err)
			}
			return
		}
		if err != nil || n != int64(len(content)) || !bytes.Equal(destination.Bytes(), content) {
			t.Fatalf("bounded copy = %d bytes/%q, %v; want %d/%q", n, destination.Bytes(), err, len(content), content)
		}
	})
}

func TestWriteRegularAtomicPublishesOrCleansUp(t *testing.T) {
	root := t.TempDir()
	if err := WriteRegularAtomic(root, "nested/tool.bin", 0o500, func(writer io.Writer) error {
		_, err := writer.Write([]byte("tool"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(filepath.Join(root, "nested", "tool.bin"))
	if err != nil || !bytes.Equal(content, []byte("tool")) {
		t.Fatalf("published content = %q, %v", content, err)
	}
	if err := WriteRegularAtomic(root, "nested/failed.bin", 0o600, func(writer io.Writer) error {
		_, _ = writer.Write([]byte("partial"))
		return fmt.Errorf("injected failure")
	}); err == nil {
		t.Fatal("write callback failure was ignored")
	}
	if _, err := os.Stat(filepath.Join(root, "nested", "failed.bin")); !os.IsNotExist(err) {
		t.Fatalf("failed destination exists: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, "nested", ".world-write-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files leaked: %v, %v", matches, err)
	}
}

func TestWriteRegularAtomicRejectsSymlinkParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	err := WriteRegularAtomic(root, "link/escape.bin", 0o600, func(writer io.Writer) error {
		_, writeErr := writer.Write([]byte("escape"))
		return writeErr
	})
	if err == nil {
		t.Fatal("symlink parent was accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "escape.bin")); !os.IsNotExist(err) {
		t.Fatalf("write escaped root: %v", err)
	}
}
