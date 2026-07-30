// Package safepath provides the single path-validation and descriptor-opening
// boundary used by workspace export and target transfers. Guest-supplied paths
// are always logical relative paths; callers never receive or provide a host
// destination.
package safepath

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
)

var (
	ErrEmpty       = errors.New("path is empty")
	ErrNotRelative = errors.New("path is not relative")
	ErrTraversal   = errors.New("path escapes its root")
	ErrUnsafe      = errors.New("path contains an unsafe component")
	ErrNotRegular  = errors.New("path is not a regular file")
	ErrTooLarge    = errors.New("file exceeds byte limit")
)

// Normalize validates an untrusted, slash-separated logical path. Backslashes
// and drive/UNC-like forms are rejected on every host so client behavior does
// not vary with the host operating system.
func Normalize(value string) (string, error) {
	if value == "" {
		return "", ErrEmpty
	}
	if strings.IndexByte(value, 0) >= 0 || strings.Contains(value, "\\") {
		return "", fmt.Errorf("%w: NUL or backslash", ErrUnsafe)
	}
	if strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") {
		return "", ErrNotRelative
	}
	components := strings.Split(value, "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return "", fmt.Errorf("%w: %q", ErrTraversal, component)
		}
		if strings.Contains(component, ":") {
			return "", fmt.Errorf("%w: drive or alternate stream syntax", ErrUnsafe)
		}
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", ErrTraversal
	}
	return clean, nil
}

// OpenRegular opens a logical path beneath root without following symlinks.
// Platform implementations preserve the validated file identity through the
// returned handle, allowing callers to hash and copy from the same descriptor.
func OpenRegular(root, logicalPath string) (*File, error) {
	normalized, err := Normalize(logicalPath)
	if err != nil {
		return nil, err
	}
	return openRegular(root, normalized)
}

// WriteRegularAtomic creates parent directories without following symlinks,
// writes a temporary regular file, and atomically publishes it at logicalPath.
// The callback is invoked only after the temporary file is safely anchored
// beneath root; any callback or commit failure removes the temporary file.
func WriteRegularAtomic(root, logicalPath string, mode fs.FileMode, write func(io.Writer) error) error {
	normalized, err := Normalize(logicalPath)
	if err != nil {
		return err
	}
	if mode.Perm() == 0 || mode&^fs.ModePerm != 0 {
		return fmt.Errorf("invalid regular-file mode %v", mode)
	}
	if write == nil {
		return fmt.Errorf("write callback is required")
	}
	return writeRegularAtomic(root, normalized, mode.Perm(), write)
}

// File is the deliberately small open-file surface used by export and transfer
// code. It keeps host path details out of higher layers.
type File struct {
	reader io.ReadSeekCloser
	info   fs.FileInfo
}

func newFile(reader io.ReadSeekCloser, info fs.FileInfo) (*File, error) {
	if info == nil || !info.Mode().IsRegular() {
		_ = reader.Close()
		return nil, ErrNotRegular
	}
	return &File{reader: reader, info: info}, nil
}

func (f *File) Read(p []byte) (int, error)         { return f.reader.Read(p) }
func (f *File) Seek(o int64, w int) (int64, error) { return f.reader.Seek(o, w) }
func (f *File) Close() error                       { return f.reader.Close() }
func (f *File) Size() int64                        { return f.info.Size() }
func (f *File) Mode() fs.FileMode                  { return f.info.Mode() }
func (f *File) Info() fs.FileInfo                  { return f.info }

// Stat refreshes metadata through the already-open handle. It is used to
// detect mutation while a sealing scan hashes a file.
func (f *File) Stat() (fs.FileInfo, error) {
	statable, ok := f.reader.(interface{ Stat() (fs.FileInfo, error) })
	if !ok {
		return f.info, nil
	}
	return statable.Stat()
}

// CopyBounded copies at most maxBytes and distinguishes an exact-limit file
// from a larger one by reading a single additional byte.
func CopyBounded(dst io.Writer, src io.Reader, maxBytes int64) (int64, error) {
	if maxBytes < 0 {
		return 0, fmt.Errorf("max bytes must be non-negative")
	}
	limited := &io.LimitedReader{R: src, N: maxBytes + 1}
	n, err := io.Copy(dst, limited)
	if err != nil {
		return n, err
	}
	if n > maxBytes {
		return n, ErrTooLarge
	}
	return n, nil
}
