package guest

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

type materializedInputs struct {
	directory string
	argv      []string
}

func materializeTemporaryInputs(root string, start transport.ExecStart, maxBytes int64) (materializedInputs, error) {
	root, err := ensurePrivateRoot(root)
	if err != nil {
		return materializedInputs{}, err
	}
	directory, err := os.MkdirTemp(root, "exec-")
	if err != nil {
		return materializedInputs{}, err
	}
	if err = os.Chmod(directory, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return materializedInputs{}, err
	}
	result := materializedInputs{directory: directory, argv: append([]string(nil), start.Argv...)}
	committed := false
	defer func() {
		if !committed {
			_ = result.cleanup()
		}
	}()

	var total int64
	for index, input := range start.TemporaryInputs {
		if int64(len(input.Bytes)) > maxBytes-total {
			return materializedInputs{}, fmt.Errorf("%w: maximum %d", ErrInvalidStart, maxBytes)
		}
		total += int64(len(input.Bytes))
		name, normalizeErr := safepath.Normalize(input.NameHint)
		if normalizeErr != nil || strings.Contains(name, "/") {
			return materializedInputs{}, fmt.Errorf("%w: temporary name %q", ErrInvalidStart, input.NameHint)
		}
		mode := os.FileMode(input.Mode)
		if mode == 0 {
			mode = 0o600
		}
		if mode&^os.FileMode(0o777) != 0 {
			return materializedInputs{}, fmt.Errorf("%w: temporary mode %#o", ErrInvalidStart, input.Mode)
		}
		path := filepath.Join(directory, fmt.Sprintf("%03d-%s", index, name))
		if err = writeExclusiveFile(path, input.Bytes, mode); err != nil {
			return materializedInputs{}, err
		}
		result.argv[input.ArgvIndex] = path
	}
	committed = true
	return result, nil
}

func ensurePrivateRoot(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(absolute, 0o700); err != nil {
			return "", err
		}
		info, err = os.Lstat(absolute)
	}
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("temporary root must be a real directory")
	}
	if err = os.Chmod(absolute, 0o700); err != nil {
		return "", err
	}
	return absolute, nil
}

func writeExclusiveFile(path string, value []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if _, err = writeAll(file, value); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Chmod(mode); err != nil {
		return err
	}
	err = file.Close()
	closed = true
	return err
}

func (inputs materializedInputs) cleanup() error {
	if inputs.directory == "" {
		return nil
	}
	parent := filepath.Dir(inputs.directory)
	resolvedParent, err := filepath.Abs(parent)
	if err != nil {
		return err
	}
	resolvedDirectory, err := filepath.Abs(inputs.directory)
	if err != nil {
		return err
	}
	if filepath.Dir(resolvedDirectory) != resolvedParent || !strings.HasPrefix(filepath.Base(resolvedDirectory), "exec-") {
		return errors.New("refusing to remove unrecognized temporary directory")
	}
	if err = os.RemoveAll(resolvedDirectory); err != nil {
		return err
	}
	if _, err = os.Lstat(resolvedDirectory); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return ErrTemporaryCleanup
		}
		return err
	}
	return nil
}

func writeAll(writer io.Writer, value []byte) (int64, error) {
	written := 0
	for written < len(value) {
		count, err := writer.Write(value[written:])
		written += count
		if err != nil {
			return int64(written), err
		}
		if count == 0 {
			return int64(written), io.ErrShortWrite
		}
	}
	return int64(written), nil
}
