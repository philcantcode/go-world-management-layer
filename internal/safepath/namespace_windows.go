//go:build windows

package safepath

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type namespaceState struct {
	root    windows.Handle
	dir     windows.Handle
	logical string
	rootID  windows.ByHandleFileInformation
	dirID   windows.ByHandleFileInformation
}

func openNamespaceState(root, logical string) (*namespaceState, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	rootHandle, err := openWindowsAbsoluteDirectory(absolute)
	if err != nil {
		return nil, fmt.Errorf("open namespace root: %w", err)
	}
	closeRoot := true
	defer func() {
		if closeRoot {
			_ = windows.CloseHandle(rootHandle)
		}
	}()
	rootID, err := windowsHandleInfo(rootHandle)
	if err != nil {
		return nil, err
	}
	if err := requireSafeWindowsDirectory(rootID); err != nil {
		return nil, err
	}

	parent := rootHandle
	opened := make([]windows.Handle, 0, len(strings.Split(logical, "/")))
	for _, part := range strings.Split(logical, "/") {
		next, openErr := openWindowsRelativeDirectory(parent, part, windows.FILE_OPEN)
		if windowsNotExist(openErr) {
			next, openErr = openWindowsRelativeDirectory(parent, part, windows.FILE_CREATE)
			if openErr == windows.STATUS_OBJECT_NAME_COLLISION || errors.Is(openErr, windows.ERROR_ALREADY_EXISTS) {
				next, openErr = openWindowsRelativeDirectory(parent, part, windows.FILE_OPEN)
			}
		}
		if openErr != nil {
			for _, handle := range opened {
				_ = windows.CloseHandle(handle)
			}
			return nil, fmt.Errorf("open namespace directory %q: %w", part, normalizeWindowsNamespaceError(openErr))
		}
		info, infoErr := windowsHandleInfo(next)
		if infoErr != nil {
			_ = windows.CloseHandle(next)
			for _, handle := range opened {
				_ = windows.CloseHandle(handle)
			}
			return nil, infoErr
		}
		if safeErr := requireSafeWindowsDirectory(info); safeErr != nil {
			_ = windows.CloseHandle(next)
			for _, handle := range opened {
				_ = windows.CloseHandle(handle)
			}
			return nil, safeErr
		}
		opened = append(opened, next)
		parent = next
	}
	dirID, err := windowsHandleInfo(parent)
	if err != nil {
		for _, handle := range opened {
			_ = windows.CloseHandle(handle)
		}
		return nil, err
	}
	for _, handle := range opened[:len(opened)-1] {
		_ = windows.CloseHandle(handle)
	}
	closeRoot = false
	return &namespaceState{
		root: rootHandle, dir: parent, logical: logical,
		rootID: rootID, dirID: dirID,
	}, nil
}

func openWindowsAbsoluteDirectory(path string) (windows.Handle, error) {
	ntPath := `\??\` + path
	if strings.HasPrefix(path, `\\`) {
		ntPath = `\??\UNC\` + strings.TrimPrefix(path, `\\`)
	}
	name, err := windows.NewNTUnicodeString(ntPath)
	if err != nil {
		return 0, err
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		ObjectName: name, Attributes: windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle, windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE, &attributes, &status, nil,
		0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, windows.FILE_OPEN,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0, 0,
	)
	return handle, normalizeWindowsNamespaceError(err)
}

func openWindowsRelativeDirectory(parent windows.Handle, name string, disposition uint32) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		RootDirectory: parent, ObjectName: objectName,
		Attributes: windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle, windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE, &attributes, &status, nil,
		windows.FILE_ATTRIBUTE_DIRECTORY, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, disposition,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0, 0,
	)
	return handle, err
}

func (n *namespaceState) revalidate() error {
	rootInfo, err := windowsHandleInfo(n.root)
	if err != nil {
		return err
	}
	if !sameWindowsObject(rootInfo, n.rootID) {
		return fmt.Errorf("%w: namespace root identity changed", ErrUnsafe)
	}
	current, err := n.openLogicalDirectory()
	if err != nil {
		return fmt.Errorf("%w: namespace directory cannot be re-opened: %v", ErrUnsafe, err)
	}
	defer windows.CloseHandle(current)
	currentInfo, err := windowsHandleInfo(current)
	if err != nil {
		return err
	}
	heldInfo, err := windowsHandleInfo(n.dir)
	if err != nil {
		return err
	}
	if !sameWindowsObject(currentInfo, n.dirID) || !sameWindowsObject(heldInfo, n.dirID) {
		return fmt.Errorf("%w: namespace directory identity changed", ErrUnsafe)
	}
	return requireSafeWindowsDirectory(currentInfo)
}

func (n *namespaceState) openLogicalDirectory() (windows.Handle, error) {
	parent := n.root
	owned := windows.Handle(0)
	for _, part := range strings.Split(n.logical, "/") {
		next, err := openWindowsRelativeDirectory(parent, part, windows.FILE_OPEN)
		if owned != 0 {
			_ = windows.CloseHandle(owned)
		}
		if err != nil {
			return 0, normalizeWindowsNamespaceError(err)
		}
		owned, parent = next, next
	}
	return owned, nil
}

func (n *namespaceState) listNames() ([]string, error) {
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	var duplicate windows.Handle
	process := windows.CurrentProcess()
	if err := windows.DuplicateHandle(process, n.dir, process, &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, fmt.Errorf("duplicate namespace directory handle: %w", err)
	}
	directory := os.NewFile(uintptr(duplicate), "safe-namespace")
	if directory == nil {
		_ = windows.CloseHandle(duplicate)
		return nil, fmt.Errorf("wrap namespace directory")
	}
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	return names, nil
}

func (n *namespaceState) readRegularBounded(name string, maximum int64) ([]byte, error) {
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	file, before, err := n.openRegular(name, windows.FILE_GENERIC_READ)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if windowsFileSize(before) > maximum {
		return nil, ErrTooLarge
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maximum {
		return nil, ErrTooLarge
	}
	after, err := windowsHandleInfo(windows.Handle(file.Fd()))
	if err != nil {
		return nil, err
	}
	if !sameWindowsRegularSnapshot(before, after) || int64(len(content)) != windowsFileSize(after) {
		return nil, fmt.Errorf("%w: regular file changed while being read", ErrUnsafe)
	}
	if err := n.revalidate(); err != nil {
		return nil, err
	}
	return content, nil
}

func (n *namespaceState) openRegular(name string, access uint32) (*os.File, windows.ByHandleFileInformation, error) {
	handle, err := openWindowsRelativeFile(n.dir, name, access, windows.FILE_OPEN)
	if err != nil {
		return nil, windows.ByHandleFileInformation{}, normalizeWindowsNamespaceError(err)
	}
	info, err := windowsHandleInfo(handle)
	if err != nil {
		_ = windows.CloseHandle(handle)
		return nil, windows.ByHandleFileInformation{}, err
	}
	if err := requireSafeWindowsRegular(info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, windows.ByHandleFileInformation{}, err
	}
	file := os.NewFile(uintptr(handle), "safe-namespace-file")
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, windows.ByHandleFileInformation{}, fmt.Errorf("wrap namespace file")
	}
	return file, info, nil
}

func openWindowsRelativeFile(parent windows.Handle, name string, access, disposition uint32) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		RootDirectory: parent, ObjectName: objectName,
		Attributes: windows.OBJ_CASE_INSENSITIVE | windows.OBJ_DONT_REPARSE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle, access, &attributes, &status, nil, windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, disposition,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT|windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_WRITE_THROUGH,
		0, 0,
	)
	return handle, err
}

func (n *namespaceState) writeRegularAtomic(name string, content []byte, mode fs.FileMode, replace bool) error {
	if err := n.revalidate(); err != nil {
		return err
	}
	if replace {
		if file, _, err := n.openRegular(name, windows.FILE_GENERIC_READ); err == nil {
			_ = file.Close()
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if existing, err := n.readRegularBounded(name, int64(len(content))); err == nil {
		if bytes.Equal(existing, content) {
			return nil
		}
		return ErrConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		if errors.Is(err, ErrTooLarge) {
			return ErrConflict
		}
		return err
	}

	temporary, err := namespaceTemporaryName()
	if err != nil {
		return err
	}
	handle, err := openWindowsRelativeFile(n.dir, temporary, windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE, windows.FILE_CREATE)
	if err != nil {
		return fmt.Errorf("create namespace staging file: %w", normalizeWindowsNamespaceError(err))
	}
	file := os.NewFile(uintptr(handle), "safe-namespace-staging")
	if file == nil {
		_ = windows.CloseHandle(handle)
		return fmt.Errorf("wrap namespace staging file")
	}
	published := false
	defer func() {
		if !published {
			_ = deleteWindowsHandle(windows.Handle(file.Fd()))
		}
		_ = file.Close()
	}()
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := file.Chmod(mode.Perm()); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := renameWindowsHandle(windows.Handle(file.Fd()), n.dir, name, replace); err != nil {
		if !replace {
			existing, readErr := n.readRegularBounded(name, int64(len(content)))
			if readErr == nil && bytes.Equal(existing, content) {
				return nil
			}
			if readErr == nil || errors.Is(readErr, ErrTooLarge) {
				return ErrConflict
			}
			if !errors.Is(readErr, os.ErrNotExist) {
				return readErr
			}
		}
		return fmt.Errorf("publish namespace file: %w", normalizeWindowsNamespaceError(err))
	}
	published = true
	return n.revalidate()
}

type windowsFileRenameInformation struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func renameWindowsHandle(handle, directory windows.Handle, name string, replace bool) error {
	encoded, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	encoded = encoded[:len(encoded)-1]
	var header windowsFileRenameInformation
	size := int(unsafe.Offsetof(header.FileName)) + len(encoded)*2
	buffer := make([]byte, size)
	information := (*windowsFileRenameInformation)(unsafe.Pointer(&buffer[0]))
	if replace {
		information.ReplaceIfExists = windows.FILE_RENAME_REPLACE_IF_EXISTS
	}
	information.RootDirectory = directory
	information.FileNameLength = uint32(len(encoded) * 2)
	copy((*[32768]uint16)(unsafe.Pointer(&information.FileName[0]))[:len(encoded):len(encoded)], encoded)
	var renameErr error
	for attempt := 0; attempt < 9; attempt++ {
		var status windows.IO_STATUS_BLOCK
		renameErr = windows.NtSetInformationFile(handle, &status, &buffer[0], uint32(len(buffer)), windows.FileRenameInformation)
		if renameErr == nil || !transientWindowsRenameContention(renameErr) {
			return renameErr
		}
		if attempt < 8 {
			time.Sleep(time.Duration(1<<attempt) * time.Millisecond)
		}
	}
	return renameErr
}

func transientWindowsRenameContention(err error) bool {
	return err == windows.STATUS_ACCESS_DENIED || err == windows.STATUS_SHARING_VIOLATION || err == windows.STATUS_CANNOT_DELETE ||
		errors.Is(err, windows.ERROR_ACCESS_DENIED) || errors.Is(err, windows.ERROR_SHARING_VIOLATION)
}

func (n *namespaceState) removeRegular(name string) error {
	if err := n.revalidate(); err != nil {
		return err
	}
	file, _, err := n.openRegular(name, windows.FILE_GENERIC_READ|windows.DELETE)
	if err != nil {
		return err
	}
	if err := deleteWindowsHandle(windows.Handle(file.Fd())); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return n.revalidate()
}

func deleteWindowsHandle(handle windows.Handle) error {
	deleteOnClose := byte(1)
	var status windows.IO_STATUS_BLOCK
	return windows.NtSetInformationFile(handle, &status, &deleteOnClose, 1, windows.FileDispositionInformation)
}

func (n *namespaceState) close() error {
	dirErr := windows.CloseHandle(n.dir)
	rootErr := windows.CloseHandle(n.root)
	return errors.Join(dirErr, rootErr)
}

func windowsHandleInfo(handle windows.Handle) (windows.ByHandleFileInformation, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return windows.ByHandleFileInformation{}, err
	}
	return info, nil
}

func requireSafeWindowsDirectory(info windows.ByHandleFileInformation) error {
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 || info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("%w: namespace directory is not a non-reparse directory", ErrUnsafe)
	}
	return nil
}

func requireSafeWindowsRegular(info windows.ByHandleFileInformation) error {
	if info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 || info.NumberOfLinks != 1 {
		return fmt.Errorf("%w: namespace entry is not a single-link non-reparse regular file", ErrUnsafe)
	}
	return nil
}

func sameWindowsObject(left, right windows.ByHandleFileInformation) bool {
	return left.VolumeSerialNumber == right.VolumeSerialNumber && left.FileIndexHigh == right.FileIndexHigh && left.FileIndexLow == right.FileIndexLow
}

func sameWindowsRegularSnapshot(left, right windows.ByHandleFileInformation) bool {
	return sameWindowsObject(left, right) && left.FileAttributes == right.FileAttributes && left.NumberOfLinks == right.NumberOfLinks &&
		left.FileSizeHigh == right.FileSizeHigh && left.FileSizeLow == right.FileSizeLow && left.LastWriteTime == right.LastWriteTime
}

func windowsFileSize(info windows.ByHandleFileInformation) int64 {
	return int64(uint64(info.FileSizeHigh)<<32 | uint64(info.FileSizeLow))
}

func windowsNotExist(err error) bool {
	return err == windows.STATUS_OBJECT_NAME_NOT_FOUND || err == windows.STATUS_OBJECT_PATH_NOT_FOUND ||
		errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND)
}

func normalizeWindowsNamespaceError(err error) error {
	if err == nil {
		return nil
	}
	if windowsNotExist(err) {
		return os.ErrNotExist
	}
	if err == windows.STATUS_OBJECT_NAME_COLLISION || errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) {
		return os.ErrExist
	}
	if err == windows.STATUS_FILE_IS_A_DIRECTORY || err == windows.STATUS_NOT_A_DIRECTORY ||
		err == windows.STATUS_REPARSE_POINT_ENCOUNTERED || err == windows.STATUS_IO_REPARSE_TAG_NOT_HANDLED ||
		errors.Is(err, windows.ERROR_CANT_ACCESS_FILE) {
		return ErrUnsafe
	}
	return err
}
