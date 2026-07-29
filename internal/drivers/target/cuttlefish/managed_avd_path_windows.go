//go:build windows

package cuttlefish

import (
	"fmt"

	"golang.org/x/sys/windows"
)

func requireManagedAVDPathNotReparse(path string) error {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := windows.GetFileAttributes(pointer)
	if err != nil {
		return fmt.Errorf("inspect exact managed AVD path attributes %q: %w", path, err)
	}
	if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("exact managed AVD path %q is a reparse point", path)
	}
	return nil
}
