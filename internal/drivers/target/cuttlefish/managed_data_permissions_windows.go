//go:build windows

package cuttlefish

import (
	"os"

	"golang.org/x/sys/windows"
)

func managedDataBackingReadOnly(path string, _ os.FileInfo) (bool, error) {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false, err
	}
	attributes, err := windows.GetFileAttributes(pointer)
	if err != nil {
		return false, err
	}
	return attributes&windows.FILE_ATTRIBUTE_READONLY != 0, nil
}
