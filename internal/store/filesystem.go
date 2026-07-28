package store

import "os"

func makeDirectory(path string) error {
	if path == "." || path == "" {
		return nil
	}
	return os.MkdirAll(path, 0o700)
}
