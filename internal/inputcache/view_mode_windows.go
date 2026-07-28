//go:build windows

package inputcache

import "io/fs"

// Windows exposes only its read-only attribute through Chmod. Match the one
// permission distinction the host can faithfully represent.
func viewModeMatches(actual fs.FileMode, requested uint32) bool {
	return actual.Perm()&0o200 == fs.FileMode(requested)&0o200
}
