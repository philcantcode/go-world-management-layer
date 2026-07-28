//go:build !windows

package inputcache

import "io/fs"

func viewModeMatches(actual fs.FileMode, requested uint32) bool {
	return actual.Perm() == fs.FileMode(requested)&fs.ModePerm
}
