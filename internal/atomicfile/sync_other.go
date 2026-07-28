//go:build !windows

package atomicfile

func isDirectorySyncUnsupported(error) bool { return false }
