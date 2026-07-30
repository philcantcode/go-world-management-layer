//go:build !linux && !windows && !darwin

package safepath

import (
	"io/fs"
)

type namespaceState struct{}

func openNamespaceState(string, string) (*namespaceState, error) {
	return nil, ErrUnsupported
}

func (*namespaceState) listNames() ([]string, error)                     { return nil, ErrUnsupported }
func (*namespaceState) readRegularBounded(string, int64) ([]byte, error) { return nil, ErrUnsupported }
func (*namespaceState) writeRegularAtomic(string, []byte, fs.FileMode, bool) error {
	return ErrUnsupported
}
func (*namespaceState) removeRegular(string) error { return ErrUnsupported }
func (*namespaceState) revalidate() error          { return ErrUnsupported }
func (*namespaceState) close() error               { return nil }
