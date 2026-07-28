package safepath

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
)

var (
	ErrConflict    = errors.New("safe namespace content conflicts")
	ErrClosed      = errors.New("safe namespace is closed")
	ErrUnsupported = errors.New("safe namespace is unsupported on this platform")
)

// Namespace pins one durable directory beneath a trusted state root. Every
// file operation is performed relative to the held directory identity by the
// platform implementation; callers never reopen the directory through a path.
type Namespace struct {
	mu     sync.Mutex
	state  *namespaceState
	closed bool
}

func OpenNamespace(root, logicalDirectory string) (*Namespace, error) {
	normalized, err := Normalize(logicalDirectory)
	if err != nil {
		return nil, err
	}
	state, err := openNamespaceState(root, normalized)
	if err != nil {
		return nil, err
	}
	return &Namespace{state: state}, nil
}

func (n *Namespace) ListNames() ([]string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.requireOpen(); err != nil {
		return nil, err
	}
	names, err := n.state.listNames()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if _, err := normalizeNamespaceName(name); err != nil {
			return nil, fmt.Errorf("%w: invalid namespace entry %q", ErrUnsafe, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (n *Namespace) ReadRegularBounded(name string, maximum int64) ([]byte, error) {
	name, err := normalizeNamespaceName(name)
	if err != nil {
		return nil, err
	}
	if maximum < 0 {
		return nil, fmt.Errorf("maximum bytes must be non-negative")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.requireOpen(); err != nil {
		return nil, err
	}
	return n.state.readRegularBounded(name, maximum)
}

// EnsureRegularAtomically publishes a complete regular file without replacing
// an existing name. An existing byte-identical file is accepted; any other
// existing object fails closed.
func (n *Namespace) EnsureRegularAtomically(name string, content []byte, mode fs.FileMode) error {
	return n.write(name, content, mode, false)
}

// ReplaceRegularAtomically publishes a complete regular file and atomically
// replaces an existing regular, single-link destination. It is intended for
// mutable compact control markers, not content-addressed evidence.
func (n *Namespace) ReplaceRegularAtomically(name string, content []byte, mode fs.FileMode) error {
	return n.write(name, content, mode, true)
}

func (n *Namespace) write(name string, content []byte, mode fs.FileMode, replace bool) error {
	name, err := normalizeNamespaceName(name)
	if err != nil {
		return err
	}
	if mode.Perm() == 0 || mode&^fs.ModePerm != 0 {
		return fmt.Errorf("invalid regular-file mode %v", mode)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.requireOpen(); err != nil {
		return err
	}
	return n.state.writeRegularAtomic(name, content, mode.Perm(), replace)
}

func (n *Namespace) RemoveRegular(name string) error {
	name, err := normalizeNamespaceName(name)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.requireOpen(); err != nil {
		return err
	}
	return n.state.removeRegular(name)
}

func (n *Namespace) CleanupPrefix(prefix string) error {
	if prefix == "" || strings.ContainsAny(prefix, "/\\:\x00") {
		return fmt.Errorf("%w: invalid cleanup prefix", ErrUnsafe)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.requireOpen(); err != nil {
		return err
	}
	names, err := n.state.listNames()
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if _, err := normalizeNamespaceName(name); err != nil {
			return fmt.Errorf("%w: invalid namespace entry %q", ErrUnsafe, name)
		}
		if err := n.state.removeRegular(name); err != nil {
			return err
		}
	}
	return n.state.revalidate()
}

func (n *Namespace) Revalidate() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.requireOpen(); err != nil {
		return err
	}
	return n.state.revalidate()
}

func (n *Namespace) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return nil
	}
	n.closed = true
	return n.state.close()
}

func (n *Namespace) requireOpen() error {
	if n == nil || n.closed || n.state == nil {
		return ErrClosed
	}
	return nil
}

func normalizeNamespaceName(value string) (string, error) {
	normalized, err := Normalize(value)
	if err != nil {
		return "", err
	}
	if strings.Contains(normalized, "/") || normalized != value || strings.HasSuffix(value, ".") || strings.HasSuffix(value, " ") {
		return "", fmt.Errorf("%w: namespace file name must be one canonical component", ErrUnsafe)
	}
	return normalized, nil
}

func namespaceTemporaryName() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create namespace temporary name: %w", err)
	}
	return ".world-ns-" + hex.EncodeToString(value[:]), nil
}
