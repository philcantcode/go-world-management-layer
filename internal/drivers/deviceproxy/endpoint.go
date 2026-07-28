package deviceproxy

import (
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

// Endpoint is a small scoped handle. A Gateway implementation owns the actual
// listener/protocol pump and supplies closeFn.
type Endpoint struct {
	serial  string
	address string
	closeFn func() error
	once    sync.Once
	err     error
}

func NewEndpoint(serial, address string, closeFn func() error) *Endpoint {
	if closeFn == nil {
		closeFn = func() error { return nil }
	}
	return &Endpoint{serial: serial, address: address, closeFn: closeFn}
}

func (e *Endpoint) Serial() string  { return e.serial }
func (e *Endpoint) Address() string { return e.address }
func (e *Endpoint) Close() error {
	e.once.Do(func() { e.err = e.closeFn() })
	return e.err
}

var _ ports.ScopedADBEndpoint = (*Endpoint)(nil)
