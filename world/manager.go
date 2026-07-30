package world

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/daemon"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/research"
	"github.com/philcantcode/go-world-management-layer/internal/rpc"
)

// Manager is the sole public control handle for one control-state tree.
// It is not safe to Open the same StatePath from two processes; the second
// Open fails with a processlock error.
//
// Manager methods apply Config.Subject before invoking production Service /
// Controller logic. There is no network hop and no gRPC dial.
type Manager struct {
	host           *daemon.Host
	facade         *rpc.WorldServer
	subject        Subject
	identity       rpc.Identity
	defaultTimeout time.Duration

	closed atomic.Bool
	mu     sync.Mutex
}

// Close stops background reconciliation, closes drivers/observers, releases
// the process lock, and closes store/ledger. Close is idempotent.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.host == nil {
		return nil
	}
	err := m.host.Close()
	m.host = nil
	m.facade = nil
	return err
}

// Subject returns the fixed policy subject installed at Open.
func (m *Manager) Subject() Subject {
	if m == nil {
		return Subject{}
	}
	return m.subject
}

// Core returns the application core for advanced/adapters hand-off.
func (m *Manager) Core() *application.Core {
	if m == nil || m.closed.Load() || m.host == nil {
		return nil
	}
	return m.host.Core()
}

// AgentDriver returns the composed agent workspace driver, if any.
func (m *Manager) AgentDriver() ports.AgentWorkspaceDriver {
	if m == nil || m.closed.Load() || m.host == nil {
		return nil
	}
	return m.host.AgentDriver()
}

// ActionEvidence returns the research action evidence store when composed.
func (m *Manager) ActionEvidence() *research.Store {
	if m == nil || m.closed.Load() || m.host == nil {
		return nil
	}
	return m.host.ActionEvidence()
}

// Material returns the material authority for forensic adapters.
func (m *Manager) Material() ports.MaterialAuthority {
	if m == nil || m.closed.Load() || m.host == nil {
		return nil
	}
	return m.host.Material()
}

// Reconcile runs one host-triggered reconciliation tick (physical adoption plus
// lease-termination scan). Startup reconciliation already completed inside Open.
func (m *Manager) Reconcile(ctx context.Context) error {
	if err := m.requireOpen(); err != nil {
		return err
	}
	return m.host.Reconcile(ctx)
}

func (m *Manager) requireOpen() error {
	if m == nil || m.closed.Load() || m.host == nil || m.facade == nil {
		return fmt.Errorf("world manager is closed")
	}
	return nil
}

func (m *Manager) withSubject(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return rpc.ContextWithIdentity(ctx, m.identity)
}

func (m *Manager) contextFor(ctx context.Context, mutation *worldv1.MutationMetadata) (context.Context, context.CancelFunc, error) {
	if err := m.requireOpen(); err != nil {
		return nil, nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.Now().Add(m.defaultTimeout)
	if current, ok := ctx.Deadline(); ok && current.Before(deadline) {
		deadline = current
	}
	if mutation != nil && mutation.Deadline != nil {
		if err := mutation.Deadline.CheckValid(); err != nil {
			return nil, nil, fmt.Errorf("mutation deadline: %w", err)
		}
		declared := mutation.Deadline.AsTime()
		if declared.Before(deadline) {
			deadline = declared
		}
	}
	bound, cancel := context.WithDeadline(m.withSubject(ctx), deadline)
	return bound, cancel, nil
}

func invokeUnary[Response any](ctx context.Context, m *Manager, mutation *worldv1.MutationMetadata, call func(context.Context) (*Response, error)) (*Response, error) {
	bound, cancel, err := m.contextFor(ctx, mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := call(bound)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("world manager returned a nil response")
	}
	return defensiveCopy(response)
}

// MutationMetadata is a type alias for the public mutation envelope.
type MutationMetadata = worldv1.MutationMetadata
