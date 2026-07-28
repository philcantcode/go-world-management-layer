package cuttlefish

import (
	"context"
	"io"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type androidTransport struct {
	gateway    Gateway
	files      ScopedFileGateway
	scope      deviceproxy.Scope
	allocation Allocation
	mu         sync.Mutex
	closed     bool
	endpoints  []ports.ScopedADBEndpoint
}

func (t *androidTransport) OpenExec(ctx context.Context, _ ports.TargetExecPlan) (ports.ExecTransport, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.transport.exec"); err != nil {
		return nil, err
	}
	return nil, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.transport.exec", "transport", "use the scoped ADB shell service for Android exec", nil)
}

func (t *androidTransport) PushFile(ctx context.Context, plan ports.TargetTransferPlan, reader io.Reader) (ports.TransferResult, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.transport.push"); err != nil {
		return ports.TransferResult{}, err
	}
	if err := plan.Validate(domain.TargetOperationPush); err != nil {
		return ports.TransferResult{}, err
	}
	if err := t.authorize(plan.Operation); err != nil {
		return ports.TransferResult{}, err
	}
	if err := t.requireOpen(); err != nil {
		return ports.TransferResult{}, err
	}
	expectedSize := int64(-1)
	expectedDigest := plan.Operation.Spec().ContentDigest
	mode := plan.Mode
	if mode == 0 {
		mode = 0o600
	}
	file, err := t.files.Put(ctx, t.scope, t.allocation, DeviceFileWritePlan{
		Area:           DeviceFileWritable,
		LogicalPath:    plan.RelativePath,
		Mode:           mode,
		MaximumBytes:   plan.MaximumBytes,
		ExpectedDigest: expectedDigest,
		ExpectedSize:   expectedSize,
	}, reader)
	if err != nil {
		return ports.TransferResult{}, classifiedDriverFailure("cuttlefish.transport.push", "adb", "scoped exact-serial ADB push failed", err)
	}
	if !expectedDigest.IsZero() && file.Digest != expectedDigest {
		return ports.TransferResult{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.transport.push", "content_digest", "pushed bytes do not match the operation digest", nil)
	}
	return ports.TransferResult{OperationID: plan.Operation.ID(), Digest: file.Digest, Bytes: file.Size}, nil
}

func (t *androidTransport) PullFile(ctx context.Context, plan ports.TargetTransferPlan) (io.ReadCloser, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.transport.pull"); err != nil {
		return nil, err
	}
	if err := plan.Validate(domain.TargetOperationPull); err != nil {
		return nil, err
	}
	if err := t.authorize(plan.Operation); err != nil {
		return nil, err
	}
	if err := t.requireOpen(); err != nil {
		return nil, err
	}
	content, err := t.files.Get(ctx, t.scope, t.allocation, plan.RelativePath, plan.MaximumBytes)
	if err != nil {
		return nil, classifiedDriverFailure("cuttlefish.transport.pull", "adb", "scoped exact-serial ADB pull failed", err)
	}
	expected := plan.Operation.Spec().ContentDigest
	if !expected.IsZero() && content.Digest() != expected {
		_ = content.Close()
		return nil, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.transport.pull", "content_digest", "pulled bytes do not match the operation digest", nil)
	}
	if err := t.requireOpen(); err != nil {
		_ = content.Close()
		return nil, err
	}
	return content, nil
}

func (t *androidTransport) OpenADB(ctx context.Context) (ports.ScopedADBEndpoint, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.transport.adb"); err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	t.mu.Unlock()
	endpoint, err := t.gateway.Open(ctx, t.scope, t.allocation)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "cuttlefish.transport.adb", "gateway", "scoped ADB gateway could not be opened", err)
	}
	if endpoint.Serial() != t.scope.Serial {
		_ = endpoint.Close()
		return nil, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.transport.adb", "serial", "gateway exposed a different device serial", nil)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = endpoint.Close()
		return nil, io.ErrClosedPipe
	}
	t.endpoints = append(t.endpoints, endpoint)
	t.mu.Unlock()
	return endpoint, nil
}

func (t *androidTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	endpoints := append([]ports.ScopedADBEndpoint(nil), t.endpoints...)
	t.mu.Unlock()
	var first error
	for _, endpoint := range endpoints {
		if err := endpoint.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (t *androidTransport) authorize(operation domain.TargetOperation) error {
	spec := operation.Spec()
	if spec.LeaseID != t.scope.LeaseID || spec.TargetID != t.scope.TargetID || spec.TargetGeneration != t.scope.Generation || spec.TargetRunID != t.scope.RunID {
		return domain.NewError(domain.CodeForbidden, "cuttlefish.transport.authorize", "operation", "operation is outside this run's lease, target, or generation", nil)
	}
	return nil
}

func (t *androidTransport) requireOpen() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.ErrClosedPipe
	}
	return nil
}

func classifiedDriverFailure(operation, field, message string, err error) error {
	code := domain.ErrorCodeOf(err)
	if code == domain.CodeInternal {
		code = domain.CodeUnavailable
	}
	return domain.NewError(code, operation, field, message, err)
}

var _ ports.TargetTransport = (*androidTransport)(nil)
