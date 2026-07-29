package cuttlefish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type androidTransport struct {
	driver        *Driver
	gateway       Gateway
	files         ScopedFileGateway
	scope         deviceproxy.Scope
	allocation    Allocation
	mu            sync.Mutex
	closed        bool
	drained       bool
	closing       bool
	active        int
	endpoints     []ports.ScopedADBEndpoint
	nextOperation uint64
	operations    map[uint64]context.CancelFunc
	closeDone     chan struct{}
	closeErr      error
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
	operationContext, endOperation, err := t.beginOperation(ctx)
	if err != nil {
		return ports.TransferResult{}, err
	}
	defer endOperation()
	normalized, err := normalizeDeviceLogicalPath(plan.RelativePath)
	if err != nil {
		return ports.TransferResult{}, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.transport.push", "relative_path", "is not a safe device-relative path", err)
	}
	expectedSize := int64(-1)
	expectedDigest := plan.Operation.Spec().ContentDigest
	mode := plan.Mode
	if mode == 0 {
		mode = 0o600
	}
	if err := t.recordLifecycle("target.transfer.opened", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		MaximumBytes int64                      `json:"maximum_bytes"`
	}{Kind: domain.TargetOperationPush, RelativePath: normalized, MaximumBytes: plan.MaximumBytes}); err != nil {
		return ports.TransferResult{}, err
	}
	file, err := t.files.Put(operationContext, t.scope, t.allocation, DeviceFileWritePlan{
		Area:           DeviceFileWritable,
		LogicalPath:    normalized,
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
	if err := t.requireOpen(); err != nil {
		return ports.TransferResult{}, err
	}
	result := ports.TransferResult{OperationID: plan.Operation.ID(), Digest: file.Digest, Bytes: file.Size}
	if err := t.recordSuccessfulPush(plan.Operation.ID(), normalized, mode, file); err != nil {
		return ports.TransferResult{}, err
	}
	return result, nil
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
	operationContext, endOperation, err := t.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer endOperation()
	normalized, err := normalizeDeviceLogicalPath(plan.RelativePath)
	if err != nil {
		return nil, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.transport.pull", "relative_path", "is not a safe device-relative path", err)
	}
	if err := t.recordLifecycle("target.transfer.opened", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		MaximumBytes int64                      `json:"maximum_bytes"`
	}{Kind: domain.TargetOperationPull, RelativePath: normalized, MaximumBytes: plan.MaximumBytes}); err != nil {
		return nil, err
	}
	content, err := t.files.Get(operationContext, t.scope, t.allocation, normalized, plan.MaximumBytes)
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
	if err := t.recordLifecycle("target.transfer.succeeded", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		Digest       domain.Digest              `json:"digest"`
		Bytes        int64                      `json:"bytes"`
	}{Kind: domain.TargetOperationPull, RelativePath: normalized, Digest: content.Digest(), Bytes: content.Size()}); err != nil {
		_ = content.Close()
		return nil, err
	}
	return content, nil
}

func (t *androidTransport) OpenADB(ctx context.Context) (ports.ScopedADBEndpoint, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.transport.adb"); err != nil {
		return nil, err
	}
	operationContext, endOperation, err := t.beginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer endOperation()
	endpoint, err := t.gateway.Open(operationContext, t.scope, t.allocation)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "cuttlefish.transport.adb", "gateway", "scoped ADB gateway could not be opened", err)
	}
	if endpoint.Serial() != t.scope.Serial {
		closeErr := endpoint.Close()
		if closeErr != nil {
			t.retainEndpoint(endpoint)
		}
		return nil, errors.Join(domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.transport.adb", "serial", "gateway exposed a different device serial", nil), closeErr)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		if err := endpoint.Close(); err != nil {
			t.retainEndpoint(endpoint)
			return nil, errors.Join(io.ErrClosedPipe, err)
		}
		return nil, io.ErrClosedPipe
	}
	t.endpoints = append(t.endpoints, endpoint)
	t.mu.Unlock()
	if err := t.recordADBAuthority(endpoint); err != nil {
		closeErr := endpoint.Close()
		if closeErr == nil {
			t.removeEndpoint(endpoint)
		}
		return nil, errors.Join(err, closeErr)
	}
	return endpoint, nil
}

func (t *androidTransport) Close() error {
	return t.closeWithContext(context.Background())
}

func (t *androidTransport) revoke() {
	t.mu.Lock()
	t.closed = true
	cancellations := make([]context.CancelFunc, 0, len(t.operations))
	for _, cancel := range t.operations {
		cancellations = append(cancellations, cancel)
	}
	t.mu.Unlock()
	for _, cancel := range cancellations {
		cancel()
	}
}

func (t *androidTransport) closeWithContext(ctx context.Context) error {
	t.revoke()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		t.mu.Lock()
		if t.drained {
			t.mu.Unlock()
			return nil
		}
		if t.closing {
			done := t.closeDone
			t.mu.Unlock()
			select {
			case <-done:
				t.mu.Lock()
				err := t.closeErr
				t.mu.Unlock()
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return fmt.Errorf("wait for scoped Android endpoint revocation: %w", ctx.Err())
			}
		} else {
			endpoints := append([]ports.ScopedADBEndpoint(nil), t.endpoints...)
			t.endpoints = nil
			t.closing = true
			t.closeDone = make(chan struct{})
			done := t.closeDone
			t.mu.Unlock()
			go t.closeEndpoints(endpoints, done)
			select {
			case <-done:
				t.mu.Lock()
				err := t.closeErr
				t.mu.Unlock()
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return fmt.Errorf("revoke scoped Android endpoints: %w", ctx.Err())
			}
		}
		for {
			t.mu.Lock()
			if t.active == 0 {
				if !t.closing && len(t.endpoints) == 0 {
					t.drained = true
					t.mu.Unlock()
					return nil
				}
				t.mu.Unlock()
				break
			}
			t.mu.Unlock()
			select {
			case <-ctx.Done():
				return fmt.Errorf("drain active scoped Android operations: %w", ctx.Err())
			case <-ticker.C:
			}
		}
	}
}

func (t *androidTransport) closeEndpoints(endpoints []ports.ScopedADBEndpoint, done chan struct{}) {
	failed := make([]ports.ScopedADBEndpoint, 0, len(endpoints))
	errorsFound := make([]error, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if err := endpoint.Close(); err != nil {
			failed = append(failed, endpoint)
			errorsFound = append(errorsFound, err)
		}
	}
	t.mu.Lock()
	t.endpoints = append(t.endpoints, failed...)
	t.closeErr = errors.Join(errorsFound...)
	t.closing = false
	close(done)
	t.mu.Unlock()
}

func (t *androidTransport) retainEndpoint(endpoint ports.ScopedADBEndpoint) {
	t.mu.Lock()
	t.endpoints = append(t.endpoints, endpoint)
	t.mu.Unlock()
}

func (t *androidTransport) removeEndpoint(endpoint ports.ScopedADBEndpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for index, current := range t.endpoints {
		if current == endpoint {
			t.endpoints = append(t.endpoints[:index], t.endpoints[index+1:]...)
			return
		}
	}
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

func (t *androidTransport) beginOperation(ctx context.Context) (context.Context, func(), error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, nil, io.ErrClosedPipe
	}
	operationContext, cancel := context.WithCancel(ctx)
	if t.operations == nil {
		t.operations = make(map[uint64]context.CancelFunc)
	}
	t.nextOperation++
	operationID := t.nextOperation
	t.operations[operationID] = cancel
	t.active++
	var once sync.Once
	return operationContext, func() {
		once.Do(func() { t.endOperation(operationID) })
	}, nil
}

func (t *androidTransport) endOperation(operationID uint64) {
	t.mu.Lock()
	cancel := t.operations[operationID]
	delete(t.operations, operationID)
	t.active--
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (t *androidTransport) recordLifecycle(kind string, operationID domain.TargetOperationID, payload any) error {
	if t.driver == nil {
		return nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return domain.NewError(domain.CodeInternal, "cuttlefish.transport.evidence", "payload", "could not encode Android lifecycle evidence", err)
	}
	return t.driver.recordRunFact(t.scope.RunID, ports.TargetRunObservation{
		Kind: kind, ObservedAt: t.driver.now().UTC(), TargetOperationID: operationID, Payload: encoded,
	}, nil)
}

func (t *androidTransport) recordSuccessfulPush(operationID domain.TargetOperationID, logicalPath string, mode uint32, file DeviceFile) error {
	if t.driver == nil {
		return nil
	}
	encoded, err := json.Marshal(struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		Digest       domain.Digest              `json:"digest"`
		Bytes        int64                      `json:"bytes"`
		Mode         uint32                     `json:"mode"`
	}{Kind: domain.TargetOperationPush, RelativePath: logicalPath, Digest: file.Digest, Bytes: file.Size, Mode: mode})
	if err != nil {
		return domain.NewError(domain.CodeInternal, "cuttlefish.transport.evidence", "payload", "could not encode Android push evidence", err)
	}
	return t.driver.recordRunFact(t.scope.RunID, ports.TargetRunObservation{
		Kind: "target.transfer.succeeded", ObservedAt: t.driver.now().UTC(), TargetOperationID: operationID, Payload: encoded,
	}, func(run *runRecord) {
		run.scopedWrites[logicalPath] = scopedWriteEvidence{file: file, mode: mode}
	})
}

func (t *androidTransport) recordADBAuthority(endpoint ports.ScopedADBEndpoint) error {
	if t.driver == nil {
		return nil
	}
	encoded, err := json.Marshal(struct {
		Serial                  string `json:"serial"`
		Address                 string `json:"address"`
		ArbitraryDeviceServices bool   `json:"arbitrary_device_services"`
		MutationCoverage        string `json:"mutation_coverage"`
	}{Serial: endpoint.Serial(), Address: endpoint.Address(), ArbitraryDeviceServices: true, MutationCoverage: "opaque"})
	if err != nil {
		return domain.NewError(domain.CodeInternal, "cuttlefish.transport.evidence", "payload", "could not encode scoped ADB authority evidence", err)
	}
	return t.driver.recordRunFact(t.scope.RunID, ports.TargetRunObservation{
		Kind: "target.adb.authority-issued", ObservedAt: t.driver.now().UTC(), Payload: encoded,
	}, func(run *runRecord) { run.adbAuthorityIssued = true })
}

func classifiedDriverFailure(operation, field, message string, err error) error {
	code := domain.ErrorCodeOf(err)
	if code == domain.CodeInternal {
		code = domain.CodeUnavailable
	}
	return domain.NewError(code, operation, field, message, err)
}

var _ ports.TargetTransport = (*androidTransport)(nil)
