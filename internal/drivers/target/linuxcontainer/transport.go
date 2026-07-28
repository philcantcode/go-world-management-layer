package linuxcontainer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

type targetTransport struct {
	driver           *Driver
	runtime          Runtime
	runtimeID        string
	root             string
	authority        RunAuthority
	uid              int
	gid              int
	enforceOwnership bool

	mu      sync.Mutex
	closed  bool
	drained bool
	execs   []ports.ExecTransport
	pulls   map[*verifiedPull]struct{}
}

func (t *targetTransport) OpenExec(ctx context.Context, plan ports.TargetExecPlan) (ports.ExecTransport, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.transport.exec"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if err := t.authorize(plan.Operation); err != nil {
		return nil, err
	}
	if err := t.requireOpen(); err != nil {
		return nil, err
	}
	exec, err := t.runtime.OpenExec(ctx, t.runtimeID, plan)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "linux_target.transport.exec", "runtime", "target exec could not be opened", err)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = exec.Close()
		return nil, io.ErrClosedPipe
	}
	t.execs = append(t.execs, exec)
	t.mu.Unlock()
	if err := t.recordLifecycle("target.exec.opened", plan.Operation.ID(), struct {
		Kind domain.TargetOperationKind `json:"kind"`
	}{Kind: plan.Operation.Spec().Kind}); err != nil {
		_ = exec.Close()
		return nil, err
	}
	return exec, nil
}

func (t *targetTransport) PushFile(ctx context.Context, plan ports.TargetTransferPlan, reader io.Reader) (ports.TransferResult, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.transport.push"); err != nil {
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
	if reader == nil {
		return ports.TransferResult{}, domain.NewError(domain.CodeInvalidArgument, "linux_target.transport.push", "reader", "is required", nil)
	}
	normalized, err := safepath.Normalize(plan.RelativePath)
	if err != nil {
		return ports.TransferResult{}, domain.NewError(domain.CodeInvalidArgument, "linux_target.transport.push", "relative_path", "is not a safe target-relative path", err)
	}
	if err := t.recordLifecycle("target.transfer.opened", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		MaximumBytes int64                      `json:"maximum_bytes"`
	}{Kind: domain.TargetOperationPush, RelativePath: normalized, MaximumBytes: plan.MaximumBytes}); err != nil {
		return ports.TransferResult{}, err
	}
	hash := sha256.New()
	var written int64
	var digest domain.Digest
	mode := fs.FileMode(plan.Mode)
	if mode == 0 {
		mode = 0o600
	}
	err = safepath.WriteRegularAtomic(t.root, normalized, mode, func(destination io.Writer) error {
		var copyErr error
		written, copyErr = safepath.CopyBounded(io.MultiWriter(destination, hash), &contextReader{ctx: ctx, reader: reader, alive: t.requireOpen}, plan.MaximumBytes)
		if copyErr != nil {
			return copyErr
		}
		digest, copyErr = domain.ParseDigest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
		if copyErr != nil {
			return copyErr
		}
		expected := plan.Operation.Spec().ContentDigest
		if !expected.IsZero() && digest != expected {
			return domain.NewError(domain.CodeIntegrityViolation, "linux_target.transport.push", "content_digest", "source bytes do not match the authorized operation digest", nil)
		}
		return t.requireOpen()
	})
	if err != nil {
		if errors.Is(err, safepath.ErrTooLarge) {
			return ports.TransferResult{}, domain.NewError(domain.CodeResourceExhausted, "linux_target.transport.push", "maximum_bytes", "source exceeds the transfer limit", err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ports.TransferResult{}, err
		}
		if domain.IsCode(err, domain.CodeIntegrityViolation) {
			return ports.TransferResult{}, err
		}
		return ports.TransferResult{}, domain.NewError(domain.CodeForbidden, "linux_target.transport.push", "relative_path", "cannot publish a regular file beneath target root", err)
	}
	if t.enforceOwnership {
		if err := setManagedFileOwner(t.root, normalized, t.uid, t.gid); err != nil {
			return ports.TransferResult{}, domain.NewError(domain.CodeUnavailable, "linux_target.transport.push", "target_owner", "pushed file could not be handed to the target identity", err)
		}
	}
	result := ports.TransferResult{OperationID: plan.Operation.ID(), Digest: digest, Bytes: written}
	if err := t.recordLifecycle("target.transfer.succeeded", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		Digest       domain.Digest              `json:"digest"`
		Bytes        int64                      `json:"bytes"`
	}{Kind: domain.TargetOperationPush, RelativePath: normalized, Digest: digest, Bytes: written}); err != nil {
		return ports.TransferResult{}, err
	}
	return result, nil
}

func (t *targetTransport) PullFile(ctx context.Context, plan ports.TargetTransferPlan) (io.ReadCloser, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.transport.pull"); err != nil {
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
	normalized, err := safepath.Normalize(plan.RelativePath)
	if err != nil {
		return nil, domain.NewError(domain.CodeInvalidArgument, "linux_target.transport.pull", "relative_path", "is not a safe target-relative path", err)
	}
	if err := t.recordLifecycle("target.transfer.opened", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		MaximumBytes int64                      `json:"maximum_bytes"`
	}{Kind: domain.TargetOperationPull, RelativePath: normalized, MaximumBytes: plan.MaximumBytes}); err != nil {
		return nil, err
	}
	file, err := safepath.OpenRegular(t.root, plan.RelativePath)
	if err != nil {
		return nil, domain.NewError(domain.CodeForbidden, "linux_target.transport.pull", "relative_path", "cannot be opened beneath target root", err)
	}
	defer file.Close()
	if file.Size() > plan.MaximumBytes {
		return nil, domain.NewError(domain.CodeResourceExhausted, "linux_target.transport.pull", "maximum_bytes", "target file exceeds transfer limit", safepath.ErrTooLarge)
	}
	var snapshot bytes.Buffer
	hash := sha256.New()
	written, err := safepath.CopyBounded(io.MultiWriter(&snapshot, hash), &contextReader{ctx: ctx, reader: file, alive: t.requireOpen}, plan.MaximumBytes)
	if err != nil {
		if errors.Is(err, safepath.ErrTooLarge) {
			return nil, domain.NewError(domain.CodeResourceExhausted, "linux_target.transport.pull", "maximum_bytes", "target file exceeded transfer limit while being read", err)
		}
		return nil, err
	}
	digest, err := domain.ParseDigest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
	if err != nil {
		return nil, err
	}
	expected := plan.Operation.Spec().ContentDigest
	if !expected.IsZero() && digest != expected {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "linux_target.transport.pull", "content_digest", "target bytes do not match the authorized operation digest", nil)
	}
	reader := &verifiedPull{reader: bytes.NewReader(snapshot.Bytes()), digest: digest, size: written}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = reader.Close()
		return nil, io.ErrClosedPipe
	}
	if t.pulls == nil {
		t.pulls = make(map[*verifiedPull]struct{})
	}
	t.pulls[reader] = struct{}{}
	reader.onClose = func() {
		t.mu.Lock()
		delete(t.pulls, reader)
		t.mu.Unlock()
	}
	t.mu.Unlock()
	if err := t.recordLifecycle("target.transfer.succeeded", plan.Operation.ID(), struct {
		Kind         domain.TargetOperationKind `json:"kind"`
		RelativePath string                     `json:"relative_path"`
		Digest       domain.Digest              `json:"digest"`
		Bytes        int64                      `json:"bytes"`
	}{Kind: domain.TargetOperationPull, RelativePath: normalized, Digest: digest, Bytes: written}); err != nil {
		_ = reader.Close()
		return nil, err
	}
	return reader, nil
}

func (t *targetTransport) OpenADB(ctx context.Context) (ports.ScopedADBEndpoint, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.transport.adb"); err != nil {
		return nil, err
	}
	return nil, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.transport.adb", "target", "ADB is unavailable for Linux targets", nil)
}

func (t *targetTransport) Close() error {
	t.mu.Lock()
	if t.drained {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.drained = true
	execs := append([]ports.ExecTransport(nil), t.execs...)
	t.execs = nil
	pulls := make([]*verifiedPull, 0, len(t.pulls))
	for reader := range t.pulls {
		pulls = append(pulls, reader)
	}
	t.pulls = nil
	t.mu.Unlock()
	var first error
	for _, exec := range execs {
		if err := exec.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, reader := range pulls {
		if err := reader.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (t *targetTransport) revoke() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
}

func (t *targetTransport) authorize(operation domain.TargetOperation) error {
	spec := operation.Spec()
	if spec.LeaseID != t.authority.LeaseID || spec.TargetID != t.authority.TargetID || spec.TargetGeneration != t.authority.Generation || spec.TargetRunID != t.authority.RunID {
		return domain.NewError(domain.CodeForbidden, "linux_target.transport.authorize", "operation", "operation is outside this run's lease, target, or generation", nil)
	}
	return nil
}

func (t *targetTransport) requireOpen() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return io.ErrClosedPipe
	}
	return nil
}

func (t *targetTransport) recordLifecycle(kind string, operationID domain.TargetOperationID, payload any) error {
	// Unit-level transports that are deliberately detached from a driver have
	// no run ledger. Production transports are always created by OpenTransport.
	if t.driver == nil {
		return nil
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return domain.NewError(domain.CodeInternal, "linux_target.transport.evidence", "payload", "could not encode lifecycle evidence", err)
	}
	return t.driver.recordLifecycleObservation(t.authority.RunID, ports.TargetRunObservation{
		Kind: kind, ObservedAt: t.driver.now().UTC(), TargetOperationID: operationID, Payload: encoded,
	})
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
	alive  func() error
}

type verifiedPull struct {
	reader  *bytes.Reader
	digest  domain.Digest
	size    int64
	mu      sync.Mutex
	closed  bool
	onClose func()
}

func (r *verifiedPull) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	return r.reader.Read(buffer)
}

func (r *verifiedPull) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	onClose := r.onClose
	r.onClose = nil
	r.mu.Unlock()
	if onClose != nil {
		onClose()
	}
	return nil
}

func (r *verifiedPull) Digest() domain.Digest { return r.digest }
func (r *verifiedPull) Size() int64           { return r.size }

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.alive != nil {
		if err := r.alive(); err != nil {
			return 0, err
		}
	}
	return r.reader.Read(buffer)
}

var _ ports.TargetTransport = (*targetTransport)(nil)
var _ ports.ContentReader = (*verifiedPull)(nil)
