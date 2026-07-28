// Package directory implements a copy-backed workspace driver. Each workspace
// is rooted at <root>/<workspace-id>, while only its merged subdirectory is
// exposed to an agent.
package directory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type Config struct {
	Root string
	Now  func() time.Time
}

type Driver struct {
	root string
	now  func() time.Time

	gate     chan struct{}
	mu       sync.Mutex
	records  map[domain.WorkspaceID]diskRecord
	requests map[string]requestRecord
}

type requestRecord struct {
	workspaceID domain.WorkspaceID
	planDigest  string
}

func New(config Config) (*Driver, error) {
	root, err := prepareRoot(config.Root)
	if err != nil {
		return nil, err
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	driver := &Driver{
		root: root, now: config.Now, gate: make(chan struct{}, 1),
		records: make(map[domain.WorkspaceID]diskRecord), requests: make(map[string]requestRecord),
	}
	driver.gate <- struct{}{}
	if err := driver.loadRecords(); err != nil {
		return nil, err
	}
	return driver, nil
}

func NewDriver(config Config) (*Driver, error) { return New(config) }

func (d *Driver) Prepare(ctx context.Context, plan ports.WorkspacePlan) (ports.WorkspaceHandle, error) {
	const operation = "workspace.directory.prepare"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if plan.Construction != domain.InputViewAllowCopy {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeCapabilityUnavailable, operation, "construction", "copy-backed workspaces cannot satisfy a reflink requirement", nil)
	}
	if err := validatePlanCapacity(plan); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	planDigest, err := workspacePlanDigest(plan)
	if err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeInternal, operation, "plan", "could not derive an idempotency identity", err)
	}
	release, err := d.acquire(ctx, operation)
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	defer release()

	workspaceID := plan.Workspace.ID()
	if prior, found := d.requests[plan.IdempotencyKey]; found {
		if prior.workspaceID != workspaceID || prior.planDigest != planDigest {
			return ports.WorkspaceHandle{}, idempotencyConflict(operation)
		}
		record, found := d.records[workspaceID]
		if !found {
			return ports.WorkspaceHandle{}, domain.NewError(domain.CodeConflict, operation, "idempotency_key", "refers to a workspace that is no longer present", nil)
		}
		return d.inspectHandle(ctx, workspaceID, record, operation)
	}
	if _, found := d.records[workspaceID]; found {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeAlreadyExists, operation, "workspace_id", "already exists under another request", nil)
	}
	if existing, loadErr := d.loadRecordIfPresent(workspaceID); loadErr != nil {
		return ports.WorkspaceHandle{}, loadErr
	} else if existing != nil {
		if err := d.registerRecord(workspaceID, *existing, operation); err != nil {
			return ports.WorkspaceHandle{}, err
		}
		if existing.IdempotencyKey == plan.IdempotencyKey && existing.PlanDigest == planDigest {
			return d.inspectHandle(ctx, workspaceID, *existing, operation)
		}
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeAlreadyExists, operation, "workspace_id", "already exists under another request", nil)
	}

	staging, err := os.MkdirTemp(d.root, "."+workspaceID.String()+".prepare-")
	if err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "workspace", "could not create a private staging directory", err)
	}
	keepStaging := false
	defer func() {
		if !keepStaging {
			cleanupManagedDirectory(d.root, staging)
		}
	}()
	if err := os.Chmod(staging, 0o700); err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "workspace", "could not restrict the staging directory", err)
	}
	merged := filepath.Join(staging, mergedDirectory)
	if err := os.Mkdir(merged, 0o700); err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "merged", "could not create the merged directory", err)
	}
	if err := materialize(ctx, merged, plan); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	now := d.now().UTC()
	baseline, usage, err := scanManaged(ctx, merged, plan.UpperByteLimit, plan.UpperInodeLimit, now)
	if err != nil {
		return ports.WorkspaceHandle{}, classifyScanError(operation, err)
	}
	if err := verifyBaseline(plan.InputView, baseline); err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeIntegrityViolation, operation, "baseline", "materialized files do not exactly match the input-view manifest", err)
	}
	record := diskRecord{
		SchemaVersion: recordSchemaVersion,
		WorkspaceID:   workspaceID.String(), IdempotencyKey: plan.IdempotencyKey, PlanDigest: planDigest,
		InputViewID: plan.InputView.ID().String(), State: domain.WorkspaceReady,
		UpperByteLimit: plan.UpperByteLimit, UpperInodeLimit: plan.UpperInodeLimit,
		ObservedAt: now, Baseline: baseline,
	}
	if err := writeDiskRecord(staging, record); err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "metadata", "could not persist workspace authority", err)
	}
	if err := contextError(ctx, operation); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	workspacePath := d.workspacePath(workspaceID)
	if err := publishDirectory(ctx, staging, workspacePath); err != nil {
		if winner, loadErr := d.loadRecordIfPresent(workspaceID); loadErr == nil && winner != nil {
			if registerErr := d.registerRecord(workspaceID, *winner, operation); registerErr != nil {
				return ports.WorkspaceHandle{}, registerErr
			}
			if winner.IdempotencyKey == plan.IdempotencyKey && winner.PlanDigest == planDigest {
				return d.inspectHandle(ctx, workspaceID, *winner, operation)
			}
			return ports.WorkspaceHandle{}, domain.NewError(domain.CodeConflict, operation, "workspace_id", "was concurrently prepared with a different plan", err)
		}
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "workspace", "could not publish the prepared workspace: "+err.Error(), err)
	}
	keepStaging = true
	if err := syncDirectory(d.root); err != nil {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "workspace", "workspace was published but its parent could not be synchronized", err)
	}
	if err := d.registerRecord(workspaceID, record, operation); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	return handleFor(workspaceID, record, workspacePath, usage, now), nil
}

func (d *Driver) Mount(ctx context.Context, workspaceID domain.WorkspaceID) (ports.WorkspaceHandle, error) {
	const operation = "workspace.directory.mount"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	release, err := d.acquire(ctx, operation)
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	defer release()
	record, err := d.requireRecord(workspaceID, operation)
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	if record.State != domain.WorkspaceReady && record.State != domain.WorkspaceMounted {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeFailedPrecondition, operation, "state", "workspace cannot be mounted from its current state", nil)
	}
	manifest, usage, err := d.scanRecord(ctx, record)
	if err != nil {
		return ports.WorkspaceHandle{}, classifyScanError(operation, err)
	}
	if err := requireUnchanged(record.Baseline, manifest); err != nil && record.State == domain.WorkspaceReady {
		return ports.WorkspaceHandle{}, domain.NewError(domain.CodeIntegrityViolation, operation, "baseline", "workspace changed before it was mounted", err)
	}
	now := d.now().UTC()
	if record.State == domain.WorkspaceReady {
		updated := record
		updated.State = domain.WorkspaceMounted
		updated.ObservedAt = now
		if err := d.persist(workspaceID, updated); err != nil {
			return ports.WorkspaceHandle{}, domain.NewError(domain.CodeUnavailable, operation, "metadata", "could not persist the mounted state", err)
		}
		record = updated
	}
	return handleFor(workspaceID, record, d.workspacePath(workspaceID), usage, now), nil
}

func (d *Driver) Inspect(ctx context.Context, workspaceID domain.WorkspaceID) (ports.WorkspaceHandle, error) {
	const operation = "workspace.directory.inspect"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	release, err := d.acquire(ctx, operation)
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	defer release()
	record, err := d.requireRecord(workspaceID, operation)
	if err != nil {
		return ports.WorkspaceHandle{}, err
	}
	return d.inspectHandle(ctx, workspaceID, record, operation)
}

func (d *Driver) Preview(ctx context.Context, workspaceID domain.WorkspaceID) (ports.WorkspacePreviewResult, error) {
	const operation = "workspace.directory.preview"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	release, err := d.acquire(ctx, operation)
	if err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	defer release()
	record, err := d.requireRecord(workspaceID, operation)
	if err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	if record.State == domain.WorkspaceSealed {
		if record.Seal == nil {
			return ports.WorkspacePreviewResult{}, domain.NewError(domain.CodeIntegrityViolation, operation, "seal", "sealed workspace has no authoritative seal", nil)
		}
		result, resultErr := sealResult(*record.Seal)
		if resultErr != nil {
			return ports.WorkspacePreviewResult{}, resultErr
		}
		return ports.WorkspacePreviewResult{ChangeSet: result.ChangeSet, ObservedAt: result.SealedAt}, nil
	}
	if record.State != domain.WorkspaceMounted {
		return ports.WorkspacePreviewResult{}, domain.NewError(domain.CodeFailedPrecondition, operation, "state", "workspace must be mounted before preview", nil)
	}
	now := d.now().UTC()
	current, _, err := d.scanRecordAt(ctx, record, now)
	if err != nil {
		return ports.WorkspacePreviewResult{}, classifyScanError(operation, err)
	}
	if record.Preview != nil && requireUnchanged(record.Preview.Manifest, current) == nil {
		return previewResult(*record.Preview)
	}
	changes, err := changesBetween(record.Baseline, current)
	if err != nil {
		return ports.WorkspacePreviewResult{}, domain.NewError(domain.CodeIntegrityViolation, operation, "changes", "could not derive an authoritative change set", err)
	}
	revision := domain.InitialRevision
	if record.Preview != nil {
		if uint64(record.Preview.Revision) == ^uint64(0) {
			return ports.WorkspacePreviewResult{}, domain.NewError(domain.CodeResourceExhausted, operation, "revision", "workspace preview revision is exhausted", nil)
		}
		revision = record.Preview.Revision + 1
	}
	preview := diskPreview{Revision: revision, ObservedAt: now, Manifest: current, Changes: diskChanges(changes)}
	updated := record
	updated.Preview = &preview
	updated.ObservedAt = now
	if err := contextError(ctx, operation); err != nil {
		return ports.WorkspacePreviewResult{}, err
	}
	if err := d.persist(workspaceID, updated); err != nil {
		return ports.WorkspacePreviewResult{}, domain.NewError(domain.CodeUnavailable, operation, "metadata", "could not persist the optimistic preview", err)
	}
	return previewResult(preview)
}

func (d *Driver) Seal(ctx context.Context, workspaceID domain.WorkspaceID, revision domain.Revision) (ports.WorkspaceSealResult, error) {
	const operation = "workspace.directory.seal"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	if !revision.IsValid() {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeInvalidArgument, operation, "revision", "must be positive", nil)
	}
	release, err := d.acquire(ctx, operation)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	defer release()
	record, err := d.requireRecord(workspaceID, operation)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	if record.State == domain.WorkspaceSealed {
		if record.Seal == nil || record.Seal.Revision != revision {
			return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeConflict, operation, "revision", "workspace was sealed at another revision", nil)
		}
		result, err := sealResult(*record.Seal)
		if err != nil {
			return ports.WorkspaceSealResult{}, err
		}
		immutablePath, err := d.ensureSnapshot(ctx, workspaceID, record, record.Seal.Manifest)
		if err != nil {
			return ports.WorkspaceSealResult{}, err
		}
		result.ImmutablePath = immutablePath
		return result, nil
	}
	if record.State != domain.WorkspaceMounted {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeFailedPrecondition, operation, "state", "workspace must be mounted before sealing", nil)
	}
	if record.Preview == nil {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeFailedPrecondition, operation, "revision", "workspace must be previewed before sealing", nil)
	}
	if record.Preview.Revision != revision {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeConflict, operation, "revision", "does not match the latest workspace preview", nil)
	}
	now := d.now().UTC()
	current, _, err := d.scanRecordAt(ctx, record, now)
	if err != nil {
		return ports.WorkspaceSealResult{}, classifyScanError(operation, err)
	}
	if err := requireUnchanged(record.Preview.Manifest, current); err != nil {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeConflict, operation, "revision", "workspace changed after the requested preview", err)
	}
	changes, err := domainChanges(record.Preview.Changes)
	if err != nil {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeIntegrityViolation, operation, "changes", "persisted preview changes are invalid", err)
	}
	changeSet, err := domain.NewChangeSet(domain.ChangeScopeAgentWorkspace, changes, revision, now)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	immutablePath, err := d.ensureSnapshot(ctx, workspaceID, record, current)
	if err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	seal := diskSeal{Revision: revision, SealedAt: now, Manifest: current, Changes: diskChanges(changes)}
	updated := record
	updated.State = domain.WorkspaceSealed
	updated.ObservedAt = now
	updated.Seal = &seal
	if err := contextError(ctx, operation); err != nil {
		return ports.WorkspaceSealResult{}, err
	}
	if err := d.persist(workspaceID, updated); err != nil {
		return ports.WorkspaceSealResult{}, domain.NewError(domain.CodeUnavailable, operation, "metadata", "could not persist the sealed state", err)
	}
	return ports.WorkspaceSealResult{ChangeSet: changeSet, SealedAt: now, ImmutablePath: immutablePath}, nil
}

func (d *Driver) Release(ctx context.Context, workspaceID domain.WorkspaceID) error {
	const operation = "workspace.directory.release"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return err
	}
	release, err := d.acquire(ctx, operation)
	if err != nil {
		return err
	}
	defer release()
	if workspaceID.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "workspace_id", "must be set", nil)
	}
	record, found := d.records[workspaceID]
	if !found {
		if _, statErr := os.Lstat(d.workspacePath(workspaceID)); os.IsNotExist(statErr) {
			return nil
		}
		loaded, loadErr := d.loadRecordIfPresent(workspaceID)
		if loadErr != nil {
			return loadErr
		}
		if loaded == nil {
			return nil
		}
		record = *loaded
		if err := d.registerRecord(workspaceID, record, operation); err != nil {
			return err
		}
	}
	if record.State != domain.WorkspaceSealed && record.State != domain.WorkspaceReleased {
		return domain.NewError(domain.CodeFailedPrecondition, operation, "state", "workspace must be sealed before release", nil)
	}
	workspacePath := d.workspacePath(workspaceID)
	if err := requireExactWorkspaceDirectory(d.root, workspacePath, workspaceID); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "workspace", "workspace path failed containment validation", err)
	}
	if record.State == domain.WorkspaceSealed {
		updated := record
		updated.State = domain.WorkspaceReleased
		updated.ObservedAt = d.now().UTC()
		if err := d.persist(workspaceID, updated); err != nil {
			return domain.NewError(domain.CodeUnavailable, operation, "metadata", "could not persist the released state", err)
		}
		record = updated
	}
	if err := removeWorkspaceTree(ctx, workspacePath); err != nil {
		if contextErr := contextError(ctx, operation); contextErr != nil {
			return contextErr
		}
		return domain.NewError(domain.CodeUnavailable, operation, "workspace", "could not remove the workspace directory", err)
	}
	d.mu.Lock()
	delete(d.records, workspaceID)
	d.mu.Unlock()
	if err := syncDirectory(d.root); err != nil {
		return domain.NewError(domain.CodeUnavailable, operation, "workspace", "workspace was removed but its parent could not be synchronized", err)
	}
	return nil
}

func (d *Driver) inspectHandle(ctx context.Context, workspaceID domain.WorkspaceID, record diskRecord, operation string) (ports.WorkspaceHandle, error) {
	if err := requireRecordIdentity(workspaceID, record, operation); err != nil {
		return ports.WorkspaceHandle{}, err
	}
	manifest, usage, err := d.scanRecord(ctx, record)
	if err != nil {
		return ports.WorkspaceHandle{}, classifyScanError(operation, err)
	}
	if record.Seal != nil {
		if err := requireUnchanged(record.Seal.Manifest, manifest); err != nil {
			return ports.WorkspaceHandle{}, domain.NewError(domain.CodeIntegrityViolation, operation, "sealed_manifest", "sealed workspace content changed", err)
		}
	} else if record.State == domain.WorkspaceReady {
		if err := requireUnchanged(record.Baseline, manifest); err != nil {
			return ports.WorkspaceHandle{}, domain.NewError(domain.CodeIntegrityViolation, operation, "baseline", "ready workspace content changed before mount", err)
		}
	}
	return handleFor(workspaceID, record, d.workspacePath(workspaceID), usage, d.now().UTC()), nil
}

func (d *Driver) scanRecord(ctx context.Context, record diskRecord) (workspaceManifest ManifestAlias, usage workspaceUsage, err error) {
	return d.scanRecordAt(ctx, record, d.now().UTC())
}

func (d *Driver) scanRecordAt(ctx context.Context, record diskRecord, at time.Time) (ManifestAlias, workspaceUsage, error) {
	workspaceID, err := domain.ParseWorkspaceID(record.WorkspaceID)
	if err != nil {
		return ManifestAlias{}, workspaceUsage{}, err
	}
	workspacePath := d.workspacePath(workspaceID)
	if err := requireExactWorkspaceDirectory(d.root, workspacePath, workspaceID); err != nil {
		return ManifestAlias{}, workspaceUsage{}, err
	}
	merged := filepath.Join(workspacePath, mergedDirectory)
	if err := requireManagedDirectory(workspacePath, merged); err != nil {
		return ManifestAlias{}, workspaceUsage{}, err
	}
	return scanManaged(ctx, merged, record.UpperByteLimit, record.UpperInodeLimit, at)
}

func (d *Driver) acquire(ctx context.Context, operation string) (func(), error) {
	select {
	case <-ctx.Done():
		return nil, contextError(ctx, operation)
	case <-d.gate:
		return func() { d.gate <- struct{}{} }, nil
	}
}

func (d *Driver) requireRecord(workspaceID domain.WorkspaceID, operation string) (diskRecord, error) {
	if workspaceID.IsZero() {
		return diskRecord{}, domain.NewError(domain.CodeInvalidArgument, operation, "workspace_id", "must be set", nil)
	}
	d.mu.Lock()
	record, found := d.records[workspaceID]
	d.mu.Unlock()
	if found {
		return record, nil
	}
	loaded, err := d.loadRecordIfPresent(workspaceID)
	if err != nil {
		return diskRecord{}, err
	}
	if loaded == nil {
		return diskRecord{}, domain.NewError(domain.CodeNotFound, operation, "workspace_id", "not found", nil)
	}
	if err := d.registerRecord(workspaceID, *loaded, operation); err != nil {
		return diskRecord{}, err
	}
	return *loaded, nil
}

func (d *Driver) persist(workspaceID domain.WorkspaceID, record diskRecord) error {
	if err := requireRecordIdentity(workspaceID, record, "workspace.directory.persist"); err != nil {
		return err
	}
	workspacePath := d.workspacePath(workspaceID)
	if err := requireExactWorkspaceDirectory(d.root, workspacePath, workspaceID); err != nil {
		return err
	}
	if err := writeDiskRecord(workspacePath, record); err != nil {
		return err
	}
	d.mu.Lock()
	d.records[workspaceID] = record
	d.mu.Unlock()
	return nil
}

func (d *Driver) registerRecord(workspaceID domain.WorkspaceID, record diskRecord, operation string) error {
	if err := requireRecordIdentity(workspaceID, record, operation); err != nil {
		return err
	}
	d.mu.Lock()
	d.records[workspaceID] = record
	d.requests[record.IdempotencyKey] = requestRecord{workspaceID: workspaceID, planDigest: record.PlanDigest}
	d.mu.Unlock()
	return nil
}

func (d *Driver) workspacePath(workspaceID domain.WorkspaceID) string {
	return filepath.Join(d.root, workspaceID.String())
}

func handleFor(workspaceID domain.WorkspaceID, record diskRecord, workspacePath string, usage workspaceUsage, observedAt time.Time) ports.WorkspaceHandle {
	return ports.WorkspaceHandle{
		WorkspaceID: workspaceID, State: record.State,
		MergedPath:    filepath.Join(workspacePath, mergedDirectory),
		PhysicalBytes: usage.bytes, Inodes: usage.inodes, ObservedAt: observedAt,
	}
}

func idempotencyConflict(operation string) error {
	return domain.NewError(domain.CodeConflict, operation, "idempotency_key", "was already used with a different workspace plan", nil)
}

func contextError(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		code := domain.CodeUnavailable
		if errors.Is(err, context.DeadlineExceeded) {
			code = domain.CodeDeadlineExceeded
		}
		return domain.NewError(code, operation, "context", "operation context is not active", err)
	}
	return nil
}

func requireRecordIdentity(workspaceID domain.WorkspaceID, record diskRecord, operation string) error {
	if workspaceID.IsZero() || record.WorkspaceID != workspaceID.String() {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "workspace_id", "persisted authority does not match the requested workspace", nil)
	}
	return nil
}

var _ ports.WorkspaceDriver = (*Driver)(nil)
