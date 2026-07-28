package ports

import (
	"context"
	"fmt"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

type WorkspacePlan struct {
	IdempotencyKey  string
	Workspace       domain.Workspace
	InputView       domain.InputViewManifest
	Content         map[string]ContentSource
	Construction    domain.InputViewConstruction
	UpperByteLimit  int64
	UpperInodeLimit int64
}

func (p WorkspacePlan) Validate() error {
	const operation = "ports.workspace_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.Workspace.ID().IsZero() || p.InputView.ID().IsZero() || !p.Construction.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, operation, "workspace", "workspace, input view, and construction are required", nil)
	}
	if p.Workspace.Spec().InputViewID != p.InputView.ID() {
		return domain.NewError(domain.CodeConflict, operation, "input_view", "does not match the workspace", nil)
	}
	if p.UpperByteLimit <= 0 || p.UpperInodeLimit <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "limits", "byte and inode limits must be positive", nil)
	}
	entries := p.InputView.Entries()
	if len(p.Content) != len(entries) {
		return domain.NewError(domain.CodeInvalidArgument, operation, "content", "must contain exactly one immutable source for each input-view entry", nil)
	}
	manifestPaths := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		spec := entry.Spec()
		normalized, err := safepath.Normalize(spec.LogicalPath)
		if err != nil || normalized != spec.LogicalPath {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("input_view.entries[%d].logical_path", index), "must be a safe normalized logical path", err)
		}
		manifestPaths[spec.LogicalPath] = struct{}{}
		source, found := p.Content[spec.LogicalPath]
		if !found || source == nil {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("content[%q]", spec.LogicalPath), "must provide immutable content", nil)
		}
		if source.Digest() != spec.Digest || source.Size() != spec.Size {
			return domain.NewError(domain.CodeIntegrityViolation, operation, fmt.Sprintf("content[%q]", spec.LogicalPath), "declared digest and size do not match the input-view manifest", nil)
		}
	}
	for logicalPath := range p.Content {
		normalized, err := safepath.Normalize(logicalPath)
		if err != nil || normalized != logicalPath {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("content[%q]", logicalPath), "key must be a safe normalized logical path", err)
		}
		if _, found := manifestPaths[logicalPath]; !found {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("content[%q]", logicalPath), "has no corresponding input-view entry", nil)
		}
	}
	return nil
}

type WorkspaceHandle struct {
	WorkspaceID   domain.WorkspaceID
	State         domain.WorkspaceState
	MergedPath    string
	PhysicalBytes int64
	Inodes        int64
	ObservedAt    time.Time
}

type WorkspaceSealResult struct {
	ChangeSet     domain.ChangeSet
	SealedAt      time.Time
	ImmutablePath string
}

// WorkspacePreviewResult is a stable, read-only workspace scan. Its revision
// is the optimistic token that Seal must receive; a changed workspace invalidates
// the token instead of allowing a caller to label arbitrary bytes with a chosen
// revision.
type WorkspacePreviewResult struct {
	ChangeSet  domain.ChangeSet
	ObservedAt time.Time
}

type WorkspaceDriver interface {
	Prepare(context.Context, WorkspacePlan) (WorkspaceHandle, error)
	Mount(context.Context, domain.WorkspaceID) (WorkspaceHandle, error)
	Inspect(context.Context, domain.WorkspaceID) (WorkspaceHandle, error)
	Preview(context.Context, domain.WorkspaceID) (WorkspacePreviewResult, error)
	Seal(context.Context, domain.WorkspaceID, domain.Revision) (WorkspaceSealResult, error)
	Release(context.Context, domain.WorkspaceID) error
}

type CacheContentPlan struct {
	SecurityScope string
	Occurrence    ArtifactOccurrence
	Reader        ContentReader
}

func (p CacheContentPlan) Validate() error {
	const operation = "ports.cache_content_plan.validate"
	if p.SecurityScope == "" || p.Reader == nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "content", "security scope and reader are required", nil)
	}
	if err := p.Occurrence.Validate(); err != nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "occurrence", "is invalid", err)
	}
	if p.Reader.Digest() != p.Occurrence.Digest || p.Reader.Size() != p.Occurrence.Size {
		return domain.NewError(domain.CodeIntegrityViolation, operation, "reader", "declared identity does not match the occurrence", nil)
	}
	return nil
}

type CachedContent struct {
	SecurityScope string
	Digest        domain.Digest
	Size          int64
	PhysicalBytes int64
	VerifiedAt    time.Time
}

type InputViewBuildPlan struct {
	SecurityScope string
	Manifest      domain.InputViewManifest
	Construction  domain.InputViewConstruction
}

func (p InputViewBuildPlan) Validate() error {
	if p.SecurityScope == "" || p.Manifest.ID().IsZero() || !p.Construction.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, "ports.input_view_build_plan.validate", "view", "security scope, manifest, and construction are required", nil)
	}
	return nil
}

type CachedInputView struct {
	SecurityScope string
	InputViewID   domain.InputViewID
	Construction  domain.InputViewConstruction
	PhysicalBytes int64
	ReadyAt       time.Time
}

type CachePin struct {
	SecurityScope string
	InputViewID   domain.InputViewID
	Owner         string
}

func (p CachePin) Validate() error {
	if p.SecurityScope == "" || p.InputViewID.IsZero() || p.Owner == "" {
		return domain.NewError(domain.CodeInvalidArgument, "ports.cache_pin.validate", "pin", "security scope, input view, and owner are required", nil)
	}
	return nil
}

type CacheReconciliation struct {
	PinsRebuilt       int
	EntriesRemoved    int
	IntegrityFailures int
	CompletedAt       time.Time
}

type InputCache interface {
	EnsureContent(context.Context, CacheContentPlan) (CachedContent, error)
	BuildView(context.Context, InputViewBuildPlan) (CachedInputView, error)
	Pin(context.Context, CachePin) error
	Unpin(context.Context, CachePin) error
	Reconcile(context.Context) (CacheReconciliation, error)
}
