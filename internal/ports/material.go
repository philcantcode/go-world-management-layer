package ports

import (
	"context"
	"fmt"
	"io"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

// ContentSource reopens immutable bytes for retryable publication without
// exposing their physical location. Every opened stream must yield exactly the
// declared digest and size.
type ContentSource interface {
	Digest() domain.Digest
	Size() int64
	Open(context.Context) (io.ReadCloser, error)
}

type ArtifactOccurrence struct {
	Reference string
	Digest    domain.Digest
	Size      int64
}

func (o ArtifactOccurrence) Validate() error {
	if o.Reference == "" || o.Digest.IsZero() || o.Size < 0 {
		return domain.NewError(domain.CodeInvalidArgument, "ports.artifact_occurrence.validate", "occurrence", "reference, digest, and non-negative size are required", nil)
	}
	return nil
}

type InputEntryPlan struct {
	Occurrence        ArtifactOccurrence
	LogicalPath       string
	Mode              uint32
	PermittedSidecars []string
}

type InputPlan struct {
	SecurityScope string
	Entries       []InputEntryPlan
}

func (p InputPlan) Validate() error {
	const operation = "ports.input_plan.validate"
	if p.SecurityScope == "" || len(p.Entries) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "input", "security scope and entries are required", nil)
	}
	seen := make(map[string]struct{}, len(p.Entries))
	for index, entry := range p.Entries {
		if err := entry.Occurrence.Validate(); err != nil {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("entries[%d].occurrence", index), "is invalid", err)
		}
		if _, err := safepath.Normalize(entry.LogicalPath); err != nil {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("entries[%d].logical_path", index), "must be a safe logical relative path", err)
		}
		if entry.Mode > 0o777 {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("entries[%d].mode", index), "must contain only regular Unix permission bits", nil)
		}
		if _, duplicate := seen[entry.LogicalPath]; duplicate {
			return domain.NewError(domain.CodeConflict, operation, "entries", "contains a duplicate logical path", nil)
		}
		seen[entry.LogicalPath] = struct{}{}
		for sidecarIndex, sidecar := range entry.PermittedSidecars {
			if sidecar == "" {
				return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("entries[%d].permitted_sidecars[%d]", index, sidecarIndex), "must not be blank", nil)
			}
		}
	}
	return nil
}

type OutputPlan struct {
	IdempotencyKey   string
	LeaseID          domain.LeaseID
	WorkspaceID      domain.WorkspaceID
	AgentWorkspaceID domain.AgentWorkspaceID
	AgentGeneration  domain.AgentGeneration
	Selections       []domain.ExportSelection
	Content          map[string]ContentSource
	Provenance       map[string]string
}

func (p OutputPlan) Validate() error {
	const operation = "ports.output_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.LeaseID.IsZero() || p.WorkspaceID.IsZero() || p.AgentWorkspaceID.IsZero() || !p.AgentGeneration.IsValid() || len(p.Selections) == 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "scope", "lease, workspace, agent generation, and selections are required", nil)
	}
	paths := make(map[string]struct{}, len(p.Selections))
	for index, selection := range p.Selections {
		spec := selection.Spec()
		if spec.RelativePath == "" || len(spec.Roles) == 0 {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("selections[%d]", index), "is uninitialized", nil)
		}
		if _, duplicate := paths[spec.RelativePath]; duplicate {
			return domain.NewError(domain.CodeConflict, operation, "selections", "contains a duplicate logical path", nil)
		}
		paths[spec.RelativePath] = struct{}{}
		source, found := p.Content[spec.RelativePath]
		if !found || source == nil || source.Digest().IsZero() || source.Size() < 0 {
			return domain.NewError(domain.CodeInvalidArgument, operation, fmt.Sprintf("content[%q]", spec.RelativePath), "must provide immutable content with a valid identity", nil)
		}
	}
	if len(p.Content) != len(paths) {
		return domain.NewError(domain.CodeInvalidArgument, operation, "content", "must contain exactly one source for each selection", nil)
	}
	for key, value := range p.Provenance {
		if key == "" || value == "" {
			return domain.NewError(domain.CodeInvalidArgument, operation, "provenance", "must not contain blank keys or values", nil)
		}
	}
	return nil
}

type ObservationBundlePlan struct {
	IdempotencyKey string
	Bundle         domain.ObservationBundle
	Content        ContentSource
}

func (p ObservationBundlePlan) Validate() error {
	const operation = "ports.observation_bundle_plan.validate"
	if err := requireIdempotency(operation, p.IdempotencyKey); err != nil {
		return err
	}
	if p.Bundle.ID().IsZero() || p.Bundle.State() != domain.ObservationBundleSealed || p.Content == nil || p.Content.Digest().IsZero() || p.Content.Size() <= 0 {
		return domain.NewError(domain.CodeInvalidArgument, operation, "bundle", "a sealed bundle and non-empty immutable content are required", nil)
	}
	return nil
}

// MaterialAuthority is the only port allowed to stream immutable source bytes
// or publish world outputs. It never exposes repository paths.
type MaterialAuthority interface {
	ResolveOccurrence(context.Context, string, string) (ArtifactOccurrence, error)
	ResolveInputView(context.Context, InputPlan) (domain.InputViewManifest, error)
	OpenContent(context.Context, ArtifactOccurrence) (ContentReader, error)
	CaptureOutputs(context.Context, OutputPlan) ([]domain.ArtifactReference, error)
	CaptureObservationBundle(context.Context, ObservationBundlePlan) (domain.ArtifactReference, error)
}
