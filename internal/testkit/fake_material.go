package testkit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type registeredOutput struct {
	content     []byte
	sensitivity domain.Sensitivity
}

// FakeMaterialAuthority is an in-memory authority that still enforces digest
// identity, immutable readers, output declarations, and idempotent publication.
type FakeMaterialAuthority struct {
	mu             sync.Mutex
	faults         *FaultInjector
	tracker        *OwnershipTracker
	contents       map[string][]byte
	outputs        map[string]registeredOutput
	outputRequests map[string]string
	outputResults  map[string][]domain.ArtifactReference
	bundleRequests map[string]string
	bundleResults  map[string]domain.ArtifactReference
	readerNo       uint64
}

func NewFakeMaterialAuthority(faults *FaultInjector, tracker *OwnershipTracker) *FakeMaterialAuthority {
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeMaterialAuthority{
		faults: faults, tracker: tracker, contents: make(map[string][]byte), outputs: make(map[string]registeredOutput),
		outputRequests: make(map[string]string), outputResults: make(map[string][]domain.ArtifactReference),
		bundleRequests: make(map[string]string), bundleResults: make(map[string]domain.ArtifactReference),
	}
}

func (a *FakeMaterialAuthority) RegisterContent(reference string, content []byte) (ports.ArtifactOccurrence, error) {
	if reference == "" {
		return ports.ArtifactOccurrence{}, domain.NewError(domain.CodeInvalidArgument, "fake_material.register_content", "reference", "must not be blank", nil)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	owned := append([]byte(nil), content...)
	a.contents[reference] = owned
	return ports.ArtifactOccurrence{Reference: reference, Digest: domain.NewDigest(owned), Size: int64(len(owned))}, nil
}

func (a *FakeMaterialAuthority) RegisterOutput(workspaceID domain.WorkspaceID, relativePath string, content []byte, sensitivity domain.Sensitivity) error {
	if workspaceID.IsZero() || relativePath == "" || !sensitivity.IsValid() {
		return domain.NewError(domain.CodeInvalidArgument, "fake_material.register_output", "output", "workspace, path, and sensitivity are required", nil)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.outputs[outputKey(workspaceID, relativePath)] = registeredOutput{content: append([]byte(nil), content...), sensitivity: sensitivity}
	return nil
}

func (a *FakeMaterialAuthority) ResolveOccurrence(ctx context.Context, securityScope, reference string) (ports.ArtifactOccurrence, error) {
	if err := ports.RequireDeadline(ctx, "fake_material.resolve_occurrence"); err != nil {
		return ports.ArtifactOccurrence{}, err
	}
	if strings.TrimSpace(securityScope) == "" || strings.TrimSpace(reference) == "" {
		return ports.ArtifactOccurrence{}, domain.NewError(domain.CodeInvalidArgument, "fake_material.resolve_occurrence", "reference", "security scope and reference are required", nil)
	}
	if err := a.faults.Check("material.resolve_occurrence"); err != nil {
		return ports.ArtifactOccurrence{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	content, found := a.contents[reference]
	if !found {
		return ports.ArtifactOccurrence{}, domain.NewError(domain.CodeNotFound, "fake_material.resolve_occurrence", "reference", "content is not registered", nil)
	}
	return ports.ArtifactOccurrence{Reference: reference, Digest: domain.NewDigest(content), Size: int64(len(content))}, nil
}

func (a *FakeMaterialAuthority) ResolveInputView(ctx context.Context, plan ports.InputPlan) (domain.InputViewManifest, error) {
	if err := ports.RequireDeadline(ctx, "fake_material.resolve_input_view"); err != nil {
		return domain.InputViewManifest{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.InputViewManifest{}, err
	}
	if err := a.faults.Check("material.resolve_input_view"); err != nil {
		return domain.InputViewManifest{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	entries := make([]domain.InputViewEntry, 0, len(plan.Entries))
	for index, input := range plan.Entries {
		content, found := a.contents[input.Occurrence.Reference]
		if !found {
			return domain.InputViewManifest{}, domain.NewError(domain.CodeNotFound, "fake_material.resolve_input_view", fmt.Sprintf("entries[%d].occurrence", index), "content is not registered", nil)
		}
		if domain.NewDigest(content) != input.Occurrence.Digest || int64(len(content)) != input.Occurrence.Size {
			return domain.InputViewManifest{}, domain.NewError(domain.CodeIntegrityViolation, "fake_material.resolve_input_view", fmt.Sprintf("entries[%d].occurrence", index), "content identity changed", nil)
		}
		entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
			LogicalPath: input.LogicalPath, OccurrenceRef: input.Occurrence.Reference,
			Digest: input.Occurrence.Digest, Size: input.Occurrence.Size, Mode: input.Mode,
			PermittedSidecars: append([]string(nil), input.PermittedSidecars...),
		})
		if err != nil {
			return domain.InputViewManifest{}, err
		}
		entries = append(entries, entry)
	}
	return domain.NewInputViewManifest(entries)
}

func (a *FakeMaterialAuthority) OpenContent(ctx context.Context, occurrence ports.ArtifactOccurrence) (ports.ContentReader, error) {
	if err := ports.RequireDeadline(ctx, "fake_material.open_content"); err != nil {
		return nil, err
	}
	if err := occurrence.Validate(); err != nil {
		return nil, err
	}
	if err := a.faults.Check("material.open_content"); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	content, found := a.contents[occurrence.Reference]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "fake_material.open_content", "reference", "content is not registered", nil)
	}
	if domain.NewDigest(content) != occurrence.Digest || int64(len(content)) != occurrence.Size {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "fake_material.open_content", "occurrence", "does not identify the registered bytes", nil)
	}
	a.readerNo++
	id := fmt.Sprintf("%s/%d", occurrence.Reference, a.readerNo)
	if err := a.tracker.Acquire("content_reader", id, occurrence.Reference); err != nil {
		return nil, err
	}
	return newMemoryContentReader(content, occurrence.Digest, func() { _ = a.tracker.Release("content_reader", id, occurrence.Reference) }), nil
}

func (a *FakeMaterialAuthority) CaptureOutputs(ctx context.Context, plan ports.OutputPlan) ([]domain.ArtifactReference, error) {
	if err := ports.RequireDeadline(ctx, "fake_material.capture_outputs"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	if err := a.faults.Check("material.capture_outputs.before"); err != nil {
		return nil, err
	}
	signature := outputPlanSignature(plan)
	a.mu.Lock()
	if previous, found := a.outputRequests[plan.IdempotencyKey]; found {
		existing := append([]domain.ArtifactReference(nil), a.outputResults[plan.IdempotencyKey]...)
		a.mu.Unlock()
		if previous != signature {
			return nil, idempotencyConflict("fake_material.capture_outputs")
		}
		return existing, nil
	}
	a.mu.Unlock()
	captured := make(map[string][]byte, len(plan.Content))
	for path, source := range plan.Content {
		content, err := readContentSource(ctx, source)
		if err != nil {
			return nil, err
		}
		captured[path] = content
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if previous, found := a.outputRequests[plan.IdempotencyKey]; found {
		if previous != signature {
			return nil, idempotencyConflict("fake_material.capture_outputs")
		}
		return append([]domain.ArtifactReference(nil), a.outputResults[plan.IdempotencyKey]...), nil
	}
	artifacts := make([]domain.ArtifactReference, 0)
	for _, selection := range plan.Selections {
		spec := selection.Spec()
		registered, found := a.outputs[outputKey(plan.WorkspaceID, spec.RelativePath)]
		if !found && len(spec.Roles) == 1 && spec.Roles[0] == "workspace-change-manifest" && strings.HasPrefix(spec.RelativePath, ".world/change-manifest") {
			registered = registeredOutput{content: captured[spec.RelativePath], sensitivity: domain.SensitivityInternal}
			found = true
		}
		if !found {
			return nil, domain.NewError(domain.CodeNotFound, "fake_material.capture_outputs", "selection", "declared output is not registered", nil)
		}
		if !bytes.Equal(captured[spec.RelativePath], registered.content) {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "fake_material.capture_outputs", "content", "published content does not match the sealed workspace output", nil)
		}
		for _, role := range spec.Roles {
			artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
				Reference: fmt.Sprintf("memory://outputs/%s/%s#%s", plan.WorkspaceID, spec.RelativePath, role),
				Digest:    domain.NewDigest(registered.content), Size: int64(len(registered.content)), Role: role,
				Sensitivity: registered.sensitivity,
			})
			if err != nil {
				return nil, err
			}
			artifacts = append(artifacts, artifact)
		}
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Spec().Reference < artifacts[j].Spec().Reference })
	a.outputRequests[plan.IdempotencyKey] = signature
	a.outputResults[plan.IdempotencyKey] = append([]domain.ArtifactReference(nil), artifacts...)
	if err := a.faults.Check("material.capture_outputs.after"); err != nil {
		return nil, err
	}
	return artifacts, nil
}

func (a *FakeMaterialAuthority) CaptureObservationBundle(ctx context.Context, plan ports.ObservationBundlePlan) (domain.ArtifactReference, error) {
	if err := ports.RequireDeadline(ctx, "fake_material.capture_bundle"); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := a.faults.Check("material.capture_bundle.before"); err != nil {
		return domain.ArtifactReference{}, err
	}
	signature := fmt.Sprintf("%s/%s/%d", plan.Bundle.ID(), plan.Content.Digest(), plan.Content.Size())
	a.mu.Lock()
	if previous, found := a.bundleRequests[plan.IdempotencyKey]; found {
		existing := a.bundleResults[plan.IdempotencyKey]
		a.mu.Unlock()
		if previous != signature {
			return domain.ArtifactReference{}, idempotencyConflict("fake_material.capture_bundle")
		}
		return existing, nil
	}
	a.mu.Unlock()
	if _, err := readContentSource(ctx, plan.Content); err != nil {
		return domain.ArtifactReference{}, err
	}
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "memory://observation-bundles/" + plan.Bundle.ID().String() + "/sealed.json",
		Digest:    plan.Content.Digest(), Size: plan.Content.Size(), Role: "observation-bundle", Sensitivity: domain.SensitivityRestricted,
	})
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	a.mu.Lock()
	if previous, found := a.bundleRequests[plan.IdempotencyKey]; found {
		existing := a.bundleResults[plan.IdempotencyKey]
		a.mu.Unlock()
		if previous != signature {
			return domain.ArtifactReference{}, idempotencyConflict("fake_material.capture_bundle")
		}
		return existing, nil
	}
	a.bundleRequests[plan.IdempotencyKey], a.bundleResults[plan.IdempotencyKey] = signature, artifact
	a.mu.Unlock()
	if err := a.faults.Check("material.capture_bundle.after"); err != nil {
		return domain.ArtifactReference{}, err
	}
	return artifact, nil
}

type memoryContentReader struct {
	mu      sync.Mutex
	reader  *bytes.Reader
	digest  domain.Digest
	size    int64
	closed  bool
	closeFn func()
}

func newMemoryContentReader(content []byte, digest domain.Digest, closeFn func()) *memoryContentReader {
	owned := append([]byte(nil), content...)
	return &memoryContentReader{reader: bytes.NewReader(owned), digest: digest, size: int64(len(owned)), closeFn: closeFn}
}

func (r *memoryContentReader) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	return r.reader.Read(buffer)
}

func (r *memoryContentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		if r.closeFn != nil {
			r.closeFn()
		}
	}
	return nil
}

func (r *memoryContentReader) Digest() domain.Digest { return r.digest }
func (r *memoryContentReader) Size() int64           { return r.size }

func outputKey(workspaceID domain.WorkspaceID, relativePath string) string {
	return workspaceID.String() + "\x00" + relativePath
}

func outputPlanSignature(plan ports.OutputPlan) string {
	parts := []string{plan.LeaseID.String(), plan.WorkspaceID.String(), plan.AgentWorkspaceID.String(), fmt.Sprint(plan.AgentGeneration)}
	for _, selection := range plan.Selections {
		spec := selection.Spec()
		source := plan.Content[spec.RelativePath]
		parts = append(parts, fmt.Sprintf("%s:%s:%s:%d", spec.RelativePath, strings.Join(spec.Roles, ","), source.Digest(), source.Size()))
	}
	provenanceKeys := make([]string, 0, len(plan.Provenance))
	for key := range plan.Provenance {
		provenanceKeys = append(provenanceKeys, key)
	}
	sort.Strings(provenanceKeys)
	for _, key := range provenanceKeys {
		parts = append(parts, key+"="+plan.Provenance[key])
	}
	return strings.Join(parts, "/")
}

type memoryContentSource struct {
	content []byte
	digest  domain.Digest
}

func NewMemoryContentSource(content []byte) ports.ContentSource {
	owned := append([]byte(nil), content...)
	return memoryContentSource{content: owned, digest: domain.NewDigest(owned)}
}

func (s memoryContentSource) Digest() domain.Digest { return s.digest }
func (s memoryContentSource) Size() int64           { return int64(len(s.content)) }
func (s memoryContentSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(s.content)), nil
}

func readContentSource(ctx context.Context, source ports.ContentSource) ([]byte, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(reader, source.Size()+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || int64(len(content)) != source.Size() || domain.NewDigest(content) != source.Digest() {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "fake_material.read_content", "content", "immutable content bytes do not match their identity", errors.Join(readErr, closeErr))
	}
	return content, nil
}

var _ ports.ContentReader = (*memoryContentReader)(nil)
var _ ports.ContentSource = memoryContentSource{}
var _ ports.MaterialAuthority = (*FakeMaterialAuthority)(nil)
