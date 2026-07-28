package forensicartifacts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const testScope = "campaign-red"

type fakeBackend struct {
	resolve func(context.Context, ResolveOccurrenceRequest) (RepositoryOccurrence, error)
	open    func(context.Context, OpenObjectRequest) (OpenedObject, error)
	outputs func(context.Context, OutputCaptureRequest) ([]PublishedOutput, error)
	bundle  func(context.Context, BundleCaptureRequest) (PublishedBundle, error)
}

func (f *fakeBackend) ResolveOccurrence(ctx context.Context, request ResolveOccurrenceRequest) (RepositoryOccurrence, error) {
	if f.resolve == nil {
		return RepositoryOccurrence{}, errors.New("unexpected ResolveOccurrence")
	}
	return f.resolve(ctx, request)
}

func (f *fakeBackend) OpenObject(ctx context.Context, request OpenObjectRequest) (OpenedObject, error) {
	if f.open == nil {
		return OpenedObject{}, errors.New("unexpected OpenObject")
	}
	return f.open(ctx, request)
}

func (f *fakeBackend) CaptureOutputs(ctx context.Context, request OutputCaptureRequest) ([]PublishedOutput, error) {
	if f.outputs == nil {
		return nil, errors.New("unexpected CaptureOutputs")
	}
	return f.outputs(ctx, request)
}

func (f *fakeBackend) CaptureObservationBundle(ctx context.Context, request BundleCaptureRequest) (PublishedBundle, error) {
	if f.bundle == nil {
		return PublishedBundle{}, errors.New("unexpected CaptureObservationBundle")
	}
	return f.bundle(ctx, request)
}

func TestResolveInputViewBuildsCanonicalAuthorizedManifest(t *testing.T) {
	firstBytes, secondBytes := []byte("first"), []byte("second")
	first := ports.ArtifactOccurrence{Reference: "selection:first", Digest: domain.NewDigest(firstBytes), Size: int64(len(firstBytes))}
	second := ports.ArtifactOccurrence{Reference: "selection:second", Digest: domain.NewDigest(secondBytes), Size: int64(len(secondBytes))}
	resolved := map[string]RepositoryOccurrence{
		first.Reference:  {Reference: "artifact://occurrences/first", Digest: first.Digest, Size: first.Size, SecurityScope: testScope, Sidecars: []string{"provenance", "metadata"}},
		second.Reference: {Reference: "artifact://occurrences/second", Digest: second.Digest, Size: second.Size, SecurityScope: testScope},
	}
	var requests []ResolveOccurrenceRequest
	backend := &fakeBackend{resolve: func(ctx context.Context, request ResolveOccurrenceRequest) (RepositoryOccurrence, error) {
		if ctx.Value(authenticationKey{}) != "principal-1" {
			return RepositoryOccurrence{}, domain.NewError(domain.CodeUnauthorized, "repository.resolve", "principal", "denied", nil)
		}
		requests = append(requests, request)
		return resolved[request.Reference], nil
	}}
	authority := newTestAuthority(t, backend)
	plan := ports.InputPlan{SecurityScope: testScope, Entries: []ports.InputEntryPlan{
		{Occurrence: second, LogicalPath: "z/specimen.bin", Mode: 0o400},
		{Occurrence: first, LogicalPath: "a/fixture.bin", Mode: 0o440, PermittedSidecars: []string{"provenance", "metadata"}},
	}}
	manifest, err := authority.ResolveInputView(authorizedContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	entries := manifest.Entries()
	if len(entries) != 2 || entries[0].Spec().LogicalPath != "a/fixture.bin" || entries[1].Spec().LogicalPath != "z/specimen.bin" {
		t.Fatalf("manifest entries = %#v", entries)
	}
	if entries[0].Spec().OccurrenceRef != "artifact://occurrences/first" {
		t.Fatalf("qualified occurrence = %q", entries[0].Spec().OccurrenceRef)
	}
	if got := entries[0].Spec().PermittedSidecars; len(got) != 2 || got[0] != "metadata" || got[1] != "provenance" {
		t.Fatalf("canonical sidecars = %v", got)
	}
	if len(requests) != 2 || requests[0].SecurityScope != testScope || requests[0].Purpose != ResolveForInputView {
		t.Fatalf("repository requests = %#v", requests)
	}
	reversed := plan
	reversed.Entries = append([]ports.InputEntryPlan(nil), plan.Entries...)
	reversed.Entries[0], reversed.Entries[1] = reversed.Entries[1], reversed.Entries[0]
	secondManifest, err := authority.ResolveInputView(authorizedContext(t), reversed)
	if err != nil {
		t.Fatal(err)
	}
	if secondManifest.ID() != manifest.ID() {
		t.Fatalf("manifest IDs differ by input order: %s != %s", secondManifest.ID(), manifest.ID())
	}
}

func TestResolveInputViewRejectsScopeIdentityAndProjectionViolations(t *testing.T) {
	content := []byte("input")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:input", Digest: domain.NewDigest(content), Size: int64(len(content))}
	base := RepositoryOccurrence{Reference: "artifact://occurrences/input", Digest: occurrence.Digest, Size: occurrence.Size, SecurityScope: testScope, Sidecars: []string{"metadata"}}
	plan := ports.InputPlan{SecurityScope: testScope, Entries: []ports.InputEntryPlan{{Occurrence: occurrence, LogicalPath: "input.bin", PermittedSidecars: []string{"metadata"}}}}
	tests := map[string]struct {
		ctx      func(*testing.T) context.Context
		mutate   func(*RepositoryOccurrence)
		plan     ports.InputPlan
		backend  error
		wantCode domain.ErrorCode
		leak     string
	}{
		"missing scope": {ctx: deadlineContext, plan: plan, wantCode: domain.CodeUnauthorized},
		"wrong requested scope": {
			ctx: authorizedContext, plan: ports.InputPlan{SecurityScope: "campaign-blue", Entries: plan.Entries}, wantCode: domain.CodeForbidden,
		},
		"repository crosses scope": {
			ctx: authorizedContext, plan: plan, mutate: func(value *RepositoryOccurrence) { value.SecurityScope = "campaign-blue" }, wantCode: domain.CodeForbidden,
		},
		"repository changes digest": {
			ctx: authorizedContext, plan: plan, mutate: func(value *RepositoryOccurrence) { value.Digest = domain.NewDigest([]byte("other")) }, wantCode: domain.CodeIntegrityViolation,
		},
		"unavailable sidecar": {
			ctx: authorizedContext, plan: plan, mutate: func(value *RepositoryOccurrence) { value.Sidecars = nil }, wantCode: domain.CodeForbidden,
		},
		"physical path reference": {
			ctx: authorizedContext, plan: plan, mutate: func(value *RepositoryOccurrence) { value.Reference = `C:\repository\object` }, wantCode: domain.CodeIntegrityViolation,
		},
		"sanitized repository denial": {
			ctx: authorizedContext, plan: plan, backend: errors.New(`open C:\secret\repository\objects\01: access denied`), wantCode: domain.CodeUnavailable, leak: "secret",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			calls := 0
			backend := &fakeBackend{resolve: func(context.Context, ResolveOccurrenceRequest) (RepositoryOccurrence, error) {
				calls++
				if test.backend != nil {
					return RepositoryOccurrence{}, test.backend
				}
				value := base
				if test.mutate != nil {
					test.mutate(&value)
				}
				return value, nil
			}}
			authority := newTestAuthority(t, backend)
			_, err := authority.ResolveInputView(test.ctx(t), test.plan)
			if !domain.IsCode(err, test.wantCode) {
				t.Fatalf("ResolveInputView() error = %v, want %s", err, test.wantCode)
			}
			if test.leak != "" && strings.Contains(strings.ToLower(err.Error()), test.leak) {
				t.Fatalf("error exposed repository detail: %v", err)
			}
			if name == "wrong requested scope" && calls != 0 {
				t.Fatalf("backend called %d times before scope rejection", calls)
			}
		})
	}
}

func TestOpenContentVerifiesStreamingDigestAndSize(t *testing.T) {
	content := []byte("verified immutable bytes")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:content", Digest: domain.NewDigest(content), Size: int64(len(content))}
	metadata := RepositoryOccurrence{Reference: "artifact://occurrences/content", Digest: occurrence.Digest, Size: occurrence.Size, SecurityScope: testScope}
	stream := &trackingReadCloser{Reader: &chunkReader{content: content, chunk: 3}}
	backend := contentBackend(metadata, stream)
	authority := newTestAuthority(t, backend)
	reader, err := authority.OpenContent(authorizedContext(t), occurrence)
	if err != nil {
		t.Fatal(err)
	}
	if reader.Digest() != occurrence.Digest || reader.Size() != occurrence.Size {
		t.Fatalf("reader identity = %s/%d", reader.Digest(), reader.Size())
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content = %q", got)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !stream.Closed() {
		t.Fatal("repository stream was not closed")
	}
}

func TestOpenContentCloseDrainsAndVerifiesPartialRead(t *testing.T) {
	content := []byte("partially consumed")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:partial", Digest: domain.NewDigest(content), Size: int64(len(content))}
	metadata := RepositoryOccurrence{Reference: "artifact://occurrences/partial", Digest: occurrence.Digest, Size: occurrence.Size, SecurityScope: testScope}
	stream := &trackingReadCloser{Reader: bytes.NewReader(content)}
	authority := newTestAuthority(t, contentBackend(metadata, stream))
	reader, err := authority.OpenContent(authorizedContext(t), occurrence)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 2)
	if _, err := reader.Read(buffer); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if !stream.Closed() {
		t.Fatal("repository stream was not closed")
	}
}

func TestOpenContentRejectsCorruptAndChangedStreams(t *testing.T) {
	declared := []byte("abc")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:bytes", Digest: domain.NewDigest(declared), Size: int64(len(declared))}
	base := RepositoryOccurrence{Reference: "artifact://occurrences/bytes", Digest: occurrence.Digest, Size: occurrence.Size, SecurityScope: testScope}
	tests := map[string]struct {
		stream       []byte
		mutateOpened func(*RepositoryOccurrence)
	}{
		"digest mismatch": {stream: []byte("xyz")},
		"short stream":    {stream: []byte("ab")},
		"long stream":     {stream: []byte("abcd")},
		"identity changes between resolve and open": {
			stream: declared, mutateOpened: func(value *RepositoryOccurrence) { value.Reference = "artifact://occurrences/replaced" },
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			stream := &trackingReadCloser{Reader: bytes.NewReader(test.stream)}
			backend := contentBackend(base, stream)
			if test.mutateOpened != nil {
				backend.open = func(context.Context, OpenObjectRequest) (OpenedObject, error) {
					changed := base
					test.mutateOpened(&changed)
					return OpenedObject{Occurrence: changed, Reader: stream}, nil
				}
			}
			authority := newTestAuthority(t, backend)
			reader, err := authority.OpenContent(authorizedContext(t), occurrence)
			if test.mutateOpened != nil {
				if !domain.IsCode(err, domain.CodeIntegrityViolation) || !stream.Closed() {
					t.Fatalf("OpenContent() error = %v, closed = %v", err, stream.Closed())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_, readErr := io.ReadAll(reader)
			if !domain.IsCode(readErr, domain.CodeIntegrityViolation) {
				t.Fatalf("ReadAll() error = %v", readErr)
			}
			if closeErr := reader.Close(); !domain.IsCode(closeErr, domain.CodeIntegrityViolation) {
				t.Fatalf("Close() error = %v", closeErr)
			}
		})
	}
}

func TestCaptureOutputsPublishesCanonicalRolesSensitivityAndProvenance(t *testing.T) {
	ids := newFixtureIDs(t)
	report := mustSelection(t, "reports/finding.json", "report", "review")
	trace := mustSelection(t, "captures/network.pcap", "pcap")
	var captured OutputCaptureRequest
	backend := &fakeBackend{outputs: func(ctx context.Context, request OutputCaptureRequest) ([]PublishedOutput, error) {
		captured = cloneOutputRequest(request)
		published := make([]PublishedOutput, 0, len(request.Items))
		for index, item := range request.Items {
			content, err := readContentSource(ctx, item.Content)
			if err != nil {
				return nil, err
			}
			output := PublishedOutput{
				LogicalPath: item.LogicalPath,
				Occurrence:  RepositoryOccurrence{Reference: "artifact://outputs/" + string(rune('a'+index)), Digest: domain.NewDigest(content), Size: int64(len(content)), SecurityScope: request.SecurityScope},
				Roles:       append([]RoleBinding(nil), item.Roles...), Provenance: cloneMap(item.Provenance), Verified: true,
			}
			published = append([]PublishedOutput{output}, published...)
		}
		return published, nil
	}}
	config := DefaultConfig()
	config.OutputRoleSensitivities = map[string]domain.Sensitivity{"pcap": domain.SensitivityRestricted, "review": domain.SensitivityPublic}
	authority, err := New(backend, config)
	if err != nil {
		t.Fatal(err)
	}
	plan := ports.OutputPlan{
		IdempotencyKey: "export-1", LeaseID: ids.leaseID, WorkspaceID: ids.workspaceID,
		AgentWorkspaceID: ids.agentID, AgentGeneration: domain.InitialAgentGeneration,
		Selections: []domain.ExportSelection{report, trace}, Content: map[string]ports.ContentSource{
			"reports/finding.json":  testContentSource{content: []byte("reports/finding.json")},
			"captures/network.pcap": testContentSource{content: []byte("captures/network.pcap")},
		}, Provenance: map[string]string{"case": "case-42"},
	}
	references, err := authority.CaptureOutputs(authorizedContext(t), plan)
	if err != nil {
		t.Fatal(err)
	}
	if captured.SecurityScope != testScope || captured.LeaseID != ids.leaseID || len(captured.Items) != 2 {
		t.Fatalf("capture request = %#v", captured)
	}
	if captured.Items[0].LogicalPath != "captures/network.pcap" || captured.Items[1].LogicalPath != "reports/finding.json" {
		t.Fatalf("capture order = %#v", captured.Items)
	}
	for _, item := range captured.Items {
		if item.Provenance["case"] != "case-42" || item.Provenance["world.lease_id"] != ids.leaseID.String() || item.Provenance["world.logical_path"] != item.LogicalPath {
			t.Fatalf("provenance for %q = %#v", item.LogicalPath, item.Provenance)
		}
	}
	if len(references) != 3 {
		t.Fatalf("artifact references = %d, want 3", len(references))
	}
	want := []struct {
		role        string
		sensitivity domain.Sensitivity
	}{{"pcap", domain.SensitivityRestricted}, {"report", domain.SensitivityInternal}, {"review", domain.SensitivityPublic}}
	for index, expected := range want {
		spec := references[index].Spec()
		if spec.Role != expected.role || spec.Sensitivity != expected.sensitivity || spec.Reference == "" || spec.Digest.IsZero() {
			t.Fatalf("reference[%d] = %#v", index, spec)
		}
	}
}

func TestCaptureOutputsRejectsUnsafeIntentAndFalseAcknowledgements(t *testing.T) {
	ids := newFixtureIDs(t)
	selection := mustSelection(t, "report.json", "report")
	basePlan := ports.OutputPlan{IdempotencyKey: "export", LeaseID: ids.leaseID, WorkspaceID: ids.workspaceID, AgentWorkspaceID: ids.agentID, AgentGeneration: 1, Selections: []domain.ExportSelection{selection}, Content: map[string]ports.ContentSource{"report.json": testContentSource{content: []byte("output")}}}
	t.Run("reserved provenance", func(t *testing.T) {
		calls := 0
		backend := &fakeBackend{outputs: func(context.Context, OutputCaptureRequest) ([]PublishedOutput, error) { calls++; return nil, nil }}
		authority := newTestAuthority(t, backend)
		plan := basePlan
		plan.Provenance = map[string]string{"world.lease_id": "spoofed"}
		_, err := authority.CaptureOutputs(authorizedContext(t), plan)
		if !domain.IsCode(err, domain.CodeForbidden) || calls != 0 {
			t.Fatalf("CaptureOutputs() error = %v, calls = %d", err, calls)
		}
	})
	t.Run("duplicate logical path", func(t *testing.T) {
		authority := newTestAuthority(t, &fakeBackend{})
		plan := basePlan
		plan.Selections = append(plan.Selections, selection)
		_, err := authority.CaptureOutputs(authorizedContext(t), plan)
		if !domain.IsCode(err, domain.CodeConflict) {
			t.Fatalf("CaptureOutputs() error = %v", err)
		}
	})
	mutations := map[string]func(*PublishedOutput){
		"unverified":          func(value *PublishedOutput) { value.Verified = false },
		"wrong scope":         func(value *PublishedOutput) { value.Occurrence.SecurityScope = "campaign-blue" },
		"wrong roles":         func(value *PublishedOutput) { value.Roles[0].Sensitivity = domain.SensitivitySecret },
		"changed provenance":  func(value *PublishedOutput) { value.Provenance["world.kind"] = "other" },
		"repository path ref": func(value *PublishedOutput) { value.Occurrence.Reference = "/srv/repository/object" },
		"path shaped URI ref": func(value *PublishedOutput) { value.Occurrence.Reference = "artifact:/srv/repository/object" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			backend := &fakeBackend{outputs: func(_ context.Context, request OutputCaptureRequest) ([]PublishedOutput, error) {
				item := request.Items[0]
				output := PublishedOutput{LogicalPath: item.LogicalPath, Occurrence: RepositoryOccurrence{Reference: "artifact://outputs/one", Digest: domain.NewDigest([]byte("output")), Size: 6, SecurityScope: request.SecurityScope}, Roles: append([]RoleBinding(nil), item.Roles...), Provenance: cloneMap(item.Provenance), Verified: true}
				mutate(&output)
				return []PublishedOutput{output}, nil
			}}
			authority := newTestAuthority(t, backend)
			_, err := authority.CaptureOutputs(authorizedContext(t), basePlan)
			if !domain.IsCode(err, domain.CodeIntegrityViolation) {
				t.Fatalf("CaptureOutputs() error = %v", err)
			}
		})
	}
}

func TestCaptureObservationBundlePublishesBoundProvenance(t *testing.T) {
	bundle := newSealedBundle(t)
	metadata := []byte("canonical bundle metadata")
	digest := domain.NewDigest(metadata)
	content := testContentSource{content: metadata}
	var captured BundleCaptureRequest
	backend := &fakeBackend{bundle: func(ctx context.Context, request BundleCaptureRequest) (PublishedBundle, error) {
		captured = request
		capturedBytes, err := readContentSource(ctx, request.Content)
		if err != nil || !bytes.Equal(capturedBytes, metadata) {
			return PublishedBundle{}, errors.New("backend did not receive sealed bytes")
		}
		return PublishedBundle{
			Occurrence: RepositoryOccurrence{Reference: "artifact://bundles/one", Digest: request.Content.Digest(), Size: request.Content.Size(), SecurityScope: request.SecurityScope},
			Role:       request.Role, Provenance: cloneMap(request.Provenance), Verified: true,
		}, nil
	}}
	authority := newTestAuthority(t, backend)
	reference, err := authority.CaptureObservationBundle(authorizedContext(t), ports.ObservationBundlePlan{IdempotencyKey: "bundle-1", Bundle: bundle, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	spec := bundle.Spec()
	if captured.BundleID != bundle.ID() || captured.TargetRunID != bundle.TargetRunID() || captured.TargetID != spec.TargetID || captured.AgentWorkspaceID != spec.AgentWorkspaceID {
		t.Fatalf("bundle request = %#v", captured)
	}
	if captured.Provenance["world.content_digest"] != digest.String() || captured.Provenance["world.content_size"] != strconv.Itoa(len(metadata)) || captured.Provenance["world.target_generation"] != "1" {
		t.Fatalf("bundle provenance = %#v", captured.Provenance)
	}
	artifact := reference.Spec()
	if artifact.Reference != "artifact://bundles/one" || artifact.Digest != digest || artifact.Role != DefaultBundleRole || artifact.Sensitivity != domain.SensitivityInternal {
		t.Fatalf("bundle artifact = %#v", artifact)
	}
}

func TestCaptureObservationBundleRejectsFalseAcknowledgement(t *testing.T) {
	bundle := newSealedBundle(t)
	content := testContentSource{content: []byte("metadata")}
	backend := &fakeBackend{bundle: func(_ context.Context, request BundleCaptureRequest) (PublishedBundle, error) {
		return PublishedBundle{
			Occurrence: RepositoryOccurrence{Reference: "artifact://bundles/one", Digest: domain.NewDigest([]byte("different")), Size: 9, SecurityScope: request.SecurityScope},
			Role:       request.Role, Provenance: cloneMap(request.Provenance), Verified: true,
		}, nil
	}}
	authority := newTestAuthority(t, backend)
	_, err := authority.CaptureObservationBundle(authorizedContext(t), ports.ObservationBundlePlan{IdempotencyKey: "bundle-1", Bundle: bundle, Content: content})
	if !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("CaptureObservationBundle() error = %v", err)
	}
}

type testContentSource struct{ content []byte }

func (s testContentSource) Digest() domain.Digest { return domain.NewDigest(s.content) }
func (s testContentSource) Size() int64           { return int64(len(s.content)) }
func (s testContentSource) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), s.content...))), nil
}

func readContentSource(ctx context.Context, source ports.ContentSource) ([]byte, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func TestRepositoryFailuresAreSanitizedAndPreserveAuthorizationCode(t *testing.T) {
	content := []byte("bytes")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:bytes", Digest: domain.NewDigest(content), Size: int64(len(content))}
	backend := &fakeBackend{resolve: func(context.Context, ResolveOccurrenceRequest) (RepositoryOccurrence, error) {
		return RepositoryOccurrence{}, domain.NewError(domain.CodeForbidden, "repository.open", "path", `denied C:\repository\private`, nil)
	}}
	authority := newTestAuthority(t, backend)
	_, err := authority.OpenContent(authorizedContext(t), occurrence)
	if !domain.IsCode(err, domain.CodeForbidden) {
		t.Fatalf("OpenContent() error = %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "repository\\private") {
		t.Fatalf("error exposed repository path: %v", err)
	}
}

func TestRepositoryStreamFailuresDoNotExposePhysicalPaths(t *testing.T) {
	content := []byte("bytes")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:bytes", Digest: domain.NewDigest(content), Size: int64(len(content))}
	metadata := RepositoryOccurrence{Reference: "artifact://occurrences/bytes", Digest: occurrence.Digest, Size: occurrence.Size, SecurityScope: testScope}
	backend := contentBackend(metadata, &trackingReadCloser{Reader: errorReader{err: errors.New(`read C:\repository\private\object: disk failure`)}})
	authority := newTestAuthority(t, backend)
	reader, err := authority.OpenContent(authorizedContext(t), occurrence)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(reader)
	if !domain.IsCode(err, domain.CodeUnavailable) {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "repository\\private") {
		t.Fatalf("stream error exposed repository path: %v", err)
	}
}

func TestMaterialOperationsRequireDeadline(t *testing.T) {
	content := []byte("bytes")
	occurrence := ports.ArtifactOccurrence{Reference: "selection:bytes", Digest: domain.NewDigest(content), Size: int64(len(content))}
	metadata := RepositoryOccurrence{Reference: "artifact://occurrences/bytes", Digest: occurrence.Digest, Size: occurrence.Size, SecurityScope: testScope}
	authority := newTestAuthority(t, contentBackend(metadata, io.NopCloser(bytes.NewReader(content))))
	ctx := WithSecurityScope(context.Background(), testScope)
	if _, err := authority.OpenContent(ctx, occurrence); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("OpenContent() error = %v", err)
	}
	plan := ports.InputPlan{SecurityScope: testScope, Entries: []ports.InputEntryPlan{{Occurrence: occurrence, LogicalPath: "input.bin"}}}
	if _, err := authority.ResolveInputView(ctx, plan); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("ResolveInputView() error = %v", err)
	}
}

func TestNewValidatesConfigurationAndTypedNilBackend(t *testing.T) {
	var typedNil *fakeBackend
	if _, err := New(typedNil, DefaultConfig()); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("New(typed nil) error = %v", err)
	}
	config := DefaultConfig()
	config.OutputRoleSensitivities = map[string]domain.Sensitivity{"pcap": "classified"}
	if _, err := New(&fakeBackend{}, config); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("New(invalid sensitivity) error = %v", err)
	}
}

type authenticationKey struct{}

func authorizedContext(t *testing.T) context.Context {
	t.Helper()
	ctx := deadlineContext(t)
	ctx = context.WithValue(ctx, authenticationKey{}, "principal-1")
	return WithSecurityScope(ctx, testScope)
}

func deadlineContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newTestAuthority(t *testing.T, backend Backend) *Authority {
	t.Helper()
	authority, err := New(backend, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func contentBackend(metadata RepositoryOccurrence, reader io.ReadCloser) *fakeBackend {
	return &fakeBackend{
		resolve: func(_ context.Context, request ResolveOccurrenceRequest) (RepositoryOccurrence, error) {
			if request.Purpose != ResolveForRead || request.SecurityScope != metadata.SecurityScope {
				return RepositoryOccurrence{}, domain.NewError(domain.CodeForbidden, "repository.resolve", "scope", "denied", nil)
			}
			return metadata, nil
		},
		open: func(_ context.Context, request OpenObjectRequest) (OpenedObject, error) {
			if request.Reference != metadata.Reference || request.SecurityScope != metadata.SecurityScope {
				return OpenedObject{}, domain.NewError(domain.CodeForbidden, "repository.open", "scope", "denied", nil)
			}
			return OpenedObject{Occurrence: metadata, Reader: reader}, nil
		},
	}
}

type trackingReadCloser struct {
	io.Reader
	mu     sync.Mutex
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *trackingReadCloser) Closed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

type chunkReader struct {
	content []byte
	offset  int
	chunk   int
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func (r *chunkReader) Read(buffer []byte) (int, error) {
	if r.offset >= len(r.content) {
		return 0, io.EOF
	}
	limit := r.chunk
	if limit > len(buffer) {
		limit = len(buffer)
	}
	if remaining := len(r.content) - r.offset; limit > remaining {
		limit = remaining
	}
	copy(buffer, r.content[r.offset:r.offset+limit])
	r.offset += limit
	return limit, nil
}

type fixtureIDs struct {
	leaseID     domain.LeaseID
	workspaceID domain.WorkspaceID
	agentID     domain.AgentWorkspaceID
}

func newFixtureIDs(t *testing.T) fixtureIDs {
	t.Helper()
	ids := newIDGenerator(t)
	return fixtureIDs{leaseID: mustGenerate(t, ids.LeaseID), workspaceID: mustGenerate(t, ids.WorkspaceID), agentID: mustGenerate(t, ids.AgentWorkspaceID)}
}

func mustSelection(t *testing.T, path string, roles ...string) domain.ExportSelection {
	t.Helper()
	selection, err := domain.NewExportSelection(domain.ExportSelectionSpec{RelativePath: path, Roles: roles})
	if err != nil {
		t.Fatal(err)
	}
	return selection
}

func cloneOutputRequest(request OutputCaptureRequest) OutputCaptureRequest {
	result := request
	result.Items = make([]OutputCaptureItem, len(request.Items))
	for index, item := range request.Items {
		result.Items[index] = item
		result.Items[index].Roles = append([]RoleBinding(nil), item.Roles...)
		result.Items[index].Provenance = cloneMap(item.Provenance)
	}
	return result
}

func newSealedBundle(t *testing.T) domain.ObservationBundle {
	t.Helper()
	ids := newIDGenerator(t)
	createdAt := time.Unix(1_900_000_000, 0).UTC()
	sessionID := mustGenerate(t, ids.ResearchSessionID)
	leaseID := mustGenerate(t, ids.LeaseID)
	agentID := mustGenerate(t, ids.AgentWorkspaceID)
	targetID := mustGenerate(t, ids.TargetID)
	runID := mustGenerate(t, ids.TargetRunID)
	bundleID := mustGenerate(t, ids.ObservationBundleID)
	eventID := mustGenerate(t, ids.EventID)
	collectorID := mustGenerate(t, ids.CollectorID)
	artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{Reference: "artifact://raw/events", Digest: domain.NewDigest([]byte("raw")), Size: 3, Role: "raw-observation", Sensitivity: domain.SensitivityRestricted})
	if err != nil {
		t.Fatal(err)
	}
	event, err := domain.NewEventEnvelope(domain.EventEnvelopeParams{
		SchemaVersion: 1, EventID: eventID, Kind: "process.exec", ResearchSessionID: sessionID,
		LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: 1, TargetID: targetID,
		TargetGeneration: 1, TargetRunID: runID, Source: "process-collector", SourceInstance: "collector-1",
		SourceSequence: 1, SourceCursor: 1, CollectorID: collectorID, CollectorPlacement: domain.CollectorPlacementHost,
		CoverageLevel: domain.CoverageLevelComplete, ObservedWallTime: createdAt, Sensitivity: domain.SensitivityInternal,
		Completeness: domain.CompletenessComplete, Confidence: 1, Origin: domain.OriginSystem,
	})
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := domain.NewCollectorCoverage(domain.CollectorCoverageSpec{CollectorID: collectorID, SignalFamily: "process", Placement: domain.CollectorPlacementHost, Level: domain.CoverageLevelComplete, Status: domain.CoverageAvailable, Required: true, StartedAt: createdAt, EndedAt: createdAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, createdAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	summary, err := domain.NewDerivedSummary(domain.DerivedSummarySpec{Text: "A process event was observed.", Citations: []domain.EvidenceCitation{{FirstCursor: 1, LastCursor: 1, Artifact: artifact}}})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := domain.NewObservationBundle(domain.ObservationBundleSpec{
		ID: bundleID, TargetRunID: runID, TargetID: targetID, TargetGeneration: 1,
		AgentWorkspaceID: agentID, AgentGeneration: 1, FirstCursor: 1, LastCursor: 1,
		RawArtifacts: []domain.ArtifactReference{artifact}, NormalizedEvents: []domain.EventEnvelope{event},
		Coverage: []domain.CollectorCoverage{coverage}, TargetChanges: changes, Summary: summary, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err = bundle.Seal(domain.InitialRevision, createdAt.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func newIDGenerator(t *testing.T) *domain.IDGenerator {
	t.Helper()
	random := bytes.NewReader(bytes.Repeat([]byte{0x42}, 4096))
	ids, err := domain.NewIDGenerator(func() time.Time { return time.Unix(1_900_000_000, 0).UTC() }, random)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func mustGenerate[T any](t *testing.T, generate func() (T, error)) T {
	t.Helper()
	value, err := generate()
	if err != nil {
		t.Fatal(err)
	}
	return value
}
