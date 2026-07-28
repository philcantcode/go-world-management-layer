// Package localmaterial provides a deliberately small, security-scoped
// filesystem material authority for single-node deployments and end-to-end
// qualification. RPC callers can name only opaque references registered at
// startup; they can never supply a host path.
package localmaterial

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

type EntryConfig struct {
	Reference     string             `json:"reference"`
	SecurityScope string             `json:"security_scope"`
	SourcePath    string             `json:"source_path"`
	LogicalPath   string             `json:"logical_path"`
	Mode          uint32             `json:"mode"`
	Role          string             `json:"role"`
	Sensitivity   domain.Sensitivity `json:"sensitivity"`
	Sidecars      []string           `json:"sidecars,omitempty"`
}

type SelectionConfig struct {
	Reference     string   `json:"reference"`
	SecurityScope string   `json:"security_scope"`
	Occurrences   []string `json:"occurrences"`
}

type Config struct {
	SourceRoot      string
	PublicationRoot string
	MaxObjectBytes  int64
	Entries         []EntryConfig
	Selections      []SelectionConfig
}

type CatalogEntry struct {
	Occurrence    ports.ArtifactOccurrence
	SecurityScope string
	LogicalPath   string
	Mode          uint32
	Role          string
	Sensitivity   domain.Sensitivity
	SourcePath    string
	Sidecars      []string
}

type Authority struct {
	sourceRoot      string
	publicationRoot string
	maxObjectBytes  int64
	entries         map[string]CatalogEntry
	selections      map[string]SelectionConfig
	mu              sync.Mutex
}

func New(config Config) (*Authority, error) {
	if strings.TrimSpace(config.SourceRoot) == "" || strings.TrimSpace(config.PublicationRoot) == "" || config.MaxObjectBytes <= 0 {
		return nil, fmt.Errorf("source root, publication root, and positive object limit are required")
	}
	sourceRoot, err := filepath.Abs(config.SourceRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve source root: %w", err)
	}
	publicationRoot, err := filepath.Abs(config.PublicationRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve publication root: %w", err)
	}
	if err := os.MkdirAll(publicationRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create publication root: %w", err)
	}
	publicationRoot = filepath.Clean(publicationRoot)
	for _, logicalDirectory := range []string{"objects/sha256", "requests"} {
		namespace, err := safepath.OpenNamespace(publicationRoot, logicalDirectory)
		if err != nil {
			return nil, fmt.Errorf("initialize publication namespace %q: %w", logicalDirectory, err)
		}
		if err := namespace.Close(); err != nil {
			return nil, fmt.Errorf("close publication namespace %q: %w", logicalDirectory, err)
		}
	}
	authority := &Authority{
		sourceRoot: filepath.Clean(sourceRoot), publicationRoot: publicationRoot,
		maxObjectBytes: config.MaxObjectBytes, entries: make(map[string]CatalogEntry, len(config.Entries)),
		selections: make(map[string]SelectionConfig, len(config.Selections)),
	}
	for index, configured := range config.Entries {
		entry, err := authority.loadEntry(configured)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", index, err)
		}
		if _, duplicate := authority.entries[entry.Occurrence.Reference]; duplicate {
			return nil, fmt.Errorf("entry %d duplicates reference %q", index, entry.Occurrence.Reference)
		}
		authority.entries[entry.Occurrence.Reference] = entry
	}
	for index, selection := range config.Selections {
		if strings.TrimSpace(selection.Reference) == "" || strings.TrimSpace(selection.SecurityScope) == "" || len(selection.Occurrences) == 0 {
			return nil, fmt.Errorf("selection %d requires reference, security scope, and occurrences", index)
		}
		key := scopedKey(selection.SecurityScope, selection.Reference)
		if _, duplicate := authority.selections[key]; duplicate {
			return nil, fmt.Errorf("selection %d duplicates %q", index, selection.Reference)
		}
		seen := make(map[string]struct{}, len(selection.Occurrences))
		selection.Occurrences = append([]string(nil), selection.Occurrences...)
		for _, reference := range selection.Occurrences {
			entry, found := authority.entries[reference]
			if !found || entry.SecurityScope != selection.SecurityScope {
				return nil, fmt.Errorf("selection %q contains an unknown or cross-scope occurrence %q", selection.Reference, reference)
			}
			if _, duplicate := seen[reference]; duplicate {
				return nil, fmt.Errorf("selection %q contains duplicate occurrence %q", selection.Reference, reference)
			}
			seen[reference] = struct{}{}
		}
		authority.selections[key] = selection
	}
	return authority, nil
}

func (a *Authority) loadEntry(config EntryConfig) (CatalogEntry, error) {
	if strings.TrimSpace(config.Reference) == "" || strings.TrimSpace(config.SecurityScope) == "" || strings.TrimSpace(config.Role) == "" || !config.Sensitivity.IsValid() {
		return CatalogEntry{}, fmt.Errorf("reference, security scope, role, and sensitivity are required")
	}
	sourcePath, err := safepath.Normalize(config.SourcePath)
	if err != nil {
		return CatalogEntry{}, fmt.Errorf("source path: %w", err)
	}
	logicalPath, err := safepath.Normalize(config.LogicalPath)
	if err != nil {
		return CatalogEntry{}, fmt.Errorf("logical path: %w", err)
	}
	if config.Mode == 0 {
		config.Mode = 0o444
	}
	if config.Mode > 0o777 {
		return CatalogEntry{}, fmt.Errorf("mode must contain only regular Unix permission bits")
	}
	seenSidecars := make(map[string]struct{}, len(config.Sidecars))
	for _, sidecar := range config.Sidecars {
		if strings.TrimSpace(sidecar) == "" {
			return CatalogEntry{}, fmt.Errorf("sidecars must not contain blanks")
		}
		if _, duplicate := seenSidecars[sidecar]; duplicate {
			return CatalogEntry{}, fmt.Errorf("sidecars must not contain duplicates")
		}
		seenSidecars[sidecar] = struct{}{}
	}
	sort.Strings(config.Sidecars)
	content, err := a.readSource(context.Background(), sourcePath)
	if err != nil {
		return CatalogEntry{}, err
	}
	digest := domain.NewDigest(content)
	if err := a.ensureObject(digest.String(), content); err != nil {
		return CatalogEntry{}, domain.NewError(domain.CodeUnavailable, "local_material.load_entry", "publication", "could not stage immutable occurrence bytes", err)
	}
	return CatalogEntry{
		Occurrence:    ports.ArtifactOccurrence{Reference: config.Reference, Digest: digest, Size: int64(len(content))},
		SecurityScope: config.SecurityScope, LogicalPath: logicalPath, Mode: config.Mode,
		Role: config.Role, Sensitivity: config.Sensitivity, SourcePath: sourcePath,
		Sidecars: append([]string(nil), config.Sidecars...),
	}, nil
}

func (a *Authority) Entry(securityScope, reference string) (CatalogEntry, error) {
	entry, found := a.entries[reference]
	if !found {
		return CatalogEntry{}, domain.NewError(domain.CodeNotFound, "local_material.entry", "reference", "is not registered", nil)
	}
	if entry.SecurityScope != securityScope {
		return CatalogEntry{}, domain.NewError(domain.CodeForbidden, "local_material.entry", "security_scope", "does not authorize the occurrence", nil)
	}
	return cloneCatalogEntry(entry), nil
}

func (a *Authority) ResolveSelection(ctx context.Context, securityScope, reference string) ([]CatalogEntry, error) {
	if err := ports.RequireDeadline(ctx, "local_material.resolve_selection"); err != nil {
		return nil, err
	}
	selection, found := a.selections[scopedKey(securityScope, reference)]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "local_material.resolve_selection", "reference", "selection is not registered in this scope", nil)
	}
	result := make([]CatalogEntry, 0, len(selection.Occurrences))
	for _, occurrence := range selection.Occurrences {
		entry, err := a.resolveEntry(ctx, securityScope, occurrence)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func (a *Authority) ResolveOccurrence(ctx context.Context, securityScope, reference string) (ports.ArtifactOccurrence, error) {
	entry, err := a.resolveEntry(ctx, securityScope, reference)
	return entry.Occurrence, err
}

func (a *Authority) resolveEntry(ctx context.Context, securityScope, reference string) (CatalogEntry, error) {
	if err := ports.RequireDeadline(ctx, "local_material.resolve_occurrence"); err != nil {
		return CatalogEntry{}, err
	}
	entry, err := a.Entry(securityScope, reference)
	if err != nil {
		return CatalogEntry{}, err
	}
	if _, err := a.readObject(ctx, entry.Occurrence); err != nil {
		return CatalogEntry{}, err
	}
	return entry, nil
}

func (a *Authority) ResolveInputView(ctx context.Context, plan ports.InputPlan) (domain.InputViewManifest, error) {
	if err := ports.RequireDeadline(ctx, "local_material.resolve_input_view"); err != nil {
		return domain.InputViewManifest{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.InputViewManifest{}, err
	}
	entries := make([]domain.InputViewEntry, 0, len(plan.Entries))
	for index, planned := range plan.Entries {
		resolved, err := a.resolveEntry(ctx, plan.SecurityScope, planned.Occurrence.Reference)
		if err != nil {
			return domain.InputViewManifest{}, err
		}
		if resolved.Occurrence != planned.Occurrence {
			return domain.InputViewManifest{}, domain.NewError(domain.CodeIntegrityViolation, "local_material.resolve_input_view", fmt.Sprintf("entries[%d]", index), "planned identity does not match registered bytes", nil)
		}
		allowed := make(map[string]struct{}, len(resolved.Sidecars))
		for _, sidecar := range resolved.Sidecars {
			allowed[sidecar] = struct{}{}
		}
		for _, sidecar := range planned.PermittedSidecars {
			if _, ok := allowed[sidecar]; !ok {
				return domain.InputViewManifest{}, domain.NewError(domain.CodeForbidden, "local_material.resolve_input_view", fmt.Sprintf("entries[%d].permitted_sidecars", index), "contains an unregistered sidecar", nil)
			}
		}
		entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
			LogicalPath: planned.LogicalPath, OccurrenceRef: planned.Occurrence.Reference,
			Digest: planned.Occurrence.Digest, Size: planned.Occurrence.Size, Mode: planned.Mode,
			PermittedSidecars: append([]string(nil), planned.PermittedSidecars...),
		})
		if err != nil {
			return domain.InputViewManifest{}, err
		}
		entries = append(entries, entry)
	}
	return domain.NewInputViewManifest(entries)
}

func (a *Authority) OpenContent(ctx context.Context, occurrence ports.ArtifactOccurrence) (ports.ContentReader, error) {
	if err := ports.RequireDeadline(ctx, "local_material.open_content"); err != nil {
		return nil, err
	}
	if err := occurrence.Validate(); err != nil {
		return nil, err
	}
	entry, found := a.entries[occurrence.Reference]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "local_material.open_content", "reference", "is not registered", nil)
	}
	if entry.Occurrence != occurrence {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "local_material.open_content", "occurrence", "does not match registered identity", nil)
	}
	content, err := a.readObject(ctx, occurrence)
	if err != nil {
		return nil, err
	}
	return &memoryReader{Reader: bytes.NewReader(content), digest: occurrence.Digest, size: occurrence.Size}, nil
}

func (a *Authority) CaptureOutputs(ctx context.Context, plan ports.OutputPlan) ([]domain.ArtifactReference, error) {
	if err := ports.RequireDeadline(ctx, "local_material.capture_outputs"); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	selections := append([]domain.ExportSelection(nil), plan.Selections...)
	sort.Slice(selections, func(i, j int) bool { return selections[i].Spec().RelativePath < selections[j].Spec().RelativePath })
	type capturedOutput struct {
		Path   string   `json:"path"`
		Digest string   `json:"digest"`
		Size   int64    `json:"size"`
		Roles  []string `json:"roles"`
	}
	captured := make([]capturedOutput, 0, len(selections))
	contents := make(map[string][]byte, len(selections))
	for _, selection := range selections {
		spec := selection.Spec()
		content, err := readContentSource(ctx, plan.Content[spec.RelativePath], a.maxObjectBytes)
		if err != nil {
			return nil, err
		}
		roles := append([]string(nil), spec.Roles...)
		sort.Strings(roles)
		contents[spec.RelativePath] = content
		captured = append(captured, capturedOutput{Path: spec.RelativePath, Digest: domain.NewDigest(content).String(), Size: int64(len(content)), Roles: roles})
	}
	marker, err := json.Marshal(struct {
		Lease      string            `json:"lease"`
		Workspace  string            `json:"workspace"`
		Generation uint64            `json:"generation"`
		Outputs    []capturedOutput  `json:"outputs"`
		Provenance map[string]string `json:"provenance"`
	}{plan.LeaseID.String(), plan.WorkspaceID.String(), uint64(plan.AgentGeneration), captured, plan.Provenance})
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, output := range captured {
		if err := a.ensureObject(output.Digest, contents[output.Path]); err != nil {
			return nil, err
		}
	}
	if err := a.ensureRequest("outputs", plan.IdempotencyKey, marker); err != nil {
		return nil, domain.NewError(domain.CodeConflict, "local_material.capture_outputs", "idempotency_key", "was reused with different output bytes or provenance", err)
	}
	result := make([]domain.ArtifactReference, 0)
	for _, output := range captured {
		digest, _ := domain.ParseDigest(output.Digest)
		for _, role := range output.Roles {
			artifact, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
				Reference: "world-local://outputs/" + plan.WorkspaceID.String() + "/" + url.PathEscape(output.Path) + "?digest=" + url.QueryEscape(output.Digest) + "&role=" + url.QueryEscape(role),
				Digest:    digest, Size: output.Size, Role: role, Sensitivity: domain.SensitivityInternal,
			})
			if err != nil {
				return nil, err
			}
			result = append(result, artifact)
		}
	}
	return result, nil
}

func (a *Authority) CaptureObservationBundle(ctx context.Context, plan ports.ObservationBundlePlan) (domain.ArtifactReference, error) {
	if err := ports.RequireDeadline(ctx, "local_material.capture_observation_bundle"); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.ArtifactReference{}, err
	}
	content, err := readContentSource(ctx, plan.Content, a.maxObjectBytes)
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	marker, err := json.Marshal(struct {
		Bundle string `json:"bundle"`
		Digest string `json:"digest"`
		Size   int64  `json:"size"`
	}{plan.Bundle.ID().String(), plan.Content.Digest().String(), plan.Content.Size()})
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.ensureObject(plan.Content.Digest().String(), content); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := a.ensureRequest("bundles", plan.IdempotencyKey, marker); err != nil {
		return domain.ArtifactReference{}, domain.NewError(domain.CodeConflict, "local_material.capture_observation_bundle", "idempotency_key", "was reused with different bundle bytes", err)
	}
	return domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "world-local://observation-bundles/" + plan.Bundle.ID().String() + "?digest=" + url.QueryEscape(plan.Content.Digest().String()),
		Digest:    plan.Content.Digest(), Size: plan.Content.Size(), Role: "observation-bundle", Sensitivity: domain.SensitivityRestricted,
	})
}

func (a *Authority) readSource(ctx context.Context, logicalPath string) ([]byte, error) {
	return readBoundedRegular(ctx, a.sourceRoot, logicalPath, a.maxObjectBytes, "local_material.read_source")
}

func (a *Authority) readObject(ctx context.Context, occurrence ports.ArtifactOccurrence) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logicalPath := objectLogicalPath(occurrence.Digest.String())
	namespace, name, err := openPublicationFile(a.publicationRoot, logicalPath)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "local_material.read_object", "content", "could not safely open immutable object namespace", err)
	}
	content, readErr := namespace.ReadRegularBounded(name, a.maxObjectBytes)
	closeErr := namespace.Close()
	if readErr != nil || closeErr != nil {
		if errors.Is(readErr, safepath.ErrUnsafe) || errors.Is(readErr, safepath.ErrNotRegular) {
			return nil, domain.NewError(domain.CodeIntegrityViolation, "local_material.read_object", "content", "immutable object is not a single-link regular file", readErr)
		}
		if errors.Is(readErr, safepath.ErrTooLarge) {
			return nil, domain.NewError(domain.CodeResourceExhausted, "local_material.read_object", "content", "exceeds configured object limit", readErr)
		}
		return nil, domain.NewError(domain.CodeUnavailable, "local_material.read_object", "content", "could not safely read immutable object", errors.Join(readErr, closeErr))
	}
	if int64(len(content)) != occurrence.Size || domain.NewDigest(content) != occurrence.Digest {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "local_material.read_object", "content", "immutable object identity changed", nil)
	}
	return content, nil
}

func readBoundedRegular(ctx context.Context, root, logicalPath string, maximumBytes int64, operation string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opened, err := safepath.OpenRegular(root, logicalPath)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, operation, "content", "could not safely open bounded regular content", err)
	}
	if opened.Size() > maximumBytes {
		_ = opened.Close()
		return nil, domain.NewError(domain.CodeResourceExhausted, operation, "content", "exceeds configured object limit", nil)
	}
	content, readErr := io.ReadAll(io.LimitReader(opened, maximumBytes+1))
	closeErr := opened.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if int64(len(content)) > maximumBytes {
		return nil, domain.NewError(domain.CodeResourceExhausted, operation, "content", "exceeds configured object limit", nil)
	}
	return content, nil
}

func (a *Authority) ensureObject(digest string, content []byte) error {
	return ensureImmutablePublication(a.publicationRoot, objectLogicalPath(digest), content)
}

func objectLogicalPath(digest string) string {
	raw := strings.TrimPrefix(digest, "sha256:")
	return path.Join("objects", "sha256", raw[:2], raw)
}

func (a *Authority) ensureRequest(namespace, key string, content []byte) error {
	hash := sha256.Sum256([]byte(namespace + "\x00" + key))
	logicalPath := path.Join("requests", namespace, hex.EncodeToString(hash[:])+".json")
	return ensureImmutablePublication(a.publicationRoot, logicalPath, content)
}

func ensureImmutablePublication(root, logicalPath string, desired []byte) error {
	namespace, name, err := openPublicationFile(root, logicalPath)
	if err != nil {
		return err
	}
	defer namespace.Close()
	if err := namespace.EnsureRegularAtomically(name, desired, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return fmt.Errorf("immutable publication conflicts: %w", err)
		}
		return err
	}
	return nil
}

func openPublicationFile(root, logicalPath string) (*safepath.Namespace, string, error) {
	normalized, err := safepath.Normalize(logicalPath)
	if err != nil {
		return nil, "", err
	}
	directory, name := path.Split(normalized)
	directory = strings.TrimSuffix(directory, "/")
	if directory == "" || name == "" {
		return nil, "", fmt.Errorf("publication path must contain a directory and file")
	}
	namespace, err := safepath.OpenNamespace(root, directory)
	if err != nil {
		return nil, "", err
	}
	return namespace, name, nil
}

func readContentSource(ctx context.Context, source ports.ContentSource, maxBytes int64) ([]byte, error) {
	if source == nil || source.Size() < 0 || source.Size() > maxBytes || source.Digest().IsZero() {
		return nil, domain.NewError(domain.CodeResourceExhausted, "local_material.read_content", "content", "source identity is invalid or exceeds the object limit", nil)
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || int64(len(content)) != source.Size() || domain.NewDigest(content) != source.Digest() {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "local_material.read_content", "content", "opened bytes do not match their immutable identity", errors.Join(readErr, closeErr))
	}
	return content, nil
}

func scopedKey(scope, reference string) string { return scope + "\x00" + reference }

func cloneCatalogEntry(entry CatalogEntry) CatalogEntry {
	entry.Sidecars = append([]string(nil), entry.Sidecars...)
	return entry
}

type memoryReader struct {
	*bytes.Reader
	digest domain.Digest
	size   int64
}

func (r *memoryReader) Close() error          { return nil }
func (r *memoryReader) Digest() domain.Digest { return r.digest }
func (r *memoryReader) Size() int64           { return r.size }

var _ ports.ContentReader = (*memoryReader)(nil)
var _ ports.MaterialAuthority = (*Authority)(nil)
