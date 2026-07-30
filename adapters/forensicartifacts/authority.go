package forensicartifacts

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	DefaultBundleRole = "world.observation-bundle"
	reservedPrefix    = "world."
)

type securityScopeContextKey struct{}

// WithSecurityScope binds repository operations to the authorization scope
// selected by the trusted control plane. Authentication credentials remain in
// the original context and are interpreted only by Backend.
func WithSecurityScope(ctx context.Context, securityScope string) context.Context {
	return context.WithValue(ctx, securityScopeContextKey{}, securityScope)
}

// SecurityScopeFromContext returns the scope attached by WithSecurityScope.
func SecurityScopeFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	scope, ok := ctx.Value(securityScopeContextKey{}).(string)
	return scope, ok && strings.TrimSpace(scope) != "" && scope == strings.TrimSpace(scope) && !strings.ContainsRune(scope, '\x00')
}

type Config struct {
	DefaultOutputSensitivity domain.Sensitivity
	OutputRoleSensitivities  map[string]domain.Sensitivity
	BundleRole               string
	BundleSensitivity        domain.Sensitivity
}

func DefaultConfig() Config {
	return Config{
		DefaultOutputSensitivity: domain.SensitivityInternal,
		BundleRole:               DefaultBundleRole,
		BundleSensitivity:        domain.SensitivityInternal,
	}
}

// Authority implements ports.MaterialAuthority.
type Authority struct {
	backend                  Backend
	defaultOutputSensitivity domain.Sensitivity
	outputRoleSensitivities  map[string]domain.Sensitivity
	bundleRole               string
	bundleSensitivity        domain.Sensitivity
}

// NewFromMaterial wraps an already-composed ports.MaterialAuthority when hosts
// obtain material composition from world.Manager.Material(). Forensic repository
// backends remain the primary construction path via New; this helper documents
// the Manager hand-off for hosts that already hold a MaterialAuthority.
func MaterialFromManager(manager materialManager) ports.MaterialAuthority {
	if manager == nil {
		return nil
	}
	return manager.Material()
}

// materialManager is the Manager subset required for forensic material hand-off.
type materialManager interface {
	Material() ports.MaterialAuthority
}

func New(backend Backend, config Config) (*Authority, error) {
	const operation = "forensic_artifacts.new"
	if isNilBackend(backend) {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "backend", "must be provided", nil)
	}
	if !config.DefaultOutputSensitivity.IsValid() {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "default_output_sensitivity", "is not recognized", nil)
	}
	if strings.TrimSpace(config.BundleRole) == "" || config.BundleRole != strings.TrimSpace(config.BundleRole) {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "bundle_role", "must not be blank", nil)
	}
	if !config.BundleSensitivity.IsValid() {
		return nil, domain.NewError(domain.CodeInvalidArgument, operation, "bundle_sensitivity", "is not recognized", nil)
	}
	overrides := make(map[string]domain.Sensitivity, len(config.OutputRoleSensitivities))
	for role, sensitivity := range config.OutputRoleSensitivities {
		if strings.TrimSpace(role) == "" || role != strings.TrimSpace(role) {
			return nil, domain.NewError(domain.CodeInvalidArgument, operation, "output_role_sensitivities", "contains a blank or non-canonical role", nil)
		}
		if !sensitivity.IsValid() {
			return nil, domain.NewError(domain.CodeInvalidArgument, operation, "output_role_sensitivities", "contains an invalid sensitivity", nil)
		}
		overrides[role] = sensitivity
	}
	return &Authority{
		backend: backend, defaultOutputSensitivity: config.DefaultOutputSensitivity,
		outputRoleSensitivities: overrides, bundleRole: config.BundleRole,
		bundleSensitivity: config.BundleSensitivity,
	}, nil
}

func (a *Authority) ResolveOccurrence(ctx context.Context, securityScope, reference string) (ports.ArtifactOccurrence, error) {
	const operation = "forensic_artifacts.resolve_occurrence"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.ArtifactOccurrence{}, err
	}
	scope, err := requireSecurityScope(ctx, operation)
	if err != nil {
		return ports.ArtifactOccurrence{}, err
	}
	if strings.TrimSpace(securityScope) == "" || securityScope != scope || strings.TrimSpace(reference) == "" {
		return ports.ArtifactOccurrence{}, scopeMismatch(operation)
	}
	resolved, err := a.backend.ResolveOccurrence(ctx, ResolveOccurrenceRequest{
		SecurityScope: scope, Reference: reference, Purpose: ResolveForInputView,
	})
	if err != nil {
		return ports.ArtifactOccurrence{}, repositoryError(operation, "reference", err)
	}
	if resolved.Reference != reference || resolved.SecurityScope != scope || resolved.Digest.IsZero() || resolved.Size < 0 {
		return ports.ArtifactOccurrence{}, domain.NewError(domain.CodeIntegrityViolation, operation, "reference", "repository returned a mismatched or invalid occurrence", nil)
	}
	return ports.ArtifactOccurrence{Reference: resolved.Reference, Digest: resolved.Digest, Size: resolved.Size}, nil
}

func (a *Authority) ResolveInputView(ctx context.Context, plan ports.InputPlan) (domain.InputViewManifest, error) {
	const operation = "forensic_artifacts.resolve_input_view"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return domain.InputViewManifest{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.InputViewManifest{}, err
	}
	scope, err := requireSecurityScope(ctx, operation)
	if err != nil {
		return domain.InputViewManifest{}, err
	}
	if scope != plan.SecurityScope {
		return domain.InputViewManifest{}, scopeMismatch(operation)
	}
	entries := make([]domain.InputViewEntry, 0, len(plan.Entries))
	for index, entryPlan := range plan.Entries {
		resolved, resolveErr := a.backend.ResolveOccurrence(ctx, ResolveOccurrenceRequest{
			SecurityScope: scope, Reference: entryPlan.Occurrence.Reference, Purpose: ResolveForInputView,
		})
		if resolveErr != nil {
			return domain.InputViewManifest{}, repositoryError(operation, fmt.Sprintf("entries[%d].occurrence", index), resolveErr)
		}
		if err := verifyResolvedOccurrence(resolved, entryPlan.Occurrence, scope); err != nil {
			return domain.InputViewManifest{}, err
		}
		if err := requireSidecars(entryPlan.PermittedSidecars, resolved.Sidecars); err != nil {
			return domain.InputViewManifest{}, domain.NewError(domain.CodeForbidden, operation, fmt.Sprintf("entries[%d].permitted_sidecars", index), "contains a sidecar unavailable to this occurrence", nil)
		}
		entry, entryErr := domain.NewInputViewEntry(domain.InputViewEntrySpec{
			LogicalPath: entryPlan.LogicalPath, OccurrenceRef: resolved.Reference,
			Digest: resolved.Digest, Size: resolved.Size, Mode: entryPlan.Mode,
			PermittedSidecars: append([]string(nil), entryPlan.PermittedSidecars...),
		})
		if entryErr != nil {
			return domain.InputViewManifest{}, entryErr
		}
		entries = append(entries, entry)
	}
	return domain.NewInputViewManifest(entries)
}

func (a *Authority) OpenContent(ctx context.Context, occurrence ports.ArtifactOccurrence) (ports.ContentReader, error) {
	const operation = "forensic_artifacts.open_content"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return nil, err
	}
	if err := occurrence.Validate(); err != nil {
		return nil, err
	}
	scope, err := requireSecurityScope(ctx, operation)
	if err != nil {
		return nil, err
	}
	resolved, err := a.backend.ResolveOccurrence(ctx, ResolveOccurrenceRequest{
		SecurityScope: scope, Reference: occurrence.Reference, Purpose: ResolveForRead,
	})
	if err != nil {
		return nil, repositoryError(operation, "occurrence", err)
	}
	if err := verifyResolvedOccurrence(resolved, occurrence, scope); err != nil {
		return nil, err
	}
	opened, err := a.backend.OpenObject(ctx, OpenObjectRequest{SecurityScope: scope, Reference: resolved.Reference})
	if err != nil {
		return nil, repositoryError(operation, "content", err)
	}
	if opened.Reader == nil {
		return nil, domain.NewError(domain.CodeIntegrityViolation, operation, "content", "repository returned no content stream", nil)
	}
	if err := verifySameOccurrence(opened.Occurrence, resolved); err != nil {
		_ = opened.Reader.Close()
		return nil, err
	}
	return newVerifiedContentReader(ctx, opened.Reader, occurrence.Digest, occurrence.Size), nil
}

func (a *Authority) CaptureOutputs(ctx context.Context, plan ports.OutputPlan) ([]domain.ArtifactReference, error) {
	const operation = "forensic_artifacts.capture_outputs"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return nil, err
	}
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	scope, err := requireSecurityScope(ctx, operation)
	if err != nil {
		return nil, err
	}
	baseProvenance, err := outputBaseProvenance(plan)
	if err != nil {
		return nil, err
	}
	selections := append([]domain.ExportSelection(nil), plan.Selections...)
	sort.Slice(selections, func(i, j int) bool { return selections[i].Spec().RelativePath < selections[j].Spec().RelativePath })
	items := make([]OutputCaptureItem, 0, len(selections))
	for _, selection := range selections {
		spec := selection.Spec()
		roles := make([]RoleBinding, 0, len(spec.Roles))
		for _, role := range spec.Roles {
			roles = append(roles, RoleBinding{Role: role, Sensitivity: a.sensitivityForRole(role)})
		}
		sortRoleBindings(roles)
		provenance := cloneMap(baseProvenance)
		provenance["world.logical_path"] = spec.RelativePath
		items = append(items, OutputCaptureItem{LogicalPath: spec.RelativePath, Content: plan.Content[spec.RelativePath], Roles: roles, Provenance: provenance})
	}
	request := OutputCaptureRequest{
		IdempotencyKey: plan.IdempotencyKey, SecurityScope: scope, LeaseID: plan.LeaseID,
		WorkspaceID: plan.WorkspaceID, AgentWorkspaceID: plan.AgentWorkspaceID,
		AgentGeneration: plan.AgentGeneration, Items: items,
	}
	published, err := a.backend.CaptureOutputs(ctx, request)
	if err != nil {
		return nil, repositoryError(operation, "outputs", err)
	}
	return verifyPublishedOutputs(operation, scope, items, published)
}

func (a *Authority) CaptureObservationBundle(ctx context.Context, plan ports.ObservationBundlePlan) (domain.ArtifactReference, error) {
	const operation = "forensic_artifacts.capture_observation_bundle"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.ArtifactReference{}, err
	}
	scope, err := requireSecurityScope(ctx, operation)
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	spec := plan.Bundle.Spec()
	provenance := map[string]string{
		"world.kind":                  "observation-bundle",
		"world.observation_bundle_id": plan.Bundle.ID().String(),
		"world.target_run_id":         spec.TargetRunID.String(),
		"world.target_id":             spec.TargetID.String(),
		"world.target_generation":     strconv.FormatUint(uint64(spec.TargetGeneration), 10),
		"world.agent_workspace_id":    spec.AgentWorkspaceID.String(),
		"world.agent_generation":      strconv.FormatUint(uint64(spec.AgentGeneration), 10),
		"world.content_digest":        plan.Content.Digest().String(),
		"world.content_size":          strconv.FormatInt(plan.Content.Size(), 10),
	}
	role := RoleBinding{Role: a.bundleRole, Sensitivity: a.bundleSensitivity}
	published, err := a.backend.CaptureObservationBundle(ctx, BundleCaptureRequest{
		IdempotencyKey: plan.IdempotencyKey, SecurityScope: scope, BundleID: plan.Bundle.ID(),
		TargetRunID: spec.TargetRunID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration,
		AgentWorkspaceID: spec.AgentWorkspaceID, AgentGeneration: spec.AgentGeneration,
		Content: plan.Content, Role: role, Provenance: provenance,
	})
	if err != nil {
		return domain.ArtifactReference{}, repositoryError(operation, "bundle", err)
	}
	if !published.Verified || published.Occurrence.SecurityScope != scope || published.Occurrence.Digest != plan.Content.Digest() || published.Occurrence.Size != plan.Content.Size() || !equalRole(published.Role, role) || !equalMap(published.Provenance, provenance) {
		return domain.ArtifactReference{}, publicationMismatch(operation, "bundle")
	}
	if err := validateRepositoryOccurrence(published.Occurrence); err != nil {
		return domain.ArtifactReference{}, err
	}
	return domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: published.Occurrence.Reference, Digest: published.Occurrence.Digest,
		Size: published.Occurrence.Size, Role: role.Role, Sensitivity: role.Sensitivity,
	})
}

func (a *Authority) sensitivityForRole(role string) domain.Sensitivity {
	if sensitivity, found := a.outputRoleSensitivities[role]; found {
		return sensitivity
	}
	return a.defaultOutputSensitivity
}

func outputBaseProvenance(plan ports.OutputPlan) (map[string]string, error) {
	const operation = "forensic_artifacts.capture_outputs"
	result := make(map[string]string, len(plan.Provenance)+5)
	for key, value := range plan.Provenance {
		if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) || strings.TrimSpace(value) == "" {
			return nil, domain.NewError(domain.CodeInvalidArgument, operation, "provenance", "must contain canonical non-blank keys and values", nil)
		}
		if strings.HasPrefix(key, reservedPrefix) {
			return nil, domain.NewError(domain.CodeForbidden, operation, "provenance", "caller provenance uses the reserved world namespace", nil)
		}
		result[key] = value
	}
	result["world.kind"] = "workspace-output"
	result["world.lease_id"] = plan.LeaseID.String()
	result["world.workspace_id"] = plan.WorkspaceID.String()
	result["world.agent_workspace_id"] = plan.AgentWorkspaceID.String()
	result["world.agent_generation"] = strconv.FormatUint(uint64(plan.AgentGeneration), 10)
	return result, nil
}

func verifyPublishedOutputs(operation, scope string, requested []OutputCaptureItem, published []PublishedOutput) ([]domain.ArtifactReference, error) {
	if len(published) != len(requested) {
		return nil, publicationMismatch(operation, "outputs")
	}
	byPath := make(map[string]PublishedOutput, len(published))
	for _, output := range published {
		if _, duplicate := byPath[output.LogicalPath]; duplicate {
			return nil, publicationMismatch(operation, "outputs")
		}
		byPath[output.LogicalPath] = output
	}
	result := make([]domain.ArtifactReference, 0)
	for _, item := range requested {
		output, found := byPath[item.LogicalPath]
		if !found || !output.Verified || output.Occurrence.SecurityScope != scope || output.Occurrence.Digest != item.Content.Digest() || output.Occurrence.Size != item.Content.Size() || !equalRoles(output.Roles, item.Roles) || !equalMap(output.Provenance, item.Provenance) {
			return nil, publicationMismatch(operation, "outputs")
		}
		if err := validateRepositoryOccurrence(output.Occurrence); err != nil {
			return nil, err
		}
		for _, role := range item.Roles {
			reference, err := domain.NewArtifactReference(domain.ArtifactReferenceSpec{
				Reference: output.Occurrence.Reference, Digest: output.Occurrence.Digest,
				Size: output.Occurrence.Size, Role: role.Role, Sensitivity: role.Sensitivity,
			})
			if err != nil {
				return nil, err
			}
			result = append(result, reference)
		}
	}
	return result, nil
}

func verifyResolvedOccurrence(resolved RepositoryOccurrence, expected ports.ArtifactOccurrence, scope string) error {
	if err := validateRepositoryOccurrence(resolved); err != nil {
		return err
	}
	if resolved.SecurityScope != scope {
		return scopeMismatch("forensic_artifacts.verify_occurrence")
	}
	if resolved.Digest != expected.Digest || resolved.Size != expected.Size {
		return domain.NewError(domain.CodeIntegrityViolation, "forensic_artifacts.verify_occurrence", "identity", "repository identity does not match the declared occurrence", nil)
	}
	return nil
}

func verifySameOccurrence(actual, expected RepositoryOccurrence) error {
	if err := validateRepositoryOccurrence(actual); err != nil {
		return err
	}
	if actual.Reference != expected.Reference || actual.Digest != expected.Digest || actual.Size != expected.Size || actual.SecurityScope != expected.SecurityScope {
		return domain.NewError(domain.CodeIntegrityViolation, "forensic_artifacts.open_content", "content", "opened object identity changed after resolution", nil)
	}
	return nil
}

func validateRepositoryOccurrence(occurrence RepositoryOccurrence) error {
	if occurrence.Digest.IsZero() || occurrence.Size < 0 || strings.TrimSpace(occurrence.SecurityScope) == "" {
		return domain.NewError(domain.CodeIntegrityViolation, "forensic_artifacts.verify_occurrence", "metadata", "repository returned invalid occurrence metadata", nil)
	}
	parsed, err := url.Parse(occurrence.Reference)
	if err != nil || occurrence.Reference != strings.TrimSpace(occurrence.Reference) || parsed.Scheme == "" || len(parsed.Scheme) == 1 || strings.EqualFold(parsed.Scheme, "file") || (parsed.Opaque == "" && parsed.Host == "") || parsed.User != nil || strings.ContainsAny(occurrence.Reference, "\\\x00") {
		return domain.NewError(domain.CodeIntegrityViolation, "forensic_artifacts.verify_occurrence", "reference", "repository returned a non-opaque or unsafe public reference", nil)
	}
	return nil
}

func requireSidecars(requested, available []string) error {
	set := make(map[string]struct{}, len(available))
	for _, sidecar := range available {
		set[sidecar] = struct{}{}
	}
	for _, sidecar := range requested {
		if _, found := set[sidecar]; !found {
			return domain.NewError(domain.CodeForbidden, "forensic_artifacts.sidecars", "sidecar", "is unavailable", nil)
		}
	}
	return nil
}

func requireSecurityScope(ctx context.Context, operation string) (string, error) {
	scope, ok := SecurityScopeFromContext(ctx)
	if !ok {
		return "", domain.NewError(domain.CodeUnauthorized, operation, "security_scope", "an authorized security scope is required", nil)
	}
	return scope, nil
}

func repositoryError(operation, field string, err error) error {
	code := domain.ErrorCodeOf(err)
	if errors.Is(err, context.DeadlineExceeded) {
		code = domain.CodeDeadlineExceeded
	} else if code == domain.CodeInternal {
		code = domain.CodeUnavailable
	}
	return domain.NewError(code, operation, field, "repository operation failed", nil)
}

func scopeMismatch(operation string) error {
	return domain.NewError(domain.CodeForbidden, operation, "security_scope", "repository operation crossed its authorized security scope", nil)
}

func publicationMismatch(operation, field string) error {
	return domain.NewError(domain.CodeIntegrityViolation, operation, field, "repository acknowledgement does not match the requested publication metadata", nil)
}

func sortRoleBindings(roles []RoleBinding) {
	sort.Slice(roles, func(i, j int) bool {
		if roles[i].Role == roles[j].Role {
			return roles[i].Sensitivity < roles[j].Sensitivity
		}
		return roles[i].Role < roles[j].Role
	})
}

func equalRoles(left, right []RoleBinding) bool {
	leftCopy, rightCopy := append([]RoleBinding(nil), left...), append([]RoleBinding(nil), right...)
	sortRoleBindings(leftCopy)
	sortRoleBindings(rightCopy)
	return reflect.DeepEqual(leftCopy, rightCopy)
}

func equalRole(left, right RoleBinding) bool { return left == right }

func equalMap(left, right map[string]string) bool { return reflect.DeepEqual(left, right) }

func cloneMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func isNilBackend(backend Backend) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ ports.MaterialAuthority = (*Authority)(nil)
