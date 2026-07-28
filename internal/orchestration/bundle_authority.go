package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

// BundleAuthority is a production, content-addressed local authority for
// sealed observation bundles. It intentionally refuses input resolution and
// workspace exports: those need a security-scope-aware repository adapter and
// must never be approximated by a shared local directory.
type BundleAuthority struct {
	root     string
	maxBytes int64
	mu       sync.Mutex
}

func (a *BundleAuthority) ResolveOccurrence(context.Context, string, string) (ports.ArtifactOccurrence, error) {
	return ports.ArtifactOccurrence{}, domain.NewError(domain.CodeCapabilityUnavailable, "bundle_authority.resolve_occurrence", "repository", "local bundle authority is not an input repository", nil)
}

func NewBundleAuthority(root string, maxBytes int64) (*BundleAuthority, error) {
	if strings.TrimSpace(root) == "" || maxBytes <= 0 {
		return nil, fmt.Errorf("bundle material root and positive byte limit are required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	absolute = filepath.Clean(absolute)
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	for _, logicalDirectory := range []string{"objects", "requests"} {
		namespace, err := openDurableNamespace(absolute, logicalDirectory)
		if err != nil {
			return nil, err
		}
		cleanupErr := cleanupDurableNamespaceStages(namespace)
		closeErr := namespace.Close()
		if cleanupErr != nil || closeErr != nil {
			return nil, errors.Join(cleanupErr, closeErr)
		}
	}
	return &BundleAuthority{root: absolute, maxBytes: maxBytes}, nil
}

func (a *BundleAuthority) ResolveInputView(context.Context, ports.InputPlan) (domain.InputViewManifest, error) {
	return domain.InputViewManifest{}, domain.NewError(domain.CodeCapabilityUnavailable, "bundle_authority.resolve_input_view", "repository", "local bundle authority is not an input repository", nil)
}

func (a *BundleAuthority) OpenContent(context.Context, ports.ArtifactOccurrence) (ports.ContentReader, error) {
	return nil, domain.NewError(domain.CodeCapabilityUnavailable, "bundle_authority.open_content", "repository", "local bundle authority does not expose unscoped content reads", nil)
}

func (a *BundleAuthority) CaptureOutputs(context.Context, ports.OutputPlan) ([]domain.ArtifactReference, error) {
	return nil, domain.NewError(domain.CodeCapabilityUnavailable, "bundle_authority.capture_outputs", "repository", "workspace export requires a security-scope-aware material authority", nil)
}

func (a *BundleAuthority) CaptureObservationBundle(ctx context.Context, plan ports.ObservationBundlePlan) (domain.ArtifactReference, error) {
	if err := ports.RequireDeadline(ctx, "bundle_authority.capture_observation_bundle"); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := plan.Validate(); err != nil {
		return domain.ArtifactReference{}, err
	}
	if plan.Content.Size() > a.maxBytes {
		return domain.ArtifactReference{}, domain.NewError(domain.CodeResourceExhausted, "bundle_authority.capture_observation_bundle", "content", "sealed bundle exceeds configured byte limit", nil)
	}
	reader, err := plan.Content.Open(ctx)
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	content, readErr := io.ReadAll(io.LimitReader(reader, a.maxBytes+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil {
		return domain.ArtifactReference{}, errors.Join(readErr, closeErr)
	}
	if int64(len(content)) != plan.Content.Size() || int64(len(content)) > a.maxBytes || domain.NewDigest(content) != plan.Content.Digest() {
		return domain.ArtifactReference{}, domain.NewError(domain.CodeIntegrityViolation, "bundle_authority.capture_observation_bundle", "content", "opened bytes do not match the sealed content identity", nil)
	}
	digestHex := strings.TrimPrefix(plan.Content.Digest().String(), "sha256:")
	objectFile := digestHex + ".json"
	requestHash := sha256.Sum256([]byte(plan.IdempotencyKey))
	requestFile := hex.EncodeToString(requestHash[:]) + ".json"
	marker := bundlePublication{BundleID: plan.Bundle.ID().String(), Digest: plan.Content.Digest().String(), Size: plan.Content.Size(), Object: objectFile}
	markerBytes, err := json.Marshal(marker)
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	objects, err := openDurableNamespace(a.root, "objects")
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	defer objects.Close()
	requests, err := openDurableNamespace(a.root, "requests")
	if err != nil {
		return domain.ArtifactReference{}, err
	}
	defer requests.Close()
	if err := ensureBundleFile(objects, objectFile, content); err != nil {
		return domain.ArtifactReference{}, err
	}
	if err := ensureBundleFile(requests, requestFile, markerBytes); err != nil {
		return domain.ArtifactReference{}, domain.NewError(domain.CodeConflict, "bundle_authority.capture_observation_bundle", "idempotency_key", "was reused for a different bundle publication", err)
	}
	return domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference: "world-material://observation-bundles/" + plan.Bundle.ID().String() + "/" + digestHex,
		Digest:    plan.Content.Digest(), Size: plan.Content.Size(), Role: "observation-bundle", Sensitivity: domain.SensitivityRestricted,
	})
}

type bundlePublication struct {
	BundleID string `json:"bundle_id"`
	Digest   string `json:"digest"`
	Size     int64  `json:"size"`
	Object   string `json:"object"`
}

func ensureBundleFile(namespace *safepath.Namespace, name string, desired []byte) error {
	if err := namespace.EnsureRegularAtomically(name, desired, 0o600); err != nil {
		if errors.Is(err, safepath.ErrConflict) {
			return fmt.Errorf("immutable file conflicts with desired bytes: %w", err)
		}
		return err
	}
	return nil
}

var _ ports.MaterialAuthority = (*BundleAuthority)(nil)
