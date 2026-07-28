// Package policyregistry publishes immutable effective policies and resolves
// the stable references carried by control-plane mutations.
package policyregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/policy"
)

const (
	referenceObjectKind = "effective_policy_reference"
	digestObjectKind    = "effective_policy_digest"
)

var (
	ErrInvalidDigest        = errors.New("invalid effective policy digest")
	ErrUnknownDigest        = errors.New("effective policy digest is not published")
	ErrPublicationIntegrity = errors.New("effective policy publication failed integrity verification")
)

// RegistryStore is the narrow durable-storage contract needed by Registry.
type RegistryStore interface {
	PutPolicy(context.Context, string, []byte) error
	Policy(context.Context, string) ([]byte, error)
	PutObject(context.Context, string, string, []byte) error
	Object(context.Context, string, string) ([]byte, error)
}

// Publication is the immutable record behind an authorized policy reference.
// Canonical policy bytes are held separately under PolicyDigest.
type Publication struct {
	Reference                   string                         `json:"reference"`
	Name                        string                         `json:"name"`
	Revision                    int64                          `json:"revision"`
	PolicyDigest                string                         `json:"policy_digest"`
	CapabilityFingerprintDigest string                         `json:"capability_fingerprint_digest"`
	Requirements                []policy.CapabilityRequirement `json:"requirements"`
	Resolutions                 []policy.CapabilityResolution  `json:"resolutions"`
	Warnings                    []policy.Warning               `json:"warnings,omitempty"`
	CapabilityFingerprint       CapabilityFingerprint          `json:"capability_fingerprint"`
}

// CapabilityFingerprint is the complete immutable input used to compile an
// effective policy. Persisting it lets resolution re-run capability evaluation
// instead of trusting stored resolution flags.
type CapabilityFingerprint struct {
	Capabilities map[string]Capability `json:"capabilities"`
	Evidence     map[string]string     `json:"evidence"`
}

type Capability struct {
	Status      policy.CapabilityStatus `json:"status"`
	Constraints map[string]string       `json:"constraints"`
	Evidence    map[string]string       `json:"evidence"`
}

// Snapshot contains a verified publication and a defensive copy of its
// canonical effective-policy document.
type Snapshot struct {
	Publication   Publication
	CanonicalJSON []byte
	Effective     *policy.EffectivePolicy
}

type Registry struct{ store RegistryStore }

func New(storage RegistryStore) (*Registry, error) {
	if storage == nil {
		return nil, errors.New("policy registry store is required")
	}
	return &Registry{store: storage}, nil
}

// Publish compiles a source policy against one immutable node fingerprint and
// publishes the resulting effective document under name@revision.
func (r *Registry) Publish(ctx context.Context, source []byte, capabilities policy.CapabilityFingerprint) (Snapshot, error) {
	effective, err := policy.Compile(source, policy.CompileOptions{Capabilities: capabilities})
	if err != nil {
		return Snapshot{}, err
	}
	return r.PublishEffective(ctx, effective, capabilities)
}

// PublishEffective idempotently publishes an already compiled policy. A
// reference can never be rebound to a different digest or capability result.
func (r *Registry) PublishEffective(ctx context.Context, effective *policy.EffectivePolicy, capabilities policy.CapabilityFingerprint) (Snapshot, error) {
	if effective == nil {
		return Snapshot{}, errors.New("effective policy is required")
	}
	if capabilities.Digest().IsZero() {
		return Snapshot{}, errors.New("capability fingerprint is required")
	}
	if effective.CapabilityFingerprintDigest() != capabilities.Digest() {
		return Snapshot{}, errors.New("effective policy capability fingerprint does not match publication fingerprint")
	}
	document := effective.Policy()
	reference := Reference(document.Metadata.Name, document.Metadata.Revision)
	publication := Publication{
		Reference:                   reference,
		Name:                        document.Metadata.Name,
		Revision:                    document.Metadata.Revision,
		PolicyDigest:                effective.Digest().String(),
		CapabilityFingerprintDigest: effective.CapabilityFingerprintDigest().String(),
		Requirements:                effective.CapabilityRequirements(),
		Resolutions:                 effective.CapabilityResolutions(),
		Warnings:                    effective.Warnings(),
		CapabilityFingerprint:       snapshotFingerprint(capabilities),
	}
	verified, err := verifyPublication(publication, effective.CanonicalJSON())
	if err != nil {
		return Snapshot{}, fmt.Errorf("verify effective policy publication: %w", err)
	}
	payload, err := json.Marshal(publication)
	if err != nil {
		return Snapshot{}, fmt.Errorf("encode policy publication: %w", err)
	}
	canonical := verified.CanonicalJSON()
	// Publishing content before its reference makes interrupted publication
	// leave at most an unreachable content-addressed document, never a dangling
	// authorized reference.
	if err := r.store.PutPolicy(ctx, publication.PolicyDigest, canonical); err != nil {
		return Snapshot{}, fmt.Errorf("store effective policy: %w", err)
	}
	if err := r.store.PutObject(ctx, referenceObjectKind, reference, payload); err != nil {
		return Snapshot{}, fmt.Errorf("store effective policy reference: %w", err)
	}
	if err := r.store.PutObject(ctx, digestObjectKind, digestBinding(publication.PolicyDigest, publication.CapabilityFingerprintDigest), payload); err != nil {
		return Snapshot{}, fmt.Errorf("store effective policy digest publication: %w", err)
	}
	return cloneSnapshot(Snapshot{Publication: publication, CanonicalJSON: canonical, Effective: verified}), nil
}

// Resolve verifies the reference record, content digest, and policy identity
// before returning it. Corrupt or partially published state fails closed.
func (r *Registry) Resolve(ctx context.Context, reference string) (Snapshot, error) {
	if strings.TrimSpace(reference) == "" {
		return Snapshot{}, errors.New("policy reference is required")
	}
	payload, err := r.store.Object(ctx, referenceObjectKind, reference)
	if err != nil {
		return Snapshot{}, err
	}
	var publication Publication
	if err := decodeExactJSON(payload, &publication); err != nil {
		return Snapshot{}, integrityError("decode effective policy reference", err)
	}
	if publication.Reference != reference || Reference(publication.Name, publication.Revision) != reference {
		return Snapshot{}, integrityError("effective policy reference identity mismatch", nil)
	}
	if err := r.requireMatchingPublication(ctx, digestObjectKind, digestBinding(publication.PolicyDigest, publication.CapabilityFingerprintDigest), payload); err != nil {
		return Snapshot{}, err
	}
	return r.resolvePublication(ctx, publication)
}

// ResolveDigest resolves only an exact policy and capability-fingerprint pair.
// A syntactically valid but unpublished pair is never treated as an opaque
// policy configuration.
func (r *Registry) ResolveDigest(ctx context.Context, policyDigest, capabilityDigest string) (Snapshot, error) {
	if _, err := domain.ParseDigest(policyDigest); err != nil {
		return Snapshot{}, fmt.Errorf("%w: policy: %v", ErrInvalidDigest, err)
	}
	if _, err := domain.ParseDigest(capabilityDigest); err != nil {
		return Snapshot{}, fmt.Errorf("%w: capability fingerprint: %v", ErrInvalidDigest, err)
	}
	payload, err := r.store.Object(ctx, digestObjectKind, digestBinding(policyDigest, capabilityDigest))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return Snapshot{}, fmt.Errorf("%w: %s with capabilities %s", ErrUnknownDigest, policyDigest, capabilityDigest)
		}
		return Snapshot{}, err
	}
	var publication Publication
	if err := decodeExactJSON(payload, &publication); err != nil {
		return Snapshot{}, integrityError("decode effective policy digest publication", err)
	}
	if publication.PolicyDigest != policyDigest || publication.CapabilityFingerprintDigest != capabilityDigest {
		return Snapshot{}, integrityError("effective policy digest publication identity mismatch", nil)
	}
	if err := r.requireMatchingPublication(ctx, referenceObjectKind, publication.Reference, payload); err != nil {
		return Snapshot{}, err
	}
	return r.resolvePublication(ctx, publication)
}

// requireMatchingPublication makes the reference and digest indexes a single
// fail-closed logical publication. The store API is intentionally narrow and
// does not expose a multi-object transaction, so readers require both immutable
// indexes to contain the exact same publication bytes before trusting either.
func (r *Registry) requireMatchingPublication(ctx context.Context, kind, id string, expected []byte) error {
	payload, err := r.store.Object(ctx, kind, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return integrityError("effective policy publication is missing its matching immutable index", err)
		}
		return err
	}
	if !bytes.Equal(payload, expected) {
		return integrityError("effective policy publication indexes disagree", nil)
	}
	return nil
}

func (r *Registry) resolvePublication(ctx context.Context, publication Publication) (Snapshot, error) {
	if publication.Reference == "" || Reference(publication.Name, publication.Revision) != publication.Reference {
		return Snapshot{}, integrityError("effective policy publication reference identity mismatch", nil)
	}
	if _, err := domain.ParseDigest(publication.PolicyDigest); err != nil {
		return Snapshot{}, integrityError("invalid stored effective policy digest", err)
	}
	if _, err := domain.ParseDigest(publication.CapabilityFingerprintDigest); err != nil {
		return Snapshot{}, integrityError("invalid stored capability fingerprint digest", err)
	}
	canonical, err := r.store.Policy(ctx, publication.PolicyDigest)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return Snapshot{}, integrityError("effective policy publication references missing canonical content", err)
		}
		return Snapshot{}, err
	}
	if actual := domain.NewDigest(canonical).String(); actual != publication.PolicyDigest {
		return Snapshot{}, integrityError(fmt.Sprintf("effective policy content digest mismatch: expected %s, got %s", publication.PolicyDigest, actual), nil)
	}
	effective, err := verifyPublication(publication, canonical)
	if err != nil {
		return Snapshot{}, err
	}
	return cloneSnapshot(Snapshot{Publication: publication, CanonicalJSON: canonical, Effective: effective}), nil
}

func Reference(name string, revision int64) string {
	return name + "@" + strconv.FormatInt(revision, 10)
}

func digestBinding(policyDigest, capabilityDigest string) string {
	return policyDigest + "@" + capabilityDigest
}

func verifyPublication(publication Publication, canonical []byte) (*policy.EffectivePolicy, error) {
	if publication.Reference == "" || Reference(publication.Name, publication.Revision) != publication.Reference {
		return nil, integrityError("effective policy publication reference identity mismatch", nil)
	}
	fingerprint, err := restoreFingerprint(publication.CapabilityFingerprint)
	if err != nil {
		return nil, integrityError("restore complete capability fingerprint", err)
	}
	if fingerprint.Digest().String() != publication.CapabilityFingerprintDigest {
		return nil, integrityError("capability fingerprint digest mismatch", nil)
	}
	effective, err := policy.Compile(canonical, policy.CompileOptions{Capabilities: fingerprint})
	if err != nil {
		return nil, integrityError("recompile canonical effective policy", err)
	}
	if effective.Digest().String() != publication.PolicyDigest || !bytes.Equal(effective.CanonicalJSON(), canonical) {
		return nil, integrityError("effective policy canonical digest mismatch", nil)
	}
	if effective.CapabilityFingerprintDigest().String() != publication.CapabilityFingerprintDigest {
		return nil, integrityError("effective policy capability fingerprint mismatch", nil)
	}
	document := effective.Policy()
	if document.Metadata.Name != publication.Name || document.Metadata.Revision != publication.Revision {
		return nil, integrityError("effective policy document identity mismatch", nil)
	}
	if publication.Warnings == nil {
		publication.Warnings = []policy.Warning{}
	}
	if !reflect.DeepEqual(effective.CapabilityRequirements(), publication.Requirements) ||
		!reflect.DeepEqual(effective.CapabilityResolutions(), publication.Resolutions) ||
		!reflect.DeepEqual(effective.Warnings(), publication.Warnings) {
		return nil, integrityError("effective policy capability resolution mismatch", nil)
	}
	return effective, nil
}

func integrityError(message string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrPublicationIntegrity, message)
	}
	return fmt.Errorf("%w: %s: %w", ErrPublicationIntegrity, message, cause)
}

func snapshotFingerprint(fingerprint policy.CapabilityFingerprint) CapabilityFingerprint {
	result := CapabilityFingerprint{
		Capabilities: make(map[string]Capability, len(fingerprint.Capabilities())),
		Evidence:     cloneStrings(fingerprint.Evidence()),
	}
	for name, capability := range fingerprint.Capabilities() {
		result.Capabilities[name] = Capability{
			Status: capability.Status(), Constraints: cloneStrings(capability.Constraints()), Evidence: cloneStrings(capability.Evidence()),
		}
	}
	return result
}

func restoreFingerprint(snapshot CapabilityFingerprint) (policy.CapabilityFingerprint, error) {
	capabilities := make(map[string]policy.Capability, len(snapshot.Capabilities))
	for name, value := range snapshot.Capabilities {
		capability, err := policy.NewCapability(value.Status, value.Constraints, value.Evidence)
		if err != nil {
			return policy.CapabilityFingerprint{}, fmt.Errorf("capability %q: %w", name, err)
		}
		capabilities[name] = capability
	}
	return policy.NewCapabilityFingerprint(capabilities, snapshot.Evidence)
}

func decodeExactJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	err := decoder.Decode(&trailing)
	if err == nil {
		return errors.New("trailing JSON value")
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func cloneSnapshot(value Snapshot) Snapshot {
	value.CanonicalJSON = append([]byte(nil), value.CanonicalJSON...)
	value.Publication.Requirements = cloneRequirements(value.Publication.Requirements)
	value.Publication.Resolutions = append([]policy.CapabilityResolution(nil), value.Publication.Resolutions...)
	for index := range value.Publication.Resolutions {
		value.Publication.Resolutions[index].Requirement = cloneRequirement(value.Publication.Resolutions[index].Requirement)
	}
	value.Publication.Warnings = append([]policy.Warning(nil), value.Publication.Warnings...)
	value.Publication.CapabilityFingerprint = cloneFingerprint(value.Publication.CapabilityFingerprint)
	return value
}

func cloneFingerprint(value CapabilityFingerprint) CapabilityFingerprint {
	result := CapabilityFingerprint{Capabilities: make(map[string]Capability, len(value.Capabilities)), Evidence: cloneStrings(value.Evidence)}
	for name, capability := range value.Capabilities {
		capability.Constraints = cloneStrings(capability.Constraints)
		capability.Evidence = cloneStrings(capability.Evidence)
		result.Capabilities[name] = capability
	}
	return result
}

func cloneRequirements(values []policy.CapabilityRequirement) []policy.CapabilityRequirement {
	result := append([]policy.CapabilityRequirement(nil), values...)
	for index := range result {
		result[index] = cloneRequirement(result[index])
	}
	return result
}

func cloneRequirement(value policy.CapabilityRequirement) policy.CapabilityRequirement {
	value.Constraints = cloneStrings(value.Constraints)
	return value
}

func cloneStrings(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

var _ RegistryStore = (*store.Store)(nil)
