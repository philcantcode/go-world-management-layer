// Package policyauthority resolves published effective policy identities and
// provides fail-closed admission checks over those immutable policies.
package policyauthority

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/policyregistry"
	"github.com/philcantcode/go-world-management-layer/policy"
)

var ErrPolicyDenied = errors.New("effective policy denied the operation")

// Violation identifies the policy-controlled input that was rejected.
type Violation struct {
	Field  string
	Reason string
}

func (v *Violation) Error() string {
	return fmt.Sprintf("effective policy denied %s: %s", v.Field, v.Reason)
}

func (v *Violation) Unwrap() error { return ErrPolicyDenied }

// Authority is a concrete fail-closed boundary over a durable policy registry.
// EffectivePolicy values returned by it are immutable and safe to share.
type Authority struct {
	registry *policyregistry.Registry
}

func New(registry *policyregistry.Registry) (*Authority, error) {
	if registry == nil {
		return nil, errors.New("policy registry is required")
	}
	return &Authority{registry: registry}, nil
}

// PublishYAML strictly compiles and publishes source YAML against the exact
// capability fingerprint used for admission.
func (a *Authority) PublishYAML(ctx context.Context, source []byte, capabilities policy.CapabilityFingerprint) (*policy.EffectivePolicy, error) {
	snapshot, err := a.registry.Publish(ctx, source, capabilities)
	if err != nil {
		return nil, err
	}
	return snapshot.Effective, nil
}

// PublishCompiled publishes an already compiled policy only when its bound
// capability digest matches the supplied complete fingerprint.
func (a *Authority) PublishCompiled(ctx context.Context, effective *policy.EffectivePolicy, capabilities policy.CapabilityFingerprint) (*policy.EffectivePolicy, error) {
	snapshot, err := a.registry.PublishEffective(ctx, effective, capabilities)
	if err != nil {
		return nil, err
	}
	return snapshot.Effective, nil
}

// Resolve accepts only canonical digests that identify a previously published
// policy/capability pair. Opaque names and unknown digest pairs fail closed.
func (a *Authority) Resolve(ctx context.Context, policyDigest, capabilityDigest string) (*policy.EffectivePolicy, error) {
	snapshot, err := a.registry.ResolveDigest(ctx, strings.TrimSpace(policyDigest), strings.TrimSpace(capabilityDigest))
	if err != nil {
		return nil, err
	}
	if snapshot.Effective == nil {
		return nil, errors.New("policy registry returned no verified effective policy")
	}
	return snapshot.Effective, nil
}

// ValidateIdentity prevents a resolved policy from being used for a different
// persisted session or physical generation.
func ValidateIdentity(effective *policy.EffectivePolicy, policyDigest, capabilityDigest string) error {
	if effective == nil {
		return deny("policy", "a verified effective policy is required")
	}
	if strings.TrimSpace(policyDigest) != effective.Digest().String() {
		return deny("policy_digest", "does not match the resolved policy")
	}
	if strings.TrimSpace(capabilityDigest) != effective.CapabilityFingerprintDigest().String() {
		return deny("capability_digest", "does not match the resolved capability fingerprint")
	}
	return nil
}

func deny(field, format string, values ...any) error {
	return &Violation{Field: field, Reason: fmt.Sprintf(format, values...)}
}

func requirePolicy(effective *policy.EffectivePolicy) (policy.Policy, error) {
	if effective == nil {
		return policy.Policy{}, deny("policy", "a verified effective policy is required")
	}
	return effective.Policy(), nil
}
