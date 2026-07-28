package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// CompileOptions supplies the immutable node capability fingerprint used for
// required/preferred capability resolution.
type CompileOptions struct {
	Capabilities CapabilityFingerprint
}

// EffectivePolicy is immutable. Every accessor returns a value or defensive
// copy, and the canonical bytes used for its digest are never exposed directly.
type EffectivePolicy struct {
	canonical             []byte
	digest                Digest
	capabilityFingerprint Digest
	requirements          []CapabilityRequirement
	resolutions           []CapabilityResolution
	warnings              []Warning
}

// Compile strictly decodes, defaults, validates, resolves capabilities, and
// canonicalizes one policy document.
func Compile(source []byte, options CompileOptions) (*EffectivePolicy, error) {
	policy, positions, requirements, err := prepare(source)
	if err != nil {
		return nil, err
	}
	resolutions, warnings, err := evaluateCapabilities(options.Capabilities, requirements, positions)
	if err != nil {
		return nil, err
	}
	canonical, err := marshalCanonical(policy)
	if err != nil {
		return nil, fmt.Errorf("canonicalize policy: %w", err)
	}
	return &EffectivePolicy{
		canonical:             canonical,
		digest:                domainDigest(canonical),
		capabilityFingerprint: options.Capabilities.Digest(),
		requirements:          cloneRequirements(requirements),
		resolutions:           cloneResolutions(resolutions),
		warnings:              cloneWarnings(warnings),
	}, nil
}

// Requirements strictly decodes, defaults, and validates a policy before
// returning the capabilities a node must resolve. It allows schedulers to
// select a compatible node without weakening Compile's fail-closed behavior.
func Requirements(source []byte) ([]CapabilityRequirement, error) {
	_, _, requirements, err := prepare(source)
	if err != nil {
		return nil, err
	}
	return cloneRequirements(requirements), nil
}

func prepare(source []byte) (Policy, map[string]sourcePosition, []CapabilityRequirement, error) {
	policy, positions, err := decodeStrict(source)
	if err != nil {
		return Policy{}, nil, nil, err
	}
	applyDefaults(&policy, positions)
	if err := validatePolicy(&policy, positions); err != nil {
		return Policy{}, nil, nil, err
	}
	return policy, positions, deriveCapabilityRequirements(&policy), nil
}

// Policy returns a deep copy of the defaulted policy DTO.
func (e *EffectivePolicy) Policy() Policy {
	if e == nil {
		return Policy{}
	}
	var policy Policy
	if err := json.Unmarshal(e.canonical, &policy); err != nil {
		panic("policy: internally generated canonical JSON could not be decoded: " + err.Error())
	}
	return policy
}

// CanonicalJSON returns a defensive copy of deterministic canonical policy JSON.
func (e *EffectivePolicy) CanonicalJSON() []byte {
	if e == nil {
		return nil
	}
	result := make([]byte, len(e.canonical))
	copy(result, e.canonical)
	return result
}

func (e *EffectivePolicy) Digest() Digest {
	if e == nil {
		return Digest{}
	}
	return e.digest
}

func (e *EffectivePolicy) CapabilityFingerprintDigest() Digest {
	if e == nil {
		return Digest{}
	}
	return e.capabilityFingerprint
}

func (e *EffectivePolicy) CapabilityRequirements() []CapabilityRequirement {
	if e == nil {
		return nil
	}
	return cloneRequirements(e.requirements)
}

func (e *EffectivePolicy) CapabilityResolutions() []CapabilityResolution {
	if e == nil {
		return nil
	}
	return cloneResolutions(e.resolutions)
}

func (e *EffectivePolicy) Warnings() []Warning {
	if e == nil {
		return nil
	}
	return cloneWarnings(e.warnings)
}

func marshalCanonical(policy Policy) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(policy); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func domainDigest(canonical []byte) Digest {
	// The domain digest provides the common sha256:<hex> representation used by
	// policy, event, provenance, and capability models.
	return newDigest(canonical)
}
