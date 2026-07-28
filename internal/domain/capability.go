package domain

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type CapabilityStatus string

const (
	CapabilityUnknown     CapabilityStatus = "unknown"
	CapabilityUnsupported CapabilityStatus = "unsupported"
	CapabilitySupported   CapabilityStatus = "supported"
)

func (s CapabilityStatus) IsValid() bool {
	return s == CapabilityUnknown || s == CapabilityUnsupported || s == CapabilitySupported
}

type Capability struct {
	status      CapabilityStatus
	constraints map[string]string
	evidence    map[string]string
}

func NewCapability(status CapabilityStatus, constraints, evidence map[string]string) (Capability, error) {
	if !status.IsValid() {
		return Capability{}, NewError(CodeInvalidArgument, "capability.new", "status", "is not recognized", nil)
	}
	if err := validateStringMap("capability.new", "constraints", constraints); err != nil {
		return Capability{}, err
	}
	if err := validateStringMap("capability.new", "evidence", evidence); err != nil {
		return Capability{}, err
	}
	return Capability{status: status, constraints: cloneMap(constraints), evidence: cloneMap(evidence)}, nil
}

func (c Capability) Status() CapabilityStatus       { return c.status }
func (c Capability) Constraints() map[string]string { return cloneMap(c.constraints) }
func (c Capability) Evidence() map[string]string    { return cloneMap(c.evidence) }

func validateStringMap(op, field string, values map[string]string) error {
	for key, value := range values {
		if err := requireNonBlank(field+".key", key); err != nil {
			return NewError(CodeInvalidArgument, op, field, "contains a blank key", err)
		}
		if err := requireNonBlank(field+"["+key+"]", value); err != nil {
			return NewError(CodeInvalidArgument, op, field, "contains a blank value", err)
		}
	}
	return nil
}

type CapabilityFingerprint struct {
	capabilities map[string]Capability
	evidence     map[string]string
	digest       Digest
}

func NewCapabilityFingerprint(capabilities map[string]Capability, evidence map[string]string) (CapabilityFingerprint, error) {
	if len(capabilities) == 0 {
		return CapabilityFingerprint{}, NewError(CodeInvalidArgument, "capability_fingerprint.new", "capabilities", "must not be empty", nil)
	}
	if err := validateStringMap("capability_fingerprint.new", "evidence", evidence); err != nil {
		return CapabilityFingerprint{}, err
	}
	owned := make(map[string]Capability, len(capabilities))
	for name, capability := range capabilities {
		if err := requireNonBlank("capabilities.name", name); err != nil {
			return CapabilityFingerprint{}, err
		}
		if !capability.status.IsValid() {
			return CapabilityFingerprint{}, NewError(CodeInvalidArgument, "capability_fingerprint.new", "capabilities["+name+"]", "has an invalid status", nil)
		}
		owned[name] = Capability{status: capability.status, constraints: cloneMap(capability.constraints), evidence: cloneMap(capability.evidence)}
	}
	canonical := canonicalCapabilityFingerprint(owned, evidence)
	return CapabilityFingerprint{capabilities: owned, evidence: cloneMap(evidence), digest: NewDigest(canonical)}, nil
}

func (f CapabilityFingerprint) Digest() Digest              { return f.digest }
func (f CapabilityFingerprint) Evidence() map[string]string { return cloneMap(f.evidence) }
func (f CapabilityFingerprint) Capability(name string) (Capability, bool) {
	capability, found := f.capabilities[name]
	if !found {
		return Capability{}, false
	}
	return Capability{status: capability.status, constraints: cloneMap(capability.constraints), evidence: cloneMap(capability.evidence)}, true
}
func (f CapabilityFingerprint) Capabilities() map[string]Capability {
	result := make(map[string]Capability, len(f.capabilities))
	for name, capability := range f.capabilities {
		result[name] = Capability{status: capability.status, constraints: cloneMap(capability.constraints), evidence: cloneMap(capability.evidence)}
	}
	return result
}

func canonicalCapabilityFingerprint(capabilities map[string]Capability, evidence map[string]string) []byte {
	var buffer bytes.Buffer
	writeCanonicalString(&buffer, "world.capability-fingerprint.v1")
	for _, name := range sortedKeys(capabilities) {
		capability := capabilities[name]
		writeCanonicalString(&buffer, name)
		writeCanonicalString(&buffer, string(capability.status))
		writeCanonicalMap(&buffer, capability.constraints)
		writeCanonicalMap(&buffer, capability.evidence)
	}
	writeCanonicalString(&buffer, "fingerprint-evidence")
	writeCanonicalMap(&buffer, evidence)
	return buffer.Bytes()
}

func writeCanonicalMap(buffer *bytes.Buffer, values map[string]string) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(values)))
	for _, key := range sortedKeys(values) {
		writeCanonicalString(buffer, key)
		writeCanonicalString(buffer, values[key])
	}
}

func writeCanonicalString(buffer *bytes.Buffer, value string) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	_, _ = buffer.WriteString(value)
}

type RequirementLevel string

const (
	RequirementRequired  RequirementLevel = "required"
	RequirementPreferred RequirementLevel = "preferred"
)

func (l RequirementLevel) IsValid() bool {
	return l == RequirementRequired || l == RequirementPreferred
}

// CapabilityRequirement is an input value. EvaluateCapabilityRequirements
// copies its constraint map before use.
type CapabilityRequirement struct {
	Name        string
	Level       RequirementLevel
	Constraints map[string]string
}

type CapabilityResolution struct {
	requirement CapabilityRequirement
	status      CapabilityStatus
	satisfied   bool
	downgraded  bool
	reason      string
}

func (r CapabilityResolution) Requirement() CapabilityRequirement {
	return CapabilityRequirement{Name: r.requirement.Name, Level: r.requirement.Level, Constraints: cloneMap(r.requirement.Constraints)}
}
func (r CapabilityResolution) Status() CapabilityStatus { return r.status }
func (r CapabilityResolution) Satisfied() bool          { return r.satisfied }
func (r CapabilityResolution) Downgraded() bool         { return r.downgraded }
func (r CapabilityResolution) Reason() string           { return r.reason }

type CapabilityEvaluation struct {
	admitted    bool
	resolutions []CapabilityResolution
}

func (e CapabilityEvaluation) Admitted() bool                      { return e.admitted }
func (e CapabilityEvaluation) Resolutions() []CapabilityResolution { return cloneSlice(e.resolutions) }
func (e CapabilityEvaluation) Downgrades() []CapabilityResolution {
	result := make([]CapabilityResolution, 0)
	for _, resolution := range e.resolutions {
		if resolution.downgraded {
			result = append(result, resolution)
		}
	}
	return result
}
func (e CapabilityEvaluation) Failures() []CapabilityResolution {
	result := make([]CapabilityResolution, 0)
	for _, resolution := range e.resolutions {
		if !resolution.satisfied && !resolution.downgraded {
			result = append(result, resolution)
		}
	}
	return result
}

func EvaluateCapabilityRequirements(fingerprint CapabilityFingerprint, requirements []CapabilityRequirement) (CapabilityEvaluation, error) {
	if fingerprint.digest.IsZero() {
		return CapabilityEvaluation{}, NewError(CodeInvalidArgument, "capability.evaluate", "fingerprint", "must be initialized", nil)
	}
	seen := make(map[string]struct{}, len(requirements))
	evaluation := CapabilityEvaluation{admitted: true, resolutions: make([]CapabilityResolution, 0, len(requirements))}
	for i, input := range requirements {
		requirement := CapabilityRequirement{Name: input.Name, Level: input.Level, Constraints: cloneMap(input.Constraints)}
		if err := requireNonBlank(fmt.Sprintf("requirements[%d].name", i), requirement.Name); err != nil {
			return CapabilityEvaluation{}, err
		}
		if !requirement.Level.IsValid() {
			return CapabilityEvaluation{}, NewError(CodeInvalidArgument, "capability.evaluate", fmt.Sprintf("requirements[%d].level", i), "is not recognized", nil)
		}
		if err := validateStringMap("capability.evaluate", fmt.Sprintf("requirements[%d].constraints", i), requirement.Constraints); err != nil {
			return CapabilityEvaluation{}, err
		}
		if _, duplicate := seen[requirement.Name]; duplicate {
			return CapabilityEvaluation{}, NewError(CodeInvalidArgument, "capability.evaluate", "requirements", "contains duplicate capability "+requirement.Name, nil)
		}
		seen[requirement.Name] = struct{}{}
		resolution := resolveCapability(fingerprint, requirement)
		if !resolution.satisfied && requirement.Level == RequirementRequired {
			evaluation.admitted = false
		}
		evaluation.resolutions = append(evaluation.resolutions, resolution)
	}
	return evaluation, nil
}

func resolveCapability(fingerprint CapabilityFingerprint, requirement CapabilityRequirement) CapabilityResolution {
	capability, found := fingerprint.capabilities[requirement.Name]
	if !found {
		capability.status = CapabilityUnknown
	}
	resolution := CapabilityResolution{requirement: requirement, status: capability.status}
	switch capability.status {
	case CapabilitySupported:
		for key, required := range requirement.Constraints {
			actual, exists := capability.constraints[key]
			if !exists || actual != required {
				resolution.reason = fmt.Sprintf("constraint %s requires %q, got %q", key, required, actual)
				resolution.downgraded = requirement.Level == RequirementPreferred
				return resolution
			}
		}
		resolution.satisfied = true
		resolution.reason = "supported"
	case CapabilityUnsupported:
		resolution.reason = "unsupported"
		resolution.downgraded = requirement.Level == RequirementPreferred
	case CapabilityUnknown:
		resolution.reason = "unknown"
		resolution.downgraded = requirement.Level == RequirementPreferred
	default:
		resolution.reason = "invalid capability status"
	}
	return resolution
}
