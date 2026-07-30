package policyauthority

import (
	"fmt"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/policy"
)

// CapabilityComponent is one independently probed part of a physical
// composition. Name is persisted into the combined fingerprint, so callers
// must use a stable composition-owned name rather than a transient ID.
type CapabilityComponent struct {
	Name        string
	Fingerprint policy.CapabilityFingerprint
	Adapter     string
}

// CapabilityFacts are facts outside an individual runtime probe. The boolean
// filesystem fields are deliberately explicit: a configured directory driver
// must never be mistaken for an OverlayFS or reflink implementation.
type CapabilityFacts struct {
	HostOS            string
	HostArchitecture  string
	WorkspaceMode     string
	OverlayFS         bool
	Reflink           bool
	DirectoryCopy     bool
	Components        []CapabilityComponent
	IntrinsicCoverage map[string][]string
}

// BuildCapabilityFingerprint creates the one complete fingerprint used both
// for effective-policy publication and for subsequent physical-plan
// admission. It preserves every component capability under a namespaced key
// while also deriving the stable policy vocabulary from observed facts.
func BuildCapabilityFingerprint(facts CapabilityFacts) (policy.CapabilityFingerprint, error) {
	hostOS := strings.ToLower(strings.TrimSpace(facts.HostOS))
	architecture := strings.ToLower(strings.TrimSpace(facts.HostArchitecture))
	workspaceMode := strings.TrimSpace(facts.WorkspaceMode)
	if hostOS == "" || architecture == "" || workspaceMode == "" {
		return policy.CapabilityFingerprint{}, fmt.Errorf("host OS, architecture, and workspace mode are required")
	}
	components := append([]CapabilityComponent(nil), facts.Components...)
	sort.Slice(components, func(i, j int) bool { return components[i].Name < components[j].Name })
	capabilities := make(map[string]policy.Capability)
	evidence := map[string]string{
		"host.os": hostOS, "host.architecture": architecture, "workspace.mode": workspaceMode,
	}
	seen := make(map[string]struct{}, len(components))
	for _, component := range components {
		name := strings.TrimSpace(component.Name)
		if !validCapabilityComponentName(name) {
			return policy.CapabilityFingerprint{}, fmt.Errorf("capability component name %q is invalid", component.Name)
		}
		if _, duplicate := seen[name]; duplicate {
			return policy.CapabilityFingerprint{}, fmt.Errorf("capability component %q is duplicated", name)
		}
		seen[name] = struct{}{}
		if component.Fingerprint.Digest().IsZero() {
			return policy.CapabilityFingerprint{}, fmt.Errorf("capability component %q has an empty fingerprint", name)
		}
		evidence["component."+name+".digest"] = component.Fingerprint.Digest().String()
		for key, value := range component.Fingerprint.Evidence() {
			evidence["component."+name+".evidence."+key] = value
		}
		for capabilityName, capability := range component.Fingerprint.Capabilities() {
			rawName := "component." + name + "." + capabilityName
			if _, duplicate := capabilities[rawName]; duplicate {
				return policy.CapabilityFingerprint{}, fmt.Errorf("combined capability %q is duplicated", rawName)
			}
			capabilities[rawName] = capability
		}
	}

	addCapability(capabilities, "node.os."+hostOS, true, map[string]string{"architecture": architecture}, nil)
	addCapability(capabilities, "filesystem.overlayfs", facts.OverlayFS, nil, map[string]string{"workspace_mode": workspaceMode})
	addCapability(capabilities, "filesystem.reflink", facts.Reflink, nil, map[string]string{"workspace_mode": workspaceMode})
	directoryCopyHost := facts.DirectoryCopy && (hostOS == "windows" || hostOS == "darwin")
	addCapability(capabilities, "host.profile.directory-copy-non-production", directoryCopyHost,
		map[string]string{"production": "false", "hosts": "windows,darwin"}, map[string]string{"workspace_mode": workspaceMode, "host.os": hostOS})
	addCapability(capabilities, "filesystem.directory-copy.non-production", directoryCopyHost,
		map[string]string{"production": "false"}, map[string]string{"workspace_mode": workspaceMode})

	agentDocker := componentSupports(components, "agent.docker")
	targetLinux := componentSupports(components, "target.linux-container")
	addCapability(capabilities, "runtime.driver.docker", agentDocker || targetLinux, nil, nil)
	addCapability(capabilities, "runtime.isolation.agent-standard", componentSupports(components, "agent.hardened-isolation"), nil, nil)
	addCapability(capabilities, "target.kind.linux-container", targetLinux, nil, nil)
	visibility, visibilityFound := findComponentCapability(components, "target.visibility-first")
	visibilitySupported := visibilityFound && visibility.Status() == policy.CapabilitySupported
	runtimeName := strings.TrimSpace(visibility.Constraints()["runtime"])
	if runtimeName != "" {
		addCapability(capabilities, "runtime.oci."+runtimeName, visibilitySupported, nil, visibility.Evidence())
	}
	addCapability(capabilities, "runtime.isolation.observable-container", visibilitySupported, nil, nil)

	androidVirtual, androidFound := findComponentCapability(components, "target.android-virtual")
	androidManaged := androidFound && androidVirtual.Status() == policy.CapabilitySupported && componentEvidenceEquals(components, "target.android-virtual", "managed", "true")
	androidAccelerated := androidManaged && (strings.EqualFold(androidVirtual.Constraints()["hardware_acceleration"], "true") || strings.EqualFold(androidVirtual.Constraints()["kvm"], "true"))
	addCapability(capabilities, "target.kind.android-virtual-device", androidManaged, nil, nil)
	addCapability(capabilities, "runtime.driver.android-emulator", androidManaged, nil, nil)
	addCapability(capabilities, "runtime.isolation.instrumented-android", androidManaged, nil, nil)
	addCapability(capabilities, "android.hardware-acceleration", androidAccelerated, nil, nil)

	for _, component := range components {
		adapter := strings.TrimSpace(component.Adapter)
		if adapter == "" {
			continue
		}
		supported := componentSupports([]CapabilityComponent{component}, "observer."+adapter)
		addCapability(capabilities, "collector.adapter."+adapter, supported, nil, map[string]string{"component": component.Name})
	}
	for _, targetKind := range sortedKeys(facts.IntrinsicCoverage) {
		families := append([]string(nil), facts.IntrinsicCoverage[targetKind]...)
		sort.Strings(families)
		previous := ""
		for _, family := range families {
			targetKind, family = strings.TrimSpace(targetKind), strings.TrimSpace(family)
			if targetKind == "" || family == "" {
				return policy.CapabilityFingerprint{}, fmt.Errorf("intrinsic coverage names must not be blank")
			}
			if family == previous {
				return policy.CapabilityFingerprint{}, fmt.Errorf("intrinsic coverage %s/%s is duplicated", targetKind, family)
			}
			previous = family
			addCapability(capabilities, "coverage."+targetKind+"."+family, true, nil, map[string]string{"source": "composition"})
		}
	}
	return policy.NewCapabilityFingerprint(capabilities, evidence)
}

func addCapability(target map[string]policy.Capability, name string, supported bool, constraints, evidence map[string]string) {
	status := policy.CapabilityUnsupported
	if supported {
		status = policy.CapabilitySupported
	}
	capability, err := policy.NewCapability(status, constraints, evidence)
	if err != nil {
		panic("policyauthority: internally generated capability is invalid: " + err.Error())
	}
	target[name] = capability
}

func componentSupports(components []CapabilityComponent, name string) bool {
	capability, found := findComponentCapability(components, name)
	return found && capability.Status() == policy.CapabilitySupported
}

func findComponentCapability(components []CapabilityComponent, name string) (policy.Capability, bool) {
	for _, component := range components {
		if capability, found := component.Fingerprint.Capability(name); found {
			return capability, true
		}
	}
	return policy.Capability{}, false
}

func componentEvidenceEquals(components []CapabilityComponent, capabilityName, key, expected string) bool {
	for _, component := range components {
		capability, found := component.Fingerprint.Capability(capabilityName)
		if found && capability.Status() == policy.CapabilitySupported && strings.EqualFold(strings.TrimSpace(component.Fingerprint.Evidence()[key]), expected) {
			return true
		}
	}
	return false
}

func validCapabilityComponentName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' && character != '.' {
			return false
		}
	}
	return true
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
