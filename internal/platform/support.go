// Package platform reports host OS support for world control-plane features.
// Callers use Report at Open to log structured capabilities and fail closed
// when a requested composition exceeds what the host can enforce.
package platform

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

// Status is the support level for one feature on the current host.
type Status string

const (
	// StatusSupported means the host can enforce the feature's production contract.
	StatusSupported Status = "supported"
	// StatusPartial means a reduced contract is available; see Detail.
	StatusPartial Status = "partial"
	// StatusUnsupported means the feature fails closed on this host.
	StatusUnsupported Status = "unsupported"
)

// Feature is one independently reported host capability.
type Feature struct {
	ID      string `json:"id"`
	Status  Status `json:"status"`
	Summary string `json:"summary"`
	Detail  string `json:"detail,omitempty"`
}

// SupportReport is the structured host support matrix logged at Open.
type SupportReport struct {
	GOOS     string    `json:"goos"`
	GOARCH   string    `json:"goarch"`
	Features []Feature `json:"features"`
	// Warnings lists human-readable partial/unsupported notes for operators.
	Warnings []string `json:"warnings"`
}

// Feature IDs used in SupportReport and operator messages.
const (
	FeatureLogicalControlPlane        = "control_plane.logical"
	FeatureSafePathNamespace          = "safepath.namespace"
	FeatureProcessLock                = "processlock.exclusive"
	FeatureDirectoryCopyWorkspace     = "workspace.directory_copy_non_production"
	FeatureOverlayFSWorkspace         = "workspace.overlayfs"
	FeatureDockerLinuxTarget          = "target.linux_container.docker"
	FeatureDockerAgent                = "agent.docker"
	FeatureAndroidManagedEmulator     = "target.android_emulator.managed"
	FeatureAndroidResourceContainment = "target.android_emulator.resource_containment"
	FeatureHostPressurePSI            = "host.pressure.psi"
	FeatureGuestProcessTreeCleanup    = "guest.process_tree_cleanup"
	FeatureCollectorJobContainment    = "collector.process_containment"
)

// Report builds the current host's structured support matrix.
func Report() SupportReport {
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	features := []Feature{
		{
			ID: FeatureLogicalControlPlane, Status: StatusSupported,
			Summary: "In-process logical control plane (leases, generations, incidents, ledger)",
		},
		safePathFeature(goos),
		processLockFeature(goos),
		directoryCopyFeature(goos),
		{
			ID: FeatureOverlayFSWorkspace, Status: overlayStatus(goos),
			Summary: "Production OverlayFS agent workspaces",
			Detail:  overlayDetail(goos),
		},
		{
			ID: FeatureDockerAgent, Status: dockerStatus(goos),
			Summary: "Digest-pinned Docker agent workspaces (requires local Docker Engine)",
			Detail:  dockerDetail(goos),
		},
		{
			ID: FeatureDockerLinuxTarget, Status: dockerStatus(goos),
			Summary: "Docker Linux container targets (requires local Docker Engine)",
			Detail:  dockerDetail(goos),
		},
		androidManagedFeature(goos),
		androidContainmentFeature(goos),
		{
			ID: FeatureHostPressurePSI, Status: linuxOnlyStatus(goos),
			Summary: "Host PSI pressure sensing",
			Detail:  linuxOnlyDetail(goos, "cgroup v2 PSI"),
		},
		guestProcessTreeFeature(goos),
		collectorContainmentFeature(goos),
	}
	sort.Slice(features, func(i, j int) bool { return features[i].ID < features[j].ID })
	warnings := make([]string, 0)
	for _, feature := range features {
		if feature.Status == StatusSupported {
			continue
		}
		message := feature.ID + ": " + string(feature.Status) + " — " + feature.Summary
		if feature.Detail != "" {
			message += " (" + feature.Detail + ")"
		}
		warnings = append(warnings, message)
	}
	return SupportReport{GOOS: goos, GOARCH: goarch, Features: features, Warnings: warnings}
}

// Feature returns one feature by ID, or false if unknown.
func (r SupportReport) Feature(id string) (Feature, bool) {
	for _, feature := range r.Features {
		if feature.ID == id {
			return feature, true
		}
	}
	return Feature{}, false
}

// StatusOf returns the status for id, or StatusUnsupported when unknown.
func (r SupportReport) StatusOf(id string) Status {
	feature, ok := r.Feature(id)
	if !ok {
		return StatusUnsupported
	}
	return feature.Status
}

// JSON returns the canonical JSON encoding of the report.
func (r SupportReport) JSON() ([]byte, error) {
	return json.Marshal(r)
}

// CompactSummary is a single-line operator summary.
func (r SupportReport) CompactSummary() string {
	supported, partial, unsupported := 0, 0, 0
	for _, feature := range r.Features {
		switch feature.Status {
		case StatusSupported:
			supported++
		case StatusPartial:
			partial++
		default:
			unsupported++
		}
	}
	return fmt.Sprintf("goos=%s goarch=%s supported=%d partial=%d unsupported=%d warnings=%d",
		r.GOOS, r.GOARCH, supported, partial, unsupported, len(r.Warnings))
}

// RequireSafePathNamespaces fails closed when durable safepath namespaces are unavailable.
func RequireSafePathNamespaces() error {
	if Report().StatusOf(FeatureSafePathNamespace) != StatusSupported {
		return fmt.Errorf("%w: durable safepath namespaces require linux, windows, or darwin", safepath.ErrUnsupported)
	}
	return nil
}

// RequireAndroidManagedHost fails closed when managed Android has no host
// authority at all (for example Darwin). Partial hosts such as Linux may open
// the driver but resource containment and admission still fail closed when
// exact Job-equivalent limits are required.
func RequireAndroidManagedHost() error {
	status := Report().StatusOf(FeatureAndroidManagedEmulator)
	if status == StatusSupported || status == StatusPartial {
		return nil
	}
	feature, _ := Report().Feature(FeatureAndroidManagedEmulator)
	detail := feature.Detail
	if detail == "" {
		detail = feature.Summary
	}
	return fmt.Errorf("android-target-driver=android-emulator is %s on %s: %s", status, runtime.GOOS, detail)
}

// DirectoryCopyHost reports whether non-production directory-copy workspace
// policies may bind to this host OS.
func DirectoryCopyHost() bool {
	switch runtime.GOOS {
	case "windows", "darwin":
		return true
	default:
		return false
	}
}

func safePathFeature(goos string) Feature {
	switch goos {
	case "linux", "windows", "darwin":
		return Feature{
			ID: FeatureSafePathNamespace, Status: StatusSupported,
			Summary: "Durable safepath namespaces for bundles, markers, and material publication",
			Detail:  "Implemented for " + goos,
		}
	default:
		return Feature{
			ID: FeatureSafePathNamespace, Status: StatusUnsupported,
			Summary: "Durable safepath namespaces for bundles, markers, and material publication",
			Detail:  "Not implemented on " + goos + "; Open fails closed",
		}
	}
}

func processLockFeature(goos string) Feature {
	switch goos {
	case "linux", "windows", "darwin", "dragonfly", "freebsd", "netbsd", "openbsd", "solaris":
		return Feature{
			ID: FeatureProcessLock, Status: StatusSupported,
			Summary: "Exclusive processlock on the control-state database",
		}
	case "aix":
		return Feature{
			ID: FeatureProcessLock, Status: StatusUnsupported,
			Summary: "Exclusive processlock on the control-state database",
			Detail:  "AIX fails closed; sibling-file lock cannot prove stable namespace ownership",
		}
	default:
		return Feature{
			ID: FeatureProcessLock, Status: StatusUnsupported,
			Summary: "Exclusive processlock on the control-state database",
			Detail:  "No processlock implementation for " + goos,
		}
	}
}

func directoryCopyFeature(goos string) Feature {
	if DirectoryCopyHost() {
		return Feature{
			ID: FeatureDirectoryCopyWorkspace, Status: StatusSupported,
			Summary: "Non-production directory-copy agent workspaces",
			Detail:  "Qualified host profile for " + goos + "; not a production OverlayFS substitute",
		}
	}
	return Feature{
		ID: FeatureDirectoryCopyWorkspace, Status: StatusUnsupported,
		Summary: "Non-production directory-copy agent workspaces",
		Detail:  "directory-copy-non-production policies require windows or darwin (node profile)",
	}
}

func overlayStatus(goos string) Status {
	if goos == "linux" {
		return StatusSupported
	}
	return StatusUnsupported
}

func overlayDetail(goos string) string {
	if goos == "linux" {
		return "Requires kernel OverlayFS and production workspace composition"
	}
	return "OverlayFS production workspaces require a Linux node"
}

func dockerStatus(goos string) Status {
	switch goos {
	case "linux", "windows", "darwin":
		// Host can drive Docker CLI; Engine presence is probed at composition time.
		return StatusSupported
	default:
		return StatusUnsupported
	}
}

func dockerDetail(goos string) string {
	switch goos {
	case "darwin":
		return "Supported via Docker Desktop or compatible Engine; bind-mount and permission semantics differ from Linux nodes"
	case "windows":
		return "Supported via Docker Engine; Linux containers require a Linux Engine/desktop backend"
	case "linux":
		return "Supported via local Docker Engine"
	default:
		return "Docker composition is not supported on " + goos
	}
}

func androidManagedFeature(goos string) Feature {
	switch goos {
	case "windows":
		return Feature{
			ID: FeatureAndroidManagedEmulator, Status: StatusSupported,
			Summary: "Managed Android SDK Emulator targets with exact process identity and Job containment",
			Detail:  "Full managed composition with Windows Job resource limits",
		}
	case "linux":
		return Feature{
			ID: FeatureAndroidManagedEmulator, Status: StatusPartial,
			Summary: "Managed Android SDK Emulator process identity without host resource containment",
			Detail:  "pidfd identity is available; CPU/memory Job-equivalent containment is unsupported — managed resource admission fails closed",
		}
	default:
		return Feature{
			ID: FeatureAndroidManagedEmulator, Status: StatusUnsupported,
			Summary: "Managed Android SDK Emulator targets",
			Detail:  "No managed host-process authority on " + goos + "; use a Windows host for full managed Android or leave android-target-driver=none",
		}
	}
}

func androidContainmentFeature(goos string) Feature {
	if goos == "windows" {
		return Feature{
			ID: FeatureAndroidResourceContainment, Status: StatusSupported,
			Summary: "Exact managed-emulator CPU and memory containment via Windows Job Objects",
		}
	}
	return Feature{
		ID: FeatureAndroidResourceContainment, Status: StatusUnsupported,
		Summary: "Exact managed-emulator CPU and memory containment via Windows Job Objects",
		Detail:  "Only Windows implements named Job resource limits for managed emulators",
	}
}

func linuxOnlyStatus(goos string) Status {
	if goos == "linux" {
		return StatusSupported
	}
	return StatusUnsupported
}

func linuxOnlyDetail(goos, facility string) string {
	if goos == "linux" {
		return facility + " available when the kernel exposes it"
	}
	return facility + " requires Linux"
}

func guestProcessTreeFeature(goos string) Feature {
	switch goos {
	case "linux":
		return Feature{
			ID: FeatureGuestProcessTreeCleanup, Status: StatusSupported,
			Summary: "world-guest process-tree cleanup with parent-death SIGKILL",
		}
	case "windows":
		return Feature{
			ID: FeatureGuestProcessTreeCleanup, Status: StatusSupported,
			Summary: "world-guest process-tree cleanup via Windows Job assignment",
		}
	case "darwin":
		return Feature{
			ID: FeatureGuestProcessTreeCleanup, Status: StatusPartial,
			Summary: "world-guest signals only the direct child process",
			Detail:  "No Job Object or PR_SET_PDEATHSIG equivalent; daemonizing guests may outlive the supervisor",
		}
	default:
		return Feature{
			ID: FeatureGuestProcessTreeCleanup, Status: StatusPartial,
			Summary: "world-guest signals only the direct child process",
			Detail:  "Platform-specific process-tree ownership is not implemented on " + goos,
		}
	}
}

func collectorContainmentFeature(goos string) Feature {
	switch goos {
	case "windows":
		return Feature{
			ID: FeatureCollectorJobContainment, Status: StatusSupported,
			Summary: "Process observers assigned to kill-on-close Jobs",
		}
	case "linux":
		return Feature{
			ID: FeatureCollectorJobContainment, Status: StatusPartial,
			Summary: "Process observers use parent-death SIGKILL for the direct child",
			Detail:  "Adapters that daemonize remain unsupported without external cgroup proof",
		}
	default:
		return Feature{
			ID: FeatureCollectorJobContainment, Status: StatusPartial,
			Summary: "Process observers without Job/cgroup containment",
			Detail:  "Collector trees may leak helpers on " + goos,
		}
	}
}

// FormatWarningLines returns log-ready lines for each warning.
func (r SupportReport) FormatWarningLines() []string {
	lines := make([]string, 0, len(r.Warnings))
	for _, warning := range r.Warnings {
		lines = append(lines, "platform support warning: "+warning)
	}
	return lines
}

// EnabledDriverNotes returns warnings for drivers the operator selected that
// exceed host support. empty driver values are ignored.
func (r SupportReport) EnabledDriverNotes(androidDriver, linuxTargetDriver, agentDriver string) []string {
	notes := make([]string, 0)
	androidDriver = strings.TrimSpace(androidDriver)
	if androidDriver != "" && androidDriver != "none" {
		if status := r.StatusOf(FeatureAndroidManagedEmulator); status != StatusSupported {
			feature, _ := r.Feature(FeatureAndroidManagedEmulator)
			notes = append(notes, fmt.Sprintf("selected android-target-driver=%s is %s on %s: %s",
				androidDriver, status, r.GOOS, feature.Detail))
		}
		if r.StatusOf(FeatureAndroidResourceContainment) != StatusSupported {
			feature, _ := r.Feature(FeatureAndroidResourceContainment)
			notes = append(notes, fmt.Sprintf("android resource containment is %s: %s",
				feature.Status, feature.Detail))
		}
	}
	if strings.TrimSpace(linuxTargetDriver) == "docker" || strings.TrimSpace(agentDriver) == "docker" {
		if status := r.StatusOf(FeatureDockerAgent); status != StatusSupported {
			feature, _ := r.Feature(FeatureDockerAgent)
			notes = append(notes, fmt.Sprintf("Docker composition is %s on %s: %s", status, r.GOOS, feature.Detail))
		}
	}
	return notes
}
