package policyauthority

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/policyregistry"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func TestAuthorityPublishesResolvesAndRejectsUntrustedIdentities(t *testing.T) {
	ctx := context.Background()
	authority, source, fingerprint := testAuthority(t)
	effective, err := authority.PublishYAML(ctx, source, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.PublishCompiled(ctx, effective, fingerprint); err != nil {
		t.Fatalf("publish compiled policy: %v", err)
	}
	invalidSource := append(append([]byte(nil), source...), []byte("\nunknownRootField: true\n")...)
	if _, err := authority.PublishYAML(ctx, invalidSource, fingerprint); err == nil {
		t.Fatal("non-strict policy YAML was published")
	}
	resolved, err := authority.Resolve(ctx, effective.Digest().String(), effective.CapabilityFingerprintDigest().String())
	if err != nil || resolved.Digest() != effective.Digest() {
		t.Fatalf("resolved = %#v, %v", resolved, err)
	}
	copy := resolved.Policy()
	copy.Metadata.Name = "mutated"
	if resolved.Policy().Metadata.Name == "mutated" {
		t.Fatal("effective policy exposed mutable state")
	}
	if _, err := authority.Resolve(ctx, "policy:opaque", "capabilities:opaque"); !errors.Is(err, policyregistry.ErrInvalidDigest) {
		t.Fatalf("opaque identity error = %v", err)
	}
	unknown := domain.NewDigest([]byte("unknown")).String()
	if _, err := authority.Resolve(ctx, unknown, effective.CapabilityFingerprintDigest().String()); !errors.Is(err, policyregistry.ErrUnknownDigest) {
		t.Fatalf("unknown identity error = %v", err)
	}
	otherFingerprint, err := policy.NewCapabilityFingerprint(fingerprint.Capabilities(), map[string]string{"node": "other"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.PublishCompiled(ctx, effective, otherFingerprint); err == nil {
		t.Fatal("compiled policy was rebound to a different capability fingerprint")
	}
}

func TestPolicyAdmissionHelpers(t *testing.T) {
	ctx := context.Background()
	authority, source, fingerprint := testAuthority(t)
	effective, err := authority.PublishYAML(ctx, source, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	document := effective.Policy()
	identity := SessionAdmission{PolicyDigest: effective.Digest().String(), CapabilityDigest: effective.CapabilityFingerprintDigest().String(), TTL: time.Hour}
	if err := ValidateSessionAcquisition(effective, identity); err != nil {
		t.Fatal(err)
	}
	identity.TTL = 3 * time.Hour
	requireDenied(t, ValidateSessionAcquisition(effective, identity))

	workspace := WorkspaceAdmission{
		Mode: document.Spec.Workspace.Mode, Construction: document.Spec.Workspace.InputView.Construction,
		UpperBytes: 1 << 30, UpperInodes: document.Spec.AgentWorkspace.Resources.Limits.WorkspaceInodes,
	}
	if err := ValidateWorkspace(effective, workspace); err != nil {
		t.Fatal(err)
	}
	workspace.UpperBytes = document.Spec.AgentWorkspace.Resources.Limits.Workspace.Bytes() + 1
	requireDenied(t, ValidateWorkspace(effective, workspace))

	export := ExportAdmission{DeclarationAuthority: "host", FileCount: 2, Bytes: 1024}
	if err := ValidateExport(effective, export); err != nil {
		t.Fatal(err)
	}
	export.ContainsNonRegular = true
	requireDenied(t, ValidateExport(effective, export))
	export.ContainsNonRegular = false
	export.DeclarationAuthority = ""
	requireDenied(t, ValidateExport(effective, export))
	export.DeclarationAuthority = "host"
	export.FinalPublication = true
	requireDenied(t, ValidateExport(effective, export))
	export.RetainsFullChangeManifest = true
	if err := ValidateExport(effective, export); err != nil {
		t.Fatal(err)
	}

	network := document.Spec.AgentWorkspace.Network
	runtime := document.Spec.AgentWorkspace.Runtime
	agent := AgentPlanAdmission{
		Runtime: AgentRuntimeAdmission{
			Driver: runtime.Driver, ImageDigest: pinnedDigest(runtime.Image), IsolationProfile: runtime.IsolationProfile,
			RootFilesystem: runtime.RootFilesystem, User: runtime.User, CapabilityDrop: runtime.Capabilities.Drop,
			CapabilityAdd: runtime.Capabilities.Add, NoNewPrivileges: runtime.NoNewPrivileges, SeccompProfile: runtime.SeccompProfile,
			UserEnforced: true, SeccompEnforced: true,
		},
		Resources: RuntimeResources{
			CPUMilli: document.Spec.AgentWorkspace.Resources.Requests.CPU.MilliCPU(), MemoryBytes: document.Spec.AgentWorkspace.Resources.Requests.Memory.Bytes(),
			WorkspaceBytes: document.Spec.AgentWorkspace.Resources.Requests.Workspace.Bytes(),
			CaptureBytes:   1 << 20, Inodes: document.Spec.AgentWorkspace.Resources.Limits.WorkspaceInodes, PIDs: 100,
		},
	}
	if err := ValidateAgentPlan(effective, agent); err != nil {
		t.Fatal(err)
	}
	agent.Resources.CPUMilli = 1
	requireDenied(t, ValidateAgentPlan(effective, agent))
	agent.Resources.CPUMilli = document.Spec.AgentWorkspace.Resources.Requests.CPU.MilliCPU()
	agent.Runtime.NoNewPrivileges = false
	requireDenied(t, ValidateAgentPlan(effective, agent))
	agent.Runtime.NoNewPrivileges = true
	agent.Runtime.CapabilityAdd = []string{"SYS_ADMIN"}
	requireDenied(t, ValidateAgentPlan(effective, agent))
	actualNetwork := NetworkAdmission{Mode: network.Mode, AllowDNS: network.AllowDNS, AllowedCIDRs: network.AllowedCIDRs, AllowedDomains: network.AllowedDomains, DenyPrivateRanges: network.DenyPrivateRanges, TargetAccess: network.TargetAccess}
	if err := ValidateNetwork(effective, actualNetwork); err != nil {
		t.Fatal(err)
	}
	actualNetwork.AllowedCIDRs = []string{"0.0.0.0/0"}
	requireDenied(t, ValidateNetwork(effective, actualNetwork))

	template := document.Spec.Targets.Templates[0]
	target := TargetAdmission{
		Template: template.Name, Kind: string(domain.TargetLinuxContainer), Driver: template.Runtime.Driver,
		Runtime: template.Runtime.Runtime, ImageDigest: pinnedDigest(template.Runtime.Image), IsolationProfile: template.Runtime.IsolationProfile,
		BaseImage: template.Runtime.BaseImage, User: template.Runtime.User, CapabilityDrop: template.Runtime.Capabilities.Drop,
		CapabilityAdd: template.Runtime.Capabilities.Add, NoNewPrivileges: template.Runtime.NoNewPrivileges,
		SeccompProfile: template.Runtime.SeccompProfile, UserEnforced: true, SeccompEnforced: true,
		MaterialMountPoint: template.Material.MountPoint, WritableStateMode: template.Material.WritableState, WritableStateEnforced: true,
		CommandAuthority: template.Interaction.CommandAuthority, ExecTransport: template.Interaction.ExecTransport,
		FileTransfer: template.Interaction.FileTransfer, NetworkEndpoints: template.Interaction.NetworkEndpoints,
		DeniedInfrastructureAuthority: template.Interaction.DeniedInfrastructureAuthority,
		ResetAfterEveryRun:            template.Reset.AfterEveryRun, ResetMode: template.Reset.Mode,
		ConcurrentTargets: 0, Resources: RuntimeResources{CPUMilli: 1000, MemoryBytes: 1 << 30, WritableStateBytes: 1 << 30, PIDs: 100},
	}
	if err := ValidateTarget(effective, target); err != nil {
		t.Fatal(err)
	}
	target.ConcurrentTargets = document.Spec.Targets.MaxConcurrent
	requireDenied(t, ValidateTarget(effective, target))
	target.ConcurrentTargets = 0
	target.Resources.CPUMilli = 0
	requireDenied(t, ValidateTarget(effective, target))

	coverage := document.Spec.Observation.RequiredCoverage[template.Kind]
	run := TargetRunAdmission{Template: template.Name, MaterialBytes: 1024, MaximumDuration: time.Hour, RequiredCoverage: coverage}
	if err := ValidateTargetRun(effective, run); err != nil {
		t.Fatal(err)
	}
	captureLimit := document.Spec.Resources.AggregateLimits.CaptureBytes.Bytes()
	run.Collectors = []CollectorAdmission{
		{Adapter: document.Spec.Observation.Collectors.LinuxMetadata.Adapter, Placement: document.Spec.Observation.Collectors.LinuxMetadata.Placement, MaximumBytes: captureLimit/2 + 1},
		{Adapter: document.Spec.Observation.Collectors.PacketCapture.Adapter, Placement: document.Spec.Observation.Collectors.PacketCapture.Placement, MaximumBytes: captureLimit/2 + 1},
	}
	requireDenied(t, ValidateTargetRun(effective, run))
	run.Collectors = nil
	run.RequiredCoverage = coverage[1:]
	requireDenied(t, ValidateTargetRun(effective, run))

	capture := CaptureAdmission{
		Name: "packetPayload", SignalFamilies: append([]string(nil), document.Spec.Observation.AllowedOnDemand.PacketPayload.SignalFamilies...),
		Duration: time.Minute, Bytes: 1 << 20, HasFlowFilter: true,
	}
	if err := ValidateCapture(effective, capture); err != nil {
		t.Fatal(err)
	}
	capture.HasFlowFilter = false
	requireDenied(t, ValidateCapture(effective, capture))
	capture.HasFlowFilter = true
	capture.SignalFamilies = []string{"policy-undeclared-family"}
	requireDenied(t, ValidateCapture(effective, capture))
	lifecycle := CaptureAdmission{Name: "worldLifecycle", SignalFamilies: []string{"target.lifecycle"}, Duration: time.Minute, Bytes: 1 << 20}
	if err := ValidateCapture(effective, lifecycle); err != nil {
		t.Fatalf("worldLifecycle capture denied: %v", err)
	}
	lifecycle.Name = "perfetto"
	requireDenied(t, ValidateCapture(effective, lifecycle))

	if err := ValidateAggregateResources(effective, RuntimeResources{CPUMilli: 1000, MemoryBytes: 1 << 30, CaptureBytes: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateAggregateResources(effective, RuntimeResources{}); err != nil {
		t.Fatalf("zero aggregate usage must remain valid: %v", err)
	}
	requireDenied(t, ValidateAggregateResources(effective, RuntimeResources{CPUMilli: document.Spec.Resources.AggregateLimits.CPU.MilliCPU() + 1}))
}

func TestDisabledPayloadProfileCannotBeAdmitted(t *testing.T) {
	_, source, fingerprint := testAuthority(t)
	disabled := []byte(strings.Replace(string(source), "enabled: authorized-on-demand", "enabled: disabled", 1))
	if string(disabled) == string(source) {
		t.Fatal("payload enabled fixture was not found")
	}
	effective, err := policy.Compile(disabled, policy.CompileOptions{Capabilities: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	err = ValidateCapture(effective, CaptureAdmission{
		Name: "payload", SignalFamilies: effective.Policy().Spec.Observation.Profiles.Payload.SignalFamilies,
		Duration: time.Minute, Bytes: 1024, HasProcessOrPathFilter: true,
	})
	requireDenied(t, err)
}

func testAuthority(t *testing.T) (*Authority, []byte, policy.CapabilityFingerprint) {
	t.Helper()
	ctx := context.Background()
	controlStore, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "control.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	registry, err := policyregistry.New(controlStore)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := New(registry)
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "examples", "environment-policy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	requirements, err := policy.Requirements(source)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := make(map[string]policy.Capability, len(requirements))
	for _, requirement := range requirements {
		capabilities[requirement.Name], err = policy.NewCapability(policy.CapabilitySupported, requirement.Constraints, map[string]string{"test": "supported"})
		if err != nil {
			t.Fatal(err)
		}
	}
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"node": "test"})
	if err != nil {
		t.Fatal(err)
	}
	return authority, source, fingerprint
}

func requireDenied(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrPolicyDenied) {
		t.Fatalf("error = %v, want policy denial", err)
	}
}
