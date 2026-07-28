package orchestration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func TestPolicyAdmissionResolveAgentRecoveryRevalidatesWithoutNewTTL(t *testing.T) {
	source, err := os.ReadFile("../../policy/deployment/e2e-directory-copy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	effective := compileSupportedPolicy(t, source)
	if err := policyauthority.ValidateTTL(effective, 0); err == nil {
		t.Fatal("fixture unexpectedly permits an acquisition with zero TTL")
	}

	content := testkit.NewMemoryContentSource([]byte("recovery input"))
	entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
		LogicalPath: "input/specimen.bin", OccurrenceRef: "memory://recovery-input",
		Digest: content.Digest(), Size: content.Size(), Mode: 0o444,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := domain.NewInputViewManifest([]domain.InputViewEntry{entry})
	if err != nil {
		t.Fatal(err)
	}
	image, err := domain.ParseDigest("sha256:6105d6cc76af4009c44e4692f219054456e7111487afb0c71077d9f887668fef")
	if err != nil {
		t.Fatal(err)
	}
	resolved := ResolvedAcquisition{
		InputView: manifest, SecurityScope: "recovery", Construction: domain.InputViewAllowCopy,
		Content: map[string]ports.ContentSource{"input/specimen.bin": content}, UpperByteLimit: 1 << 20, UpperInodeLimit: 128,
		PolicyDigest: effective.Digest(), CapabilityDigest: effective.CapabilityFingerprintDigest(), ImageDigest: image,
		Resources: admission.Resources{CPUMilli: 250, MemoryBytes: 32 << 20, CaptureBytes: 1 << 20, PIDs: 128},
	}
	reporter := enforcedAgentPhysicalReport()
	resolver, err := NewPolicyAdmissionResolver(PolicyAdmissionConfig{
		Base: &recoveryResolverStub{resolved: resolved}, Policies: effectiveResolverStub{effective: effective},
		WorkspaceMode: "directory-copy-non-production", AgentPhysical: reporter,
		ResourceInventory: emptyPolicyResourceInventory,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := application.RecoverIncidentRequest{IncidentID: "incident-agent-policy-test"}
	request.Resource = application.RecoveryResourceAgent
	request.Strategy = string(ports.ResetRecreate)
	view := application.ResearchSessionView{
		Session: application.SessionRecord{ID: "session-agent-policy-test"},
		Lease:   application.LeaseRecord{ID: "lease-agent-policy-test"},
		Agent: application.AgentWorkspaceRecord{ID: "agent-policy-test", CurrentGeneration: 1, Generations: []application.AgentGenerationRecord{{
			Generation: 1, State: domain.AgentGenerationProvisioning,
		}}},
	}
	view.Incidents = []application.IncidentRecord{{
		ID: request.IncidentID, Classification: domain.IncidentAgentWorkspaceFailure, State: domain.IncidentRecovering,
		SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: view.Agent.ID, AgentGeneration: 1,
	}}
	actual, err := resolver.ResolveAgentRecovery(ctx, request, view)
	if err != nil {
		t.Fatal(err)
	}
	if actual.PolicyDigest != effective.Digest() || actual.CapabilityDigest != effective.CapabilityFingerprintDigest() {
		t.Fatal("recovery did not retain the exact effective-policy identity")
	}
}

func TestPolicyAdmissionAdmitAgentWorkspacePlanBindsExactPhysicalReport(t *testing.T) {
	source, err := os.ReadFile("../../policy/deployment/e2e-directory-copy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	effective := compileSupportedPolicy(t, source)
	plan := agentPhysicalPlan(t, effective)
	raw := enforcedAgentPhysicalReport()
	raw.Resources.WorkspaceBytes = ports.PhysicalLimitFact{Support: ports.PhysicalSupportUnsupported, Detail: "runtime does not bound workspace bytes"}
	raw.Resources.Inodes = ports.PhysicalLimitFact{Support: ports.PhysicalSupportUnsupported, Detail: "runtime does not bound workspace inodes"}
	raw.Resources.CaptureBytes = ports.PhysicalLimitFact{Support: ports.PhysicalSupportUnsupported, Detail: "capture storage is outside the runtime"}
	rawReporter := &agentPhysicalReporterStub{configured: raw}
	reporter, err := policyauthority.NewAgentPhysicalPolicyReporter(rawReporter, policyauthority.AgentPhysicalEnforcement{
		DirectoryWorkspace: true, BoundedLedgerCapture: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	configured, err := reporter.AgentWorkspacePhysicalPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewPolicyAdmissionResolver(PolicyAdmissionConfig{
		Base: &recoveryResolverStub{}, Policies: effectiveResolverStub{effective: effective},
		WorkspaceMode: "directory-copy-non-production", AgentPhysical: configured, AgentReporter: reporter,
		ResourceInventory: emptyPolicyResourceInventory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.AdmitAgentWorkspacePlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	rawReporter.mutate = func(report *ports.AgentWorkspacePhysicalPolicyReport) {
		report.Resources.CPUMilli.Value = 0
	}
	if err := resolver.AdmitAgentWorkspacePlan(ctx, plan); err == nil {
		t.Fatal("agent plan with a zero physical CPU value was admitted")
	}
	rawReporter.mutate = func(report *ports.AgentWorkspacePhysicalPolicyReport) {
		report.Resources.CaptureBytes.Support = ports.PhysicalSupportUnsupported
	}
	if err := resolver.AdmitAgentWorkspacePlan(ctx, plan); err != nil {
		t.Fatalf("composition did not restore bounded capture enforcement: %v", err)
	}
	rawReporter.mutate = func(report *ports.AgentWorkspacePhysicalPolicyReport) {
		report.Resources.SwapBytes.Support = ports.PhysicalSupportUnsupported
	}
	if err := resolver.AdmitAgentWorkspacePlan(ctx, plan); err == nil {
		t.Fatal("agent plan with an unenforced zero-swap limit was admitted")
	}
}

func TestLinuxTargetZeroSwapStillRequiresPhysicalEnforcement(t *testing.T) {
	source, err := os.ReadFile("../../policy/deployment/e2e-directory-copy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	effective := compileSupportedPolicy(t, source)
	resources := enforcedAgentPhysicalReport().Resources
	resources.SwapBytes.Value = 0
	resources.SwapBytes.Support = ports.PhysicalSupportUnsupported
	if err := requireTargetResourceSupport(effective, "linux-visible", resources); err == nil {
		t.Fatal("Linux target with an unenforced zero-swap limit was admitted")
	}
}

func TestPolicyResetModeMapsPolicyActionsToPublicSelections(t *testing.T) {
	for _, test := range []struct {
		action string
		want   ports.ResetMode
		valid  bool
	}{
		{action: "recreate-new-target-generation", want: ports.ResetRecreate, valid: true},
		{action: "baseline-new-target-generation", want: ports.ResetBaseline, valid: true},
		{action: "finalize-run-and-report"},
		{action: "recreate"},
	} {
		got, valid := policyResetMode(test.action)
		if got != test.want || valid != test.valid {
			t.Errorf("policyResetMode(%q) = %q/%t, want %q/%t", test.action, got, valid, test.want, test.valid)
		}
	}
}

func emptyPolicyResourceInventory(context.Context) ([]application.ResearchSessionView, error) {
	return nil, nil
}

func compileSupportedPolicy(t *testing.T, source []byte) *policy.EffectivePolicy {
	t.Helper()
	requirements, err := policy.Requirements(source)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := make(map[string]policy.Capability, len(requirements))
	for _, requirement := range requirements {
		capability, err := policy.NewCapability(policy.CapabilitySupported, requirement.Constraints, nil)
		if err != nil {
			t.Fatal(err)
		}
		capabilities[requirement.Name] = capability
	}
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"test": "agent-recovery"})
	if err != nil {
		t.Fatal(err)
	}
	effective, err := policy.Compile(source, policy.CompileOptions{Capabilities: fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	return effective
}

func enforcedAgentPhysicalReport() ports.AgentWorkspacePhysicalPolicyReport {
	enforced := func() ports.PhysicalLimitFact { return ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced} }
	return ports.AgentWorkspacePhysicalPolicyReport{
		Runtime: ports.ContainerRuntimePhysicalFacts{
			Driver: "docker", IsolationProfile: "agent-standard", RootFilesystem: "readOnly", User: "65532:65532",
			CapabilityDrop: []string{"ALL"}, NoNewPrivileges: true, SeccompProfile: "runtime-default",
			UserEnforced: true, SeccompEnforced: true, CapabilitySupport: ports.PhysicalSupportEnforced,
			NoNewPrivilegesSupport: ports.PhysicalSupportEnforced, UserSupport: ports.PhysicalSupportEnforced,
			SeccompSupport: ports.PhysicalSupportEnforced,
		},
		Network: ports.ContainerNetworkPhysicalFacts{
			Mode: "none", DenyPrivateRanges: true, TargetAccess: "none", Support: ports.PhysicalSupportEnforced,
		},
		Resources: ports.ContainerResourcePhysicalFacts{
			CPUMilli: enforced(), MemoryBytes: enforced(), SwapBytes: enforced(), WorkspaceBytes: enforced(),
			WritableStateBytes: enforced(), CaptureBytes: enforced(), Inodes: enforced(), PIDs: enforced(),
		},
	}
}

type recoveryResolverStub struct {
	resolved ResolvedAcquisition
	err      error
}

func (r *recoveryResolverStub) ResolveAgentRecovery(context.Context, application.RecoverIncidentRequest, application.ResearchSessionView) (ResolvedAcquisition, error) {
	return r.resolved, r.err
}

func (r *recoveryResolverStub) ResolvePersistedAgent(context.Context, application.ResearchSessionView) (ResolvedAcquisition, error) {
	return r.resolved, r.err
}

func (r *recoveryResolverStub) ResolveAcquisition(context.Context, application.AcquireRequest) (ResolvedAcquisition, error) {
	return ResolvedAcquisition{}, fmt.Errorf("unexpected acquisition resolution")
}

func (r *recoveryResolverStub) ResolveTarget(context.Context, application.CreateTargetRequest, application.TargetRecord) (ports.TargetPlan, error) {
	return ports.TargetPlan{}, fmt.Errorf("unexpected target resolution")
}

func (r *recoveryResolverStub) ResolveTargetMaterial(context.Context, application.StartTargetRunRequest, application.TargetRecord) (ResolvedTargetRun, error) {
	return ResolvedTargetRun{}, fmt.Errorf("unexpected target-run resolution")
}

type effectiveResolverStub struct{ effective *policy.EffectivePolicy }

func (r effectiveResolverStub) Resolve(_ context.Context, policyDigest, capabilityDigest string) (*policy.EffectivePolicy, error) {
	if r.effective == nil || policyDigest != r.effective.Digest().String() || capabilityDigest != r.effective.CapabilityFingerprintDigest().String() {
		return nil, fmt.Errorf("effective policy is not published")
	}
	return r.effective, nil
}

type agentPhysicalReporterStub struct {
	configured ports.AgentWorkspacePhysicalPolicyReport
	mutate     func(*ports.AgentWorkspacePhysicalPolicyReport)
}

func (r *agentPhysicalReporterStub) AgentWorkspacePhysicalPolicy(context.Context) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	return r.configured, nil
}

func (r *agentPhysicalReporterStub) AgentWorkspacePlanPhysicalPolicy(_ context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspacePhysicalPolicyReport, error) {
	report := r.configured
	report.Runtime.ImageDigest = plan.ImageDigest.String()
	report.Resources.CPUMilli.Value = plan.Resources.CPUMilli
	report.Resources.MemoryBytes.Value = plan.Resources.MemoryBytes
	report.Resources.SwapBytes.Value = plan.Resources.SwapBytes
	report.Resources.WorkspaceBytes.Value = plan.Resources.StorageBytes
	report.Resources.CaptureBytes.Value = plan.Resources.CaptureBytes
	report.Resources.Inodes.Value = plan.Resources.Inodes
	report.Resources.PIDs.Value = plan.Resources.PIDs
	if r.mutate != nil {
		r.mutate(&report)
	}
	return report, nil
}

func agentPhysicalPlan(t *testing.T, effective *policy.EffectivePolicy) ports.AgentWorkspacePlan {
	t.Helper()
	leaseID, err := domain.NewLeaseID()
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := domain.NewAgentWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	workspaceID, err := domain.NewWorkspaceID()
	if err != nil {
		t.Fatal(err)
	}
	inputViewID := domain.NewInputViewID([]byte("physical-plan-input"))
	now := time.Now().UTC()
	generation, err := domain.NewAgentWorkspaceGeneration(domain.AgentWorkspaceGenerationSpec{
		AgentWorkspaceID: agentID, Generation: 1, WorkspaceID: workspaceID, InputViewID: inputViewID,
		PolicyDigest: effective.Digest(), CapabilityFingerprintDigest: effective.CapabilityFingerprintDigest(), CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := domain.NewWorkspace(domain.WorkspaceSpec{
		ID: workspaceID, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: 1, InputViewID: inputViewID, CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	image, err := domain.ParseDigest("sha256:6105d6cc76af4009c44e4692f219054456e7111487afb0c71077d9f887668fef")
	if err != nil {
		t.Fatal(err)
	}
	return ports.AgentWorkspacePlan{
		IdempotencyKey: "agent-physical-plan", LeaseID: leaseID, Generation: generation, Workspace: workspace,
		ImageDigest: image, PolicyDigest: effective.Digest(), CapabilityFingerprintDigest: effective.CapabilityFingerprintDigest(),
		Resources: admission.Resources{CPUMilli: 250, MemoryBytes: 32 << 20, StorageBytes: 1 << 20, CaptureBytes: 1 << 20, Inodes: 128, PIDs: 128},
	}
}
