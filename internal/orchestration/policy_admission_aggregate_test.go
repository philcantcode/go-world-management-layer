package orchestration

import (
	"context"
	"errors"
	"math"
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

func TestPolicyAdmissionAcquisitionUsesAuthoritativeAggregateAgentAndTargetResources(t *testing.T) {
	source, err := os.ReadFile("../../policy/deployment/e2e-directory-copy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	effective := compileSupportedPolicy(t, source)
	content := testkit.NewMemoryContentSource([]byte("aggregate admission input"))
	entry, err := domain.NewInputViewEntry(domain.InputViewEntrySpec{
		LogicalPath: "input/specimen.bin", OccurrenceRef: "memory://aggregate-admission",
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
	resources := admission.Resources{
		CPUMilli: 750, MemoryBytes: 64 << 20, StorageBytes: 1 << 20,
		CaptureBytes: 1 << 20, Inodes: 128, PIDs: 128,
	}
	base, err := NewStaticProvisioningResolver(StaticProvisioningConfig{
		Agents: map[string]StaticAgentPlan{manifest.ID().String(): {
			InputView: manifest, SecurityScope: "aggregate-test", Construction: domain.InputViewAllowCopy,
			Content: map[string]ports.ContentSource{"input/specimen.bin": content}, UpperByteLimit: 1 << 20, UpperInodeLimit: 128,
			PolicyDigest: effective.Digest(), CapabilityDigest: effective.CapabilityFingerprintDigest(), ImageDigest: image, Resources: resources,
		}},
		Targets: map[string]StaticTargetPlan{"linux-visible": {
			PolicyDigest: effective.Digest(), CapabilityDigest: effective.CapabilityFingerprintDigest(),
			Template: ports.TargetTemplate{
				Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: "runc",
				ImageDigest: image, IsolationProfile: "observable-container",
			},
			Resources: resources,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	view := aggregateResourceView(t, effective, manifest.ID(), "existing/physical/agent")
	views := []application.ResearchSessionView{view}
	resolver, err := NewPolicyAdmissionResolver(PolicyAdmissionConfig{
		Base: base, Policies: effectiveResolverStub{effective: effective}, WorkspaceMode: "directory-copy-non-production",
		AgentPhysical: enforcedAgentPhysicalReport(), ResourceInventory: func(context.Context) ([]application.ResearchSessionView, error) {
			return views, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	newTargetID, err := domain.NewTargetID()
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.validateAggregateCandidate(ctx, effective, aggregateCandidate{
		targetID: newTargetID.String(), resources: aggregateRuntimeResources(resources),
	}); !errors.Is(err, policyauthority.ErrPolicyDenied) {
		t.Fatalf("agent + target + target candidate aggregate error = %v, want policy denial", err)
	}
	if concurrent, err := resolver.admitAggregateCandidate(ctx, effective, aggregateCandidate{
		targetID: views[0].Targets[0].ID, resources: aggregateRuntimeResources(resources),
	}); err != nil || concurrent != 0 {
		t.Fatalf("replacement target aggregate admission = concurrent %d, %v", concurrent, err)
	}
	request := application.AcquireRequest{
		Meta: application.MutationMeta{IdempotencyKey: "aggregate-candidate"}, InputViewID: manifest.ID().String(),
		PolicyDigest: effective.Digest().String(), CapabilityDigest: effective.CapabilityFingerprintDigest().String(),
		TTL: effective.Policy().Spec.Lease.TTL.Duration(),
	}
	if _, err := resolver.ResolveAcquisition(ctx, request); !errors.Is(err, policyauthority.ErrPolicyDenied) {
		t.Fatalf("agent + target + candidate aggregate error = %v, want policy denial", err)
	}

	// A crash after Core persisted the acquisition but before its physical
	// provisioning keys were bound is still an exact replacement, identified
	// by the immutable logical acquisition key.
	views[0].Session.AcquisitionIdempotencyKey = request.Meta.IdempotencyKey
	views[0].Agent.Generations[0].AgentProvisioningKey = ""
	if _, err := resolver.ResolveAcquisition(ctx, request); err != nil {
		t.Fatalf("unbound exact acquisition replay was double-counted: %v", err)
	}

	// An exact replay replaces its already-counted agent instead of charging
	// the same durable generation twice. The live target remains in the total.
	views[0].Agent.Generations[0].AgentProvisioningKey = domain.DeriveIdempotencyKey(request.Meta.IdempotencyKey, "physical/agent")
	if _, err := resolver.ResolveAcquisition(ctx, request); err != nil {
		t.Fatalf("exact aggregate replay was double-counted: %v", err)
	}

	// The same crash window exists for target creation: the logical target is
	// authoritative before its physical provisioning key is bound.
	views[0].Targets[0].CreationIdempotencyKey = "target-unbound-replay"
	if concurrent, err := resolver.admitAggregateCandidate(ctx, effective, aggregateCandidate{
		targetCreationKey: "target-unbound-replay", resources: aggregateRuntimeResources(resources),
	}); err != nil || concurrent != 0 {
		t.Fatalf("unbound exact target replay = concurrent %d, %v", concurrent, err)
	}
}

func TestAddAggregateResourcesRejectsOverflow(t *testing.T) {
	_, err := addAggregateResources(
		policyauthority.RuntimeResources{CPUMilli: math.MaxInt64},
		policyauthority.RuntimeResources{CPUMilli: 1},
	)
	if !errors.Is(err, policyauthority.ErrPolicyDenied) {
		t.Fatalf("aggregate overflow error = %v, want policy denial", err)
	}
}

func aggregateResourceView(t *testing.T, effective *policy.EffectivePolicy, inputViewID domain.InputViewID, agentProvisioningKey string) application.ResearchSessionView {
	t.Helper()
	sessionID, err := domain.NewResearchSessionID()
	if err != nil {
		t.Fatal(err)
	}
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
	targetID, err := domain.NewTargetID()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	pair := struct{ policy, capability string }{effective.Digest().String(), effective.CapabilityFingerprintDigest().String()}
	return application.ResearchSessionView{
		Session: application.SessionRecord{
			ID: sessionID.String(), State: domain.ResearchSessionLeased, LeaseID: leaseID.String(), AgentWorkspaceID: agentID.String(),
			AcquisitionIdempotencyKey: "existing-acquisition",
			InputViewID:               inputViewID.String(), PolicyDigest: pair.policy, CapabilityDigest: pair.capability, CreatedAt: now, UpdatedAt: now,
		},
		Lease: application.LeaseRecord{
			ID: leaseID.String(), SessionID: sessionID.String(), AgentWorkspaceID: agentID.String(), AgentGeneration: 1,
			InputViewID: inputViewID.String(), PolicyDigest: pair.policy, CapabilityDigest: pair.capability,
			State: domain.LeaseActive, ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
		},
		Agent: application.AgentWorkspaceRecord{
			ID: agentID.String(), SessionID: sessionID.String(), CurrentGeneration: 1, CreatedAt: now, UpdatedAt: now,
			Generations: []application.AgentGenerationRecord{{
				Generation: 1, WorkspaceID: workspaceID.String(), InputViewID: inputViewID.String(),
				PolicyDigest: pair.policy, CapabilityDigest: pair.capability, AgentProvisioningKey: agentProvisioningKey,
				State: domain.AgentGenerationReady, CreatedAt: now, UpdatedAt: now,
			}},
		},
		Targets: []application.TargetRecord{{
			ID: targetID.String(), SessionID: sessionID.String(), LeaseID: leaseID.String(), Template: "linux-visible",
			CreationIdempotencyKey: "existing-target",
			Kind:                   domain.TargetLinuxContainer, CurrentGeneration: 1, CreatedAt: now, UpdatedAt: now,
			Generations: []application.TargetGenerationRecord{{
				Generation: 1, PolicyDigest: pair.policy, CapabilityDigest: pair.capability,
				State: domain.TargetGenerationReady, CreatedAt: now, UpdatedAt: now,
			}},
		}},
	}
}
