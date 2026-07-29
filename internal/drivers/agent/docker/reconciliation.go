package docker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

type expectedAgentContainer struct {
	input       ports.AgentWorkspacePlan
	plan        ContainerPlan
	ref         ports.AgentWorkspaceRef
	cleanupOnly bool
}

// ReconcileAgentWorkspaces inventories Docker and rebuilds the driver's
// generation and request maps from exact physical matches. It only observes
// and adopts; it never destroys or quarantines a resource.
// ReconcileAgentWorkspaces keeps cleanup-only generations out of the
// executable workspace map while retaining their complete immutable plans as
// Stop/Destroy authority.
func (d *Driver) ReconcileAgentWorkspaces(ctx context.Context, request ports.AgentWorkspaceReconciliationRequest) (ports.AgentWorkspaceReconciliationReport, error) {
	if err := requireContext(ctx, "docker.reconcile"); err != nil {
		return ports.AgentWorkspaceReconciliationReport{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()

	prepared, err := d.prepareExpectedAgentContainers(request)
	if err != nil {
		return ports.AgentWorkspaceReconciliationReport{}, err
	}
	report := ports.AgentWorkspaceReconciliationReport{Expected: make([]ports.AgentWorkspaceReconciliation, len(prepared)), ObservedAt: d.now().UTC()}
	for index, item := range prepared {
		report.Expected[index] = ports.AgentWorkspaceReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "physical inventory did not complete"}
	}
	inventory, supported := d.engine.(EngineInventory)
	if !supported {
		return report, domain.NewError(domain.CodeCapabilityUnavailable, "docker.reconcile", "inventory", "Docker engine does not provide authoritative inventory", nil)
	}
	states, err := inventory.ListContainers(ctx)
	if err != nil {
		return report, domain.NewError(domain.CodeUnavailable, "docker.reconcile", "inventory", "Docker inventory failed", err)
	}
	if err := validateContainerInventory(states); err != nil {
		return report, domain.NewError(domain.CodeIntegrityViolation, "docker.reconcile", "inventory", "Docker inventory is ambiguous", err)
	}

	claimed := make([]bool, len(states))
	adopted := make([]workspaceRecord, 0, len(prepared))
	cleanupOnly := make([]workspaceRecord, 0, len(prepared))
	for expectedIndex, item := range prepared {
		candidates := agentCandidates(states, item)
		for _, candidate := range candidates {
			claimed[candidate] = true
		}
		switch len(candidates) {
		case 0:
			report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceMissing, Diagnostic: "authoritative Docker inventory contains no matching resource"}
		case 1:
			state := states[candidates[0]]
			if err := validateContainerIdentity(state, item.plan); err != nil {
				report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{Ref: item.ref, ContainerID: state.ID, Classification: ports.PhysicalResourceForeign, Diagnostic: err.Error()}
				continue
			}
			if err := requireCoherentContainerState(state, "docker.reconcile"); err != nil {
				report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{Ref: item.ref, ContainerID: state.ID, Classification: ports.PhysicalResourceForeign, Diagnostic: err.Error()}
				continue
			}
			if item.cleanupOnly {
				cleanupOnly = append(cleanupOnly, cleanupOnlyAgentRecord(item, state, report.ObservedAt))
				report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{
					Ref: item.ref, ContainerID: state.ID, Classification: ports.PhysicalResourceUncertain, PlanMatched: true,
					Diagnostic: "exact persisted cleanup plan and physical configuration match; generation was not adopted for work",
				}
				continue
			}
			record, proven, err := d.provenReconciledAgentRecord(item, state, report.ObservedAt)
			if err != nil {
				report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{Ref: item.ref, ContainerID: state.ID, Classification: ports.PhysicalResourceForeign, Diagnostic: err.Error()}
				continue
			}
			if !proven {
				cleanupOnly = append(cleanupOnly, cleanupOnlyAgentRecord(item, state, report.ObservedAt))
				report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{
					Ref: item.ref, ContainerID: state.ID, Classification: ports.PhysicalResourceUncertain, PlanMatched: true,
					Diagnostic: "exact container identity matches, but this daemon has not completed framed guest readiness for it",
				}
				continue
			}
			adopted = append(adopted, record)
			report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{Ref: item.ref, ContainerID: state.ID, Classification: ports.PhysicalResourceAdopted, PlanMatched: true, Diagnostic: "exact persisted plan, physical configuration, and framed guest readiness match"}
		default:
			report.Expected[expectedIndex] = ports.AgentWorkspaceReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "multiple Docker resources claim the same generation identity"}
		}
	}
	d.classifyUnclaimedAgentContainers(&report, states, claimed)
	if err := requireContext(ctx, "docker.reconcile"); err != nil {
		for index := range report.Expected {
			if report.Expected[index].Classification == ports.PhysicalResourceAdopted {
				report.Expected[index].Classification = ports.PhysicalResourceUncertain
				report.Expected[index].Diagnostic = "context ended before adoption could be committed"
			}
		}
		return report, err
	}
	d.rebuildAgentMaps(adopted, cleanupOnly)
	return report, nil
}

// cleanupOnlyAgentRecord retains complete plan authority for Stop/Destroy
// after a daemon restart without making the workspace executable. Provision
// must still perform a fresh framed guest readiness probe before it can become
// Ready or be adopted for work.
func cleanupOnlyAgentRecord(expected expectedAgentContainer, state ContainerState, observedAt time.Time) workspaceRecord {
	return workspaceRecord{
		plan: expected.input, containerPlan: expected.plan, containerID: state.ID,
		status: ports.AgentWorkspaceStatus{
			AgentWorkspaceID: expected.ref.ID, Generation: expected.ref.Generation,
			State: domain.AgentGenerationProvisioning, Ready: false,
			ContainerID: state.ID, CgroupID: state.CgroupID, ObservedAt: observedAt,
		},
	}
}

func (d *Driver) provenReconciledAgentRecord(expected expectedAgentContainer, state ContainerState, observedAt time.Time) (workspaceRecord, bool, error) {
	if state.Running {
		if err := requireLiveContainerState(state, "docker.reconcile"); err != nil {
			return workspaceRecord{}, false, err
		}
	}
	key := workspaceKey(expected.ref.ID, expected.ref.Generation)
	d.mu.Lock()
	record, found := d.workspaces[key]
	d.mu.Unlock()
	if !found || record.containerID != state.ID || record.status.AgentWorkspaceID != expected.ref.ID || record.status.Generation != expected.ref.Generation ||
		record.status.ContainerID != state.ID || !record.status.Ready || record.status.State != domain.AgentGenerationReady ||
		record.status.GuestProtocol != uint32(transport.ProtocolVersion) {
		return workspaceRecord{}, false, nil
	}
	samePlan, err := sameAgentWorkspacePlanIdentity(record.plan, expected.input)
	if err != nil {
		return workspaceRecord{}, false, fmt.Errorf("compare readiness-proven workspace plan: %w", err)
	}
	if !samePlan {
		return workspaceRecord{}, false, fmt.Errorf("readiness proof belongs to a different semantic workspace plan")
	}
	if err := requireLiveContainerState(state, "docker.reconcile"); err != nil {
		return workspaceRecord{}, false, err
	}
	record.plan = expected.input
	record.containerPlan = expected.plan
	record.status = readyAgentStatus(expected.plan, state, observedAt)
	return record, true, nil
}

func (d *Driver) prepareExpectedAgentContainers(request ports.AgentWorkspaceReconciliationRequest) ([]expectedAgentContainer, error) {
	count := len(request.Active) + len(request.CleanupOnly)
	if count > dockercli.MaximumInventoryContainers {
		return nil, domain.NewError(domain.CodeResourceExhausted, "docker.reconcile", "expected", "expected generation set exceeds the reconciliation safety bound", nil)
	}
	prepared := make([]expectedAgentContainer, 0, count)
	refs := make(map[string]struct{}, count)
	requests := make(map[string]struct{}, count)
	appendPlans := func(expected []ports.AgentWorkspacePlan, cleanupOnly bool) error {
		for index, input := range expected {
			plan, err := BuildContainerPlan(input, d.build)
			if err != nil {
				return fmt.Errorf("expected agent workspace %d: %w", index, err)
			}
			key := workspaceKey(plan.AgentWorkspaceID, plan.Generation)
			if _, duplicate := refs[key]; duplicate {
				return domain.NewError(domain.CodeInvalidArgument, "docker.reconcile", "expected", "contains duplicate workspace generations", nil)
			}
			if _, duplicate := requests[input.IdempotencyKey]; duplicate {
				return domain.NewError(domain.CodeInvalidArgument, "docker.reconcile", "expected", "contains duplicate idempotency keys", nil)
			}
			refs[key], requests[input.IdempotencyKey] = struct{}{}, struct{}{}
			prepared = append(prepared, expectedAgentContainer{input: input, plan: plan, ref: ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation}, cleanupOnly: cleanupOnly})
		}
		return nil
	}
	if err := appendPlans(request.Active, false); err != nil {
		return nil, err
	}
	if err := appendPlans(request.CleanupOnly, true); err != nil {
		return nil, err
	}
	return prepared, nil
}

func agentCandidates(states []ContainerState, expected expectedAgentContainer) []int {
	result := make([]int, 0, 1)
	for index, state := range states {
		ref, hasRef := agentRefFromLabels(state.Labels)
		if state.Name == expected.plan.Name || hasRef && ref == expected.ref {
			result = append(result, index)
		}
	}
	return result
}

func (d *Driver) resolveAgentDestroy(ctx context.Context, ref ports.AgentWorkspaceRef, record workspaceRecord, found bool) (string, bool, error) {
	if found {
		state, err := d.engine.Inspect(ctx, record.containerID)
		if err == nil {
			if err := validateContainerIdentity(state, record.containerPlan); err != nil {
				return "", false, err
			}
			return state.ID, false, nil
		}
	}
	inventory, supported := d.engine.(EngineInventory)
	if !supported {
		return "", false, domain.NewError(domain.CodeCapabilityUnavailable, "docker.destroy", "inventory", "cannot prove physical absence after restart", nil)
	}
	states, err := inventory.ListContainers(ctx)
	if err != nil {
		return "", false, domain.NewError(domain.CodeUnavailable, "docker.destroy", "inventory", "Docker inventory failed", err)
	}
	if err := validateContainerInventory(states); err != nil {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "docker.destroy", "inventory", "Docker inventory is ambiguous", err)
	}
	candidates := agentRefCandidates(states, ref)
	if len(candidates) == 0 {
		return "", true, nil
	}
	if len(candidates) > 1 {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "docker.destroy", "identity", "multiple Docker resources claim the generation", nil)
	}
	state := candidates[0]
	if !found {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "docker.destroy", "identity", "present container has no reconciled complete persisted workspace plan", nil)
	}
	if err := validateContainerIdentity(state, record.containerPlan); err != nil {
		return "", false, err
	}
	return state.ID, false, nil
}

func agentRefCandidates(states []ContainerState, ref ports.AgentWorkspaceRef) []ContainerState {
	name := containerName(ref.ID, ref.Generation)
	result := make([]ContainerState, 0, 1)
	for _, state := range states {
		candidateRef, hasRef := agentRefFromLabels(state.Labels)
		if state.Name == name || hasRef && candidateRef == ref {
			result = append(result, state)
		}
	}
	return result
}

func (d *Driver) classifyUnclaimedAgentContainers(report *ports.AgentWorkspaceReconciliationReport, states []ContainerState, claimed []bool) {
	byRef := make(map[string][]int)
	for index, state := range states {
		if claimed[index] {
			continue
		}
		if state.Labels["world.role"] != agentRoleLabel {
			if strings.HasPrefix(state.Name, "world-agent-") {
				report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: state.ID, Name: state.Name, Classification: ports.PhysicalResourceForeign, Diagnostic: "world agent name is not accompanied by an exact agent role identity"})
			}
			continue
		}
		ref, ok := agentRefFromLabels(state.Labels)
		if !ok {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: state.ID, Name: state.Name, Classification: ports.PhysicalResourceForeign, Diagnostic: "agent role label has malformed generation identity"})
			continue
		}
		byRef[workspaceKey(ref.ID, ref.Generation)] = append(byRef[workspaceKey(ref.ID, ref.Generation)], index)
	}
	keys := make([]string, 0, len(byRef))
	for key := range byRef {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		indices := byRef[key]
		for _, index := range indices {
			state := states[index]
			ref, _ := agentRefFromLabels(state.Labels)
			classification := ports.PhysicalResourceOrphan
			diagnostic := "valid world-owned generation is absent from the expected durable plans"
			if len(indices) > 1 {
				classification = ports.PhysicalResourceUncertain
				diagnostic = "multiple unclaimed resources have the same generation identity"
			} else if err := validateUnclaimedAgentContainer(state, ref); err != nil {
				classification = ports.PhysicalResourceForeign
				diagnostic = err.Error()
			}
			report.Unclaimed = append(report.Unclaimed, ports.AgentWorkspaceReconciliation{Ref: ref, ContainerID: state.ID, Classification: classification, Diagnostic: diagnostic})
		}
	}
}

func (d *Driver) rebuildAgentMaps(adopted, cleanupRecords []workspaceRecord) {
	workspaces := make(map[string]workspaceRecord, len(adopted))
	cleanupOnly := make(map[string]workspaceRecord, len(cleanupRecords))
	requests := make(map[string]string, len(adopted))
	for _, record := range adopted {
		key := workspaceKey(record.containerPlan.AgentWorkspaceID, record.containerPlan.Generation)
		workspaces[key] = record
		requests[record.plan.IdempotencyKey] = key
	}
	for _, record := range cleanupRecords {
		key := workspaceKey(record.containerPlan.AgentWorkspaceID, record.containerPlan.Generation)
		cleanupOnly[key] = record
	}
	d.mu.Lock()
	d.workspaces, d.cleanupOnly, d.idempotency = workspaces, cleanupOnly, requests
	d.mu.Unlock()
}

func validateContainerInventory(states []ContainerState) error {
	ids := make(map[string]struct{}, len(states))
	for _, state := range states {
		if err := dockercli.RequireCanonicalContainerID(state.ID); err != nil {
			return fmt.Errorf("container has a non-canonical runtime ID %q: %w", state.ID, err)
		}
		if _, duplicate := ids[state.ID]; duplicate {
			return fmt.Errorf("duplicate runtime ID %q", state.ID)
		}
		ids[state.ID] = struct{}{}
	}
	return nil
}

func validateUnclaimedAgentContainer(state ContainerState, ref ports.AgentWorkspaceRef) error {
	if !validAgentWorldLabels(state.Labels, ref) {
		return fmt.Errorf("world-owned agent labels are incomplete or contain an unexpected world label")
	}
	if state.Name != containerName(ref.ID, ref.Generation) {
		return fmt.Errorf("world-owned agent resource has a non-canonical name")
	}
	return nil
}

func validateContainerIdentity(state ContainerState, plan ContainerPlan) error {
	if dockercli.RequireCanonicalContainerID(state.ID) != nil || state.Name != plan.Name {
		return domain.NewError(domain.CodeIntegrityViolation, "docker.inspect", "name", "container name or runtime identity does not match the world plan", nil)
	}
	if !dockercli.ExactWorldLabels(state.Labels, plan.Labels) {
		return domain.NewError(domain.CodeIntegrityViolation, "docker.inspect", "labels", "container identity or plan provenance labels do not exactly match", nil)
	}
	if err := validateAgentConfiguration(state.Configuration, plan); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "docker.inspect", "configuration", "container physical configuration does not match the world plan", err)
	}
	return nil
}

func validateAgentConfiguration(actual dockercli.Configuration, plan ContainerPlan) error {
	return dockercli.ConfigurationDifference(actual, expectedAgentConfiguration(plan))
}

func agentRefFromLabels(labels map[string]string) (ports.AgentWorkspaceRef, bool) {
	id, err := domain.ParseAgentWorkspaceID(labels["world.agent-workspace"])
	if err != nil {
		return ports.AgentWorkspaceRef{}, false
	}
	generation, err := strconv.ParseUint(labels["world.agent-generation"], 10, 64)
	if err != nil || generation == 0 {
		return ports.AgentWorkspaceRef{}, false
	}
	return ports.AgentWorkspaceRef{ID: id, Generation: domain.AgentGeneration(generation)}, true
}

func validAgentWorldLabels(labels map[string]string, ref ports.AgentWorkspaceRef) bool {
	if labels["world.role"] != agentRoleLabel || labels["world.agent-workspace"] != ref.ID.String() || labels["world.agent-generation"] != strconv.FormatUint(uint64(ref.Generation), 10) {
		return false
	}
	if _, err := domain.ParseLeaseID(labels["world.lease"]); err != nil {
		return false
	}
	if _, err := domain.ParseWorkspaceID(labels["world.workspace"]); err != nil {
		return false
	}
	for _, name := range []string{"world.policy-digest", "world.capability-digest", planDigestLabel} {
		if _, err := domain.ParseDigest(labels[name]); err != nil {
			return false
		}
	}
	expected := map[string]string{
		"world.role": agentRoleLabel, "world.lease": labels["world.lease"], "world.agent-workspace": ref.ID.String(),
		"world.agent-generation": strconv.FormatUint(uint64(ref.Generation), 10), "world.workspace": labels["world.workspace"],
		"world.policy-digest": labels["world.policy-digest"], "world.capability-digest": labels["world.capability-digest"], planDigestLabel: labels[planDigestLabel],
	}
	return dockercli.ExactWorldLabels(labels, expected)
}

func expectedAgentConfiguration(plan ContainerPlan) dockercli.Configuration {
	memorySwap, _ := dockercli.MemorySwapTotal(plan.Resources.MemoryBytes, plan.Resources.SwapBytes)
	configuration := dockercli.RestrictedContainerConfiguration()
	configuration.Image = plan.Image
	configuration.Runtime = plan.Runtime
	configuration.Hostname = plan.Name
	configuration.Entrypoint = append([]string(nil), plan.Entrypoint[:1]...)
	configuration.Command = append([]string(nil), plan.Entrypoint[1:]...)
	configuration.User = plan.User
	configuration.AttachStdin = true
	configuration.OpenStdin = true
	configuration.StdinOnce = true
	configuration.MemoryBytes = plan.Resources.MemoryBytes
	configuration.MemorySwapBytes = memorySwap
	configuration.NanoCPUs = dockercli.NanoCPUs(plan.Resources.CPUMilli)
	configuration.PIDs = plan.Resources.PIDs
	for _, mount := range plan.Mounts {
		dockercli.AddRestrictedBindMount(&configuration, mount.Source, mount.Target, mount.ReadOnly)
	}
	return configuration
}

var _ ports.AgentWorkspaceReconciler = (*Driver)(nil)
