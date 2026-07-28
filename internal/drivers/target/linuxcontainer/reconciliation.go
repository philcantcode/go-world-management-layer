package linuxcontainer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type expectedTargetContainer struct {
	input ports.TargetPlan
	plan  ContainerPlan
	ref   ports.TargetRef
}

// ReconcileTargets inventories the runtime and adopts only unique resources
// whose complete labels and physical Docker configuration match a persisted
// expected plan.
func (d *Driver) ReconcileTargets(ctx context.Context, expected []ports.TargetPlan) (ports.TargetReconciliationReport, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.reconcile"); err != nil {
		return ports.TargetReconciliationReport{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()

	prepared, err := d.prepareExpectedTargets(expected)
	if err != nil {
		return ports.TargetReconciliationReport{}, err
	}
	report := ports.TargetReconciliationReport{Expected: make([]ports.TargetReconciliation, len(prepared)), ObservedAt: d.now().UTC()}
	for index, item := range prepared {
		report.Expected[index] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "physical inventory did not complete"}
	}
	inventory, supported := d.runtime.(RuntimeInventory)
	if !supported {
		return report, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.reconcile", "inventory", "target runtime does not provide authoritative inventory", nil)
	}
	states, err := inventory.ListContainers(ctx)
	if err != nil {
		return report, domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "inventory", "target runtime inventory failed", err)
	}
	if err := validateRuntimeInventory(states); err != nil {
		return report, domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "inventory", "target runtime inventory is ambiguous", err)
	}

	claimed := make([]bool, len(states))
	adopted := make([]targetRecord, 0, len(prepared))
	for expectedIndex, item := range prepared {
		candidates := targetCandidates(states, item)
		for _, candidate := range candidates {
			claimed[candidate] = true
		}
		switch len(candidates) {
		case 0:
			report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceMissing, Diagnostic: "authoritative runtime inventory contains no matching resource"}
		case 1:
			state := states[candidates[0]]
			if err := validateRuntimeIdentity(state, item.plan); err != nil {
				report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, RuntimeID: state.ID, Classification: ports.PhysicalResourceForeign, Diagnostic: err.Error()}
				continue
			}
			status := adoptedTargetStatus(item.plan, state, d.now().UTC())
			adopted = append(adopted, targetRecord{input: item.input, plan: item.plan, runtimeID: state.ID, status: status})
			report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, RuntimeID: state.ID, Classification: ports.PhysicalResourceAdopted, Diagnostic: "exact persisted plan and physical configuration match"}
		default:
			report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "multiple runtime resources claim the same generation identity"}
		}
	}
	d.classifyUnclaimedTargets(&report, states, claimed)
	if err := ports.RequireDeadline(ctx, "linux_target.reconcile"); err != nil {
		for index := range report.Expected {
			if report.Expected[index].Classification == ports.PhysicalResourceAdopted {
				report.Expected[index].Classification = ports.PhysicalResourceUncertain
				report.Expected[index].Diagnostic = "context ended before adoption could be committed"
			}
		}
		return report, err
	}
	d.rebuildTargetMaps(adopted)
	return report, nil
}

func (d *Driver) prepareExpectedTargets(expected []ports.TargetPlan) ([]expectedTargetContainer, error) {
	if len(expected) > dockercli.MaximumInventoryContainers {
		return nil, domain.NewError(domain.CodeResourceExhausted, "linux_target.reconcile", "expected", "expected generation set exceeds the reconciliation safety bound", nil)
	}
	prepared := make([]expectedTargetContainer, 0, len(expected))
	refs := make(map[string]struct{}, len(expected))
	requests := make(map[string]struct{}, len(expected))
	for index, input := range expected {
		plan, err := BuildContainerPlan(input, d.build)
		if err != nil {
			return nil, fmt.Errorf("expected target %d: %w", index, err)
		}
		key := targetKey(plan.TargetID, plan.Generation)
		if _, duplicate := refs[key]; duplicate {
			return nil, domain.NewError(domain.CodeInvalidArgument, "linux_target.reconcile", "expected", "contains duplicate target generations", nil)
		}
		if _, duplicate := requests[input.IdempotencyKey]; duplicate {
			return nil, domain.NewError(domain.CodeInvalidArgument, "linux_target.reconcile", "expected", "contains duplicate idempotency keys", nil)
		}
		refs[key], requests[input.IdempotencyKey] = struct{}{}, struct{}{}
		prepared = append(prepared, expectedTargetContainer{input: input, plan: plan, ref: ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}})
	}
	return prepared, nil
}

func targetCandidates(states []RuntimeState, expected expectedTargetContainer) []int {
	result := make([]int, 0, 1)
	for index, state := range states {
		ref, hasRef := targetRefFromLabels(state.Labels)
		if state.Name == expected.plan.Name || hasRef && ref == expected.ref {
			result = append(result, index)
		}
	}
	return result
}

func (d *Driver) resolveTargetDestroy(ctx context.Context, ref ports.TargetRef, record targetRecord, found bool) (string, bool, error) {
	if found {
		state, err := d.runtime.Inspect(ctx, record.runtimeID)
		if err == nil {
			if err := validateRuntimeIdentity(state, record.plan); err != nil {
				return "", false, err
			}
			return state.ID, false, nil
		}
	}
	inventory, supported := d.runtime.(RuntimeInventory)
	if !supported {
		return "", false, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.destroy", "inventory", "cannot prove physical absence after restart", nil)
	}
	states, err := inventory.ListContainers(ctx)
	if err != nil {
		return "", false, domain.NewError(domain.CodeUnavailable, "linux_target.destroy", "inventory", "target runtime inventory failed", err)
	}
	if err := validateRuntimeInventory(states); err != nil {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "linux_target.destroy", "inventory", "target runtime inventory is ambiguous", err)
	}
	candidates := targetRefCandidates(states, ref)
	if len(candidates) == 0 {
		return "", true, nil
	}
	if len(candidates) > 1 {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "linux_target.destroy", "identity", "multiple runtime resources claim the generation", nil)
	}
	state := candidates[0]
	if found {
		if err := validateRuntimeIdentity(state, record.plan); err != nil {
			return "", false, err
		}
	} else if err := validateUnclaimedTarget(state, ref); err != nil {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "linux_target.destroy", "identity", "refusing to remove a foreign runtime resource", err)
	}
	return state.ID, false, nil
}

func targetRefCandidates(states []RuntimeState, ref ports.TargetRef) []RuntimeState {
	name := targetContainerName(ref.ID, ref.Generation)
	result := make([]RuntimeState, 0, 1)
	for _, state := range states {
		candidateRef, hasRef := targetRefFromLabels(state.Labels)
		if state.Name == name || hasRef && candidateRef == ref {
			result = append(result, state)
		}
	}
	return result
}

func (d *Driver) classifyUnclaimedTargets(report *ports.TargetReconciliationReport, states []RuntimeState, claimed []bool) {
	byRef := make(map[string][]int)
	for index, state := range states {
		if claimed[index] {
			continue
		}
		if state.Labels["world.role"] != targetRoleLabel {
			if strings.HasPrefix(state.Name, "world-target-") {
				report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: state.ID, Name: state.Name, Classification: ports.PhysicalResourceForeign, Diagnostic: "world target name is not accompanied by an exact target role identity"})
			}
			continue
		}
		ref, ok := targetRefFromLabels(state.Labels)
		if !ok {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: state.ID, Name: state.Name, Classification: ports.PhysicalResourceForeign, Diagnostic: "target role label has malformed generation identity"})
			continue
		}
		byRef[targetKey(ref.ID, ref.Generation)] = append(byRef[targetKey(ref.ID, ref.Generation)], index)
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
			ref, _ := targetRefFromLabels(state.Labels)
			classification := ports.PhysicalResourceOrphan
			diagnostic := "valid world-owned generation is absent from the expected durable plans"
			if len(indices) > 1 {
				classification = ports.PhysicalResourceUncertain
				diagnostic = "multiple unclaimed resources have the same generation identity"
			} else if err := validateUnclaimedTarget(state, ref); err != nil {
				classification = ports.PhysicalResourceForeign
				diagnostic = err.Error()
			}
			report.Unclaimed = append(report.Unclaimed, ports.TargetReconciliation{Ref: ref, RuntimeID: state.ID, Classification: classification, Diagnostic: diagnostic})
		}
	}
}

func (d *Driver) rebuildTargetMaps(records []targetRecord) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, record := range d.targets {
		delete(d.idempotency, record.input.IdempotencyKey)
		delete(d.materialized, key)
	}
	d.targets = make(map[string]targetRecord, len(records))
	for _, record := range records {
		key := targetKey(record.plan.TargetID, record.plan.Generation)
		d.targets[key] = record
		d.idempotency[record.input.IdempotencyKey] = key
	}
}

func validateRuntimeInventory(states []RuntimeState) error {
	ids := make(map[string]struct{}, len(states))
	for _, state := range states {
		if strings.TrimSpace(state.ID) == "" {
			return fmt.Errorf("runtime resource has an empty ID")
		}
		if _, duplicate := ids[state.ID]; duplicate {
			return fmt.Errorf("duplicate runtime ID %q", state.ID)
		}
		ids[state.ID] = struct{}{}
	}
	return nil
}

func validateUnclaimedTarget(state RuntimeState, ref ports.TargetRef) error {
	if !validTargetWorldLabels(state.Labels, ref) {
		return fmt.Errorf("world-owned target labels are incomplete or contain an unexpected world label")
	}
	if state.Name != targetContainerName(ref.ID, ref.Generation) {
		return fmt.Errorf("world-owned target resource has a non-canonical name")
	}
	return nil
}

func validateRuntimeIdentity(state RuntimeState, plan ContainerPlan) error {
	if state.ID == "" || state.Name != plan.Name {
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.inspect", "name", "runtime name or identity does not match the world plan", nil)
	}
	if !dockercli.ExactWorldLabels(state.Labels, plan.Labels) {
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.inspect", "labels", "runtime identity or plan provenance labels do not exactly match", nil)
	}
	if err := dockercli.ConfigurationDifference(state.Configuration, expectedTargetConfiguration(plan)); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.inspect", "configuration", "runtime physical configuration does not match the world plan", err)
	}
	return nil
}

func targetRefFromLabels(labels map[string]string) (ports.TargetRef, bool) {
	id, err := domain.ParseTargetID(labels["world.target"])
	if err != nil {
		return ports.TargetRef{}, false
	}
	generation, err := strconv.ParseUint(labels["world.target-generation"], 10, 64)
	if err != nil || generation == 0 {
		return ports.TargetRef{}, false
	}
	return ports.TargetRef{ID: id, Generation: domain.TargetGeneration(generation)}, true
}

func validTargetWorldLabels(labels map[string]string, ref ports.TargetRef) bool {
	if labels["world.role"] != targetRoleLabel || labels["world.target"] != ref.ID.String() || labels["world.target-generation"] != strconv.FormatUint(uint64(ref.Generation), 10) {
		return false
	}
	if _, err := domain.ParseLeaseID(labels["world.lease"]); err != nil {
		return false
	}
	for _, name := range []string{"world.policy-digest", "world.capability-digest", planDigestLabel} {
		if _, err := domain.ParseDigest(labels[name]); err != nil {
			return false
		}
	}
	expected := map[string]string{
		"world.role": targetRoleLabel, "world.lease": labels["world.lease"], "world.target": ref.ID.String(),
		"world.target-generation": strconv.FormatUint(uint64(ref.Generation), 10), "world.policy-digest": labels["world.policy-digest"],
		"world.capability-digest": labels["world.capability-digest"], planDigestLabel: labels[planDigestLabel],
	}
	return dockercli.ExactWorldLabels(labels, expected)
}

func adoptedTargetStatus(plan ContainerPlan, state RuntimeState, observedAt time.Time) ports.TargetStatus {
	status := ports.TargetStatus{
		TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetLinuxContainer, State: domain.TargetGenerationReady,
		Ready: state.Running, RuntimeID: state.ID, CgroupID: state.CgroupID, ObservedAt: observedAt,
	}
	if !state.Running {
		status.State = domain.TargetGenerationFailed
	}
	return status
}

func expectedTargetConfiguration(plan ContainerPlan) dockercli.Configuration {
	memorySwap, _ := dockercli.MemorySwapTotal(plan.Resources.MemoryBytes, plan.Resources.SwapBytes)
	configuration := dockercli.Configuration{
		Image: plan.Image, Runtime: plan.Runtime, Entrypoint: []string{"/usr/local/bin/world-idle"}, User: plan.User, ReadOnlyRoot: true, NetworkMode: "none", Init: true, InitKnown: true,
		CapabilitiesAdd: append([]string(nil), plan.Capabilities...), CapabilitiesDrop: []string{"ALL"},
		SecurityOptions: dockercli.HardenedSecurityOptions(), Tmpfs: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,mode=1777"}, MemoryBytes: plan.Resources.MemoryBytes,
		MemorySwapBytes: memorySwap, NanoCPUs: dockercli.NanoCPUs(plan.Resources.CPUMilli), PIDs: plan.Resources.PIDs,
	}
	configuration.Mounts = []dockercli.Mount{
		{Type: "bind", Source: plan.writableRoot(), Destination: TargetMount},
		{Type: "bind", Source: plan.materialRoot(), Destination: TargetMaterialMount, ReadOnly: true},
	}
	return configuration
}

func targetDirectory(root string, ref ports.TargetRef) string {
	return filepath.Join(root, ref.ID.String(), "generations", strconv.FormatUint(uint64(ref.Generation), 10))
}

func removeTargetDirectoryIfPresent(root, directory string) error {
	if _, err := os.Lstat(directory); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return removeTargetDirectory(root, directory)
}

var _ ports.TargetReconciler = (*Driver)(nil)
