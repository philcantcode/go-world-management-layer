package cuttlefish

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const maximumReconciliationManifests = 4096

type expectedAndroidTarget struct {
	input     ports.TargetPlan
	ref       ports.TargetRef
	directory string
}

func (d *Driver) ReconcileTargets(ctx context.Context, request ports.TargetReconciliationRequest) (ports.TargetReconciliationReport, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.reconcile"); err != nil {
		return ports.TargetReconciliationReport{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	all := make([]ports.TargetPlan, 0, len(request.Active)+len(request.CleanupOnly))
	all = append(all, request.Active...)
	all = append(all, request.CleanupOnly...)
	prepared, err := d.prepareExpectedAndroidTargets(all)
	if err != nil {
		return ports.TargetReconciliationReport{}, err
	}
	activeCount := len(request.Active)
	active := prepared[:activeCount]
	report := ports.TargetReconciliationReport{Expected: make([]ports.TargetReconciliation, len(prepared)), ObservedAt: d.now().UTC()}
	for index, item := range prepared {
		report.Expected[index] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "authoritative Android runtime inventory did not complete"}
	}
	inventoryBackend, supported := d.backend.(BackendInventory)
	if !supported {
		return report, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.reconcile", "inventory", "Android backend does not provide authoritative runtime inventory", nil)
	}
	inventory, err := loadExactAndroidRuntimeInventory(ctx, inventoryBackend)
	if err != nil {
		return report, err
	}
	createMutated, err := d.resumeExpectedCreateIntents(ctx, active, inventory)
	if err != nil {
		return report, classifiedDriverFailure("cuttlefish.reconcile", "create_recovery", "incomplete durable Android creation could not be resumed: "+err.Error(), err)
	}
	resetMutated, err := d.resumeExpectedResetTransitions(ctx, active, inventory)
	if err != nil {
		return report, classifiedDriverFailure("cuttlefish.reconcile", "reset_recovery", "incomplete durable Android reset could not be resumed: "+err.Error(), err)
	}
	if createMutated || resetMutated {
		inventory, err = loadExactAndroidRuntimeInventory(ctx, inventoryBackend)
		if err != nil {
			return report, err
		}
	}
	claimedRuntime := make(map[string]struct{})
	claimedManifest := make(map[string]struct{}, len(prepared))
	adopted := make([]deviceRecord, 0, len(prepared))
	cleanupOnly := make([]cleanupDeviceRecord, 0, len(request.CleanupOnly))
	for index, item := range prepared {
		claimedManifest[filepath.Clean(filepath.Join(item.directory, targetPlanManifestFilename))] = struct{}{}
		reconciliation, record, ok := d.reconcileExpectedAndroidTarget(ctx, item, inventory, index < activeCount)
		reconciliation.PlanMatched = ok && reconciliation.RuntimeID != "" &&
			(reconciliation.Classification == ports.PhysicalResourceAdopted || reconciliation.Classification == ports.PhysicalResourceUncertain)
		missingCleanup := !ok && reconciliation.Classification == ports.PhysicalResourceMissing && record.plan.Allocation.InstanceName != ""
		if index < activeCount && ok {
			adopted = append(adopted, record)
			claimedRuntime[record.instance.RuntimeID] = struct{}{}
		} else if (index >= activeCount && ok) || missingCleanup {
			cleanupOnly = append(cleanupOnly, cleanupDeviceRecord{record: record, runtimePresent: ok})
			reconciliation.CleanupRequired = missingCleanup
			if ok {
				claimedRuntime[record.instance.RuntimeID] = struct{}{}
			}
		}
		report.Expected[index] = reconciliation
	}
	manifestPaths, scanConflicts, err := scanAndroidTargetManifests(d.build.TargetRoot)
	if err != nil {
		return report, domain.NewError(domain.CodeUnavailable, "cuttlefish.reconcile", "manifests", "could not scan bounded Android target manifests", err)
	}
	report.Conflicts = append(report.Conflicts, scanConflicts...)
	d.classifyUnclaimedAndroidTargets(ctx, &report, manifestPaths, claimedManifest, claimedRuntime, inventory)
	if err := ports.RequireDeadline(ctx, "cuttlefish.reconcile"); err != nil {
		for index := range report.Expected {
			report.Expected[index].CleanupRequired = false
			if report.Expected[index].Classification == ports.PhysicalResourceAdopted {
				report.Expected[index].Classification = ports.PhysicalResourceUncertain
				report.Expected[index].PlanMatched = false
				report.Expected[index].Diagnostic = "context ended before Android target adoption could be committed"
			}
		}
		return report, err
	}
	resetResults, err := d.recoverResetOutcomes(ctx, adopted, inventory)
	if err != nil {
		return report, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.reconcile", "reset_outcomes", "durable Android reset replay state is invalid", err)
	}
	quarantineResults, err := recoverQuarantineOutcomes(adopted)
	if err != nil {
		return report, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.reconcile", "quarantine_outcomes", "durable Android quarantine replay state is invalid", err)
	}
	if err := d.commitReconciledAndroidTargets(adopted, cleanupOnly, resetResults, quarantineResults); err != nil {
		return report, err
	}
	return report, nil
}

func loadExactAndroidRuntimeInventory(ctx context.Context, backend BackendInventory) (map[string]struct{}, error) {
	runtimeIDs, err := backend.ListRuntimeIDs(ctx)
	if err != nil {
		return nil, domain.NewError(domain.CodeUnavailable, "cuttlefish.reconcile", "inventory", "Android runtime inventory failed", err)
	}
	inventory, err := exactRuntimeIDSet(runtimeIDs)
	if err != nil {
		return nil, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.reconcile", "inventory", "Android runtime inventory is ambiguous", err)
	}
	return inventory, nil
}

func (d *Driver) reconcileExpectedAndroidTarget(ctx context.Context, item expectedAndroidTarget, inventory map[string]struct{}, active bool) (ports.TargetReconciliation, deviceRecord, bool) {
	result := ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain}
	targetPath := filepath.Join(item.directory, targetPlanManifestFilename)
	if _, err := os.Lstat(targetPath); errors.Is(err, os.ErrNotExist) {
		if info, directoryErr := os.Lstat(item.directory); directoryErr == nil {
			result.Classification = ports.PhysicalResourceForeign
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				result.Diagnostic = "expected generation state path exists but is not an exact regular directory"
			} else {
				result.Diagnostic = "expected generation state directory exists without its exact target manifest"
			}
			return result, deviceRecord{}, false
		} else if !errors.Is(directoryErr, os.ErrNotExist) {
			result.Diagnostic = "expected generation state directory could not be inspected: " + directoryErr.Error()
			return result, deviceRecord{}, false
		}
		_, authoritativeLookup := d.allocator.(AllocationLookup)
		allocation, found, lookupErr := d.lookupExpectedAllocation(ctx, item.ref)
		if lookupErr != nil {
			result.Diagnostic = "durable allocation lookup failed: " + lookupErr.Error()
			return result, deviceRecord{}, false
		}
		if !found {
			if !authoritativeLookup {
				result.Diagnostic = "no target manifest exists and the allocator cannot authoritatively prove endpoint absence"
				return result, deviceRecord{}, false
			}
			result.Classification = ports.PhysicalResourceMissing
			result.Diagnostic = "authoritative target state, durable allocation, and emulator inventory contain no physical resource for the expected generation"
			return result, deviceRecord{}, false
		}
		if _, exists := inventory[allocation.InstanceName]; exists {
			result.RuntimeID = allocation.InstanceName
			result.Classification = ports.PhysicalResourceForeign
			result.Diagnostic = "runtime owns the expected durable endpoint but its exact target/runtime manifests are absent"
			return result, deviceRecord{}, false
		}
		expectedPlan, planErr := BuildVirtualDevicePlan(item.input, d.build, allocation)
		if planErr != nil {
			result.Diagnostic = "durable allocation could not be bound to the exact expected Android plan: " + planErr.Error()
			return result, deviceRecord{}, false
		}
		planSignature, signatureErr := targetPlanSignature(item.input)
		if signatureErr != nil {
			result.Diagnostic = "expected target request signature is invalid: " + signatureErr.Error()
			return result, deviceRecord{}, false
		}
		result.Classification = ports.PhysicalResourceMissing
		result.Diagnostic = "authoritative emulator inventory contains no runtime for the durable expected assignment"
		return result, deviceRecord{input: item.input, planSignature: planSignature, plan: expectedPlan, instance: instanceFromPlan(expectedPlan)}, false
	} else if err != nil {
		result.Diagnostic = "target manifest could not be inspected: " + err.Error()
		return result, deviceRecord{}, false
	}
	target, runtimeManifest, err := loadTargetRuntimeManifests(item.directory)
	if err != nil {
		result.Classification = ports.PhysicalResourceForeign
		result.Diagnostic = "target/runtime manifests are invalid: " + err.Error()
		return result, deviceRecord{}, false
	}
	expectedPlan, err := BuildVirtualDevicePlan(item.input, d.build, target.Plan.Allocation)
	if err != nil || validateExpectedManifests(expectedPlan, target, runtimeManifest) != nil {
		result.RuntimeID = runtimeManifest.Instance.RuntimeID
		result.Classification = ports.PhysicalResourceForeign
		result.Diagnostic = "persisted target/runtime plan differs from the expected authority plan"
		return result, deviceRecord{}, false
	}
	planSignature, err := targetPlanSignature(item.input)
	if err != nil {
		result.RuntimeID = runtimeManifest.Instance.RuntimeID
		result.Classification = ports.PhysicalResourceForeign
		result.Diagnostic = "expected target request signature is invalid: " + err.Error()
		return result, deviceRecord{}, false
	}
	record := deviceRecord{input: item.input, planSignature: planSignature, plan: expectedPlan, instance: runtimeManifest.Instance}
	result.RuntimeID = runtimeManifest.Instance.RuntimeID
	if _, exists := inventory[runtimeManifest.Instance.RuntimeID]; !exists {
		result.RuntimeID = ""
		result.Classification = ports.PhysicalResourceMissing
		result.Diagnostic = "authoritative emulator inventory contains no exact persisted runtime"
		return result, record, false
	}
	stopped, hasStoppedBoundary, stopErr := loadStoppedTargetAuthority(target, runtimeManifest)
	if stopErr != nil {
		result.Classification = ports.PhysicalResourceForeign
		result.Diagnostic = "durable Android stopped-target authority is invalid: " + stopErr.Error()
		return result, deviceRecord{}, false
	}
	if hasStoppedBoundary {
		observedStopped, err := d.adoptStoppedAndroidRuntime(ctx, runtimeManifest.Instance, stopped.Containment)
		if err != nil {
			result.Classification = ports.PhysicalResourceUncertain
			result.Diagnostic = "exact stopped Android runtime could not be live-verified and adopted: " + err.Error()
			return result, deviceRecord{}, false
		}
		if err := d.adoptExpectedAllocation(ctx, item.ref, expectedPlan.Allocation); err != nil {
			result.Classification = ports.PhysicalResourceUncertain
			result.Diagnostic = "exact durable allocation could not be re-adopted: " + err.Error()
			return result, deviceRecord{}, false
		}
		status := ports.TargetStatus{
			TargetID: expectedPlan.TargetID, Generation: expectedPlan.Generation, Kind: domain.TargetAndroidVirtualDevice,
			State: stopped.State, Ready: false, RuntimeID: runtimeManifest.Instance.RuntimeID,
			DeviceSerial: expectedPlan.Allocation.Serial, ObservedAt: observedStopped.ObservedAt,
		}
		result.Classification = ports.PhysicalResourceAdopted
		result.Diagnostic = "exact persisted target, runtime, stopped authority, endpoint, and containment proof match"
		record.status = status
		return result, record, true
	}
	var state ReadinessState
	if active {
		state, err = d.backend.WaitReady(ctx, runtimeManifest.Instance)
	} else {
		state, err = d.backend.Inspect(ctx, runtimeManifest.Instance)
	}
	if err != nil {
		if stopped, stoppedRecord, stoppedOK := d.reconcileUnexpectedStoppedAndroidTarget(ctx, item, expectedPlan, record, active, err); stoppedOK {
			return stopped, stoppedRecord, true
		} else if stopped.Diagnostic != "" {
			result.Diagnostic = stopped.Diagnostic
			return result, deviceRecord{}, false
		}
		result.Classification = ports.PhysicalResourceUncertain
		result.Diagnostic = "exact runtime readiness/resource proof failed: " + err.Error()
		return result, deviceRecord{}, false
	}
	if !state.Ready() || !reflect.DeepEqual(state.Identity, runtimeManifest.Readiness.Identity) {
		result.Classification = ports.PhysicalResourceForeign
		result.Diagnostic = "observed Android readiness or device identity differs from the persisted runtime manifest"
		return result, deviceRecord{}, false
	}
	if err := d.adoptExpectedAllocation(ctx, item.ref, expectedPlan.Allocation); err != nil {
		result.Classification = ports.PhysicalResourceUncertain
		result.Diagnostic = "exact durable allocation could not be re-adopted: " + err.Error()
		return result, deviceRecord{}, false
	}
	status := ports.TargetStatus{
		TargetID: expectedPlan.TargetID, Generation: expectedPlan.Generation, Kind: domain.TargetAndroidVirtualDevice,
		State: domain.TargetGenerationReady, Ready: true, RuntimeID: runtimeManifest.Instance.RuntimeID,
		DeviceSerial: expectedPlan.Allocation.Serial, ObservedAt: state.ObservedAt,
	}
	result.Classification = ports.PhysicalResourceAdopted
	result.Diagnostic = "exact persisted target/runtime plan, endpoint, readiness, and Android identity match"
	record.status = status
	return result, record, true
}

func (d *Driver) reconcileUnexpectedStoppedAndroidTarget(
	ctx context.Context,
	item expectedAndroidTarget,
	plan VirtualDevicePlan,
	record deviceRecord,
	active bool,
	readinessErr error,
) (ports.TargetReconciliation, deviceRecord, bool) {
	result := ports.TargetReconciliation{Ref: item.ref, RuntimeID: record.instance.RuntimeID, Classification: ports.PhysicalResourceUncertain}
	inspector, supported := d.backend.(BackendStoppedInspector)
	if !supported {
		return ports.TargetReconciliation{}, deviceRecord{}, false
	}
	proof, err := inspector.InspectStopped(ctx, record.instance)
	if err != nil {
		result.Diagnostic = "exact runtime readiness/resource proof failed: " + errors.Join(readinessErr, fmt.Errorf("stopped-runtime proof failed: %w", err)).Error()
		return result, deviceRecord{}, false
	}
	if err := validateStoppedAdoption(record.instance, proof); err != nil {
		result.Diagnostic = "exact runtime readiness/resource proof failed: " + errors.Join(readinessErr, fmt.Errorf("stopped-runtime proof is incomplete: %w", err)).Error()
		return result, deviceRecord{}, false
	}
	if err := d.adoptExpectedAllocation(ctx, item.ref, plan.Allocation); err != nil {
		result.Diagnostic = "exact stopped runtime was proven but its durable allocation could not be re-adopted: " + err.Error()
		return result, deviceRecord{}, false
	}
	record.status = ports.TargetStatus{
		TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetAndroidVirtualDevice,
		State: domain.TargetGenerationResettable, Ready: false, RuntimeID: record.instance.RuntimeID,
		DeviceSerial: plan.Allocation.Serial, ObservedAt: proof.ObservedAt,
	}
	if active {
		result.Diagnostic = "exact plan-owned Android runtime is stopped and preserved; execution remains unavailable pending authorized interrupted-run recovery"
		return result, record, true
	}
	result.Classification = ports.PhysicalResourceAdopted
	result.Diagnostic = "exact cleanup-only Android runtime is stopped, unreachable, preserved, and bound to its persisted plan"
	return result, record, true
}

func (d *Driver) adoptStoppedAndroidRuntime(ctx context.Context, instance Instance, containment BackendQuarantineState) (BackendQuarantineState, error) {
	adopter, supported := d.backend.(BackendStoppedAdopter)
	if !supported {
		return BackendQuarantineState{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.reconcile", "stopped_adoption", "backend cannot live-verify and adopt a durably stopped runtime", nil)
	}
	observed, err := adopter.AdoptStopped(ctx, instance, containment)
	if err != nil {
		return BackendQuarantineState{}, err
	}
	if observed.RuntimeID != instance.RuntimeID || !observed.ExecutionStopped || !observed.NetworkUnreachable || !observed.StatePreserved || observed.ObservedAt.Before(containment.ObservedAt) {
		return BackendQuarantineState{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.reconcile", "stopped_adoption", "backend returned incomplete, stale, or foreign stopped-runtime evidence", nil)
	}
	return observed, nil
}

func (d *Driver) prepareExpectedAndroidTargets(expected []ports.TargetPlan) ([]expectedAndroidTarget, error) {
	if len(expected) > maximumReconciliationManifests {
		return nil, domain.NewError(domain.CodeResourceExhausted, "cuttlefish.reconcile", "expected", "expected generation set exceeds the reconciliation safety bound", nil)
	}
	result := make([]expectedAndroidTarget, 0, len(expected))
	refs := make(map[string]struct{}, len(expected))
	keys := make(map[string]struct{}, len(expected))
	for index, input := range expected {
		if err := input.Validate(); err != nil {
			return nil, fmt.Errorf("expected Android target %d: %w", index, err)
		}
		if input.Template.Kind != domain.TargetAndroidVirtualDevice {
			return nil, fmt.Errorf("expected Android target %d has another target kind", index)
		}
		spec := input.Generation.Spec()
		key := deviceKey(spec.TargetID, spec.Generation)
		if _, duplicate := refs[key]; duplicate {
			return nil, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.reconcile", "expected", "contains duplicate target generations", nil)
		}
		if _, duplicate := keys[input.IdempotencyKey]; duplicate {
			return nil, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.reconcile", "expected", "contains duplicate idempotency keys", nil)
		}
		refs[key], keys[input.IdempotencyKey] = struct{}{}, struct{}{}
		directory := filepath.Join(d.build.TargetRoot, spec.TargetID.String(), "generations", strconv.FormatUint(uint64(spec.Generation), 10))
		result = append(result, expectedAndroidTarget{input: input, ref: ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}, directory: directory})
	}
	return result, nil
}

func (d *Driver) classifyUnclaimedAndroidTargets(ctx context.Context, report *ports.TargetReconciliationReport, paths []string, claimedPaths, claimedRuntime map[string]struct{}, inventory map[string]struct{}) {
	manifestRuntime := make(map[string]struct{})
	for _, path := range paths {
		clean := filepath.Clean(path)
		directory := filepath.Dir(path)
		target, runtimeManifest, err := loadTargetRuntimeManifests(directory)
		if _, claimed := claimedPaths[clean]; claimed {
			if err == nil {
				manifestRuntime[runtimeManifest.Instance.RuntimeID] = struct{}{}
			}
			continue
		}
		if err != nil {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{Name: path, Classification: ports.PhysicalResourceForeign, Diagnostic: "unclaimed Android manifest is invalid: " + err.Error()})
			continue
		}
		ref := ports.TargetRef{ID: target.Plan.TargetID, Generation: target.Plan.Generation}
		if ref.Validate() != nil || target.Plan.Validate(d.build.TargetRoot, d.build.SystemImageRoot) != nil || filepath.Clean(target.Plan.StateDirectory) != filepath.Clean(directory) {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: runtimeManifest.Instance.RuntimeID, Name: path, Classification: ports.PhysicalResourceForeign, Diagnostic: "unclaimed manifest has a non-canonical physical identity"})
			continue
		}
		manifestRuntime[runtimeManifest.Instance.RuntimeID] = struct{}{}
		if _, found := inventory[runtimeManifest.Instance.RuntimeID]; !found {
			continue
		}
		if _, claimed := claimedRuntime[runtimeManifest.Instance.RuntimeID]; claimed {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: runtimeManifest.Instance.RuntimeID, Name: target.Plan.Name, Classification: ports.PhysicalResourceForeign, Diagnostic: "one runtime is claimed by both expected and unclaimed manifests"})
			continue
		}
		stopped, hasStoppedBoundary, stopErr := loadStoppedTargetAuthority(target, runtimeManifest)
		if stopErr != nil {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: runtimeManifest.Instance.RuntimeID, Name: target.Plan.Name, Classification: ports.PhysicalResourceForeign, Diagnostic: "unclaimed stopped-target authority is invalid: " + stopErr.Error()})
			continue
		}
		if hasStoppedBoundary {
			if _, adoptErr := d.adoptStoppedAndroidRuntime(ctx, runtimeManifest.Instance, stopped.Containment); adoptErr != nil {
				report.Unclaimed = append(report.Unclaimed, ports.TargetReconciliation{Ref: ref, RuntimeID: runtimeManifest.Instance.RuntimeID, Classification: ports.PhysicalResourceUncertain, Diagnostic: "unclaimed stopped Android runtime could not be live-verified: " + adoptErr.Error()})
				continue
			}
			report.Unclaimed = append(report.Unclaimed, ports.TargetReconciliation{Ref: ref, RuntimeID: runtimeManifest.Instance.RuntimeID, Classification: ports.PhysicalResourceOrphan, Diagnostic: "valid stopped world-owned Android runtime is absent from expected durable plans"})
			continue
		}
		state, inspectErr := d.backend.Inspect(ctx, runtimeManifest.Instance)
		classification := ports.PhysicalResourceOrphan
		diagnostic := "valid world-owned Android runtime is absent from expected durable plans"
		if inspectErr != nil {
			classification = ports.PhysicalResourceUncertain
			diagnostic = "unclaimed exact runtime inspection failed: " + inspectErr.Error()
		} else if !state.Ready() || !reflect.DeepEqual(state.Identity, runtimeManifest.Readiness.Identity) {
			classification = ports.PhysicalResourceForeign
			diagnostic = "unclaimed runtime differs from its persisted Android identity"
		}
		report.Unclaimed = append(report.Unclaimed, ports.TargetReconciliation{Ref: ref, RuntimeID: runtimeManifest.Instance.RuntimeID, Classification: classification, Diagnostic: diagnostic})
	}
	for runtimeID := range inventory {
		if _, claimed := claimedRuntime[runtimeID]; claimed {
			continue
		}
		if _, manifested := manifestRuntime[runtimeID]; manifested {
			continue
		}
		if strings.HasPrefix(runtimeID, "world-emulator-") {
			report.Conflicts = append(report.Conflicts, ports.PhysicalResourceConflict{ResourceID: runtimeID, Name: runtimeID, Classification: ports.PhysicalResourceForeign, Diagnostic: "world-named Android runtime has no exact target/runtime manifest"})
		}
	}
	sort.Slice(report.Unclaimed, func(i, j int) bool { return report.Unclaimed[i].RuntimeID < report.Unclaimed[j].RuntimeID })
	sort.Slice(report.Conflicts, func(i, j int) bool {
		if report.Conflicts[i].ResourceID == report.Conflicts[j].ResourceID {
			return report.Conflicts[i].Name < report.Conflicts[j].Name
		}
		return report.Conflicts[i].ResourceID < report.Conflicts[j].ResourceID
	})
}

func (d *Driver) resolveAndroidDestroy(ctx context.Context, ref ports.TargetRef, record deviceRecord, found bool) (deviceRecord, bool, error) {
	if found {
		return record, false, nil
	}
	directory := filepath.Join(d.build.TargetRoot, ref.ID.String(), "generations", strconv.FormatUint(uint64(ref.Generation), 10))
	if _, err := os.Lstat(directory); err == nil {
		return deviceRecord{}, false, cleanupOnlyPlanRequiredError()
	} else if !errors.Is(err, os.ErrNotExist) {
		return deviceRecord{}, false, domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "state_directory", "target state directory could not be inspected", err)
	}
	_, allocated, err := d.lookupExpectedAllocation(ctx, ref)
	if err != nil {
		return deviceRecord{}, false, domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "allocation", "durable endpoint lookup failed", err)
	}
	if allocated {
		return deviceRecord{}, false, cleanupOnlyPlanRequiredError()
	}
	return deviceRecord{}, true, nil
}

func cleanupOnlyPlanRequiredError() error {
	return domain.NewError(
		domain.CodeFailedPrecondition,
		"cuttlefish.destroy",
		"cleanup_plan",
		"present Android generation requires an exact cleanup-only reconciliation plan before destruction",
		nil,
	)
}

func scanAndroidTargetManifests(root string) ([]string, []ports.PhysicalResourceConflict, error) {
	paths := make([]string, 0)
	conflicts := make([]ports.PhysicalResourceConflict, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) && path == root {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() || entry.Name() != targetPlanManifestFilename {
			return nil
		}
		if len(paths) >= maximumReconciliationManifests {
			return fmt.Errorf("Android target manifest count exceeds %d", maximumReconciliationManifests)
		}
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			conflicts = append(conflicts, ports.PhysicalResourceConflict{Name: path, Classification: ports.PhysicalResourceForeign, Diagnostic: "target manifest path is not a regular file"})
			return nil
		}
		paths = append(paths, filepath.Clean(path))
		return nil
	})
	sort.Strings(paths)
	return paths, conflicts, err
}

func exactRuntimeIDSet(values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !safeInstanceName(value) {
			return nil, fmt.Errorf("runtime inventory contains unsafe or empty identity %q", value)
		}
		if _, duplicate := result[value]; duplicate {
			return nil, fmt.Errorf("runtime inventory duplicates identity %q", value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}

func (d *Driver) lookupExpectedAllocation(ctx context.Context, ref ports.TargetRef) (Allocation, bool, error) {
	lookup, ok := d.allocator.(AllocationLookup)
	if !ok {
		return Allocation{}, false, nil
	}
	return lookup.LookupExpected(ctx, ref.ID, ref.Generation)
}

func (d *Driver) adoptExpectedAllocation(ctx context.Context, ref ports.TargetRef, allocation Allocation) error {
	if adopter, ok := d.allocator.(AllocationAdopter); ok {
		return adopter.AdoptExpected(ctx, ref.ID, ref.Generation, allocation)
	}
	reserved, err := d.allocator.Reserve(ctx, ref.ID, ref.Generation)
	if err != nil {
		return err
	}
	if reserved != allocation {
		return fmt.Errorf("allocator returned another physical endpoint")
	}
	return nil
}

func recoverQuarantineOutcomes(records []deviceRecord) (map[string]quarantineOutcome, error) {
	result := make(map[string]quarantineOutcome)
	for _, record := range records {
		manifest, found, err := loadGenerationQuarantine(record.plan)
		if err != nil {
			return nil, err
		}
		if !found {
			if record.status.State == domain.TargetGenerationQuarantined {
				return nil, fmt.Errorf("quarantined generation has no exact durable request")
			}
			continue
		}
		if record.status.State != domain.TargetGenerationQuarantined || manifest.RuntimeID != record.instance.RuntimeID || manifest.Allocation != record.instance.Allocation {
			return nil, fmt.Errorf("quarantine request does not bind the adopted stopped runtime")
		}
		plan, err := manifest.QuarantinePlan.restore()
		if err != nil {
			return nil, err
		}
		containment := manifest.Containment.restore()
		evidence := ports.TargetQuarantineEvidence{
			Target: plan.Target, RuntimeID: manifest.RuntimeID,
			ExecutionStopped: containment.ExecutionStopped, NetworkUnreachable: containment.NetworkUnreachable,
			StatePreserved: containment.StatePreserved, ObservedAt: containment.ObservedAt,
		}
		if err := evidence.Validate(plan.Target); err != nil {
			return nil, err
		}
		outcome := quarantineOutcome{plan: plan, evidence: evidence}
		if prior, duplicate := result[plan.IdempotencyKey]; duplicate && prior != outcome {
			return nil, fmt.Errorf("multiple Android quarantine requests reuse idempotency key %q", plan.IdempotencyKey)
		}
		result[plan.IdempotencyKey] = outcome
	}
	return result, nil
}

func (d *Driver) commitReconciledAndroidTargets(records []deviceRecord, cleanupRecords []cleanupDeviceRecord, resetResults map[string]resetOutcome, quarantineResults map[string]quarantineOutcome) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, run := range d.runs {
		if !run.stopped {
			return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.reconcile", "runs", "cannot replace in-memory ownership while a run is active", nil)
		}
	}
	d.targets = make(map[string]deviceRecord, len(records))
	d.cleanupOnly = make(map[string]cleanupDeviceRecord, len(cleanupRecords))
	d.runs = make(map[string]*runRecord)
	d.idempotency = make(map[string]string, len(records))
	d.resetResults = make(map[string]resetOutcome, len(resetResults))
	d.quarantines = make(map[string]quarantineOutcome, len(quarantineResults))
	for _, record := range records {
		key := deviceKey(record.plan.TargetID, record.plan.Generation)
		d.targets[key] = record
		d.idempotency[record.input.IdempotencyKey] = key
	}
	for _, record := range cleanupRecords {
		key := deviceKey(record.record.plan.TargetID, record.record.plan.Generation)
		d.cleanupOnly[key] = record
	}
	for key, outcome := range resetResults {
		d.resetResults[key] = outcome
	}
	for key, outcome := range quarantineResults {
		d.quarantines[key] = outcome
	}
	return nil
}

var _ ports.TargetReconciler = (*Driver)(nil)
