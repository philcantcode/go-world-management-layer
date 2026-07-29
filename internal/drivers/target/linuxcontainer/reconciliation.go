package linuxcontainer

import (
	"context"
	"errors"
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
	input       ports.TargetPlan
	plan        ContainerPlan
	ref         ports.TargetRef
	cleanupOnly bool
}

// ReconcileTargets inventories the runtime and adopts only unique resources
// whose complete labels and physical Docker configuration match a persisted
// expected plan.
// ReconcileTargets inventories cleanup-only generations without
// replaying reset/quarantine work, probing the guest, or adopting them for
// execution. Their full plans are retained only as Destroy authority.
func (d *Driver) ReconcileTargets(ctx context.Context, request ports.TargetReconciliationRequest) (ports.TargetReconciliationReport, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.reconcile"); err != nil {
		return ports.TargetReconciliationReport{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()

	prepared, err := d.prepareExpectedTargets(request)
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
	// A reset intent is durable destructive authority. Resume it before the
	// adoption pass, then take a fresh authoritative inventory so neither the
	// retired generation nor a partially-created successor is classified from
	// stale observations.
	resetAuthorityPresent := make(map[string]bool, len(prepared))
	resetRecoveryErrors := make(map[string]error)
	for _, item := range prepared {
		if item.cleanupOnly {
			continue
		}
		key := targetKey(item.ref.ID, item.ref.Generation)
		intent, _, intentFound, receiptFound, loadErr := loadResetRecords(item.plan.TargetDirectory, d.build.TargetRoot, &item.input)
		if loadErr != nil {
			resetRecoveryErrors[key] = loadErr
			continue
		}
		if !intentFound {
			continue
		}
		resetAuthorityPresent[key] = true
		if receiptFound {
			continue
		}
		if err := d.resumePersistedReset(ctx, inventory, item, intent, states); err != nil {
			resetRecoveryErrors[key] = err
			continue
		}
		states, err = inventory.ListContainers(ctx)
		if err != nil {
			return report, domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "inventory", "target runtime inventory failed after reset recovery", err)
		}
		if err := validateRuntimeInventory(states); err != nil {
			return report, domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "inventory", "target runtime inventory is ambiguous after reset recovery", err)
		}
	}

	claimed := make([]bool, len(states))
	adopted := make([]targetRecord, 0, len(prepared))
	cleanupOnly := make([]targetRecord, 0, len(request.CleanupOnly))
	for expectedIndex, item := range prepared {
		candidates := targetCandidates(states, item)
		for _, candidate := range candidates {
			claimed[candidate] = true
		}
		switch len(candidates) {
		case 0:
			key := targetKey(item.ref.ID, item.ref.Generation)
			if recoveryErr := resetRecoveryErrors[key]; recoveryErr != nil {
				report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "durable reset recovery did not complete: " + recoveryErr.Error()}
			} else if resetAuthorityPresent[key] {
				report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "durable reset authority exists but its successor runtime is absent"}
			} else {
				report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceMissing, Diagnostic: "authoritative runtime inventory contains no matching resource"}
				directoryPresent, statErr := targetDirectoryPresent(item.plan.TargetDirectory)
				if statErr != nil {
					report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, Classification: ports.PhysicalResourceUncertain, Diagnostic: "target directory presence could not be established: " + statErr.Error()}
					continue
				}
				if directoryPresent {
					// A complete expected plan is sufficient to retain exact
					// cleanup authority, but a missing active generation remains
					// non-executable and startup still fails unless a separate
					// durable operation authorizes its destruction.
					cleanupOnly = append(cleanupOnly, cleanupOnlyTargetRecord(item, RuntimeState{}, report.ObservedAt))
					report.Expected[expectedIndex].CleanupRequired = true
					report.Expected[expectedIndex].Diagnostic = "authoritative runtime inventory contains no matching resource; exact persisted target directory remains"
				}
			}
		case 1:
			state := states[candidates[0]]
			if err := validateRuntimeIdentity(state, item.plan); err != nil {
				report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, RuntimeID: state.ID, Classification: ports.PhysicalResourceForeign, Diagnostic: err.Error()}
				continue
			}
			if err := requireCoherentRuntimeState(state, "linux_target.reconcile"); err != nil {
				report.Expected[expectedIndex] = ports.TargetReconciliation{Ref: item.ref, RuntimeID: state.ID, Classification: ports.PhysicalResourceForeign, Diagnostic: err.Error()}
				continue
			}
			if item.cleanupOnly {
				cleanupOnly = append(cleanupOnly, cleanupOnlyTargetRecord(item, state, report.ObservedAt))
				report.Expected[expectedIndex] = ports.TargetReconciliation{
					Ref: item.ref, RuntimeID: state.ID, Classification: ports.PhysicalResourceUncertain, PlanMatched: true,
					Diagnostic: "exact persisted cleanup plan and physical configuration match; generation was not adopted for work",
				}
				continue
			}
			resetIntent, resetReceipt, resetIntentFound, resetReceiptFound, err := loadResetRecords(item.plan.TargetDirectory, d.build.TargetRoot, &item.input)
			if err != nil {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "durable reset state is invalid: "+err.Error())
				continue
			}
			var reset *resetOutcome
			if resetIntentFound {
				if !resetReceiptFound {
					report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "durable reset intent has no completed physical receipt")
					continue
				}
				outcome := resetReceipt.outcome(resetIntent)
				if outcome.result.Status.RuntimeID != state.ID {
					report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceForeign, "durable reset receipt identifies another physical runtime")
					continue
				}
				reset = &outcome
			}
			quarantineIntent, quarantineReceipt, quarantineIntentFound, quarantineReceiptFound, err := loadQuarantineRecords(item.plan.TargetDirectory, d.build.TargetRoot, &item.plan)
			if err != nil {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "durable quarantine state is invalid: "+err.Error())
				continue
			}
			if quarantineIntentFound {
				evidence, recoveredState, recoveryErr := d.recoverPersistedQuarantine(ctx, item, quarantineIntent, quarantineReceipt, quarantineReceiptFound, state)
				if recoveryErr != nil {
					report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "durable quarantine could not be verified: "+recoveryErr.Error())
					continue
				}
				state = recoveredState
				status := adoptedQuarantinedTargetStatus(item.plan, state, evidence.ObservedAt)
				storedPlan, storedEvidence := quarantineIntent.Plan, evidence
				adopted = append(adopted, targetRecord{
					input: item.input, plan: item.plan, runtimeID: state.ID, status: status, reset: reset,
					quarantinePlan: &storedPlan, quarantine: &storedEvidence,
				})
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceAdopted, "exact stopped runtime and durable quarantine evidence match")
				continue
			}
			claim, claimed, err := loadTargetGenerationRunClaim(item.plan.TargetDirectory)
			if err != nil {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "durable target generation run claim is invalid: "+err.Error())
				continue
			}
			if claimed && !claim.matchesTarget(item.plan) {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceForeign, "durable run claim identifies another target generation")
				continue
			}
			if claimed {
				diagnostic := "durably claimed generation is stopped and requires reset"
				if state.Running {
					stopped, stopErr := d.requireOwnedRuntimeStopped(ctx, targetRecord{input: item.input, plan: item.plan, runtimeID: state.ID}, ports.StopForce)
					if stopped.ID == "" || stopped.Running {
						report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "durably claimed generation could not be contained without guest execution: "+fmt.Sprint(stopErr))
						continue
					}
					state = stopped
					diagnostic = "durably claimed running generation was contained without guest execution and requires interrupted-run recovery"
					if stopErr != nil {
						diagnostic += "; runtime stop reported an error after the stopped observation: " + stopErr.Error()
					}
				} else if err := requireStoppedRuntimeState(state, "linux_target.reconcile", dockercli.StoppedStatusExited); err != nil {
					report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceForeign, err.Error())
					continue
				}
				status := adoptedResettableTargetStatus(item.plan, state, d.now().UTC())
				adopted = append(adopted, targetRecord{input: item.input, plan: item.plan, runtimeID: state.ID, status: status, reset: reset})
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceAdopted, diagnostic)
				continue
			}
			if !state.Running {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "unclaimed matching target is unexpectedly stopped")
				continue
			}
			if err := requireLiveRuntimeState(state, "linux_target.reconcile"); err != nil {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceForeign, err.Error())
				continue
			}
			if err := d.requireGuestReadiness(ctx, state.ID, item.plan); err != nil {
				report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceUncertain, "world-guest readiness self-test failed: "+err.Error())
				continue
			}
			status := adoptedTargetStatus(item.plan, state, d.now().UTC())
			adopted = append(adopted, targetRecord{input: item.input, plan: item.plan, runtimeID: state.ID, status: status, reset: reset})
			report.Expected[expectedIndex] = matchedTargetObservation(item.ref, state.ID, ports.PhysicalResourceAdopted, "exact persisted plan and physical configuration match")
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
	d.rebuildTargetMaps(adopted, cleanupOnly)
	return report, nil
}

func matchedTargetObservation(ref ports.TargetRef, runtimeID string, classification ports.PhysicalResourceClassification, diagnostic string) ports.TargetReconciliation {
	return ports.TargetReconciliation{Ref: ref, RuntimeID: runtimeID, Classification: classification, PlanMatched: true, Diagnostic: diagnostic}
}

func cleanupOnlyTargetRecord(expected expectedTargetContainer, state RuntimeState, observedAt time.Time) targetRecord {
	return targetRecord{
		input: expected.input, plan: expected.plan, runtimeID: state.ID,
		status: ports.TargetStatus{
			TargetID: expected.ref.ID, Generation: expected.ref.Generation, Kind: domain.TargetLinuxContainer,
			State: domain.TargetGenerationProvisioning, Ready: false, RuntimeID: state.ID, CgroupID: state.CgroupID, ObservedAt: observedAt,
		},
	}
}

func (d *Driver) resumePersistedReset(ctx context.Context, inventory RuntimeInventory, item expectedTargetContainer, intent persistedResetIntent, states []RuntimeState) error {
	nextCandidates := targetCandidates(states, item)
	var nextState RuntimeState
	switch len(nextCandidates) {
	case 0:
		runtimeID, state, err := d.createRuntime(ctx, intent.NextPlan)
		if err != nil {
			// Preserve the intent and any partial runtime. A later pass can inspect
			// and resume it without inventing destructive authority.
			return domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "reset.create", "could not resume creation of the reset successor", err)
		}
		if runtimeID == "" || state.ID != runtimeID {
			return domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "reset.create", "reset successor creation returned an inconsistent runtime identity", nil)
		}
		nextState = state
	case 1:
		nextState = states[nextCandidates[0]]
		if err := validateRuntimeIdentity(nextState, intent.NextPlan); err != nil {
			return err
		}
		resumed, resumeErr := d.resumeExactRuntime(ctx, intent.NextPlan, nextState, "linux_target.reconcile")
		if resumeErr != nil {
			return resumeErr
		}
		nextState = resumed
	default:
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "reset.identity", "multiple runtime resources claim the reset successor", nil)
	}

	previousRef := intent.Reset.Previous
	previousCandidates := targetRefCandidates(states, previousRef)
	if len(previousCandidates) > 1 {
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "reset.previous", "multiple runtime resources claim the retired generation", nil)
	}
	if len(previousCandidates) == 1 {
		previousState := previousCandidates[0]
		if previousState.ID != intent.PreviousRuntimeID {
			return domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "reset.previous", "retired generation runtime identity differs from the durable reset authority", nil)
		}
		if err := validateRuntimeIdentity(previousState, intent.PreviousPlan); err != nil {
			return err
		}
		previous := targetRecord{plan: intent.PreviousPlan, runtimeID: previousState.ID}
		if _, err := d.requireOwnedRuntimeStopped(ctx, previous, ports.StopForce); err != nil {
			return domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "reset.previous_stop", "could not contain the retired generation", err)
		}
		if err := d.runtime.Remove(ctx, previousState.ID); err != nil {
			return domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "reset.previous_remove", "could not remove the retired generation", err)
		}
		observed, err := inventory.ListContainers(ctx)
		if err != nil {
			return domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "reset.previous_absence", "could not prove retired generation absence", err)
		}
		if err := validateRuntimeInventory(observed); err != nil {
			return domain.NewError(domain.CodeIntegrityViolation, "linux_target.reconcile", "reset.previous_absence", "runtime inventory is ambiguous after retired generation removal", err)
		}
		if len(targetRefCandidates(observed, previousRef)) != 0 {
			return domain.NewError(domain.CodeFailedPrecondition, "linux_target.reconcile", "reset.previous_absence", "retired generation remains present after removal", nil)
		}
	}

	status := ports.TargetStatus{
		TargetID: intent.NextPlan.TargetID, Generation: intent.NextPlan.Generation, Kind: domain.TargetLinuxContainer,
		State: domain.TargetGenerationReady, Ready: true, RuntimeID: nextState.ID, CgroupID: nextState.CgroupID, ObservedAt: d.now().UTC(),
	}
	result := ports.TargetResult{Status: status, Created: true}
	var outcomeErr error
	if err := removeTargetDirectoryIfPresent(d.build.TargetRoot, intent.PreviousPlan.TargetDirectory); err != nil {
		outcomeErr = domain.NewError(domain.CodeUnavailable, "linux_target.reset", "cleanup", "replacement is ready but the retired target directory could not be removed", err)
	}
	receipt, err := newResetReceipt(intent, result, outcomeErr)
	if err != nil {
		return domain.NewError(domain.CodeInternal, "linux_target.reconcile", "reset.receipt", "could not construct the recovered reset receipt", err)
	}
	if err := persistResetReceipt(intent.NextPlan.TargetDirectory, receipt); err != nil {
		return domain.NewError(domain.CodeUnavailable, "linux_target.reconcile", "reset.receipt", "could not persist the recovered reset receipt", err)
	}
	return nil
}

func (d *Driver) recoverPersistedQuarantine(ctx context.Context, item expectedTargetContainer, intent persistedQuarantineIntent, receipt persistedQuarantineReceipt, receiptFound bool, state RuntimeState) (ports.TargetQuarantineEvidence, RuntimeState, error) {
	if state.ID != intent.RuntimeID {
		return ports.TargetQuarantineEvidence{}, state, fmt.Errorf("runtime identity differs from the durable quarantine intent")
	}
	containment, supported := d.runtime.(RuntimeContainment)
	if !supported {
		return ports.TargetQuarantineEvidence{}, state, fmt.Errorf("runtime cannot prove containment")
	}
	if receiptFound {
		if state.Running {
			_, containErr := containment.Quarantine(ctx, state.ID)
			return ports.TargetQuarantineEvidence{}, state, errors.Join(fmt.Errorf("runtime was observed running after durable quarantine evidence"), containErr)
		}
		if err := requireStoppedRuntimeState(state, "linux_target.reconcile", dockercli.StoppedStatusExited); err != nil {
			return ports.TargetQuarantineEvidence{}, state, err
		}
		return receipt.Evidence, state, nil
	}
	observed, err := containment.Quarantine(ctx, state.ID)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	evidence := ports.TargetQuarantineEvidence{
		Target: item.ref, RuntimeID: observed.RuntimeID, ExecutionStopped: observed.ExecutionStopped,
		NetworkUnreachable: observed.NetworkUnreachable, StatePreserved: observed.StatePreserved, ObservedAt: observed.ObservedAt.UTC(),
	}
	if err := evidence.Validate(item.ref); err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	if evidence.RuntimeID != state.ID {
		return ports.TargetQuarantineEvidence{}, state, fmt.Errorf("containment evidence identifies another runtime")
	}
	verified, err := d.runtime.Inspect(ctx, state.ID)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	if err := requireExactRuntimeID(verified, state.ID, "linux_target.reconcile"); err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	if err := validateRuntimeIdentity(verified, item.plan); err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	if err := requireStoppedRuntimeState(verified, "linux_target.reconcile", dockercli.StoppedStatusExited); err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	receipt, err = newQuarantineReceipt(intent, evidence)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	if err := persistQuarantineReceipt(item.plan.TargetDirectory, receipt); err != nil {
		return ports.TargetQuarantineEvidence{}, state, err
	}
	return evidence, verified, nil
}

func (d *Driver) prepareExpectedTargets(request ports.TargetReconciliationRequest) ([]expectedTargetContainer, error) {
	count := len(request.Active) + len(request.CleanupOnly)
	if count > dockercli.MaximumInventoryContainers {
		return nil, domain.NewError(domain.CodeResourceExhausted, "linux_target.reconcile", "expected", "expected generation set exceeds the reconciliation safety bound", nil)
	}
	prepared := make([]expectedTargetContainer, 0, count)
	refs := make(map[string]struct{}, count)
	requests := make(map[string]struct{}, count)
	appendPlans := func(expected []ports.TargetPlan, cleanupOnly bool) error {
		for index, input := range expected {
			plan, err := BuildContainerPlan(input, d.build)
			if err != nil {
				return fmt.Errorf("expected target %d: %w", index, err)
			}
			key := targetKey(plan.TargetID, plan.Generation)
			if _, duplicate := refs[key]; duplicate {
				return domain.NewError(domain.CodeInvalidArgument, "linux_target.reconcile", "expected", "contains duplicate target generations", nil)
			}
			if _, duplicate := requests[input.IdempotencyKey]; duplicate {
				return domain.NewError(domain.CodeInvalidArgument, "linux_target.reconcile", "expected", "contains duplicate idempotency keys", nil)
			}
			refs[key], requests[input.IdempotencyKey] = struct{}{}, struct{}{}
			prepared = append(prepared, expectedTargetContainer{input: input, plan: plan, ref: ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}, cleanupOnly: cleanupOnly})
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
	if !found {
		return "", false, domain.NewError(domain.CodeIntegrityViolation, "linux_target.destroy", "identity", "present runtime has no reconciled complete persisted target plan", nil)
	}
	if err := validateRuntimeIdentity(state, record.plan); err != nil {
		return "", false, err
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

func (d *Driver) rebuildTargetMaps(records, cleanupRecords []targetRecord) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, record := range d.targets {
		delete(d.idempotency, record.input.IdempotencyKey)
		delete(d.materialized, key)
	}
	d.targets = make(map[string]targetRecord, len(records))
	d.cleanupOnly = make(map[string]targetRecord, len(cleanupRecords))
	d.resetResults = make(map[string]resetOutcome)
	d.quarantines = make(map[string]quarantineOutcome)
	for _, record := range records {
		key := targetKey(record.plan.TargetID, record.plan.Generation)
		d.targets[key] = record
		d.idempotency[record.input.IdempotencyKey] = key
		if record.reset != nil {
			d.resetResults[record.reset.plan.IdempotencyKey] = *record.reset
		}
		if record.quarantinePlan != nil && record.quarantine != nil {
			d.quarantines[record.quarantinePlan.IdempotencyKey] = quarantineOutcome{plan: *record.quarantinePlan, evidence: *record.quarantine}
		}
	}
	for _, record := range cleanupRecords {
		d.cleanupOnly[targetKey(record.plan.TargetID, record.plan.Generation)] = record
	}
}

func validateRuntimeInventory(states []RuntimeState) error {
	ids := make(map[string]struct{}, len(states))
	for _, state := range states {
		if err := dockercli.RequireCanonicalContainerID(state.ID); err != nil {
			return fmt.Errorf("runtime resource has a non-canonical ID %q: %w", state.ID, err)
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
	if err := dockercli.RequireCanonicalContainerID(state.ID); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.inspect", "runtime_id", "runtime identity is non-canonical", err)
	}
	if state.Name != plan.Name {
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

func adoptedResettableTargetStatus(plan ContainerPlan, state RuntimeState, observedAt time.Time) ports.TargetStatus {
	return ports.TargetStatus{
		TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetLinuxContainer,
		State: domain.TargetGenerationResettable, Ready: false, RuntimeID: state.ID,
		CgroupID: state.CgroupID, ObservedAt: observedAt,
	}
}

func adoptedQuarantinedTargetStatus(plan ContainerPlan, state RuntimeState, observedAt time.Time) ports.TargetStatus {
	return ports.TargetStatus{
		TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetLinuxContainer,
		State: domain.TargetGenerationQuarantined, Ready: false, RuntimeID: state.ID,
		CgroupID: state.CgroupID, ObservedAt: observedAt.UTC(),
	}
}

func expectedTargetConfiguration(plan ContainerPlan) dockercli.Configuration {
	memorySwap, _ := dockercli.MemorySwapTotal(plan.Resources.MemoryBytes, plan.Resources.SwapBytes)
	configuration := dockercli.RestrictedContainerConfiguration()
	configuration.Image = plan.Image
	configuration.Runtime = plan.Runtime
	configuration.Hostname = plan.Name
	configuration.Entrypoint = []string{"/usr/local/bin/world-idle"}
	configuration.User = plan.User
	configuration.CapabilitiesAdd = append([]string(nil), plan.Capabilities...)
	configuration.MemoryBytes = plan.Resources.MemoryBytes
	configuration.MemorySwapBytes = memorySwap
	configuration.NanoCPUs = dockercli.NanoCPUs(plan.Resources.CPUMilli)
	configuration.PIDs = plan.Resources.PIDs
	for _, mount := range restrictedTargetBindMounts(plan) {
		dockercli.AddRestrictedBindMount(&configuration, mount.source, mount.target, mount.readOnly)
	}
	return configuration
}

func targetDirectory(root string, ref ports.TargetRef) string {
	return filepath.Join(root, ref.ID.String(), "generations", strconv.FormatUint(uint64(ref.Generation), 10))
}

func removeTargetDirectoryIfPresent(root, directory string) error {
	present, err := targetDirectoryPresent(directory)
	if err != nil || !present {
		return err
	}
	return removeTargetDirectory(root, directory)
}

func targetDirectoryPresent(directory string) (bool, error) {
	_, err := os.Lstat(directory)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

var _ ports.TargetReconciler = (*Driver)(nil)
