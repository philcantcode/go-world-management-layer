package cuttlefish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	resetTransitionManifestFilename = "world-reset-transition.json"
	resetOutcomeManifestFilename    = "world-reset-outcome.json"
)

type persistedResetPlan struct {
	IdempotencyKey     string          `json:"idempotency_key"`
	LeaseID            string          `json:"lease_id"`
	TargetID           string          `json:"target_id"`
	PreviousGeneration uint64          `json:"previous_generation"`
	NextGeneration     uint64          `json:"next_generation"`
	Mode               ports.ResetMode `json:"mode"`
	SnapshotName       string          `json:"snapshot_name,omitempty"`
	IncidentID         string          `json:"incident_id,omitempty"`
}

type resetTransitionManifest struct {
	Version            int                `json:"version"`
	ResetPlanDigest    string             `json:"reset_plan_digest"`
	ResetPlan          persistedResetPlan `json:"reset_plan"`
	PreviousPlanDigest string             `json:"previous_plan_digest"`
	PreviousPlan       VirtualDevicePlan  `json:"previous_plan"`
	PreviousInstance   Instance           `json:"previous_instance"`
	NextPlanDigest     string             `json:"next_plan_digest"`
	NextRuntimeID      string             `json:"next_runtime_id"`
	NextAllocation     Allocation         `json:"next_allocation"`
	PersistedAt        time.Time          `json:"persisted_at"`
}

type resetOutcomeManifest struct {
	Version         int                  `json:"version"`
	ResetPlanDigest string               `json:"reset_plan_digest"`
	NextPlanDigest  string               `json:"next_plan_digest"`
	NextRuntimeID   string               `json:"next_runtime_id"`
	NextAllocation  Allocation           `json:"next_allocation"`
	Result          ports.TargetResult   `json:"result"`
	Error           *persistedResetError `json:"error,omitempty"`
	PersistedAt     time.Time            `json:"persisted_at"`
}

type persistedResetError struct {
	Code      domain.ErrorCode  `json:"code"`
	Operation string            `json:"operation"`
	Field     string            `json:"field,omitempty"`
	Message   string            `json:"message"`
	Details   map[string]string `json:"details,omitempty"`
}

func persistedResetPlanFrom(plan ports.ResetPlan) persistedResetPlan {
	incidentID := ""
	if !plan.IncidentID.IsZero() {
		incidentID = plan.IncidentID.String()
	}
	return persistedResetPlan{
		IdempotencyKey: plan.IdempotencyKey, LeaseID: plan.LeaseID.String(), TargetID: plan.Previous.ID.String(),
		PreviousGeneration: uint64(plan.Previous.Generation), NextGeneration: uint64(plan.NextGeneration), Mode: plan.Mode,
		SnapshotName: plan.SnapshotName, IncidentID: incidentID,
	}
}

func (p persistedResetPlan) restore() (ports.ResetPlan, error) {
	leaseID, err := domain.ParseLeaseID(p.LeaseID)
	if err != nil {
		return ports.ResetPlan{}, err
	}
	targetID, err := domain.ParseTargetID(p.TargetID)
	if err != nil {
		return ports.ResetPlan{}, err
	}
	var incidentID domain.IncidentID
	if p.IncidentID != "" {
		incidentID, err = domain.ParseIncidentID(p.IncidentID)
		if err != nil {
			return ports.ResetPlan{}, err
		}
	}
	plan := ports.ResetPlan{
		IdempotencyKey: p.IdempotencyKey, LeaseID: leaseID,
		Previous:       ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(p.PreviousGeneration)},
		NextGeneration: domain.TargetGeneration(p.NextGeneration), Mode: p.Mode, SnapshotName: p.SnapshotName, IncidentID: incidentID,
	}
	return plan, plan.Validate()
}

func resetPlanDigest(plan ports.ResetPlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	return canonicalAndroidPlanDigest("world.android-reset-plan.v1\n", persistedResetPlanFrom(plan))
}

func commitExpectedResetTransition(previous deviceRecord, next VirtualDevicePlan, reset ports.ResetPlan, at time.Time) (resetTransitionManifest, error) {
	expected, err := expectedResetTransition(previous, next, reset, at)
	if err != nil {
		return resetTransitionManifest{}, err
	}
	if existing, found, err := loadExpectedResetTransition(next, reset); err != nil {
		return resetTransitionManifest{}, err
	} else if found {
		return existing, nil
	}
	if err := os.MkdirAll(next.StateDirectory, 0o700); err != nil {
		return resetTransitionManifest{}, err
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		return resetTransitionManifest{}, err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(next.StateDirectory, resetTransitionManifestFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		if existing, found, loadErr := loadExpectedResetTransition(next, reset); loadErr == nil && found {
			return existing, nil
		}
		return resetTransitionManifest{}, fmt.Errorf("commit immutable Android reset transition: %w", err)
	}
	return expected, nil
}

func expectedResetTransition(previous deviceRecord, next VirtualDevicePlan, reset ports.ResetPlan, at time.Time) (resetTransitionManifest, error) {
	if err := reset.Validate(); err != nil {
		return resetTransitionManifest{}, err
	}
	if reset.Previous.ID != previous.plan.TargetID || reset.Previous.Generation != previous.plan.Generation || reset.NextGeneration != next.Generation || next.TargetID != previous.plan.TargetID {
		return resetTransitionManifest{}, fmt.Errorf("Android reset transition does not bind consecutive target plans")
	}
	if !instanceMatchesPlan(previous.instance, previous.plan) {
		return resetTransitionManifest{}, fmt.Errorf("Android reset transition previous runtime differs from its plan")
	}
	resetDigest, err := resetPlanDigest(reset)
	if err != nil {
		return resetTransitionManifest{}, err
	}
	previousDigest, err := virtualDevicePlanDigest(previous.plan)
	if err != nil {
		return resetTransitionManifest{}, err
	}
	nextDigest, err := virtualDevicePlanDigest(next)
	if err != nil {
		return resetTransitionManifest{}, err
	}
	nextInstance := instanceFromPlan(next)
	return resetTransitionManifest{
		Version: manifestVersion, ResetPlanDigest: resetDigest.String(), ResetPlan: persistedResetPlanFrom(reset),
		PreviousPlanDigest: previousDigest.String(), PreviousPlan: previous.plan, PreviousInstance: previous.instance,
		NextPlanDigest: nextDigest.String(), NextRuntimeID: nextInstance.RuntimeID, NextAllocation: next.Allocation, PersistedAt: at.UTC(),
	}, nil
}

func loadExpectedResetTransition(next VirtualDevicePlan, expectedPlan ports.ResetPlan) (resetTransitionManifest, bool, error) {
	path := filepath.Join(next.StateDirectory, resetTransitionManifestFilename)
	var manifest resetTransitionManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return resetTransitionManifest{}, false, nil
	} else if err != nil {
		return resetTransitionManifest{}, false, err
	}
	plan, err := validateResetTransition(manifest, next)
	if err != nil {
		return resetTransitionManifest{}, false, err
	}
	if plan != expectedPlan {
		return resetTransitionManifest{}, false, fmt.Errorf("Android reset transition belongs to another exact reset plan")
	}
	return manifest, true, nil
}

func loadResetTransition(next VirtualDevicePlan) (resetTransitionManifest, ports.ResetPlan, bool, error) {
	path := filepath.Join(next.StateDirectory, resetTransitionManifestFilename)
	var manifest resetTransitionManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return resetTransitionManifest{}, ports.ResetPlan{}, false, nil
	} else if err != nil {
		return resetTransitionManifest{}, ports.ResetPlan{}, false, err
	}
	plan, err := validateResetTransition(manifest, next)
	return manifest, plan, true, err
}

func validateResetTransition(manifest resetTransitionManifest, next VirtualDevicePlan) (ports.ResetPlan, error) {
	plan, err := manifest.ResetPlan.restore()
	if err != nil {
		return ports.ResetPlan{}, err
	}
	resetDigest, err := resetPlanDigest(plan)
	if err != nil || manifest.ResetPlanDigest != resetDigest.String() {
		return ports.ResetPlan{}, fmt.Errorf("Android reset transition plan digest is invalid")
	}
	previousDigest, previousErr := virtualDevicePlanDigest(manifest.PreviousPlan)
	nextDigest, nextErr := virtualDevicePlanDigest(next)
	if previousErr != nil || nextErr != nil || manifest.PreviousPlanDigest != previousDigest.String() || manifest.NextPlanDigest != nextDigest.String() {
		return ports.ResetPlan{}, fmt.Errorf("Android reset transition target-plan digest is invalid")
	}
	if manifest.Version != manifestVersion || manifest.PersistedAt.IsZero() ||
		plan.Previous.ID != manifest.PreviousPlan.TargetID || plan.Previous.Generation != manifest.PreviousPlan.Generation ||
		plan.Previous.ID != next.TargetID || plan.NextGeneration != next.Generation || !instanceMatchesPlan(manifest.PreviousInstance, manifest.PreviousPlan) ||
		manifest.NextRuntimeID != instanceFromPlan(next).RuntimeID || manifest.NextAllocation != next.Allocation {
		return ports.ResetPlan{}, fmt.Errorf("Android reset transition does not bind the exact previous and next target/runtime plans")
	}
	return plan, nil
}

func commitExpectedResetOutcome(next VirtualDevicePlan, transition resetTransitionManifest, result ports.TargetResult, outcomeErr error, at time.Time) (resetOutcome, error) {
	expected, err := expectedResetOutcome(next, transition, result, outcomeErr, at)
	if err != nil {
		return resetOutcome{}, err
	}
	if existing, found, err := loadExpectedResetOutcome(next, transition); err != nil {
		return resetOutcome{}, err
	} else if found {
		return existing, nil
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		return resetOutcome{}, err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(next.StateDirectory, resetOutcomeManifestFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		if existing, found, loadErr := loadExpectedResetOutcome(next, transition); loadErr == nil && found {
			return existing, nil
		}
		return resetOutcome{}, fmt.Errorf("commit immutable Android reset outcome: %w", err)
	}
	plan, err := transition.ResetPlan.restore()
	if err != nil {
		return resetOutcome{}, err
	}
	return resetOutcome{targetID: plan.Previous.ID, plan: plan, result: result, err: outcomeErr}, nil
}

func expectedResetOutcome(next VirtualDevicePlan, transition resetTransitionManifest, result ports.TargetResult, outcomeErr error, at time.Time) (resetOutcomeManifest, error) {
	if err := validateResetResult(next, result); err != nil {
		return resetOutcomeManifest{}, err
	}
	return resetOutcomeManifest{
		Version: manifestVersion, ResetPlanDigest: transition.ResetPlanDigest, NextPlanDigest: transition.NextPlanDigest,
		NextRuntimeID: transition.NextRuntimeID, NextAllocation: transition.NextAllocation,
		Result: result, Error: persistResetError(outcomeErr), PersistedAt: at.UTC(),
	}, nil
}

func loadExpectedResetOutcome(next VirtualDevicePlan, transition resetTransitionManifest) (resetOutcome, bool, error) {
	path := filepath.Join(next.StateDirectory, resetOutcomeManifestFilename)
	var manifest resetOutcomeManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return resetOutcome{}, false, nil
	} else if err != nil {
		return resetOutcome{}, false, err
	}
	if manifest.Version != manifestVersion || manifest.PersistedAt.IsZero() || manifest.ResetPlanDigest != transition.ResetPlanDigest ||
		manifest.NextPlanDigest != transition.NextPlanDigest || manifest.NextRuntimeID != transition.NextRuntimeID || manifest.NextAllocation != transition.NextAllocation {
		return resetOutcome{}, false, fmt.Errorf("Android reset outcome does not bind its exact transition and next runtime")
	}
	if err := validateResetResult(next, manifest.Result); err != nil {
		return resetOutcome{}, false, err
	}
	if err := manifest.Error.validate(); err != nil {
		return resetOutcome{}, false, err
	}
	plan, err := transition.ResetPlan.restore()
	if err != nil {
		return resetOutcome{}, false, err
	}
	return resetOutcome{targetID: plan.Previous.ID, plan: plan, result: manifest.Result, err: manifest.Error.restore()}, true, nil
}

func validateResetResult(next VirtualDevicePlan, result ports.TargetResult) error {
	status := result.Status
	if !result.Created || status.TargetID != next.TargetID || status.Generation != next.Generation || status.Kind != domain.TargetAndroidVirtualDevice ||
		status.State != domain.TargetGenerationReady || !status.Ready || status.RuntimeID != instanceFromPlan(next).RuntimeID ||
		status.DeviceSerial != next.Allocation.Serial || status.CgroupID != "" || status.ObservedAt.IsZero() {
		return fmt.Errorf("Android reset outcome does not identify the exact ready replacement generation")
	}
	return nil
}

func persistResetError(err error) *persistedResetError {
	if err == nil {
		return nil
	}
	var domainErr *domain.Error
	if errors.As(err, &domainErr) {
		return &persistedResetError{
			Code: domainErr.Code(), Operation: domainErr.Operation(), Field: domainErr.Field(),
			Message: domainErr.Message(), Details: domainErr.Details(),
		}
	}
	return &persistedResetError{Code: domain.CodeInternal, Operation: "cuttlefish.reset.recovered", Message: err.Error()}
}

func (e *persistedResetError) restore() error {
	if e == nil {
		return nil
	}
	return domain.NewDetailedError(e.Code, e.Operation, e.Field, e.Message, e.Details, nil)
}

func (e *persistedResetError) validate() error {
	if e == nil {
		return nil
	}
	if e.Code == "" || e.Operation == "" || e.Message == "" {
		return fmt.Errorf("persisted reset error is incomplete")
	}
	return nil
}

func resetOutcomesEqual(left, right resetOutcome) bool {
	return left.targetID == right.targetID && left.plan == right.plan && reflect.DeepEqual(left.result, right.result) && errorsEqual(left.err, right.err)
}

func errorsEqual(left, right error) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Error() == right.Error() && domain.ErrorCodeOf(left) == domain.ErrorCodeOf(right)
}

type androidInstanceManifestState struct {
	any       bool
	complete  bool
	readiness ReadinessState
}

// resumeExpectedResetTransitions completes only transitions whose immutable
// intent binds the exact expected N+1 plan. This runs before normal adoption
// so a crash after the transition commit cannot strand startup at Missing.
func (d *Driver) resumeExpectedResetTransitions(ctx context.Context, expected []expectedAndroidTarget, inventory map[string]struct{}) (bool, error) {
	mutated := false
	for _, item := range expected {
		transitionPath := filepath.Join(item.directory, resetTransitionManifestFilename)
		if _, err := os.Lstat(transitionPath); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return mutated, fmt.Errorf("inspect reset transition for generation %d: %w", item.ref.Generation, err)
		}
		transition, next, reset, err := d.loadExpectedResetTransitionForRecovery(item)
		if err != nil {
			return mutated, fmt.Errorf("validate reset transition for generation %d: %w", item.ref.Generation, err)
		}
		outcome, finalized, err := loadExpectedResetOutcome(next, transition)
		if err != nil {
			return mutated, fmt.Errorf("load reset outcome for generation %d: %w", item.ref.Generation, err)
		}
		_, previousPresent := inventory[transition.PreviousInstance.RuntimeID]
		if !finalized && previousPresent {
			if err := requirePreviousResetAuthority(transition); err != nil {
				return mutated, fmt.Errorf("validate live previous reset authority for generation %d: %w", item.ref.Generation, err)
			}
		}
		if err := d.ensureExpectedResetAllocation(ctx, item.ref, transition, !finalized); err != nil {
			return mutated, fmt.Errorf("validate reset allocation for generation %d: %w", item.ref.Generation, err)
		}
		if finalized {
			manifestState, err := inspectAndroidInstanceManifests(next)
			if err != nil {
				return mutated, fmt.Errorf("inspect finalized reset replacement manifests: %w", err)
			}
			if !manifestState.complete {
				return mutated, fmt.Errorf("finalized reset replacement manifests are incomplete")
			}
			if _, present := inventory[transition.NextRuntimeID]; !present {
				return mutated, fmt.Errorf("finalized reset replacement runtime %q is absent", transition.NextRuntimeID)
			}
			if outcome.err == nil && previousPresent {
				return mutated, fmt.Errorf("successful reset outcome conflicts with live previous runtime %q", transition.PreviousInstance.RuntimeID)
			}
			continue
		}
		instance, readiness, replacementMutated, err := d.ensureManifestedAndroidInstance(ctx, next, inventory)
		if err != nil {
			return mutated, fmt.Errorf("recover reset replacement for generation %d: %w", item.ref.Generation, err)
		}
		mutated = mutated || replacementMutated
		if previousPresent {
			if err := d.backend.Destroy(ctx, transition.PreviousInstance); err != nil {
				return mutated, fmt.Errorf("retire previous reset runtime %q: %w", transition.PreviousInstance.RuntimeID, err)
			}
			delete(inventory, transition.PreviousInstance.RuntimeID)
			mutated = true
		}
		result := readyResetTargetResult(next, instance, laterTime(readiness.ObservedAt, d.now()))
		if _, err := commitExpectedResetOutcome(next, transition, result, nil, d.now()); err != nil {
			return mutated, fmt.Errorf("commit recovered reset outcome: %w", err)
		}
		inventory[instance.RuntimeID] = struct{}{}
		mutated = true
		if reset.Previous.ID != result.Status.TargetID || reset.NextGeneration != result.Status.Generation {
			return mutated, fmt.Errorf("recovered reset outcome differs from its exact transition")
		}
	}
	return mutated, nil
}

func (d *Driver) loadExpectedResetTransitionForRecovery(item expectedAndroidTarget) (resetTransitionManifest, VirtualDevicePlan, ports.ResetPlan, error) {
	var transition resetTransitionManifest
	if err := readStrictManifest(filepath.Join(item.directory, resetTransitionManifestFilename), &transition); err != nil {
		return resetTransitionManifest{}, VirtualDevicePlan{}, ports.ResetPlan{}, err
	}
	next, err := BuildVirtualDevicePlan(item.input, d.build, transition.NextAllocation)
	if err != nil {
		return resetTransitionManifest{}, VirtualDevicePlan{}, ports.ResetPlan{}, err
	}
	reset, err := validateResetTransition(transition, next)
	if err != nil {
		return resetTransitionManifest{}, VirtualDevicePlan{}, ports.ResetPlan{}, err
	}
	spec := item.input.Generation.Spec()
	if reset.LeaseID != item.input.LeaseID || reset.Previous.ID != item.ref.ID || reset.NextGeneration != item.ref.Generation ||
		spec.PreviousGeneration != reset.Previous.Generation || spec.RecoveryIncidentID != reset.IncidentID ||
		(reset.Mode != ports.ResetBaseline && reset.Mode != ports.ResetRecreate) {
		return resetTransitionManifest{}, VirtualDevicePlan{}, ports.ResetPlan{}, fmt.Errorf("reset transition does not bind the expected generation provenance")
	}
	if filepath.Clean(next.StateDirectory) != filepath.Clean(item.directory) {
		return resetTransitionManifest{}, VirtualDevicePlan{}, ports.ResetPlan{}, fmt.Errorf("reset transition state directory differs from expected generation")
	}
	if err := transition.PreviousPlan.Validate(d.build.TargetRoot, d.build.SystemImageRoot); err != nil {
		return resetTransitionManifest{}, VirtualDevicePlan{}, ports.ResetPlan{}, fmt.Errorf("validate previous reset plan: %w", err)
	}
	return transition, next, reset, nil
}

func requirePreviousResetAuthority(transition resetTransitionManifest) error {
	previousTarget, previousRuntime, err := loadTargetRuntimeManifests(transition.PreviousPlan.StateDirectory)
	if err != nil {
		return fmt.Errorf("load previous reset authority: %w", err)
	}
	if err := validateExpectedManifests(transition.PreviousPlan, previousTarget, previousRuntime); err != nil || !instancesEqual(previousRuntime.Instance, transition.PreviousInstance) {
		return fmt.Errorf("previous reset authority differs from the immutable transition")
	}
	return nil
}

func (d *Driver) ensureExpectedResetAllocation(ctx context.Context, ref ports.TargetRef, transition resetTransitionManifest, allowReserve bool) error {
	allocation, found, err := d.lookupExpectedAllocation(ctx, ref)
	if err != nil {
		return err
	}
	if found {
		if allocation != transition.NextAllocation {
			return fmt.Errorf("durable reset allocation differs from the immutable transition")
		}
		return nil
	}
	if !allowReserve {
		return fmt.Errorf("finalized reset replacement lacks its durable allocation")
	}
	allocation, err = d.allocator.Reserve(ctx, ref.ID, ref.Generation)
	if err != nil {
		return err
	}
	if allocation != transition.NextAllocation {
		return fmt.Errorf("allocator cannot restore the immutable reset allocation")
	}
	return nil
}

func (d *Driver) ensureManifestedAndroidInstance(ctx context.Context, next VirtualDevicePlan, inventory map[string]struct{}) (Instance, ReadinessState, bool, error) {
	manifestState, err := inspectAndroidInstanceManifests(next)
	if err != nil {
		return Instance{}, ReadinessState{}, false, err
	}
	instance := instanceFromPlan(next)
	_, present := inventory[instance.RuntimeID]
	if present {
		recoveryMutated := false
		if !manifestState.complete {
			if recovery, supported := d.backend.(BackendUnstartedRecovery); supported {
				recoveryMutated, err = recovery.ResumeUnstarted(ctx, instance)
				if err != nil {
					return Instance{}, ReadinessState{}, false, fmt.Errorf("resume exact configured Android runtime before readiness proof: %w", err)
				}
			}
		}
		// Recovery must repeat the backend's complete readiness contract.
		// For the managed emulator this includes rooted/debuggable identity,
		// exact /data block-device sizing, and durable host-process binding;
		// Inspect alone is intentionally insufficient to create manifests.
		readiness, err := d.backend.WaitReady(ctx, instance)
		if err != nil || !readiness.Ready() {
			if err == nil {
				err = incompleteAndroidReadinessError(readiness, instance.Allocation.InstanceName)
			}
			return Instance{}, ReadinessState{}, false, fmt.Errorf("re-prove exact replacement runtime readiness and resources: %w", err)
		}
		if manifestState.complete && !reflect.DeepEqual(readiness.Identity, manifestState.readiness.Identity) {
			return Instance{}, ReadinessState{}, false, fmt.Errorf("replacement runtime identity differs from its durable manifest")
		}
		if !manifestState.complete {
			if err := persistTargetRuntimeManifests(next, instance, readiness, d.now()); err != nil {
				return Instance{}, ReadinessState{}, false, err
			}
			return instance, readiness, true, nil
		}
		return instance, readiness, recoveryMutated, nil
	}
	if manifestState.any {
		return Instance{}, ReadinessState{}, false, fmt.Errorf("replacement manifests exist but the exact runtime is absent")
	}
	reserved, err := d.allocator.Reserve(ctx, next.TargetID, next.Generation)
	if err != nil {
		return Instance{}, ReadinessState{}, false, err
	}
	if reserved != next.Allocation {
		return Instance{}, ReadinessState{}, false, fmt.Errorf("allocator returned another replacement assignment")
	}
	created, readiness, err := d.createInstance(ctx, next)
	if err != nil {
		return Instance{}, ReadinessState{}, false, err
	}
	inventory[created.RuntimeID] = struct{}{}
	if err := persistTargetRuntimeManifests(next, created, readiness, d.now()); err != nil {
		return Instance{}, ReadinessState{}, true, err
	}
	return created, readiness, true, nil
}

func inspectAndroidInstanceManifests(next VirtualDevicePlan) (androidInstanceManifestState, error) {
	targetPath := filepath.Join(next.StateDirectory, targetPlanManifestFilename)
	runtimePath := filepath.Join(next.StateDirectory, runtimePlanManifestFilename)
	targetExists, err := strictManifestExists(targetPath)
	if err != nil {
		return androidInstanceManifestState{}, err
	}
	runtimeExists, err := strictManifestExists(runtimePath)
	if err != nil {
		return androidInstanceManifestState{}, err
	}
	state := androidInstanceManifestState{any: targetExists || runtimeExists, complete: targetExists && runtimeExists}
	digest, err := virtualDevicePlanDigest(next)
	if err != nil {
		return androidInstanceManifestState{}, err
	}
	if targetExists {
		var target targetPlanManifest
		if err := readStrictManifest(targetPath, &target); err != nil {
			return androidInstanceManifestState{}, err
		}
		if target.Version != manifestVersion || target.PlanDigest != digest.String() || target.PersistedAt.IsZero() {
			return androidInstanceManifestState{}, fmt.Errorf("partial Android target manifest differs from its exact plan")
		}
	}
	if runtimeExists {
		var runtime runtimePlanManifest
		if err := readStrictManifest(runtimePath, &runtime); err != nil {
			return androidInstanceManifestState{}, err
		}
		if runtime.Version != manifestVersion || runtime.PlanDigest != digest.String() || !instanceMatchesPlan(runtime.Instance, next) || !runtime.Readiness.Ready() || runtime.PersistedAt.IsZero() {
			return androidInstanceManifestState{}, fmt.Errorf("partial Android runtime manifest differs from its exact plan")
		}
		state.readiness = runtime.Readiness
	}
	return state, nil
}

func strictManifestExists(path string) (bool, error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func laterTime(left, right time.Time) time.Time {
	if left.After(right) {
		return left
	}
	return right
}

func (d *Driver) recoverResetOutcomes(ctx context.Context, records []deviceRecord, inventory map[string]struct{}) (map[string]resetOutcome, error) {
	result := make(map[string]resetOutcome)
	for _, record := range records {
		transition, plan, found, err := loadResetTransition(record.plan)
		if err != nil {
			return nil, fmt.Errorf("load reset transition for generation %d: %w", record.plan.Generation, err)
		}
		if !found {
			continue
		}
		if err := transition.PreviousPlan.Validate(d.build.TargetRoot, d.build.SystemImageRoot); err != nil {
			return nil, fmt.Errorf("validate previous reset generation: %w", err)
		}
		outcome, finalized, err := loadExpectedResetOutcome(record.plan, transition)
		if err != nil {
			return nil, fmt.Errorf("load reset outcome for generation %d: %w", record.plan.Generation, err)
		}
		_, previousStillExists := inventory[transition.PreviousInstance.RuntimeID]
		if !finalized {
			if previousStillExists {
				return nil, fmt.Errorf("reset transition replacement is present but previous runtime %q has not been retired", transition.PreviousInstance.RuntimeID)
			}
			if !record.status.Ready || record.status.State != domain.TargetGenerationReady {
				return nil, fmt.Errorf("unfinished reset transition cannot reconstruct a ready replacement outcome")
			}
			canonicalResult := ports.TargetResult{Status: record.status, Created: true}
			outcome, err = commitExpectedResetOutcome(record.plan, transition, canonicalResult, nil, d.now())
			if err != nil {
				return nil, fmt.Errorf("commit recovered reset outcome: %w", err)
			}
		} else if outcome.err == nil && previousStillExists {
			return nil, fmt.Errorf("successful reset outcome conflicts with a live previous runtime %q", transition.PreviousInstance.RuntimeID)
		}
		if !previousStillExists {
			if err := d.allocator.Release(ctx, transition.PreviousPlan.Allocation); err != nil {
				return nil, fmt.Errorf("release retired reset allocation: %w", err)
			}
		}
		if outcome.targetID != plan.Previous.ID || outcome.plan != plan {
			return nil, fmt.Errorf("recovered reset outcome differs from its transition plan")
		}
		if prior, duplicate := result[plan.IdempotencyKey]; duplicate && !resetOutcomesEqual(prior, outcome) {
			return nil, fmt.Errorf("multiple Android reset transitions reuse idempotency key %q", plan.IdempotencyKey)
		}
		result[plan.IdempotencyKey] = outcome
	}
	return result, nil
}
