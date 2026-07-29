package linuxcontainer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	generationRunClaimFile   = "mutable-run-claim.json"
	maximumRunClaimSize      = int64(64 << 10)
	stoppedRunBoundaryFile   = "container-stopped.json"
	maximumRunBoundarySize   = int64(4096)
	runPlanDigestDomain      = "world.linux-target.run-plan.v1\x00"
	runBoundarySchemaVersion = 1
	runClaimSchemaVersion    = 1
)

var errGenerationAlreadyClaimed = errors.New("target generation is already claimed by another run")

// persistedGenerationRunClaim is immutable generation-level authority. Its
// existence means the writable target generation can never be assigned to a
// second run, even after the driver process restarts.
type persistedGenerationRunClaim struct {
	SchemaVersion    int                     `json:"schema_version"`
	LeaseID          domain.LeaseID          `json:"lease_id"`
	TargetID         domain.TargetID         `json:"target_id"`
	TargetGeneration domain.TargetGeneration `json:"target_generation"`
	RunID            domain.TargetRunID      `json:"run_id"`
	IdempotencyKey   string                  `json:"idempotency_key"`
	Materialization  domain.Digest           `json:"materialization_digest"`
	PlanDigest       domain.Digest           `json:"plan_digest"`
}

type persistedStoppedRunBoundary struct {
	SchemaVersion    int                     `json:"schema_version"`
	LeaseID          domain.LeaseID          `json:"lease_id"`
	TargetID         domain.TargetID         `json:"target_id"`
	TargetGeneration domain.TargetGeneration `json:"target_generation"`
	RunID            domain.TargetRunID      `json:"run_id"`
	PlanDigest       domain.Digest           `json:"plan_digest"`
	RuntimeID        string                  `json:"runtime_id"`
	Mode             ports.StopMode          `json:"mode"`
	StoppedAt        time.Time               `json:"stopped_at"`
}

func claimTargetGenerationRun(directory string, plan ports.TargetRunPlan) (persistedGenerationRunClaim, bool, error) {
	want, err := generationRunClaim(plan)
	if err != nil {
		return persistedGenerationRunClaim{}, false, err
	}
	payload, err := json.Marshal(want)
	if err != nil {
		return persistedGenerationRunClaim{}, false, err
	}
	if err := persistRunRecord(directory, generationRunClaimFile, payload, maximumRunClaimSize); err == nil {
		return want, true, nil
	} else if !errors.Is(err, fs.ErrExist) {
		return persistedGenerationRunClaim{}, false, err
	}
	existing, found, err := loadTargetGenerationRunClaim(directory)
	if err != nil {
		return persistedGenerationRunClaim{}, false, err
	}
	if !found || !reflect.DeepEqual(existing, want) {
		return persistedGenerationRunClaim{}, false, errGenerationAlreadyClaimed
	}
	return existing, false, nil
}

func requireTargetGenerationRunClaim(directory string, plan ports.TargetRunPlan) (persistedGenerationRunClaim, error) {
	want, err := generationRunClaim(plan)
	if err != nil {
		return persistedGenerationRunClaim{}, err
	}
	existing, found, err := loadTargetGenerationRunClaim(directory)
	if err != nil {
		return persistedGenerationRunClaim{}, err
	}
	if !found {
		return persistedGenerationRunClaim{}, fmt.Errorf("durable target generation run claim is missing")
	}
	if !reflect.DeepEqual(existing, want) {
		return persistedGenerationRunClaim{}, errGenerationAlreadyClaimed
	}
	return existing, nil
}

func loadTargetGenerationRunClaim(directory string) (persistedGenerationRunClaim, bool, error) {
	payload, err := loadRunRecord(directory, generationRunClaimFile, maximumRunClaimSize)
	if errors.Is(err, fs.ErrNotExist) {
		return persistedGenerationRunClaim{}, false, nil
	}
	if err != nil {
		return persistedGenerationRunClaim{}, false, err
	}
	var claim persistedGenerationRunClaim
	if err := json.Unmarshal(payload, &claim); err != nil {
		return persistedGenerationRunClaim{}, false, err
	}
	if err := claim.validate(); err != nil {
		return persistedGenerationRunClaim{}, false, err
	}
	return claim, true, nil
}

func generationRunClaim(plan ports.TargetRunPlan) (persistedGenerationRunClaim, error) {
	digest, err := targetRunPlanDigest(plan)
	if err != nil {
		return persistedGenerationRunClaim{}, err
	}
	spec := plan.Run.Spec()
	claim := persistedGenerationRunClaim{
		SchemaVersion: runClaimSchemaVersion, LeaseID: spec.LeaseID, TargetID: spec.TargetID,
		TargetGeneration: spec.TargetGeneration, RunID: spec.ID, IdempotencyKey: plan.IdempotencyKey,
		Materialization: spec.MaterializationDigest, PlanDigest: digest,
	}
	return claim, claim.validate()
}

func (c persistedGenerationRunClaim) validate() error {
	if c.SchemaVersion != runClaimSchemaVersion || c.LeaseID.IsZero() || c.TargetID.IsZero() || !c.TargetGeneration.IsValid() || c.RunID.IsZero() || !domain.IsCanonicalIdempotencyKey(c.IdempotencyKey) || c.Materialization.IsZero() || c.PlanDigest.IsZero() {
		return fmt.Errorf("target generation run claim is incomplete or unsupported")
	}
	return nil
}

func (c persistedGenerationRunClaim) matchesTarget(plan ContainerPlan) bool {
	return c.LeaseID == plan.LeaseID && c.TargetID == plan.TargetID && c.TargetGeneration == plan.Generation
}

func targetRunPlanDigest(plan ports.TargetRunPlan) (domain.Digest, error) {
	type materialIdentity struct {
		Artifact      domain.ArtifactReferenceSpec `json:"artifact"`
		LogicalPath   string                       `json:"logical_path"`
		Mode          uint32                       `json:"mode"`
		ContentDigest domain.Digest                `json:"content_digest"`
		ContentSize   int64                        `json:"content_size"`
	}
	identity := struct {
		SchemaVersion    int                   `json:"schema_version"`
		IdempotencyKey   string                `json:"idempotency_key"`
		Run              domain.TargetRunSpec  `json:"run"`
		RequiredCoverage []string              `json:"required_coverage"`
		Collectors       []ports.CollectorSpec `json:"collectors"`
		Material         []materialIdentity    `json:"material"`
		MaximumDuration  int64                 `json:"maximum_duration_ns"`
	}{
		SchemaVersion: 1, IdempotencyKey: plan.IdempotencyKey, Run: plan.Run.Spec(),
		RequiredCoverage: append([]string(nil), plan.RequiredCoverage...),
		Collectors:       append([]ports.CollectorSpec(nil), plan.Collectors...),
		MaximumDuration:  int64(plan.MaximumDuration),
	}
	identity.Material = make([]materialIdentity, len(plan.Material))
	for index, material := range plan.Material {
		identity.Material[index] = materialIdentity{
			Artifact: material.Artifact.Spec(), LogicalPath: material.LogicalPath, Mode: material.Mode,
			ContentDigest: material.Content.Digest(), ContentSize: material.Content.Size(),
		}
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(append([]byte(runPlanDigestDomain), payload...)), nil
}

// sameTargetRunPlanIdentity reuses the durable generation-claim digest so an
// in-memory idempotency replay and crash recovery cannot disagree about what
// constitutes the exact run plan.
func sameTargetRunPlanIdentity(existing, requested ports.TargetRunPlan) (bool, error) {
	existingDigest, err := targetRunPlanDigest(existing)
	if err != nil {
		return false, err
	}
	requestedDigest, err := targetRunPlanDigest(requested)
	if err != nil {
		return false, err
	}
	return existingDigest == requestedDigest, nil
}

func persistStoppedRunBoundary(directory string, claim persistedGenerationRunClaim, runtimeID string, mode ports.StopMode, stoppedAt time.Time) (persistedStoppedRunBoundary, error) {
	want := persistedStoppedRunBoundary{
		SchemaVersion: runBoundarySchemaVersion, LeaseID: claim.LeaseID, TargetID: claim.TargetID,
		TargetGeneration: claim.TargetGeneration, RunID: claim.RunID, PlanDigest: claim.PlanDigest,
		RuntimeID: runtimeID, Mode: mode, StoppedAt: stoppedAt.UTC(),
	}
	if err := want.validate(claim, runtimeID); err != nil {
		return persistedStoppedRunBoundary{}, err
	}
	payload, err := json.Marshal(want)
	if err != nil {
		return persistedStoppedRunBoundary{}, err
	}
	if err := persistRunRecord(directory, stoppedRunBoundaryFile, payload, maximumRunBoundarySize); err == nil {
		return want, nil
	} else if !errors.Is(err, fs.ErrExist) {
		return persistedStoppedRunBoundary{}, err
	}
	existing, found, err := loadStoppedRunBoundary(directory, claim, runtimeID)
	if err != nil {
		return persistedStoppedRunBoundary{}, err
	}
	if !found {
		return persistedStoppedRunBoundary{}, fmt.Errorf("durable stopped-run boundary disappeared")
	}
	return existing, nil
}

func loadStoppedRunBoundary(directory string, claim persistedGenerationRunClaim, runtimeID string) (persistedStoppedRunBoundary, bool, error) {
	payload, err := loadRunRecord(directory, stoppedRunBoundaryFile, maximumRunBoundarySize)
	if errors.Is(err, fs.ErrNotExist) {
		return persistedStoppedRunBoundary{}, false, nil
	}
	if err != nil {
		return persistedStoppedRunBoundary{}, false, err
	}
	var boundary persistedStoppedRunBoundary
	if err := json.Unmarshal(payload, &boundary); err != nil {
		return persistedStoppedRunBoundary{}, false, err
	}
	if err := boundary.validate(claim, runtimeID); err != nil {
		return persistedStoppedRunBoundary{}, false, err
	}
	return boundary, true, nil
}

func (b persistedStoppedRunBoundary) validate(claim persistedGenerationRunClaim, runtimeID string) error {
	if b.SchemaVersion != runBoundarySchemaVersion || b.LeaseID != claim.LeaseID || b.TargetID != claim.TargetID || b.TargetGeneration != claim.TargetGeneration || b.RunID != claim.RunID || b.PlanDigest != claim.PlanDigest || b.RuntimeID == "" || b.RuntimeID != runtimeID || !b.Mode.IsValid() || b.StoppedAt.IsZero() {
		return fmt.Errorf("stopped-run boundary does not match the claimed physical run")
	}
	return nil
}

func (d *Driver) requireOwnedRuntimeStopped(ctx context.Context, target targetRecord, mode ports.StopMode) (RuntimeState, error) {
	state, err := d.inspectOwnedRuntime(ctx, target, "linux_target.stop_runtime")
	if err != nil {
		return RuntimeState{}, err
	}
	if !state.Running {
		if err := requireStoppedRuntimeState(state, "linux_target.stop_runtime", dockercli.StoppedStatusCreated, dockercli.StoppedStatusExited); err != nil {
			return RuntimeState{}, err
		}
		return state, nil
	}
	stopErr := d.runtime.Stop(ctx, target.runtimeID, mode)
	stopped, inspectErr := d.inspectOwnedRuntime(ctx, target, "linux_target.stop_runtime")
	if inspectErr != nil {
		if stopErr != nil {
			stopErr = domain.NewError(domain.CodeUnavailable, "linux_target.stop_runtime", "runtime.stop", "could not stop the exact target runtime", stopErr)
		}
		return RuntimeState{}, errors.Join(stopErr, inspectErr)
	}
	if stopped.Running {
		return stopped, errors.Join(stopErr, domain.NewError(domain.CodeFailedPrecondition, "linux_target.stop_runtime", "runtime.running", "target runtime still executes after stop", nil))
	}
	if err := requireStoppedRuntimeState(stopped, "linux_target.stop_runtime", dockercli.StoppedStatusExited); err != nil {
		return RuntimeState{}, errors.Join(stopErr, err)
	}
	if stopErr != nil {
		return stopped, domain.NewError(domain.CodeUnavailable, "linux_target.stop_runtime", "runtime.stop", "runtime reported a stop failure despite the stopped observation", stopErr)
	}
	return stopped, nil
}

func (d *Driver) requireOwnedRuntimeStillStopped(ctx context.Context, target targetRecord) error {
	state, err := d.inspectOwnedRuntime(ctx, target, "linux_target.stop_run")
	if err != nil {
		return err
	}
	if err := requireStoppedRuntimeState(state, "linux_target.stop_run", dockercli.StoppedStatusExited); err != nil {
		return domain.NewError(domain.CodeConflict, "linux_target.stop_run", "runtime.stopped_state", "target runtime did not remain in the exact stopped state while its writable state was being sealed", err)
	}
	return nil
}

func (d *Driver) inspectOwnedRuntime(ctx context.Context, target targetRecord, operation string) (RuntimeState, error) {
	state, err := d.runtime.Inspect(ctx, target.runtimeID)
	if err != nil {
		return RuntimeState{}, domain.NewError(domain.CodeUnavailable, operation, "runtime.inspect", "could not inspect the exact target runtime", err)
	}
	if err := requireExactRuntimeID(state, target.runtimeID, operation); err != nil {
		return RuntimeState{}, err
	}
	if err := validateRuntimeIdentity(state, target.plan); err != nil {
		return RuntimeState{}, err
	}
	return state, nil
}

func (d *Driver) requireStoppedRunBoundary(ctx context.Context, target targetRecord, run *runRecord, mode ports.StopMode, minimum time.Time) (persistedStoppedRunBoundary, error) {
	state, stopErr := d.requireOwnedRuntimeStopped(ctx, target, mode)
	if state.ID == "" || state.Running {
		return persistedStoppedRunBoundary{}, stopErr
	}
	stoppedAt := d.now().UTC()
	if stoppedAt.Before(minimum) {
		stoppedAt = minimum
	}
	d.markTargetResettable(target, state, stoppedAt)
	claim, claimErr := requireTargetGenerationRunClaim(target.plan.TargetDirectory, run.plan)
	if claimErr != nil {
		return persistedStoppedRunBoundary{}, errors.Join(stopErr, domain.NewError(domain.CodeIntegrityViolation, "linux_target.stop_run", "run_claim", "durable target generation run claim is missing or invalid", claimErr))
	}
	boundary, boundaryErr := persistStoppedRunBoundary(run.directory, claim, target.runtimeID, mode, stoppedAt)
	if boundaryErr != nil {
		boundaryErr = domain.NewError(domain.CodeUnavailable, "linux_target.stop_run", "stopped_boundary", "could not durably record the stopped container boundary", boundaryErr)
	}
	return boundary, errors.Join(stopErr, boundaryErr)
}

func (d *Driver) markTargetResettable(target targetRecord, state RuntimeState, observedAt time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := targetKey(target.plan.TargetID, target.plan.Generation)
	current, found := d.targets[key]
	if !found || current.runtimeID != target.runtimeID {
		return
	}
	current.status.State = domain.TargetGenerationResettable
	current.status.Ready = false
	current.status.CgroupID = state.CgroupID
	current.status.ObservedAt = observedAt.UTC()
	d.targets[key] = current
}
