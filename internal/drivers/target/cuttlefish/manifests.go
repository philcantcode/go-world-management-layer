package cuttlefish

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	targetPlanManifestFilename   = "world-target-plan.json"
	runtimePlanManifestFilename  = "world-runtime-plan.json"
	runPlanManifestFilename      = "world-run-plan.json"
	runStartManifestFilename     = "world-run-start.json"
	runStopManifestFilename      = "world-run-stop.json"
	generationUseFilename        = "world-generation-use.json"
	generationQuarantineFilename = "world-generation-quarantine.json"
	manifestVersion              = 1
	maximumManifestBytes         = int64(8 << 20)
)

const (
	generationUseRunPrepared = "run-prepared"
)

type targetPlanManifest struct {
	Version     int               `json:"version"`
	PlanDigest  string            `json:"plan_digest"`
	Plan        VirtualDevicePlan `json:"plan"`
	PersistedAt time.Time         `json:"persisted_at"`
}

type runtimePlanManifest struct {
	Version     int            `json:"version"`
	PlanDigest  string         `json:"plan_digest"`
	Instance    Instance       `json:"instance"`
	Readiness   ReadinessState `json:"readiness"`
	PersistedAt time.Time      `json:"persisted_at"`
}

type runPlanManifest struct {
	Version     int                     `json:"version"`
	PlanDigest  string                  `json:"plan_digest"`
	Plan        persistedRunPlan        `json:"plan"`
	Scope       deviceproxy.Scope       `json:"scope"`
	Allocation  Allocation              `json:"allocation"`
	RuntimeID   string                  `json:"runtime_id"`
	Prepared    ports.PreparedTargetRun `json:"prepared"`
	PersistedAt time.Time               `json:"persisted_at"`
}

// runStartManifest is an immutable execution boundary. Its durable presence
// means StartRun committed even if the controller died before its in-memory
// state or observation timer could be updated.
type runStartManifest struct {
	Version               int                     `json:"version"`
	RunPlanDigest         string                  `json:"run_plan_digest"`
	LeaseID               domain.LeaseID          `json:"lease_id"`
	TargetID              domain.TargetID         `json:"target_id"`
	TargetGeneration      domain.TargetGeneration `json:"target_generation"`
	RunID                 domain.TargetRunID      `json:"run_id"`
	RuntimeID             string                  `json:"runtime_id"`
	Allocation            Allocation              `json:"allocation"`
	MaterializationDigest domain.Digest           `json:"materialization_digest"`
	StartedAt             time.Time               `json:"started_at"`
}

// runStopManifest is the immutable authority boundary created only after the
// exact guest has been contained. It deliberately lives beside the retained
// run plan so restart reconciliation can distinguish a world-owned stopped AVD
// from an unrelated offline runtime and replay the exact driver receipt.
type runStopManifest struct {
	Version          int                           `json:"version"`
	RunPlanDigest    string                        `json:"run_plan_digest"`
	TargetPlanDigest string                        `json:"target_plan_digest"`
	LeaseID          domain.LeaseID                `json:"lease_id"`
	TargetID         domain.TargetID               `json:"target_id"`
	TargetGeneration domain.TargetGeneration       `json:"target_generation"`
	RunID            domain.TargetRunID            `json:"run_id"`
	RuntimeID        string                        `json:"runtime_id"`
	Allocation       Allocation                    `json:"allocation"`
	Containment      persistedBackendContainment   `json:"containment"`
	Receipt          persistedTargetRunStopReceipt `json:"receipt"`
}

type persistedBackendContainment struct {
	RuntimeID          string    `json:"runtime_id"`
	ExecutionStopped   bool      `json:"execution_stopped"`
	NetworkUnreachable bool      `json:"network_unreachable"`
	StatePreserved     bool      `json:"state_preserved"`
	ObservedAt         time.Time `json:"observed_at"`
}

type persistedTargetRunStopReceipt struct {
	RunID         domain.TargetRunID              `json:"run_id"`
	Outcome       ports.RunOutcome                `json:"outcome"`
	FailureKind   ports.TargetRunFailureKind      `json:"failure_kind,omitempty"`
	StartedAt     time.Time                       `json:"started_at,omitempty"`
	StoppedAt     time.Time                       `json:"stopped_at"`
	Observations  []persistedTargetRunObservation `json:"observations"`
	TargetChanges persistedTargetChangeSet        `json:"target_changes"`
}

type persistedTargetRunObservation struct {
	Kind              string          `json:"kind"`
	ObservedAt        time.Time       `json:"observed_at"`
	TargetOperationID string          `json:"target_operation_id,omitempty"`
	Payload           json.RawMessage `json:"payload,omitempty"`
}

type persistedTargetChangeSet struct {
	Scope             domain.ChangeScope      `json:"scope"`
	Entries           []persistedTargetChange `json:"entries"`
	WorkspaceRevision uint64                  `json:"workspace_revision"`
	SealedAt          time.Time               `json:"sealed_at"`
}

type persistedTargetChange struct {
	Kind         domain.ChangeKind `json:"kind"`
	Path         string            `json:"path"`
	PreviousPath string            `json:"previous_path,omitempty"`
	BeforeDigest string            `json:"before_digest,omitempty"`
	AfterDigest  string            `json:"after_digest,omitempty"`
	Metadata     map[string]string `json:"metadata"`
}

// generationUseManifest is the durable single-use gate for mutable Android
// targets. Once a scoped run has been prepared (or the generation has been
// quarantined), no different run may receive authority over that generation.
// Reset creates a new target generation with a new state directory and gate.
type generationUseManifest struct {
	Version          int                     `json:"version"`
	TargetID         domain.TargetID         `json:"target_id"`
	Generation       domain.TargetGeneration `json:"generation"`
	TargetPlanDigest string                  `json:"target_plan_digest"`
	RunID            string                  `json:"run_id,omitempty"`
	RunPlanDigest    string                  `json:"run_plan_digest,omitempty"`
	Reason           string                  `json:"reason"`
	PersistedAt      time.Time               `json:"persisted_at"`
}

// generationQuarantineManifest is the immutable authority that allows a
// quarantined, offline generation to be live-verified and removed after a
// daemon restart without ever restarting the preserved guest.
type generationQuarantineManifest struct {
	Version              int                           `json:"version"`
	TargetPlanDigest     string                        `json:"target_plan_digest"`
	QuarantinePlanDigest string                        `json:"quarantine_plan_digest"`
	QuarantinePlan       persistedTargetQuarantinePlan `json:"quarantine_plan"`
	TargetID             domain.TargetID               `json:"target_id"`
	Generation           domain.TargetGeneration       `json:"generation"`
	RuntimeID            string                        `json:"runtime_id"`
	Allocation           Allocation                    `json:"allocation"`
	Containment          persistedBackendContainment   `json:"containment"`
}

type persistedTargetQuarantinePlan struct {
	IdempotencyKey string `json:"idempotency_key"`
	TargetID       string `json:"target_id"`
	Generation     uint64 `json:"generation"`
	Reason         string `json:"reason"`
}

type persistedRunPlan struct {
	IdempotencyKey   string                    `json:"idempotency_key"`
	Run              domain.TargetRunSpec      `json:"run"`
	RequiredCoverage []string                  `json:"required_coverage"`
	Collectors       []ports.CollectorSpec     `json:"collectors"`
	Material         []persistedTargetMaterial `json:"material"`
	MaximumDuration  time.Duration             `json:"maximum_duration"`
}

type persistedTargetMaterial struct {
	Artifact    domain.ArtifactReferenceSpec `json:"artifact"`
	LogicalPath string                       `json:"logical_path"`
	Mode        uint32                       `json:"mode"`
}

type targetPlanIdentity struct {
	IdempotencyKey              string                       `json:"idempotency_key"`
	LeaseID                     string                       `json:"lease_id"`
	TargetID                    string                       `json:"target_id"`
	ResearchSessionID           string                       `json:"research_session_id"`
	TargetKind                  domain.TargetKind            `json:"target_kind"`
	TargetGeneration            uint64                       `json:"target_generation"`
	TargetRevision              uint64                       `json:"target_revision"`
	TargetUpdatedAt             time.Time                    `json:"target_updated_at"`
	Generation                  targetGenerationPlanIdentity `json:"generation"`
	GenerationState             domain.TargetGenerationState `json:"generation_state"`
	GenerationRevision          uint64                       `json:"generation_revision"`
	GenerationUpdatedAt         time.Time                    `json:"generation_updated_at"`
	Template                    ports.TargetTemplate         `json:"template"`
	PolicyDigest                string                       `json:"policy_digest"`
	CapabilityFingerprintDigest string                       `json:"capability_fingerprint_digest"`
	Resources                   admission.Resources          `json:"resources"`
}

type targetGenerationPlanIdentity struct {
	TargetID                    string    `json:"target_id"`
	Generation                  uint64    `json:"generation"`
	PolicyDigest                string    `json:"policy_digest"`
	CapabilityFingerprintDigest string    `json:"capability_fingerprint_digest"`
	PreviousGeneration          uint64    `json:"previous_generation"`
	RecoveryIncidentID          string    `json:"recovery_incident_id,omitempty"`
	CreatedAt                   time.Time `json:"created_at"`
}

func persistTargetRuntimeManifests(plan VirtualDevicePlan, instance Instance, readiness ReadinessState, at time.Time) error {
	digest, err := virtualDevicePlanDigest(plan)
	if err != nil {
		return err
	}
	if !readiness.Ready() {
		return fmt.Errorf("cannot persist a runtime manifest without complete observed readiness")
	}
	if err := os.MkdirAll(plan.StateDirectory, 0o700); err != nil {
		return err
	}
	target := targetPlanManifest{Version: manifestVersion, PlanDigest: digest.String(), Plan: plan, PersistedAt: at.UTC()}
	runtime := runtimePlanManifest{Version: manifestVersion, PlanDigest: digest.String(), Instance: instance, Readiness: readiness, PersistedAt: at.UTC()}
	if err := atomicfile.WriteJSON(filepath.Join(plan.StateDirectory, targetPlanManifestFilename), target, 0o600); err != nil {
		return fmt.Errorf("persist exact target plan: %w", err)
	}
	if err := atomicfile.WriteJSON(filepath.Join(plan.StateDirectory, runtimePlanManifestFilename), runtime, 0o600); err != nil {
		return fmt.Errorf("persist exact runtime plan: %w", err)
	}
	return nil
}

func loadTargetRuntimeManifests(directory string) (targetPlanManifest, runtimePlanManifest, error) {
	var target targetPlanManifest
	if err := readStrictManifest(filepath.Join(directory, targetPlanManifestFilename), &target); err != nil {
		return targetPlanManifest{}, runtimePlanManifest{}, err
	}
	var runtime runtimePlanManifest
	if err := readStrictManifest(filepath.Join(directory, runtimePlanManifestFilename), &runtime); err != nil {
		return targetPlanManifest{}, runtimePlanManifest{}, err
	}
	if target.Version != manifestVersion || runtime.Version != manifestVersion {
		return targetPlanManifest{}, runtimePlanManifest{}, fmt.Errorf("unsupported Android target/runtime manifest version")
	}
	digest, err := virtualDevicePlanDigest(target.Plan)
	if err != nil || target.PlanDigest != digest.String() || runtime.PlanDigest != target.PlanDigest {
		return targetPlanManifest{}, runtimePlanManifest{}, fmt.Errorf("Android target/runtime manifest digest is invalid")
	}
	if !runtime.Readiness.Ready() {
		return targetPlanManifest{}, runtimePlanManifest{}, fmt.Errorf("persisted runtime readiness is incomplete")
	}
	if !instanceMatchesPlan(runtime.Instance, target.Plan) {
		return targetPlanManifest{}, runtimePlanManifest{}, fmt.Errorf("runtime manifest does not bind the exact target plan")
	}
	return target, runtime, nil
}

func validateExpectedManifests(expected VirtualDevicePlan, target targetPlanManifest, runtime runtimePlanManifest) error {
	digest, err := virtualDevicePlanDigest(expected)
	if err != nil {
		return err
	}
	if target.PlanDigest != digest.String() || runtime.PlanDigest != digest.String() {
		return fmt.Errorf("persisted target/runtime plan digest differs from the expected plan")
	}
	return nil
}

func persistRunPlanManifest(directory string, input ports.TargetRunPlan, scope deviceproxy.Scope, allocation Allocation, runtimeID string, prepared ports.PreparedTargetRun, at time.Time) error {
	plan := persistedRunPlanFrom(input)
	digest, err := persistedRunPlanDigest(plan)
	if err != nil {
		return err
	}
	manifest := runPlanManifest{
		Version: manifestVersion, PlanDigest: digest.String(), Plan: plan, Scope: scope,
		Allocation: allocation, RuntimeID: runtimeID, Prepared: prepared, PersistedAt: at.UTC(),
	}
	if err := validateRunPlanManifest(manifest); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(directory, runPlanManifestFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		existing, loadErr := loadExpectedRunManifest(directory, input)
		if loadErr == nil && existing.Scope == scope && existing.Allocation == allocation && existing.RuntimeID == runtimeID && reflect.DeepEqual(existing.Prepared, prepared) {
			return nil
		}
		return errors.Join(fmt.Errorf("commit immutable Android run preparation intent: %w", err), loadErr)
	}
	return nil
}

func commitExpectedRunStart(directory string, input ports.TargetRunPlan, allocation Allocation, runtimeID string, startedAt, minimumStartedAt time.Time) (runStartManifest, error) {
	expected, err := expectedRunStartManifest(input, allocation, runtimeID, startedAt)
	if err != nil {
		return runStartManifest{}, err
	}
	if existing, found, loadErr := loadExpectedRunStart(directory, input, allocation, runtimeID, minimumStartedAt); loadErr != nil {
		return runStartManifest{}, loadErr
	} else if found {
		return existing, nil
	}
	encoded, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		return runStartManifest{}, err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(directory, runStartManifestFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		if existing, found, loadErr := loadExpectedRunStart(directory, input, allocation, runtimeID, minimumStartedAt); loadErr == nil && found {
			return existing, nil
		}
		return runStartManifest{}, fmt.Errorf("commit immutable Android run start: %w", err)
	}
	return expected, nil
}

func loadExpectedRunStart(directory string, input ports.TargetRunPlan, allocation Allocation, runtimeID string, minimumStartedAt time.Time) (runStartManifest, bool, error) {
	path := filepath.Join(directory, runStartManifestFilename)
	var manifest runStartManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return runStartManifest{}, false, nil
	} else if err != nil {
		return runStartManifest{}, false, err
	}
	expected, err := expectedRunStartManifest(input, allocation, runtimeID, manifest.StartedAt)
	if err != nil {
		return runStartManifest{}, false, err
	}
	if !reflect.DeepEqual(manifest, expected) || manifest.StartedAt.Before(minimumStartedAt) {
		return runStartManifest{}, false, fmt.Errorf("Android run-start manifest does not bind the exact run, runtime, allocation, and materialization")
	}
	return manifest, true, nil
}

func expectedRunStartManifest(input ports.TargetRunPlan, allocation Allocation, runtimeID string, startedAt time.Time) (runStartManifest, error) {
	if err := input.Validate(); err != nil {
		return runStartManifest{}, err
	}
	if err := allocation.Validate(); err != nil {
		return runStartManifest{}, err
	}
	if strings.TrimSpace(runtimeID) == "" || startedAt.IsZero() {
		return runStartManifest{}, fmt.Errorf("Android run start requires runtime identity and start time")
	}
	digest, err := persistedRunPlanDigest(persistedRunPlanFrom(input))
	if err != nil {
		return runStartManifest{}, err
	}
	spec := input.Run.Spec()
	return runStartManifest{
		Version: manifestVersion, RunPlanDigest: digest.String(), LeaseID: spec.LeaseID,
		TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration, RunID: spec.ID,
		RuntimeID: runtimeID, Allocation: allocation, MaterializationDigest: spec.MaterializationDigest,
		StartedAt: startedAt.UTC(),
	}, nil
}

type stoppedRunAuthority struct {
	Directory   string
	Run         runPlanManifest
	Stop        runStopManifest
	Receipt     ports.TargetRunStopReceipt
	Containment BackendQuarantineState
}

type stoppedTargetAuthority struct {
	Containment BackendQuarantineState
	State       domain.TargetGenerationState
}

func loadStoppedTargetAuthority(target targetPlanManifest, runtime runtimePlanManifest) (stoppedTargetAuthority, bool, error) {
	quarantine, quarantined, err := loadGenerationQuarantine(target.Plan)
	if err != nil {
		return stoppedTargetAuthority{}, false, err
	}
	if quarantined {
		if quarantine.RuntimeID != runtime.Instance.RuntimeID || quarantine.Allocation != runtime.Instance.Allocation {
			return stoppedTargetAuthority{}, false, fmt.Errorf("Android generation quarantine does not bind the exact persisted runtime")
		}
		return stoppedTargetAuthority{Containment: quarantine.Containment.restore(), State: domain.TargetGenerationQuarantined}, true, nil
	}
	run, stopped, err := loadStoppedRunAuthority(target, runtime)
	if err != nil || !stopped {
		return stoppedTargetAuthority{}, false, err
	}
	return stoppedTargetAuthority{Containment: run.Containment, State: domain.TargetGenerationResettable}, true, nil
}

func commitExpectedRunStop(directory string, input ports.TargetRunPlan, target VirtualDevicePlan, allocation Allocation, runtimeID string, containment BackendQuarantineState, receipt ports.TargetRunStopReceipt) (ports.TargetRunStopReceipt, BackendQuarantineState, error) {
	if strings.TrimSpace(directory) == "" {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, fmt.Errorf("Android run stop requires its exact durable run directory")
	}
	if existing, existingContainment, found, err := loadExpectedRunStop(directory, input, target, allocation, runtimeID); err != nil {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, err
	} else if found {
		return existing, existingContainment, nil
	}
	manifest, err := expectedRunStopManifest(input, target, allocation, runtimeID, containment, receipt)
	if err != nil {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, err
	}
	// Marshal compactly so json.RawMessage observation payload bytes remain
	// byte-for-byte stable when the receipt is reloaded and replayed.
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(directory, runStopManifestFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		existing, existingContainment, found, loadErr := loadExpectedRunStop(directory, input, target, allocation, runtimeID)
		if loadErr == nil && found {
			return existing, existingContainment, nil
		}
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, errors.Join(fmt.Errorf("commit immutable Android run stop: %w", err), loadErr)
	}
	restored, err := manifest.Receipt.restore()
	if err != nil {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, err
	}
	return restored, manifest.Containment.restore(), nil
}

func loadExpectedRunStop(directory string, input ports.TargetRunPlan, target VirtualDevicePlan, allocation Allocation, runtimeID string) (ports.TargetRunStopReceipt, BackendQuarantineState, bool, error) {
	persisted, err := loadExpectedRunManifest(directory, input)
	if err != nil {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, err
	}
	if persisted.Allocation != allocation || persisted.RuntimeID != runtimeID {
		return ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, fmt.Errorf("persisted Android run does not bind the expected allocation and runtime")
	}
	_, receipt, containment, found, err := loadBoundRunStop(directory, target, persisted)
	return receipt, containment, found, err
}

func expectedRunStopManifest(input ports.TargetRunPlan, target VirtualDevicePlan, allocation Allocation, runtimeID string, containment BackendQuarantineState, receipt ports.TargetRunStopReceipt) (runStopManifest, error) {
	if target.TargetID.IsZero() || !target.Generation.IsValid() {
		return runStopManifest{}, fmt.Errorf("Android run stop requires an initialized target plan")
	}
	if err := allocation.Validate(); err != nil {
		return runStopManifest{}, err
	}
	if target.Allocation != allocation || strings.TrimSpace(runtimeID) == "" {
		return runStopManifest{}, fmt.Errorf("Android run stop does not bind the target allocation and runtime")
	}
	if containment.RuntimeID != runtimeID || !containment.ExecutionStopped || !containment.NetworkUnreachable || !containment.StatePreserved || containment.ObservedAt.IsZero() {
		return runStopManifest{}, fmt.Errorf("Android run stop requires exact complete containment evidence")
	}
	if err := receipt.Validate(); err != nil {
		return runStopManifest{}, err
	}
	spec := input.Run.Spec()
	if receipt.RunID != spec.ID || containment.ObservedAt.After(receipt.StoppedAt) {
		return runStopManifest{}, fmt.Errorf("Android stop receipt does not bind the exact run containment interval")
	}
	runDigest, err := persistedRunPlanDigest(persistedRunPlanFrom(input))
	if err != nil {
		return runStopManifest{}, err
	}
	targetDigest, err := virtualDevicePlanDigest(target)
	if err != nil {
		return runStopManifest{}, err
	}
	return runStopManifest{
		Version: manifestVersion, RunPlanDigest: runDigest.String(), TargetPlanDigest: targetDigest.String(),
		LeaseID: spec.LeaseID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration, RunID: spec.ID,
		RuntimeID: runtimeID, Allocation: allocation, Containment: persistBackendContainment(containment),
		Receipt: persistTargetRunStopReceipt(receipt),
	}, nil
}

func loadBoundRunStop(directory string, target VirtualDevicePlan, run runPlanManifest) (runStopManifest, ports.TargetRunStopReceipt, BackendQuarantineState, bool, error) {
	path := filepath.Join(directory, runStopManifestFilename)
	var manifest runStopManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, nil
	} else if err != nil {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, err
	}
	if err := validateRunPlanManifest(run); err != nil {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, err
	}
	targetDigest, err := virtualDevicePlanDigest(target)
	if err != nil {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, err
	}
	runSpec := run.Plan.Run
	if manifest.Version != manifestVersion || manifest.RunPlanDigest != run.PlanDigest || manifest.TargetPlanDigest != targetDigest.String() ||
		manifest.LeaseID != runSpec.LeaseID || manifest.TargetID != target.TargetID || manifest.TargetID != runSpec.TargetID ||
		manifest.TargetGeneration != target.Generation || manifest.TargetGeneration != runSpec.TargetGeneration || manifest.RunID != runSpec.ID ||
		manifest.RuntimeID != run.RuntimeID || manifest.Allocation != run.Allocation || manifest.Allocation != target.Allocation {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, fmt.Errorf("Android run-stop manifest does not bind the exact target, run, runtime, and allocation")
	}
	containment := manifest.Containment.restore()
	if containment.RuntimeID != run.RuntimeID || !containment.ExecutionStopped || !containment.NetworkUnreachable || !containment.StatePreserved || containment.ObservedAt.IsZero() {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, fmt.Errorf("Android run-stop manifest has incomplete or foreign containment evidence")
	}
	receipt, err := manifest.Receipt.restore()
	if err != nil {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, err
	}
	if err := validateStoppedRunReceipt(directory, run, receipt, containment); err != nil {
		return runStopManifest{}, ports.TargetRunStopReceipt{}, BackendQuarantineState{}, false, err
	}
	return manifest, receipt, containment, true, nil
}

func loadStoppedRunAuthority(target targetPlanManifest, runtime runtimePlanManifest) (stoppedRunAuthority, bool, error) {
	use, found, err := loadGenerationUseManifest(target.Plan)
	if err != nil || !found {
		return stoppedRunAuthority{}, false, err
	}
	if use.Reason != generationUseRunPrepared {
		return stoppedRunAuthority{}, false, nil
	}
	runsDirectory := filepath.Join(target.Plan.StateDirectory, "runs")
	entries, err := os.ReadDir(runsDirectory)
	if err != nil {
		return stoppedRunAuthority{}, false, err
	}
	if len(entries) != 1 || entries[0].Name() != use.RunID || !entries[0].IsDir() || entries[0].Type()&os.ModeSymlink != 0 {
		return stoppedRunAuthority{}, false, fmt.Errorf("Android generation-use authority does not identify one exact regular run directory")
	}
	directory := filepath.Join(runsDirectory, use.RunID)
	var run runPlanManifest
	if err := readStrictManifest(filepath.Join(directory, runPlanManifestFilename), &run); err != nil {
		return stoppedRunAuthority{}, false, err
	}
	if err := validateRunPlanManifest(run); err != nil {
		return stoppedRunAuthority{}, false, err
	}
	if run.PlanDigest != use.RunPlanDigest || run.Plan.Run.ID.String() != use.RunID || run.RuntimeID != runtime.Instance.RuntimeID || run.Allocation != runtime.Instance.Allocation {
		return stoppedRunAuthority{}, false, fmt.Errorf("Android generation-use, run, and runtime manifests disagree")
	}
	expectedAttachment, err := androidObservationAttachment(target.Plan, run.RuntimeID)
	if err != nil || run.Prepared.Attachment != expectedAttachment {
		return stoppedRunAuthority{}, false, fmt.Errorf("Android run manifest observation attachment differs from the exact target ADB authority")
	}
	stop, receipt, containment, stopped, err := loadBoundRunStop(directory, target.Plan, run)
	if err != nil || !stopped {
		return stoppedRunAuthority{}, false, err
	}
	return stoppedRunAuthority{Directory: directory, Run: run, Stop: stop, Receipt: receipt, Containment: containment}, true, nil
}

func validateRunPlanManifest(manifest runPlanManifest) error {
	if manifest.Version != manifestVersion || manifest.PersistedAt.IsZero() {
		return fmt.Errorf("unsupported or incomplete Android run manifest")
	}
	digest, err := persistedRunPlanDigest(manifest.Plan)
	if err != nil || manifest.PlanDigest != digest.String() {
		return fmt.Errorf("Android run manifest digest is invalid")
	}
	if _, err := domain.NewTargetRun(manifest.Plan.Run); err != nil {
		return fmt.Errorf("Android run manifest has an invalid run identity: %w", err)
	}
	if err := manifest.Scope.Validate(); err != nil {
		return err
	}
	if err := manifest.Allocation.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(manifest.RuntimeID) == "" || manifest.Scope.RunID != manifest.Plan.Run.ID || manifest.Scope.TargetID != manifest.Plan.Run.TargetID || manifest.Scope.Generation != manifest.Plan.Run.TargetGeneration || manifest.Scope.Serial != manifest.Allocation.Serial {
		return fmt.Errorf("Android run manifest scope does not bind the exact run allocation")
	}
	if err := manifest.Prepared.Attachment.Validate(); err != nil {
		return err
	}
	if manifest.Prepared.Attachment.ADBDevice.IsZero() || manifest.Prepared.Attachment.ADBDevice.Serial != manifest.Allocation.Serial {
		return fmt.Errorf("Android prepared-run evidence does not bind the exact ADB allocation")
	}
	if manifest.Prepared.RunID != manifest.Plan.Run.ID || manifest.Prepared.TargetID != manifest.Plan.Run.TargetID || manifest.Prepared.TargetGeneration != manifest.Plan.Run.TargetGeneration || manifest.Prepared.MaterializationDigest != manifest.Plan.Run.MaterializationDigest || !reflect.DeepEqual(manifest.Prepared.RequiredCoverage, manifest.Plan.RequiredCoverage) || manifest.Prepared.Attachment.TargetKind != domain.TargetAndroidVirtualDevice || manifest.Prepared.Attachment.RuntimeID != manifest.RuntimeID || manifest.Prepared.PreparedAt.Before(manifest.Plan.Run.CreatedAt) {
		return fmt.Errorf("Android prepared-run evidence does not bind the exact persisted run")
	}
	return nil
}

func validateStoppedRunReceipt(directory string, run runPlanManifest, receipt ports.TargetRunStopReceipt, containment BackendQuarantineState) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.RunID != run.Plan.Run.ID || containment.ObservedAt.After(receipt.StoppedAt) || !receipt.TargetChanges.SealedAt().Equal(receipt.StoppedAt) {
		return fmt.Errorf("Android stop receipt does not bind the exact run and containment interval")
	}
	start, started, err := loadPersistedRunStart(directory, run)
	if err != nil {
		return err
	}
	if started != !receipt.StartedAt.IsZero() || started && !start.StartedAt.Equal(receipt.StartedAt) {
		return fmt.Errorf("Android stop receipt disagrees with the immutable run-start boundary")
	}
	last := receipt.Observations[len(receipt.Observations)-1]
	expectedKind := "target.run.stopped"
	switch receipt.FailureKind {
	case ports.TargetRunFailureNeverStarted:
		expectedKind = "target.run.never_started"
	case ports.TargetRunFailureDurationExceeded:
		expectedKind = "target.run.duration_exceeded"
	case ports.TargetRunFailureTarget:
		expectedKind = "target.run.control-plane-failure"
	}
	if last.Kind != expectedKind || !last.ObservedAt.Equal(receipt.StoppedAt) {
		return fmt.Errorf("Android stop receipt has an invalid terminal observation")
	}
	var payload struct {
		FailureKind ports.TargetRunFailureKind `json:"failure_kind,omitempty"`
		RuntimeID   string                     `json:"runtime_id"`
		Serial      string                     `json:"serial"`
	}
	if err := json.Unmarshal(last.Payload, &payload); err != nil || payload.FailureKind != receipt.FailureKind || payload.RuntimeID != run.RuntimeID || payload.Serial != run.Allocation.Serial {
		return fmt.Errorf("Android stop receipt terminal observation is not bound to the exact runtime")
	}
	return nil
}

func loadPersistedRunStart(directory string, run runPlanManifest) (runStartManifest, bool, error) {
	var manifest runStartManifest
	if err := readStrictManifest(filepath.Join(directory, runStartManifestFilename), &manifest); errors.Is(err, os.ErrNotExist) {
		return runStartManifest{}, false, nil
	} else if err != nil {
		return runStartManifest{}, false, err
	}
	spec := run.Plan.Run
	if manifest.Version != manifestVersion || manifest.RunPlanDigest != run.PlanDigest || manifest.LeaseID != spec.LeaseID || manifest.TargetID != spec.TargetID || manifest.TargetGeneration != spec.TargetGeneration || manifest.RunID != spec.ID || manifest.RuntimeID != run.RuntimeID || manifest.Allocation != run.Allocation || manifest.MaterializationDigest != spec.MaterializationDigest || manifest.StartedAt.Before(spec.CreatedAt) {
		return runStartManifest{}, false, fmt.Errorf("Android run-start manifest does not bind the persisted run")
	}
	return manifest, true, nil
}

func persistBackendContainment(value BackendQuarantineState) persistedBackendContainment {
	return persistedBackendContainment{
		RuntimeID: value.RuntimeID, ExecutionStopped: value.ExecutionStopped, NetworkUnreachable: value.NetworkUnreachable,
		StatePreserved: value.StatePreserved, ObservedAt: value.ObservedAt.UTC(),
	}
}

func (value persistedBackendContainment) restore() BackendQuarantineState {
	return BackendQuarantineState{
		RuntimeID: value.RuntimeID, ExecutionStopped: value.ExecutionStopped, NetworkUnreachable: value.NetworkUnreachable,
		StatePreserved: value.StatePreserved, ObservedAt: value.ObservedAt.UTC(),
	}
}

func persistTargetRunStopReceipt(value ports.TargetRunStopReceipt) persistedTargetRunStopReceipt {
	result := persistedTargetRunStopReceipt{
		RunID: value.RunID, Outcome: value.Outcome, FailureKind: value.FailureKind,
		StartedAt: value.StartedAt.UTC(), StoppedAt: value.StoppedAt.UTC(), TargetChanges: persistTargetChangeSet(value.TargetChanges),
		Observations: make([]persistedTargetRunObservation, len(value.Observations)),
	}
	for index, observation := range value.Observations {
		result.Observations[index] = persistedTargetRunObservation{
			Kind: observation.Kind, ObservedAt: observation.ObservedAt.UTC(), TargetOperationID: observation.TargetOperationID.String(),
			Payload: append(json.RawMessage(nil), observation.Payload...),
		}
	}
	return result
}

func (value persistedTargetRunStopReceipt) restore() (ports.TargetRunStopReceipt, error) {
	changes, err := value.TargetChanges.restore()
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	receipt := ports.TargetRunStopReceipt{
		RunID: value.RunID, Outcome: value.Outcome, FailureKind: value.FailureKind,
		StartedAt: value.StartedAt.UTC(), StoppedAt: value.StoppedAt.UTC(), TargetChanges: changes,
		Observations: make([]ports.TargetRunObservation, len(value.Observations)),
	}
	for index, observation := range value.Observations {
		var operationID domain.TargetOperationID
		if observation.TargetOperationID != "" {
			operationID, err = domain.ParseTargetOperationID(observation.TargetOperationID)
			if err != nil {
				return ports.TargetRunStopReceipt{}, err
			}
		}
		receipt.Observations[index] = ports.TargetRunObservation{
			Kind: observation.Kind, ObservedAt: observation.ObservedAt.UTC(), TargetOperationID: operationID,
			Payload: append(json.RawMessage(nil), observation.Payload...),
		}
	}
	return receipt, receipt.Validate()
}

func persistTargetChangeSet(value domain.ChangeSet) persistedTargetChangeSet {
	result := persistedTargetChangeSet{
		Scope: value.Scope(), WorkspaceRevision: uint64(value.WorkspaceRevision()), SealedAt: value.SealedAt().UTC(),
		Entries: make([]persistedTargetChange, 0, len(value.Entries())),
	}
	for _, entry := range value.Entries() {
		spec := entry.Spec()
		result.Entries = append(result.Entries, persistedTargetChange{
			Kind: spec.Kind, Path: spec.Path, PreviousPath: spec.PreviousPath, BeforeDigest: spec.BeforeDigest.String(),
			AfterDigest: spec.AfterDigest.String(), Metadata: cloneLabels(spec.Metadata),
		})
	}
	return result
}

func (value persistedTargetChangeSet) restore() (domain.ChangeSet, error) {
	entries := make([]domain.ChangeEntry, len(value.Entries))
	for index, persisted := range value.Entries {
		var before, after domain.Digest
		var err error
		if persisted.BeforeDigest != "" {
			before, err = domain.ParseDigest(persisted.BeforeDigest)
			if err != nil {
				return domain.ChangeSet{}, err
			}
		}
		if persisted.AfterDigest != "" {
			after, err = domain.ParseDigest(persisted.AfterDigest)
			if err != nil {
				return domain.ChangeSet{}, err
			}
		}
		entries[index], err = domain.NewChangeEntry(domain.ChangeEntrySpec{
			Kind: persisted.Kind, Path: persisted.Path, PreviousPath: persisted.PreviousPath,
			BeforeDigest: before, AfterDigest: after, Metadata: cloneLabels(persisted.Metadata),
		})
		if err != nil {
			return domain.ChangeSet{}, err
		}
	}
	return domain.NewChangeSet(value.Scope, entries, domain.Revision(value.WorkspaceRevision), value.SealedAt.UTC())
}

func persistRunGenerationUse(plan VirtualDevicePlan, input ports.TargetRunPlan, at time.Time) error {
	targetDigest, err := virtualDevicePlanDigest(plan)
	if err != nil {
		return err
	}
	runDigest, err := persistedRunPlanDigest(persistedRunPlanFrom(input))
	if err != nil {
		return err
	}
	manifest := generationUseManifest{
		Version: manifestVersion, TargetID: plan.TargetID, Generation: plan.Generation,
		TargetPlanDigest: targetDigest.String(), RunID: input.Run.ID().String(), RunPlanDigest: runDigest.String(),
		Reason: generationUseRunPrepared, PersistedAt: at.UTC(),
	}
	return persistGenerationUseManifest(plan, manifest)
}

func persistedTargetQuarantinePlanFrom(plan ports.TargetQuarantinePlan) persistedTargetQuarantinePlan {
	return persistedTargetQuarantinePlan{
		IdempotencyKey: plan.IdempotencyKey, TargetID: plan.Target.ID.String(),
		Generation: uint64(plan.Target.Generation), Reason: plan.Reason,
	}
}

func (p persistedTargetQuarantinePlan) restore() (ports.TargetQuarantinePlan, error) {
	targetID, err := domain.ParseTargetID(p.TargetID)
	if err != nil {
		return ports.TargetQuarantinePlan{}, err
	}
	plan := ports.TargetQuarantinePlan{
		IdempotencyKey: p.IdempotencyKey,
		Target:         ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(p.Generation)},
		Reason:         p.Reason,
	}
	return plan, plan.Validate()
}

func targetQuarantinePlanDigest(plan ports.TargetQuarantinePlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	return canonicalAndroidPlanDigest("world.android-target-quarantine.v1\n", persistedTargetQuarantinePlanFrom(plan))
}

func commitExpectedGenerationQuarantine(plan VirtualDevicePlan, instance Instance, quarantine ports.TargetQuarantinePlan, containment BackendQuarantineState) (BackendQuarantineState, error) {
	if err := quarantine.Validate(); err != nil {
		return BackendQuarantineState{}, err
	}
	if quarantine.Target.ID != plan.TargetID || quarantine.Target.Generation != plan.Generation {
		return BackendQuarantineState{}, fmt.Errorf("Android generation quarantine request identifies another target")
	}
	if existing, found, err := loadGenerationQuarantine(plan); err != nil {
		return BackendQuarantineState{}, err
	} else if found {
		existingPlan, restoreErr := existing.QuarantinePlan.restore()
		if restoreErr != nil || existingPlan != quarantine || existing.RuntimeID != instance.RuntimeID || existing.Allocation != instance.Allocation {
			return BackendQuarantineState{}, fmt.Errorf("Android generation quarantine authority identifies another exact request or runtime")
		}
		return existing.Containment.restore(), nil
	}
	targetDigest, err := virtualDevicePlanDigest(plan)
	if err != nil {
		return BackendQuarantineState{}, err
	}
	quarantineDigest, err := targetQuarantinePlanDigest(quarantine)
	if err != nil {
		return BackendQuarantineState{}, err
	}
	if instance.RuntimeID == "" || instance.Allocation != plan.Allocation || containment.RuntimeID != instance.RuntimeID || !containment.ExecutionStopped || !containment.NetworkUnreachable || !containment.StatePreserved || containment.ObservedAt.IsZero() {
		return BackendQuarantineState{}, fmt.Errorf("Android generation quarantine requires exact complete containment evidence")
	}
	manifest := generationQuarantineManifest{
		Version: manifestVersion, TargetPlanDigest: targetDigest.String(),
		QuarantinePlanDigest: quarantineDigest.String(), QuarantinePlan: persistedTargetQuarantinePlanFrom(quarantine), TargetID: plan.TargetID,
		Generation: plan.Generation, RuntimeID: instance.RuntimeID, Allocation: instance.Allocation,
		Containment: persistBackendContainment(containment),
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return BackendQuarantineState{}, err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(plan.StateDirectory, generationQuarantineFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		existing, found, loadErr := loadGenerationQuarantine(plan)
		if loadErr == nil && found {
			existingPlan, restoreErr := existing.QuarantinePlan.restore()
			if restoreErr == nil && existingPlan == quarantine && existing.RuntimeID == instance.RuntimeID && existing.Allocation == instance.Allocation {
				return existing.Containment.restore(), nil
			}
		}
		return BackendQuarantineState{}, errors.Join(fmt.Errorf("commit immutable Android generation quarantine: %w", err), loadErr)
	}
	return manifest.Containment.restore(), nil
}

func loadGenerationQuarantine(plan VirtualDevicePlan) (generationQuarantineManifest, bool, error) {
	var manifest generationQuarantineManifest
	if err := readStrictManifest(filepath.Join(plan.StateDirectory, generationQuarantineFilename), &manifest); errors.Is(err, os.ErrNotExist) {
		return generationQuarantineManifest{}, false, nil
	} else if err != nil {
		return generationQuarantineManifest{}, false, err
	}
	targetDigest, err := virtualDevicePlanDigest(plan)
	if err != nil {
		return generationQuarantineManifest{}, false, err
	}
	containment := manifest.Containment.restore()
	quarantine, quarantineErr := manifest.QuarantinePlan.restore()
	quarantineDigest, digestErr := targetQuarantinePlanDigest(quarantine)
	if quarantineErr != nil || digestErr != nil || manifest.QuarantinePlanDigest != quarantineDigest.String() || quarantine.Target.ID != plan.TargetID || quarantine.Target.Generation != plan.Generation ||
		manifest.Version != manifestVersion || manifest.TargetPlanDigest != targetDigest.String() || manifest.TargetID != plan.TargetID || manifest.Generation != plan.Generation || manifest.RuntimeID == "" || manifest.Allocation != plan.Allocation || containment.RuntimeID != manifest.RuntimeID || !containment.ExecutionStopped || !containment.NetworkUnreachable || !containment.StatePreserved || containment.ObservedAt.IsZero() {
		return generationQuarantineManifest{}, false, fmt.Errorf("Android generation-quarantine manifest does not bind the exact target and complete containment evidence")
	}
	return manifest, true, nil
}

func persistGenerationUseManifest(plan VirtualDevicePlan, manifest generationUseManifest) error {
	if existing, found, err := loadGenerationUseManifest(plan); err != nil {
		return err
	} else if found {
		if existing.Reason == manifest.Reason && existing.RunID == manifest.RunID && existing.RunPlanDigest == manifest.RunPlanDigest {
			return nil
		}
		return fmt.Errorf("Android target generation is already bound to another use")
	}
	if err := os.MkdirAll(plan.StateDirectory, 0o700); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(plan.StateDirectory, generationUseFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		existing, found, loadErr := loadGenerationUseManifest(plan)
		if loadErr == nil && found && existing.Reason == manifest.Reason && existing.RunID == manifest.RunID && existing.RunPlanDigest == manifest.RunPlanDigest {
			return nil
		}
		return errors.Join(fmt.Errorf("commit immutable Android generation use: %w", err), loadErr)
	}
	return nil
}

func loadGenerationUseManifest(plan VirtualDevicePlan) (generationUseManifest, bool, error) {
	path := filepath.Join(plan.StateDirectory, generationUseFilename)
	var manifest generationUseManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return generationUseManifest{}, false, nil
	} else if err != nil {
		return generationUseManifest{}, false, err
	}
	targetDigest, err := virtualDevicePlanDigest(plan)
	if err != nil {
		return generationUseManifest{}, false, err
	}
	if manifest.Version != manifestVersion || manifest.TargetID != plan.TargetID || manifest.Generation != plan.Generation || manifest.TargetPlanDigest != targetDigest.String() || manifest.PersistedAt.IsZero() {
		return generationUseManifest{}, false, fmt.Errorf("Android generation-use manifest does not bind the exact target plan")
	}
	switch manifest.Reason {
	case generationUseRunPrepared:
		if manifest.RunID == "" || manifest.RunPlanDigest == "" {
			return generationUseManifest{}, false, fmt.Errorf("Android run generation-use manifest is incomplete")
		}
		if _, err := domain.ParseTargetRunID(manifest.RunID); err != nil {
			return generationUseManifest{}, false, fmt.Errorf("Android run generation-use identity is invalid: %w", err)
		}
		if _, err := domain.ParseDigest(manifest.RunPlanDigest); err != nil {
			return generationUseManifest{}, false, fmt.Errorf("Android run generation-use digest is invalid: %w", err)
		}
	default:
		return generationUseManifest{}, false, fmt.Errorf("Android generation-use reason is invalid")
	}
	return manifest, true, nil
}

func requireGenerationAvailableForRun(plan VirtualDevicePlan) error {
	if _, found, err := loadGenerationQuarantine(plan); err != nil {
		return fmt.Errorf("inspect durable Android generation-quarantine gate: %w", err)
	} else if found {
		return fmt.Errorf("target generation is quarantined and cannot receive run authority")
	}
	_, found, err := loadGenerationUseManifest(plan)
	if err != nil {
		return fmt.Errorf("inspect durable Android generation-use gate: %w", err)
	}
	if found {
		return fmt.Errorf("target generation was already assigned mutable Android authority and requires reset")
	}
	runsDirectory := filepath.Join(plan.StateDirectory, "runs")
	entries, err := os.ReadDir(runsDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect prior Android run state: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("target generation has prior or interrupted Android run state and requires recovery or reset")
	}
	return nil
}

func loadExpectedRunManifest(directory string, input ports.TargetRunPlan) (runPlanManifest, error) {
	var manifest runPlanManifest
	if err := readStrictManifest(filepath.Join(directory, runPlanManifestFilename), &manifest); err != nil {
		return runPlanManifest{}, err
	}
	if err := validateRunPlanManifest(manifest); err != nil {
		return runPlanManifest{}, err
	}
	expected := persistedRunPlanFrom(input)
	digest, err := persistedRunPlanDigest(expected)
	if err != nil {
		return runPlanManifest{}, err
	}
	actualDigest, actualErr := persistedRunPlanDigest(manifest.Plan)
	if actualErr != nil || manifest.PlanDigest != digest.String() || manifest.PlanDigest != actualDigest.String() {
		return runPlanManifest{}, fmt.Errorf("persisted run plan differs from the exact interrupted plan")
	}
	if err := manifest.Scope.Validate(); err != nil {
		return runPlanManifest{}, err
	}
	if manifest.Scope.RunID != input.Run.ID() || manifest.Scope.TargetID != input.Run.Spec().TargetID || manifest.Scope.Generation != input.Run.Spec().TargetGeneration || manifest.Scope.Serial != manifest.Allocation.Serial {
		return runPlanManifest{}, fmt.Errorf("persisted run scope does not bind the exact interrupted run allocation")
	}
	spec := input.Run.Spec()
	if manifest.Prepared.RunID != spec.ID || manifest.Prepared.TargetID != spec.TargetID || manifest.Prepared.TargetGeneration != spec.TargetGeneration || manifest.Prepared.MaterializationDigest != spec.MaterializationDigest || !reflect.DeepEqual(manifest.Prepared.RequiredCoverage, input.RequiredCoverage) || manifest.Prepared.Attachment.TargetKind != domain.TargetAndroidVirtualDevice || manifest.Prepared.Attachment.RuntimeID != manifest.RuntimeID || manifest.Prepared.PreparedAt.Before(spec.CreatedAt) {
		return runPlanManifest{}, fmt.Errorf("persisted prepared-run evidence does not bind the exact interrupted run")
	}
	return manifest, nil
}

func persistedRunPlanFrom(input ports.TargetRunPlan) persistedRunPlan {
	material := make([]persistedTargetMaterial, len(input.Material))
	for index, entry := range input.Material {
		material[index] = persistedTargetMaterial{Artifact: entry.Artifact.Spec(), LogicalPath: entry.LogicalPath, Mode: entry.Mode}
	}
	return persistedRunPlan{
		IdempotencyKey: input.IdempotencyKey, Run: input.Run.Spec(), RequiredCoverage: append([]string(nil), input.RequiredCoverage...),
		Collectors: append([]ports.CollectorSpec(nil), input.Collectors...), Material: material, MaximumDuration: input.MaximumDuration,
	}
}

func virtualDevicePlanDigest(plan VirtualDevicePlan) (domain.Digest, error) {
	return canonicalAndroidPlanDigest("world.android-target-plan.v1\n", plan)
}

func persistedRunPlanDigest(plan persistedRunPlan) (domain.Digest, error) {
	return canonicalAndroidPlanDigest("world.android-run-plan.v1\n", plan)
}

func targetPlanSignature(plan ports.TargetPlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	target := plan.Target
	generation := plan.Generation
	generationSpec := generation.Spec()
	identity := targetPlanIdentity{
		IdempotencyKey: plan.IdempotencyKey, LeaseID: plan.LeaseID.String(),
		TargetID: target.ID().String(), ResearchSessionID: target.ResearchSessionID().String(), TargetKind: target.Kind(),
		TargetGeneration: uint64(target.CurrentGeneration()), TargetRevision: uint64(target.Revision()), TargetUpdatedAt: target.UpdatedAt().UTC(),
		Generation: targetGenerationPlanIdentity{
			TargetID: generationSpec.TargetID.String(), Generation: uint64(generationSpec.Generation),
			PolicyDigest: generationSpec.PolicyDigest.String(), CapabilityFingerprintDigest: generationSpec.CapabilityFingerprintDigest.String(),
			PreviousGeneration: uint64(generationSpec.PreviousGeneration), RecoveryIncidentID: optionalIDString(generationSpec.RecoveryIncidentID),
			CreatedAt: generationSpec.CreatedAt.UTC(),
		},
		GenerationState: generation.State(), GenerationRevision: uint64(generation.Revision()), GenerationUpdatedAt: generation.UpdatedAt().UTC(),
		Template: plan.Template, PolicyDigest: plan.PolicyDigest.String(), CapabilityFingerprintDigest: plan.CapabilityFingerprintDigest.String(),
		Resources: plan.Resources.Clone(),
	}
	return canonicalAndroidPlanDigest("world.android-target-request.v1\n", identity)
}

func optionalIDString(id domain.IncidentID) string {
	if id.IsZero() {
		return ""
	}
	return id.String()
}

func runPlanSignature(plan ports.TargetRunPlan) (domain.Digest, error) {
	if err := plan.Validate(); err != nil {
		return domain.Digest{}, err
	}
	return persistedRunPlanDigest(persistedRunPlanFrom(plan))
}

func canonicalAndroidPlanDigest(domainSeparator string, plan any) (domain.Digest, error) {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(append([]byte(domainSeparator), encoded...)), nil
}

func readStrictManifest(path string, destination any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maximumManifestBytes {
		return fmt.Errorf("manifest %q is not a bounded regular file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maximumManifestBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("manifest contains trailing JSON values")
		}
		return err
	}
	return nil
}
