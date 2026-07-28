package linuxcontainer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/runevidence"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

type CollectorReadiness interface {
	AwaitReady(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error
}

type CollectorReadinessFunc func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error

func (f CollectorReadinessFunc) AwaitReady(ctx context.Context, runID domain.TargetRunID, requirements []ports.ObservationRequirement) error {
	return f(ctx, runID, append([]ports.ObservationRequirement(nil), requirements...))
}

// IntrinsicSignalFamily is the only signal family the Linux target driver can
// produce without an external collector coordinator.
const IntrinsicSignalFamily = "target.lifecycle"

// SupportsIntrinsicCoverage reports whether the driver can authoritatively
// produce a required family from facts it directly observes.
func SupportsIntrinsicCoverage(family string) bool {
	return family == IntrinsicSignalFamily
}

type Config struct {
	Build      BuildConfig
	Runtime    Runtime
	Collectors CollectorReadiness
	Random     io.Reader
	Now        func() time.Time
	AfterFunc  func(time.Duration, func()) RunTimer
}

// RunTimer is the narrow timer surface needed to enforce a physical run
// deadline. Tests can inject a deterministic implementation without sleeping.
type RunTimer interface {
	Stop() bool
}

type Driver struct {
	build       BuildConfig
	runtime     Runtime
	collectors  CollectorReadiness
	random      io.Reader
	now         func() time.Time
	afterFunc   func(time.Duration, func()) RunTimer
	lifecycleMu sync.Mutex

	mu           sync.Mutex
	targets      map[string]targetRecord
	runs         map[string]*runRecord
	idempotency  map[string]string
	resetResults map[string]resetOutcome
	quarantines  map[string]quarantineOutcome
	materialized map[string]*materializationState
}

type resetOutcome struct {
	targetID domain.TargetID
	plan     ports.ResetPlan
	result   ports.TargetResult
	err      error
}

type quarantineOutcome struct {
	plan     ports.TargetQuarantinePlan
	evidence ports.TargetQuarantineEvidence
}

type targetRecord struct {
	input      ports.TargetPlan
	plan       ContainerPlan
	runtimeID  string
	status     ports.TargetStatus
	quarantine *ports.TargetQuarantineEvidence
}

type materializationState struct {
	digest domain.Digest
	done   chan struct{}
	err    error
}

type RunAuthority struct {
	LeaseID    domain.LeaseID
	TargetID   domain.TargetID
	Generation domain.TargetGeneration
	RunID      domain.TargetRunID
	secret     [32]byte
}

func (a RunAuthority) Matches(candidate RunAuthority) bool {
	return a.LeaseID == candidate.LeaseID && a.TargetID == candidate.TargetID && a.Generation == candidate.Generation && a.RunID == candidate.RunID && subtle.ConstantTimeCompare(a.secret[:], candidate.secret[:]) == 1
}

type runRecord struct {
	plan         ports.TargetRunPlan
	authority    RunAuthority
	directory    string
	prepared     ports.PreparedTargetRun
	started      bool
	startedAt    time.Time
	stopped      bool
	quarantined  bool
	result       *ports.TargetRunStopReceipt
	timer        RunTimer
	transports   map[*targetTransport]struct{}
	observations []ports.TargetRunObservation
}

func New(config Config) (*Driver, error) {
	if config.Runtime == nil || config.Collectors == nil {
		return nil, fmt.Errorf("target runtime and collector-readiness gate are required")
	}
	if config.Build.TargetRoot == "" || config.Build.ImageRepository == "" {
		return nil, fmt.Errorf("target root and image repository are required")
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.AfterFunc == nil {
		config.AfterFunc = func(duration time.Duration, callback func()) RunTimer {
			return time.AfterFunc(duration, callback)
		}
	}
	return &Driver{build: config.Build, runtime: config.Runtime, collectors: config.Collectors, random: config.Random, now: config.Now, afterFunc: config.AfterFunc, targets: make(map[string]targetRecord), runs: make(map[string]*runRecord), idempotency: make(map[string]string), resetResults: make(map[string]resetOutcome), quarantines: make(map[string]quarantineOutcome), materialized: make(map[string]*materializationState)}, nil
}

func NewDriver(config Config) (*Driver, error) { return New(config) }

func (d *Driver) Probe(ctx context.Context, template ports.TargetTemplate) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := template.Validate(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if template.Kind != domain.TargetLinuxContainer {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.probe", "template.kind", "template is not a Linux container", nil)
	}
	capabilities, err := d.runtime.Probe(ctx)
	if err != nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.probe", "runtime", "container runtime probe failed", err)
	}
	runtimeCapability, _ := domain.NewCapability(domain.CapabilitySupported, map[string]string{"api_version": capabilities.APIVersion, "cgroup_version": capabilities.CgroupVersion}, map[string]string{"runtime_version": capabilities.Version})
	visibility, _ := domain.NewCapability(domain.CapabilitySupported, map[string]string{
		"runtime": "runc", "sibling": "true", "arbitrary_exec": "true", "bounded_transfer": "true",
		"ptrace": strconv.FormatBool(d.build.AllowPtrace),
	}, nil)
	return domain.NewCapabilityFingerprint(map[string]domain.Capability{"target.linux-container": runtimeCapability, "target.visibility-first": visibility}, map[string]string{"os": capabilities.OSType})
}

func (d *Driver) Create(ctx context.Context, input ports.TargetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.create"); err != nil {
		return ports.TargetResult{}, err
	}
	plan, err := BuildContainerPlan(input, d.build)
	if err != nil {
		return ports.TargetResult{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	key := targetKey(plan.TargetID, plan.Generation)
	d.mu.Lock()
	if existingKey, found := d.idempotency[input.IdempotencyKey]; found {
		record, exists := d.targets[existingKey]
		d.mu.Unlock()
		if !exists || existingKey != key {
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "linux_target.create", "idempotency_key", "was used for a different target generation", nil)
		}
		return ports.TargetResult{Status: record.status, Created: false}, nil
	}
	if _, exists := d.targets[key]; exists {
		d.mu.Unlock()
		return ports.TargetResult{}, domain.NewError(domain.CodeAlreadyExists, "linux_target.create", "generation", "target generation already exists", nil)
	}
	d.mu.Unlock()
	if err := prepareTargetDirectories(d.build.TargetRoot, plan); err != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "linux_target.create", "target_directory", "could not create target directory", err)
	}
	runtimeID, state, err := d.createRuntime(ctx, plan)
	if err != nil {
		_ = removeTargetDirectory(d.build.TargetRoot, plan.TargetDirectory)
		return ports.TargetResult{}, err
	}
	status := ports.TargetStatus{TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetLinuxContainer, State: domain.TargetGenerationReady, Ready: true, RuntimeID: runtimeID, CgroupID: state.CgroupID, ObservedAt: d.now().UTC()}
	d.mu.Lock()
	d.targets[key] = targetRecord{input: input, plan: plan, runtimeID: runtimeID, status: status}
	d.idempotency[input.IdempotencyKey] = key
	d.mu.Unlock()
	return ports.TargetResult{Status: status, Created: true}, nil
}

func (d *Driver) createRuntime(ctx context.Context, plan ContainerPlan) (string, RuntimeState, error) {
	runtimeID, err := d.runtime.Create(ctx, plan)
	if err != nil {
		return "", RuntimeState{}, domain.NewError(domain.CodeUnavailable, "linux_target.create", "runtime.create", "container create failed", err)
	}
	if err := d.runtime.Start(ctx, runtimeID); err != nil {
		d.cleanupRuntime(runtimeID)
		return "", RuntimeState{}, domain.NewError(domain.CodeUnavailable, "linux_target.create", "runtime.start", "container start failed", err)
	}
	state, err := d.runtime.Inspect(ctx, runtimeID)
	if err != nil || !state.Running {
		d.cleanupRuntime(runtimeID)
		return "", RuntimeState{}, domain.NewError(domain.CodeFailedPrecondition, "linux_target.create", "readiness", "target container did not become running", err)
	}
	if err := validateRuntimeIdentity(state, plan); err != nil {
		d.cleanupRuntime(runtimeID)
		return "", RuntimeState{}, err
	}
	return runtimeID, state, nil
}

func (d *Driver) cleanupRuntime(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = d.runtime.Remove(ctx, id)
}

func (d *Driver) PrepareRun(ctx context.Context, input ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.prepare_run"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := input.Validate(); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	spec := input.Run.Spec()
	record, err := d.requireTarget(spec.TargetID, spec.TargetGeneration)
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if record.quarantine != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInvalidState, "linux_target.prepare_run", "target", "target generation is quarantined", nil)
	}
	if spec.LeaseID != record.plan.LeaseID {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeForbidden, "linux_target.prepare_run", "lease_id", "run is not assigned to this target", nil)
	}
	runKey := spec.ID.String()
	d.mu.Lock()
	if existingKey, found := d.idempotency[input.IdempotencyKey]; found {
		run, exists := d.runs[existingKey]
		d.mu.Unlock()
		if !exists || existingKey != runKey {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "linux_target.prepare_run", "idempotency_key", "was used for a different run", nil)
		}
		return runevidence.ClonePrepared(run.prepared), nil
	}
	if _, exists := d.runs[runKey]; exists {
		d.mu.Unlock()
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeAlreadyExists, "linux_target.prepare_run", "run", "run already prepared", nil)
	}
	d.mu.Unlock()
	if err := d.ensureMaterialized(ctx, record, input.Material); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	directory := filepath.Join(record.plan.TargetDirectory, "runs", spec.ID.String())
	if err := prepareManagedDirectory(d.build.TargetRoot, directory); err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.prepare_run", "run_directory", "could not create independent run directory", err)
	}
	authority := RunAuthority{LeaseID: spec.LeaseID, TargetID: spec.TargetID, Generation: spec.TargetGeneration, RunID: spec.ID}
	if _, err := io.ReadFull(d.random, authority.secret[:]); err != nil {
		_ = removeManagedDirectory(d.build.TargetRoot, directory)
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "linux_target.prepare_run", "credential", "could not create run credential", err)
	}
	preparedAt := runevidence.AtOrAfter(d.now(), spec.CreatedAt)
	prepared := ports.PreparedTargetRun{
		RunID: spec.ID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration,
		MaterializationDigest: spec.MaterializationDigest,
		RequiredCoverage:      append([]string(nil), input.RequiredCoverage...),
		Attachment:            ports.ObservationAttachment{TargetKind: domain.TargetLinuxContainer, RuntimeID: record.runtimeID},
		PreparedAt:            preparedAt,
	}
	d.mu.Lock()
	currentTarget, exists := d.targets[targetKey(spec.TargetID, spec.TargetGeneration)]
	if !exists || currentTarget.quarantine != nil {
		d.mu.Unlock()
		_ = removeManagedDirectory(d.build.TargetRoot, directory)
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInvalidState, "linux_target.prepare_run", "target", "target generation was quarantined while material was prepared", nil)
	}
	d.runs[runKey] = &runRecord{plan: runevidence.ClonePlan(input), authority: authority, directory: directory, prepared: prepared, transports: make(map[*targetTransport]struct{})}
	d.idempotency[input.IdempotencyKey] = runKey
	d.mu.Unlock()
	return runevidence.ClonePrepared(prepared), nil
}

func (d *Driver) StartRun(ctx context.Context, runID domain.TargetRunID) error {
	if err := ports.RequireDeadline(ctx, "linux_target.start_run"); err != nil {
		return err
	}
	run, err := d.requireRun(runID)
	if err != nil {
		return err
	}
	if run.stopped || run.quarantined {
		return domain.NewError(domain.CodeInvalidState, "linux_target.start_run", "run", "run was already stopped", nil)
	}
	if run.started {
		return nil
	}
	requirements := requiredExternalRequirements(run.plan.Collectors)
	if len(requirements) > 0 {
		if err := d.collectors.AwaitReady(ctx, runID, requirements); err != nil {
			return domain.NewError(domain.CodeFailedPrecondition, "linux_target.start_run", "collectors", "required external collectors are not ready", err)
		}
	}
	target, err := d.requireTarget(run.authority.TargetID, run.authority.Generation)
	if err != nil {
		return err
	}
	state, err := d.runtime.Inspect(ctx, target.runtimeID)
	if err != nil {
		return domain.NewError(domain.CodeUnavailable, "linux_target.start_run", "runtime", "could not inspect the prepared runtime", err)
	}
	if state.ID != target.runtimeID || !state.Running {
		return domain.NewError(domain.CodeFailedPrecondition, "linux_target.start_run", "runtime", "prepared runtime identity is not running", nil)
	}
	if err := validateRuntimeIdentity(state, target.plan); err != nil {
		return err
	}
	if err := verifyMaterialProjection(ctx, target.plan.materialRoot(), run.plan.Material); err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "linux_target.start_run", "material", "prepared material projection could not be proven exact: "+err.Error(), err)
	}
	startedAt := runevidence.AtOrAfter(d.now(), run.prepared.PreparedAt)
	startPayload, err := json.Marshal(struct {
		RuntimeID             string        `json:"runtime_id"`
		CgroupID              string        `json:"cgroup_id,omitempty"`
		MaterializationDigest domain.Digest `json:"materialization_digest"`
		MaterialEntries       int           `json:"material_entries"`
	}{
		RuntimeID: target.runtimeID, CgroupID: state.CgroupID,
		MaterializationDigest: run.plan.Run.Spec().MaterializationDigest,
		MaterialEntries:       len(run.plan.Material),
	})
	if err != nil {
		return domain.NewError(domain.CodeInternal, "linux_target.start_run", "evidence", "could not encode verified lifecycle evidence", err)
	}
	d.mu.Lock()
	current := d.runs[runID.String()]
	if current == nil {
		d.mu.Unlock()
		return domain.NewError(domain.CodeNotFound, "linux_target.start_run", "run_id", "run was removed while awaiting collector readiness", nil)
	}
	if current.stopped || current.quarantined {
		d.mu.Unlock()
		return domain.NewError(domain.CodeInvalidState, "linux_target.start_run", "run", "run stopped while awaiting collector readiness", nil)
	}
	if current.started {
		d.mu.Unlock()
		return nil
	}
	currentTarget, exists := d.targets[targetKey(current.authority.TargetID, current.authority.Generation)]
	if !exists || currentTarget.quarantine != nil || currentTarget.runtimeID != target.runtimeID {
		d.mu.Unlock()
		return domain.NewError(domain.CodeFailedPrecondition, "linux_target.start_run", "target", "target generation was removed while awaiting collector readiness", nil)
	}
	current.started = true
	current.startedAt = startedAt
	current.observations = append(current.observations, ports.TargetRunObservation{
		Kind: "target.run.started", ObservedAt: startedAt, Payload: startPayload,
	})
	duration := current.plan.MaximumDuration
	d.mu.Unlock()

	// Construct the timer without holding d.mu: an injected deterministic timer
	// is allowed to invoke an already-due callback synchronously. The stopped
	// recheck below cancels a timer that lost a race with StopRun or expiry.
	timer := d.afterFunc(duration, func() { d.expireRun(runID) })
	d.mu.Lock()
	current = d.runs[runID.String()]
	if current == nil || current.stopped {
		d.mu.Unlock()
		timer.Stop()
		return nil
	}
	current.timer = timer
	d.mu.Unlock()
	return nil
}

func (d *Driver) OpenTransport(ctx context.Context, runID domain.TargetRunID) (ports.TargetTransport, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.open_transport"); err != nil {
		return nil, err
	}
	run, err := d.requireRun(runID)
	if err != nil {
		return nil, err
	}
	if !run.started || run.stopped || run.quarantined {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "linux_target.open_transport", "run", "run is not active", nil)
	}
	target, err := d.requireTarget(run.authority.TargetID, run.authority.Generation)
	if err != nil {
		return nil, err
	}
	if target.quarantine != nil {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "linux_target.open_transport", "target", "target generation is quarantined", nil)
	}
	uid, gid, err := dockercli.ParseNumericUser(target.plan.User)
	if err != nil {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "linux_target.open_transport", "target.user", "target container identity is invalid", err)
	}
	transport := &targetTransport{
		driver: d, runtime: d.runtime, runtimeID: target.runtimeID, root: target.plan.writableRoot(), authority: run.authority,
		uid: uid, gid: gid, enforceOwnership: true,
	}
	openPayload, err := json.Marshal(struct {
		RuntimeID string `json:"runtime_id"`
	}{RuntimeID: target.runtimeID})
	if err != nil {
		return nil, domain.NewError(domain.CodeInternal, "linux_target.open_transport", "evidence", "could not encode transport lifecycle evidence", err)
	}
	d.mu.Lock()
	current := d.runs[runID.String()]
	currentTarget := d.targets[targetKey(run.authority.TargetID, run.authority.Generation)]
	if current != nil && current.started && !current.stopped && !current.quarantined && currentTarget.quarantine == nil {
		current.transports[transport] = struct{}{}
		d.appendLifecycleObservationLocked(current, ports.TargetRunObservation{
			Kind: "target.transport.opened", ObservedAt: d.now().UTC(), Payload: openPayload,
		})
		d.mu.Unlock()
		return transport, nil
	}
	d.mu.Unlock()
	return nil, domain.NewError(domain.CodeFailedPrecondition, "linux_target.open_transport", "run", "run stopped while opening transport", nil)
}

func (d *Driver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.stop_run"); err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	if !mode.IsValid() {
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeInvalidArgument, "linux_target.stop_run", "mode", "is not recognized", nil)
	}
	if run, err := d.requireRun(runID); err == nil && run.quarantined {
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeInvalidState, "linux_target.stop_run", "run", "quarantined run state is preserved for evidence", nil)
	}
	result, transports, err := d.finishRun(runID, false, d.now().UTC())
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	closeTargetTransports(transports)
	return result, nil
}

func (d *Driver) Quarantine(ctx context.Context, plan ports.TargetQuarantinePlan) (ports.TargetQuarantineEvidence, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.quarantine"); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	containment, ok := d.runtime.(RuntimeContainment)
	if !ok {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.quarantine", "runtime", "runtime cannot prove containment", nil)
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.mu.Lock()
	if prior, found := d.quarantines[plan.IdempotencyKey]; found {
		d.mu.Unlock()
		if prior.plan != plan {
			return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeConflict, "linux_target.quarantine", "idempotency_key", "was used for another quarantine", nil)
		}
		return prior.evidence, nil
	}
	d.mu.Unlock()
	record, err := d.requireTarget(plan.Target.ID, plan.Target.Generation)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if record.quarantine != nil {
		evidence := *record.quarantine
		d.mu.Lock()
		if d.quarantines == nil {
			d.quarantines = make(map[string]quarantineOutcome)
		}
		d.quarantines[plan.IdempotencyKey] = quarantineOutcome{plan: plan, evidence: evidence}
		d.mu.Unlock()
		return evidence, nil
	}
	observed, err := containment.Quarantine(ctx, record.runtimeID)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeUnavailable, "linux_target.quarantine", "runtime", "runtime containment failed", err)
	}
	evidence := ports.TargetQuarantineEvidence{
		Target: plan.Target, RuntimeID: observed.RuntimeID, ExecutionStopped: observed.ExecutionStopped,
		NetworkUnreachable: observed.NetworkUnreachable, StatePreserved: observed.StatePreserved, ObservedAt: observed.ObservedAt,
	}
	if err := evidence.Validate(plan.Target); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if observed.RuntimeID != record.runtimeID {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeIntegrityViolation, "linux_target.quarantine", "runtime_id", "runtime evidence identifies a different realization", nil)
	}
	d.mu.Lock()
	current, found := d.targets[targetKey(plan.Target.ID, plan.Target.Generation)]
	if !found || current.runtimeID != record.runtimeID {
		d.mu.Unlock()
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeConflict, "linux_target.quarantine", "target", "target generation changed during containment", nil)
	}
	current.status.State = domain.TargetGenerationQuarantined
	current.status.Ready = false
	current.status.ObservedAt = evidence.ObservedAt
	current.quarantine = &evidence
	d.targets[targetKey(plan.Target.ID, plan.Target.Generation)] = current
	if d.quarantines == nil {
		d.quarantines = make(map[string]quarantineOutcome)
	}
	d.quarantines[plan.IdempotencyKey] = quarantineOutcome{plan: plan, evidence: evidence}
	transports := make([]*targetTransport, 0)
	for _, run := range d.runs {
		if run.authority.TargetID != plan.Target.ID || run.authority.Generation != plan.Target.Generation {
			continue
		}
		run.quarantined = true
		if run.timer != nil {
			run.timer.Stop()
			run.timer = nil
		}
		for transport := range run.transports {
			transport.revoke()
			transports = append(transports, transport)
		}
		run.transports = nil
	}
	d.mu.Unlock()
	closeTargetTransports(transports)
	return evidence, nil
}

func (d *Driver) expireRun(runID domain.TargetRunID) {
	result, transports, err := d.finishRun(runID, true, d.now().UTC())
	if err != nil || result.RunID.IsZero() {
		return
	}
	closeTargetTransports(transports)
}

func (d *Driver) finishRun(runID domain.TargetRunID, limitExceeded bool, stoppedAt time.Time) (ports.TargetRunStopReceipt, []*targetTransport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.runs[runID.String()]
	if current == nil {
		return ports.TargetRunStopReceipt{}, nil, domain.NewError(domain.CodeNotFound, "linux_target.stop_run", "run_id", "run is not prepared by this driver", nil)
	}
	if current.stopped {
		return runevidence.CloneStopReceipt(*current.result), nil, nil
	}
	if _, found := d.targets[targetKey(current.authority.TargetID, current.authority.Generation)]; !found {
		return ports.TargetRunStopReceipt{}, nil, domain.NewError(domain.CodeFailedPrecondition, "linux_target.stop_run", "target", "target generation disappeared before run evidence was sealed", nil)
	}
	minimumStop := current.prepared.PreparedAt
	if current.started {
		minimumStop = current.startedAt
	}
	if count := len(current.observations); count > 0 && current.observations[count-1].ObservedAt.After(minimumStop) {
		minimumStop = current.observations[count-1].ObservedAt
	}
	stoppedAt = runevidence.AtOrAfter(stoppedAt, minimumStop)
	outcome := ports.RunCompleted
	failureKind := ports.TargetRunFailureNone
	stopKind := "target.run.stopped"
	if !current.started {
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureNeverStarted
		stopKind = "target.run.never-started"
	} else if limitExceeded {
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureDurationExceeded
		stopKind = "target.run.duration-exceeded"
	}
	stopPayload, err := json.Marshal(struct {
		FailureKind ports.TargetRunFailureKind `json:"failure_kind,omitempty"`
	}{FailureKind: failureKind})
	if err != nil {
		return ports.TargetRunStopReceipt{}, nil, domain.NewError(domain.CodeInternal, "linux_target.stop_run", "evidence", "could not encode stop observation", err)
	}
	observations := append([]ports.TargetRunObservation(nil), current.observations...)
	observations = append(observations, ports.TargetRunObservation{Kind: stopKind, ObservedAt: stoppedAt, Payload: stopPayload})
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, stoppedAt)
	if err != nil {
		return ports.TargetRunStopReceipt{}, nil, err
	}
	result := ports.TargetRunStopReceipt{
		RunID: current.plan.Run.ID(), Outcome: outcome, FailureKind: failureKind,
		StartedAt: current.startedAt, StoppedAt: stoppedAt,
		Observations: observations, TargetChanges: changes,
	}
	if err := result.Validate(); err != nil {
		return ports.TargetRunStopReceipt{}, nil, err
	}
	if current.timer != nil {
		current.timer.Stop()
		current.timer = nil
	}
	current.stopped = true
	stored := runevidence.CloneStopReceipt(result)
	current.result = &stored
	transports := make([]*targetTransport, 0, len(current.transports))
	for transport := range current.transports {
		// Revoke synchronously with the state transition. Potentially blocking
		// resource closes happen after releasing the driver lock, but no new
		// operation may cross the deadline once finishRun returns.
		transport.revoke()
		transports = append(transports, transport)
	}
	current.transports = nil
	return runevidence.CloneStopReceipt(result), transports, nil
}

func closeTargetTransports(transports []*targetTransport) {
	for _, transport := range transports {
		_ = transport.Close()
	}
}

func requiredExternalRequirements(collectors []ports.CollectorSpec) []ports.ObservationRequirement {
	result := make([]ports.ObservationRequirement, 0, len(collectors))
	for _, collector := range collectors {
		if collector.Requirement.Required && !SupportsIntrinsicCoverage(collector.Requirement.SignalFamily) {
			result = append(result, collector.Requirement)
		}
	}
	return result
}

func (d *Driver) recordLifecycleObservation(runID domain.TargetRunID, observation ports.TargetRunObservation) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	run := d.runs[runID.String()]
	if run == nil || !run.started || run.stopped || run.quarantined {
		return io.ErrClosedPipe
	}
	d.appendLifecycleObservationLocked(run, observation)
	return nil
}

func (d *Driver) appendLifecycleObservationLocked(run *runRecord, observation ports.TargetRunObservation) {
	minimum := run.startedAt
	if count := len(run.observations); count > 0 && run.observations[count-1].ObservedAt.After(minimum) {
		minimum = run.observations[count-1].ObservedAt
	}
	observation.ObservedAt = runevidence.AtOrAfter(observation.ObservedAt, minimum)
	observation.Payload = append(json.RawMessage(nil), observation.Payload...)
	run.observations = append(run.observations, observation)
}

func (d *Driver) Reset(ctx context.Context, targetID domain.TargetID, reset ports.ResetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "linux_target.reset"); err != nil {
		return ports.TargetResult{}, err
	}
	if err := reset.Validate(); err != nil {
		return ports.TargetResult{}, err
	}
	// Serialize resets so an idempotent retry cannot race the destructive
	// portion of its first attempt (and two keys cannot retire one generation
	// concurrently).
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.mu.Lock()
	if prior, found := d.resetResults[reset.IdempotencyKey]; found {
		d.mu.Unlock()
		if prior.targetID != targetID || prior.plan != reset {
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "linux_target.reset", "idempotency_key", "was used for a different reset", nil)
		}
		return prior.result, prior.err
	}
	d.mu.Unlock()
	if targetID != reset.Previous.ID {
		return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "linux_target.reset", "target_id", "does not match reset plan", nil)
	}
	if reset.Mode == ports.ResetSnapshot {
		return ports.TargetResult{}, domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.reset", "mode", "snapshot reset is not supported by this runtime", nil)
	}
	previous, err := d.requireTarget(targetID, reset.Previous.Generation)
	if err != nil {
		return ports.TargetResult{}, err
	}
	if previous.quarantine != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeInvalidState, "linux_target.reset", "target", "quarantined target generation cannot be reset", nil)
	}
	if previous.plan.LeaseID != reset.LeaseID {
		return ports.TargetResult{}, domain.NewError(domain.CodeForbidden, "linux_target.reset", "lease_id", "reset is outside the target lease", nil)
	}
	if d.hasUnstoppedRun(targetID, reset.Previous.Generation) {
		return ports.TargetResult{}, domain.NewError(domain.CodeFailedPrecondition, "linux_target.reset", "run", "all prepared runs must be stopped before reset", nil)
	}
	next := previous.plan
	next.Generation = reset.NextGeneration
	next.Name = targetContainerName(targetID, reset.NextGeneration)
	next.TargetDirectory = filepath.Join(d.build.TargetRoot, targetID.String(), "generations", strconv.FormatUint(uint64(reset.NextGeneration), 10))
	next.MountSources = []string{next.writableRoot(), next.materialRoot()}
	next.Labels = cloneStrings(previous.plan.Labels)
	next.Labels["world.target-generation"] = strconv.FormatUint(uint64(reset.NextGeneration), 10)
	if err := setPlanDigest(&next); err != nil {
		return ports.TargetResult{}, err
	}
	if err := next.Validate(d.build.TargetRoot); err != nil {
		return ports.TargetResult{}, err
	}
	if err := prepareTargetDirectories(d.build.TargetRoot, next); err != nil {
		return ports.TargetResult{}, err
	}
	runtimeID, state, err := d.createRuntime(ctx, next)
	if err != nil {
		_ = removeTargetDirectory(d.build.TargetRoot, next.TargetDirectory)
		return ports.TargetResult{}, err
	}
	cleanupNext := true
	defer func() {
		if cleanupNext {
			d.cleanupRuntime(runtimeID)
			_ = removeTargetDirectory(d.build.TargetRoot, next.TargetDirectory)
		}
	}()
	if err := d.runtime.Stop(ctx, previous.runtimeID, ports.StopForce); err != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "linux_target.reset", "runtime.stop", "could not stop previous target", err)
	}
	if err := d.runtime.Remove(ctx, previous.runtimeID); err != nil {
		// The previous container still exists after a failed remove. Restore its
		// ready state when possible before returning, while the proven next
		// generation is discarded by the deferred cleanup.
		_ = d.runtime.Start(ctx, previous.runtimeID)
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "linux_target.reset", "runtime.remove", "could not remove previous target", err)
	}
	if err := removeTargetDirectory(d.build.TargetRoot, previous.plan.TargetDirectory); err != nil {
		// The old runtime is already gone, so retain the ready replacement and
		// commit its bookkeeping. Returning the replacement with an error lets
		// callers reconcile without retrying an already-destructive transition.
		status := ports.TargetStatus{TargetID: targetID, Generation: reset.NextGeneration, Kind: domain.TargetLinuxContainer, State: domain.TargetGenerationReady, Ready: true, RuntimeID: runtimeID, CgroupID: state.CgroupID, ObservedAt: d.now().UTC()}
		result := ports.TargetResult{Status: status, Created: true}
		outcomeErr := domain.NewError(domain.CodeUnavailable, "linux_target.reset", "cleanup", "replacement is ready but the retired target directory could not be removed", err)
		d.commitReset(previous, next, runtimeID, status, targetID, reset, result, outcomeErr)
		cleanupNext = false
		return result, outcomeErr
	}
	status := ports.TargetStatus{TargetID: targetID, Generation: reset.NextGeneration, Kind: domain.TargetLinuxContainer, State: domain.TargetGenerationReady, Ready: true, RuntimeID: runtimeID, CgroupID: state.CgroupID, ObservedAt: d.now().UTC()}
	result := ports.TargetResult{Status: status, Created: true}
	d.commitReset(previous, next, runtimeID, status, targetID, reset, result, nil)
	cleanupNext = false
	return result, nil
}

func (d *Driver) commitReset(previous targetRecord, next ContainerPlan, runtimeID string, status ports.TargetStatus, targetID domain.TargetID, reset ports.ResetPlan, result ports.TargetResult, outcomeErr error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removeRunsLocked(previous.plan.TargetID, previous.plan.Generation)
	for key, outcome := range d.quarantines {
		if outcome.plan.Target.ID == previous.plan.TargetID && outcome.plan.Target.Generation == previous.plan.Generation {
			delete(d.quarantines, key)
		}
	}
	delete(d.materialized, targetKey(previous.plan.TargetID, previous.plan.Generation))
	delete(d.targets, targetKey(previous.plan.TargetID, previous.plan.Generation))
	d.targets[targetKey(previous.plan.TargetID, next.Generation)] = targetRecord{input: previous.input, plan: next, runtimeID: runtimeID, status: status}
	d.resetResults[reset.IdempotencyKey] = resetOutcome{targetID: targetID, plan: reset, result: result, err: outcomeErr}
}

func (d *Driver) Destroy(ctx context.Context, ref ports.TargetRef) error {
	if err := ports.RequireDeadline(ctx, "linux_target.destroy"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.mu.Lock()
	record, found := d.targets[targetKey(ref.ID, ref.Generation)]
	d.mu.Unlock()
	if d.hasUnstoppedRun(ref.ID, ref.Generation) {
		return domain.NewError(domain.CodeFailedPrecondition, "linux_target.destroy", "run", "all prepared runs must be stopped before destroy", nil)
	}
	runtimeID, absent, err := d.resolveTargetDestroy(ctx, ref, record, found)
	if err != nil {
		return err
	}
	if !absent {
		if err := d.runtime.Remove(ctx, runtimeID); err != nil {
			return domain.NewError(domain.CodeUnavailable, "linux_target.destroy", "runtime", "could not remove target", err)
		}
		if inventory, supported := d.runtime.(RuntimeInventory); supported {
			states, err := inventory.ListContainers(ctx)
			if err != nil {
				return domain.NewError(domain.CodeUnavailable, "linux_target.destroy", "inventory", "could not prove target absence after removal", err)
			}
			if candidates := targetRefCandidates(states, ref); len(candidates) != 0 {
				return domain.NewError(domain.CodeFailedPrecondition, "linux_target.destroy", "absence", "runtime removal did not produce authoritative absence", nil)
			}
		} else if !found {
			return domain.NewError(domain.CodeCapabilityUnavailable, "linux_target.destroy", "inventory", "cannot prove target absence after restart", nil)
		}
	}
	directory := targetDirectory(d.build.TargetRoot, ref)
	if found {
		directory = record.plan.TargetDirectory
	}
	if err := removeTargetDirectoryIfPresent(d.build.TargetRoot, directory); err != nil {
		return domain.NewError(domain.CodeUnavailable, "linux_target.destroy", "cleanup", "could not remove target directory", err)
	}
	d.mu.Lock()
	delete(d.targets, targetKey(ref.ID, ref.Generation))
	delete(d.materialized, targetKey(ref.ID, ref.Generation))
	if found {
		delete(d.idempotency, record.input.IdempotencyKey)
	}
	d.removeRunsLocked(ref.ID, ref.Generation)
	for key, outcome := range d.quarantines {
		if outcome.plan.Target == ref {
			delete(d.quarantines, key)
		}
	}
	d.mu.Unlock()
	return nil
}

func (d *Driver) requireTarget(id domain.TargetID, generation domain.TargetGeneration) (targetRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, found := d.targets[targetKey(id, generation)]
	if !found {
		return targetRecord{}, domain.NewError(domain.CodeNotFound, "linux_target.target", "generation", "target generation is not owned by this driver", nil)
	}
	return record, nil
}

func (d *Driver) requireRun(id domain.TargetRunID) (*runRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	run, found := d.runs[id.String()]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "linux_target.run", "run_id", "run is not prepared by this driver", nil)
	}
	copy := *run
	return &copy, nil
}

func (d *Driver) hasUnstoppedRun(id domain.TargetID, generation domain.TargetGeneration) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, run := range d.runs {
		if run.authority.TargetID == id && run.authority.Generation == generation && !run.stopped {
			return true
		}
	}
	return false
}

func (d *Driver) removeRunsLocked(id domain.TargetID, generation domain.TargetGeneration) {
	for key, run := range d.runs {
		if run.authority.TargetID != id || run.authority.Generation != generation {
			continue
		}
		if run.timer != nil {
			run.timer.Stop()
		}
		delete(d.idempotency, run.plan.IdempotencyKey)
		delete(d.runs, key)
	}
}

func (d *Driver) ensureMaterialized(ctx context.Context, target targetRecord, material []ports.TargetMaterialPlan) error {
	digest, err := ports.TargetMaterializationDigest(material)
	if err != nil {
		return err
	}
	key := targetKey(target.plan.TargetID, target.plan.Generation)
	d.mu.Lock()
	if state, found := d.materialized[key]; found {
		if state.digest != digest {
			d.mu.Unlock()
			return domain.NewError(domain.CodeConflict, "linux_target.materialize", "materialization_digest", "target generation already has a different immutable projection", nil)
		}
		done := state.done
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return state.err
		}
	}
	state := &materializationState{digest: digest, done: make(chan struct{})}
	d.materialized[key] = state
	d.mu.Unlock()

	err = materializeTarget(ctx, d.build.TargetRoot, target.plan, material)
	d.mu.Lock()
	state.err = err
	close(state.done)
	if err != nil && d.materialized[key] == state {
		delete(d.materialized, key)
	}
	d.mu.Unlock()
	return err
}

func materializeTarget(ctx context.Context, targetRoot string, plan ContainerPlan, material []ports.TargetMaterialPlan) error {
	root := plan.materialRoot()
	if err := clearManagedDirectory(targetRoot, root); err != nil {
		return domain.NewError(domain.CodeUnavailable, "linux_target.materialize", "material_root", "could not prepare an exact material projection", err)
	}
	entries := append([]ports.TargetMaterialPlan(nil), material...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].LogicalPath < entries[j].LogicalPath })
	for index, entry := range entries {
		if err := writeTargetMaterial(ctx, root, entry); err != nil {
			_ = clearManagedDirectory(targetRoot, root)
			return domain.NewError(domain.CodeIntegrityViolation, "linux_target.materialize", fmt.Sprintf("material[%d]", index), "content could not be verified and published", err)
		}
	}
	if err := applyTargetTreeOwnership(targetRoot, root, plan.User); err != nil {
		_ = clearManagedDirectory(targetRoot, root)
		return domain.NewError(domain.CodeUnavailable, "linux_target.materialize", "material_owner", "material could not be handed to the target identity", err)
	}
	if err := sealMaterialDirectories(targetRoot, root); err != nil {
		_ = clearManagedDirectory(targetRoot, root)
		return domain.NewError(domain.CodeUnavailable, "linux_target.materialize", "material_root", "could not seal material directories", err)
	}
	return nil
}

// verifyMaterialProjection proves the complete material tree from the same
// descriptor used to hash each regular file. It rejects missing, additional,
// redirected, or permission-drifted entries before a run can start.
func verifyMaterialProjection(ctx context.Context, root string, material []ports.TargetMaterialPlan) error {
	expectedFiles := make(map[string]ports.TargetMaterialPlan, len(material))
	expectedDirectories := map[string]struct{}{".": {}}
	for _, entry := range material {
		normalized, err := safepath.Normalize(entry.LogicalPath)
		if err != nil {
			return err
		}
		expectedFiles[normalized] = entry
		parts := strings.Split(normalized, "/")
		for index := 1; index < len(parts); index++ {
			expectedDirectories[strings.Join(parts[:index], "/")] = struct{}{}
		}
	}
	seenFiles := make(map[string]struct{}, len(expectedFiles))
	seenDirectories := make(map[string]struct{}, len(expectedDirectories))
	err := filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		logicalPath := filepath.ToSlash(relative)
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("material projection contains redirected entry %q", logicalPath)
		}
		if entry.IsDir() {
			if _, expected := expectedDirectories[logicalPath]; !expected {
				return fmt.Errorf("material projection contains unexpected directory %q", logicalPath)
			}
			seenDirectories[logicalPath] = struct{}{}
			return nil
		}
		planned, expected := expectedFiles[logicalPath]
		if !expected {
			return fmt.Errorf("material projection contains unexpected file %q", logicalPath)
		}
		if _, duplicate := seenFiles[logicalPath]; duplicate {
			return fmt.Errorf("material projection contains duplicate file %q", logicalPath)
		}
		if err := verifyMaterialFile(ctx, root, logicalPath, planned); err != nil {
			return fmt.Errorf("verify %q: %w", logicalPath, err)
		}
		seenFiles[logicalPath] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	if len(seenFiles) != len(expectedFiles) || len(seenDirectories) != len(expectedDirectories) {
		return fmt.Errorf("material projection is missing planned files or directories")
	}
	return nil
}

func verifyMaterialFile(ctx context.Context, root, logicalPath string, planned ports.TargetMaterialPlan) error {
	file, err := safepath.OpenRegular(root, logicalPath)
	if err != nil {
		return err
	}
	defer file.Close()
	spec := planned.Artifact.Spec()
	expectedMode := fs.FileMode(planned.Mode).Perm()
	before := file.Info()
	if before.Size() != spec.Size || !materialModeMatches(before.Mode(), expectedMode) {
		return fmt.Errorf("size or mode differs from the authorized projection")
	}
	hash := sha256.New()
	written, err := safepath.CopyBounded(hash, &contextReader{ctx: ctx, reader: file}, spec.Size)
	if err != nil {
		return err
	}
	actual, err := domain.ParseDigest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
	if err != nil {
		return err
	}
	after, err := file.Stat()
	if err != nil {
		return err
	}
	if written != spec.Size || actual != spec.Digest || after.Size() != spec.Size ||
		!materialModeMatches(after.Mode(), expectedMode) || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		return fmt.Errorf("bytes or metadata differ from the authorized projection")
	}
	return nil
}

func writeTargetMaterial(ctx context.Context, root string, material ports.TargetMaterialPlan) error {
	spec := material.Artifact.Spec()
	return safepath.WriteRegularAtomic(root, material.LogicalPath, fs.FileMode(material.Mode), func(destination io.Writer) error {
		reader, err := material.Content.Open(ctx)
		if err != nil {
			return err
		}
		hash := sha256.New()
		written, copyErr := safepath.CopyBounded(io.MultiWriter(destination, hash), &contextReader{ctx: ctx, reader: reader}, spec.Size)
		closeErr := reader.Close()
		if copyErr != nil || closeErr != nil {
			return errors.Join(copyErr, closeErr)
		}
		actual, parseErr := domain.ParseDigest("sha256:" + hex.EncodeToString(hash.Sum(nil)))
		if parseErr != nil {
			return parseErr
		}
		if written != spec.Size || actual != spec.Digest {
			return domain.NewError(domain.CodeIntegrityViolation, "linux_target.materialize", "content", "streamed bytes do not match the declared artifact", nil)
		}
		return nil
	})
}

func prepareTargetDirectories(targetRoot string, plan ContainerPlan) error {
	for _, directory := range []string{plan.TargetDirectory, plan.materialRoot(), plan.writableRoot()} {
		if err := prepareManagedDirectory(targetRoot, directory); err != nil {
			return err
		}
	}
	return applyTargetDirectoryOwnership(targetRoot, plan)
}

func applyTargetDirectoryOwnership(targetRoot string, plan ContainerPlan) error {
	if plan.User == "" {
		return nil
	}
	uid, gid, err := dockercli.ParseNumericUser(plan.User)
	if err != nil {
		return err
	}
	for _, directory := range []string{plan.TargetDirectory, plan.materialRoot(), plan.writableRoot()} {
		if err := setManagedDirectoryOwner(targetRoot, directory, uid, gid); err != nil {
			return err
		}
	}
	return nil
}

func applyTargetTreeOwnership(targetRoot, directory, user string) error {
	if user == "" {
		return nil
	}
	uid, gid, err := dockercli.ParseNumericUser(user)
	if err != nil {
		return err
	}
	return setManagedTreeOwner(targetRoot, directory, uid, gid)
}

func sealMaterialDirectories(root, directory string) error {
	return sealManagedDirectory(root, directory)
}

func removeTargetDirectory(root, directory string) error {
	return removeManagedDirectory(root, directory)
}

func cloneStrings(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}

func targetKey(id domain.TargetID, generation domain.TargetGeneration) string {
	return id.String() + "/" + strconv.FormatUint(uint64(generation), 10)
}

var _ ports.TargetDriver = (*Driver)(nil)
