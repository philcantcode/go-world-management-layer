package cuttlefish

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/deviceproxy"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/runevidence"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type Allocator interface {
	Reserve(context.Context, domain.TargetID, domain.TargetGeneration) (Allocation, error)
	Release(context.Context, Allocation) error
}

type allocatorCloser interface{ Close() error }

type Gateway interface {
	Open(context.Context, deviceproxy.Scope, Allocation) (ports.ScopedADBEndpoint, error)
}

type CollectorReadiness interface {
	AwaitReady(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error
}

type CollectorReadinessFunc func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error

func (f CollectorReadinessFunc) AwaitReady(ctx context.Context, id domain.TargetRunID, requirements []ports.ObservationRequirement) error {
	return f(ctx, id, append([]ports.ObservationRequirement(nil), requirements...))
}

type Config struct {
	Build      BuildConfig
	Backend    Backend
	Allocator  Allocator
	Gateway    Gateway
	Files      ScopedFileGateway
	Collectors CollectorReadiness
	Random     io.Reader
	Now        func() time.Time
}

type Driver struct {
	build              BuildConfig
	backend            Backend
	allocator          Allocator
	gateway            Gateway
	files              ScopedFileGateway
	collectors         CollectorReadiness
	random             io.Reader
	now                func() time.Time
	commitResetOutcome func(VirtualDevicePlan, resetTransitionManifest, ports.TargetResult, error, time.Time) (resetOutcome, error)
	resetCheckpoint    func(resetCheckpoint) error
	prepareCheckpoint  func(prepareCheckpoint) error
	createCheckpoint   func(createCheckpoint) error

	mu           sync.Mutex
	lifecycleMu  sync.Mutex
	targets      map[string]deviceRecord
	cleanupOnly  map[string]cleanupDeviceRecord
	runs         map[string]*runRecord
	idempotency  map[string]string
	resetResults map[string]resetOutcome
	quarantines  map[string]quarantineOutcome
}

type resetCheckpoint string

const (
	resetCheckpointTransitionCommitted resetCheckpoint = "transition_committed"
	resetCheckpointReplacementManifest resetCheckpoint = "replacement_manifest_committed"
	resetCheckpointPreviousRetired     resetCheckpoint = "previous_retired"
	resetCheckpointOutcomeCommitted    resetCheckpoint = "outcome_committed"
)

type createCheckpoint string

const (
	createCheckpointAllocationReserved createCheckpoint = "allocation_reserved"
	createCheckpointIntentCommitted    createCheckpoint = "intent_committed"
	createCheckpointRuntimeCreated     createCheckpoint = "runtime_created"
	createCheckpointRuntimeReady       createCheckpoint = "runtime_ready"
	createCheckpointManifestsCommitted createCheckpoint = "manifests_committed"
)

type prepareCheckpoint string

const (
	prepareCheckpointIntentCommitted   prepareCheckpoint = "intent_committed"
	prepareCheckpointMaterialized      prepareCheckpoint = "materialized"
	prepareCheckpointGenerationClaimed prepareCheckpoint = "generation_claimed"
)

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

type deviceRecord struct {
	input         ports.TargetPlan
	planSignature domain.Digest
	plan          VirtualDevicePlan
	instance      Instance
	status        ports.TargetStatus
}

type cleanupDeviceRecord struct {
	record         deviceRecord
	runtimePresent bool
}

type runRecord struct {
	plan                 ports.TargetRunPlan
	planSignature        domain.Digest
	scope                deviceproxy.Scope
	allocation           Allocation
	sourceInstance       string
	directory            string
	prepared             ports.PreparedTargetRun
	starting             bool
	startDone            chan struct{}
	startCancel          context.CancelFunc
	started              bool
	startedAt            time.Time
	finishing            bool
	controlPlaneLost     bool
	interruptedExecution bool
	durationExceeded     bool
	stopped              bool
	quarantined          bool
	receipt              *ports.TargetRunStopReceipt
	observations         []ports.TargetRunObservation
	deadlineCancel       context.CancelFunc
	transports           map[*androidTransport]struct{}
	scopedWrites         map[string]scopedWriteEvidence
	adbAuthorityIssued   bool
	opaqueMutationReason string
	containment          *BackendQuarantineState
}

type scopedWriteEvidence struct {
	file DeviceFile
	mode uint32
}

func New(config Config) (*Driver, error) {
	if config.Backend == nil || config.Allocator == nil || config.Gateway == nil || config.Files == nil || config.Collectors == nil {
		return nil, fmt.Errorf("backend, allocator, scoped ADB endpoint and file gateways, and collector gate are required")
	}
	if config.Build.TargetRoot == "" || config.Build.SystemImageRoot == "" || config.Build.BackendVersion == "" || config.Build.RuntimeVersion == "" || config.Build.DeviceConfigDigest.IsZero() {
		return nil, fmt.Errorf("complete virtual-device build configuration is required")
	}
	if _, err := ports.ParseADBServerEndpoint(config.Build.ADBServerEndpoint); err != nil {
		return nil, fmt.Errorf("virtual-device observation ADB server: %w", err)
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Driver{build: config.Build, backend: config.Backend, allocator: config.Allocator, gateway: config.Gateway, files: config.Files, collectors: config.Collectors, random: config.Random, now: config.Now, commitResetOutcome: commitExpectedResetOutcome, targets: make(map[string]deviceRecord), cleanupOnly: make(map[string]cleanupDeviceRecord), runs: make(map[string]*runRecord), idempotency: make(map[string]string), resetResults: make(map[string]resetOutcome), quarantines: make(map[string]quarantineOutcome)}, nil
}

func NewDriver(config Config) (*Driver, error) { return New(config) }

// Close releases durable allocator ownership after all run capabilities have
// been stopped. Live target generations and their manifests are deliberately
// preserved for restart reconciliation.
func (d *Driver) Close() error {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.mu.Lock()
	for _, run := range d.runs {
		if !run.stopped && !run.quarantined {
			d.mu.Unlock()
			return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.close", "runs", "all prepared Android runs must be stopped or quarantined before allocator ownership is released", nil)
		}
	}
	d.mu.Unlock()
	if closer, ok := d.allocator.(allocatorCloser); ok {
		return closer.Close()
	}
	return nil
}

func (d *Driver) Probe(ctx context.Context, template ports.TargetTemplate) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := template.Validate(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if template.Kind != domain.TargetAndroidVirtualDevice {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.probe", "template.kind", "template is not an Android virtual device", nil)
	}
	probe, err := d.backend.Probe(ctx, template)
	if err != nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.probe", "backend", "virtual-device backend probe failed", err)
	}
	if err := d.validateObservedBackendVersions(probe); err != nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.probe", "backend_identity", "observed backend/runtime identity does not match driver configuration", err)
	}
	virtualConstraints := map[string]string{"backend_version": probe.BackendVersion, "runtime_version": probe.RuntimeVersion}
	if probe.KVMKnown {
		virtualConstraints["kvm"] = strconv.FormatBool(probe.KVM)
	}
	virtualConstraints["managed"] = strconv.FormatBool(probe.Managed)
	if probe.HardwareAccelerationKnown {
		virtualConstraints["hardware_acceleration"] = strconv.FormatBool(probe.HardwareAcceleration)
	}
	if probe.HeadlessKnown {
		virtualConstraints["headless"] = strconv.FormatBool(probe.Headless)
	}
	if probe.RootedKnown {
		virtualConstraints["rooted"] = strconv.FormatBool(probe.Rooted)
	}
	if probe.DebuggableKnown {
		virtualConstraints["debuggable"] = strconv.FormatBool(probe.Debuggable)
	}
	virtual, _ := domain.NewCapability(domain.CapabilitySupported, virtualConstraints, nil)
	resetStatus := domain.CapabilityUnsupported
	resetConstraints := map[string]string{"snapshot": "false"}
	resetEvidence := map[string]string{"reason": "backend cannot create an independently reachable replacement generation"}
	if len(probe.ResetModes) > 0 {
		resetModes, err := canonicalResetModes(probe.ResetModes)
		if err != nil {
			return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.probe", "reset_modes", "backend returned invalid reset capabilities", err)
		}
		resetStatus = domain.CapabilitySupported
		resetConstraints["modes"] = resetModes
		resetEvidence = nil
	}
	reset, _ := domain.NewCapability(resetStatus, resetConstraints, resetEvidence)
	adb, _ := domain.NewCapability(domain.CapabilitySupported, map[string]string{"serial_scope": "one", "arbitrary_device_services": "true", "guest_state_mutable": "true", "host_services": "synthetic-or-denied"}, map[string]string{"guest_mutation": "assigned-device ADB services can still modify Android guest state despite material/writable path separation"})
	backendKind := probe.BackendKind
	if backendKind == "" {
		backendKind = "cuttlefish"
	}
	evidence := normalizedProbeEvidence(probe.Evidence, d.build.DeviceConfigDigest)
	evidence["driver"] = backendKind
	return domain.NewCapabilityFingerprint(map[string]domain.Capability{"target.android-virtual": virtual, "target.android-reset": reset, "target.scoped-adb": adb}, evidence)
}

func normalizedProbeEvidence(observed map[string]string, deviceConfigDigest domain.Digest) map[string]string {
	evidence := cloneLabels(observed)
	for _, imageSpecific := range []string{"system_image_package", "system_image_directory", "system_image_digest"} {
		delete(evidence, imageSpecific)
	}
	evidence["device_config_digest"] = deviceConfigDigest.String()
	return evidence
}

func (d *Driver) validateObservedBackendVersions(capabilities BackendCapabilities) error {
	if strings.TrimSpace(capabilities.BackendVersion) == "" || strings.TrimSpace(capabilities.RuntimeVersion) == "" {
		return fmt.Errorf("backend and runtime versions must be observed and non-blank")
	}
	if capabilities.BackendVersion != d.build.BackendVersion || capabilities.RuntimeVersion != d.build.RuntimeVersion {
		return fmt.Errorf("observed backend/runtime %q/%q differs from configured reset identity %q/%q", capabilities.BackendVersion, capabilities.RuntimeVersion, d.build.BackendVersion, d.build.RuntimeVersion)
	}
	return nil
}

func (d *Driver) Create(ctx context.Context, input ports.TargetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.create"); err != nil {
		return ports.TargetResult{}, err
	}
	if err := input.Validate(); err != nil {
		return ports.TargetResult{}, err
	}
	requestedSignature, err := targetPlanSignature(input)
	if err != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeInternal, "cuttlefish.create", "plan_signature", "could not bind the exact target request: "+err.Error(), err)
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	spec := input.Generation.Spec()
	key := deviceKey(spec.TargetID, spec.Generation)
	d.mu.Lock()
	if prior, found := d.idempotency[input.IdempotencyKey]; found {
		record, exists := d.targets[prior]
		d.mu.Unlock()
		if !exists || prior != key {
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.create", "idempotency_key", "was used for another device generation", nil)
		}
		if record.planSignature.IsZero() || record.planSignature != requestedSignature {
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.create", "idempotency_key", "was reused with a different target plan", nil)
		}
		return ports.TargetResult{Status: record.status, Created: false}, nil
	}
	if _, exists := d.targets[key]; exists {
		d.mu.Unlock()
		return ports.TargetResult{}, domain.NewError(domain.CodeAlreadyExists, "cuttlefish.create", "generation", "target generation already exists", nil)
	}
	if _, exists := d.cleanupOnly[key]; exists {
		d.mu.Unlock()
		return ports.TargetResult{}, domain.NewError(domain.CodeAlreadyExists, "cuttlefish.create", "generation", "target generation is retained for cleanup", nil)
	}
	d.mu.Unlock()
	allocation, err := d.allocator.Reserve(ctx, spec.TargetID, spec.Generation)
	if err != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeResourceExhausted, "cuttlefish.create", "allocation", "could not reserve collision-free device endpoints", err)
	}
	keepAllocation := false
	defer func() {
		if !keepAllocation {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = d.allocator.Release(cleanup, allocation)
		}
	}()
	plan, err := BuildVirtualDevicePlan(input, d.build, allocation)
	if err != nil {
		return ports.TargetResult{}, err
	}
	if err := d.passCreateCheckpoint(createCheckpointAllocationReserved); err != nil {
		keepAllocation = true
		return ports.TargetResult{}, err
	}
	_, intentFound, err := loadExpectedCreateIntent(input, plan)
	if err != nil {
		keepAllocation = true
		return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.create", "intent", "durable create intent belongs to another exact target request", err)
	}
	if _, err := commitExpectedCreateIntent(input, plan, d.now()); err != nil {
		keepAllocation = true
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "intent", "exact create intent could not be committed before launching the runtime", err)
	}
	if err := d.passCreateCheckpoint(createCheckpointIntentCommitted); err != nil {
		keepAllocation = true
		return ports.TargetResult{}, err
	}
	var instance Instance
	var state ReadinessState
	if intentFound {
		inventoryBackend, supported := d.backend.(BackendInventory)
		if !supported {
			keepAllocation = true
			return ports.TargetResult{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.create", "inventory", "backend cannot resume a durable Android create intent", nil)
		}
		inventory, inventoryErr := loadExactAndroidRuntimeInventory(ctx, inventoryBackend)
		if inventoryErr != nil {
			keepAllocation = true
			return ports.TargetResult{}, inventoryErr
		}
		instance, state, _, err = d.ensureManifestedAndroidInstance(ctx, plan, inventory)
		if err != nil {
			keepAllocation = true
			return ports.TargetResult{}, classifiedDriverFailure("cuttlefish.create", "recovery", "durable Android create intent could not be resumed", err)
		}
	} else {
		instance, state, err = d.createInstance(ctx, plan)
		if err != nil {
			if mustRetainAllocation(err) {
				keepAllocation = true
			} else if removeErr := os.RemoveAll(plan.StateDirectory); removeErr != nil {
				err = errors.Join(err, removeErr)
			}
			return ports.TargetResult{}, err
		}
		if err := d.passCreateCheckpoint(createCheckpointRuntimeReady); err != nil {
			keepAllocation = true
			return ports.TargetResult{}, err
		}
		if err := persistTargetRuntimeManifests(plan, instance, state, d.now()); err != nil {
			cause := domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "manifest", "exact target/runtime plan could not be persisted", err)
			cleanupErr := d.cleanupFailedInstance(instance, cause)
			if mustRetainAllocation(cleanupErr) {
				keepAllocation = true
			} else if removeErr := os.RemoveAll(plan.StateDirectory); removeErr != nil {
				cleanupErr = errors.Join(cleanupErr, removeErr)
			}
			return ports.TargetResult{}, cleanupErr
		}
	}
	if err := d.passCreateCheckpoint(createCheckpointManifestsCommitted); err != nil {
		keepAllocation = true
		return ports.TargetResult{}, err
	}
	status := ports.TargetStatus{TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetAndroidVirtualDevice, State: domain.TargetGenerationReady, Ready: state.Ready(), RuntimeID: instance.RuntimeID, DeviceSerial: instance.Allocation.Serial, ObservedAt: d.now().UTC()}
	d.mu.Lock()
	d.targets[key] = deviceRecord{input: input, planSignature: requestedSignature, plan: plan, instance: instance, status: status}
	d.idempotency[input.IdempotencyKey] = key
	d.mu.Unlock()
	keepAllocation = true
	return ports.TargetResult{Status: status, Created: true}, nil
}

func (d *Driver) passCreateCheckpoint(checkpoint createCheckpoint) error {
	if d.createCheckpoint == nil {
		return nil
	}
	if err := d.createCheckpoint(checkpoint); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "checkpoint", "target creation was interrupted after "+string(checkpoint), err)
	}
	return nil
}

func (d *Driver) createInstance(ctx context.Context, plan VirtualDevicePlan) (Instance, ReadinessState, error) {
	instance, err := d.backend.Create(ctx, plan)
	if err != nil {
		return Instance{}, ReadinessState{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "backend.create", "device creation failed", err)
	}
	if err := d.passCreateCheckpoint(createCheckpointRuntimeCreated); err != nil {
		return Instance{}, ReadinessState{}, &retainedPhysicalStateError{cause: err}
	}
	if err := d.backend.Start(ctx, instance); err != nil {
		cause := domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "backend.start", "device start failed", err)
		return Instance{}, ReadinessState{}, d.cleanupFailedInstance(instance, cause)
	}
	state, err := d.backend.WaitReady(ctx, instance)
	if err != nil || !state.Ready() {
		if err == nil {
			err = incompleteAndroidReadinessError(state, instance.Allocation.InstanceName)
		}
		cause := domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.create", "readiness", "multi-signal Android readiness was not reached: "+err.Error(), err)
		return Instance{}, ReadinessState{}, d.cleanupFailedInstance(instance, cause)
	}
	if !instanceMatchesPlan(instance, plan) {
		cause := domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.create", "instance", "backend returned a different physical runtime plan", nil)
		return Instance{}, ReadinessState{}, d.cleanupFailedInstance(instance, cause)
	}
	return instance, state, nil
}

func (d *Driver) cleanupInstance(instance Instance) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return d.backend.Destroy(ctx, instance)
}

type retainedPhysicalStateError struct{ cause error }

func (e *retainedPhysicalStateError) Error() string { return e.cause.Error() }
func (e *retainedPhysicalStateError) Unwrap() error { return e.cause }

func (d *Driver) cleanupFailedInstance(instance Instance, cause error) error {
	if cleanupErr := d.cleanupInstance(instance); cleanupErr != nil {
		return &retainedPhysicalStateError{cause: errors.Join(cause, cleanupErr)}
	}
	return cause
}

func mustRetainAllocation(err error) bool {
	var retained *retainedPhysicalStateError
	return errors.As(err, &retained)
}

func (d *Driver) restoreInstance(instance Instance) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	state, inspectErr := d.backend.Inspect(ctx, instance)
	if inspectErr == nil && state.Ready() {
		return nil
	}
	if err := d.backend.Start(ctx, instance); err != nil {
		return errors.Join(inspectErr, err)
	}
	state, err := d.backend.WaitReady(ctx, instance)
	if err != nil {
		return errors.Join(inspectErr, err)
	}
	if !state.Ready() {
		return errors.Join(inspectErr, fmt.Errorf("restored instance did not become ready"))
	}
	return nil
}

func (d *Driver) PrepareRun(ctx context.Context, input ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	return d.prepareRun(ctx, input)
}

func (d *Driver) prepareRun(ctx context.Context, input ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.prepare_run"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := input.Validate(); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	requestedSignature, err := runPlanSignature(input)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInternal, "cuttlefish.prepare_run", "plan_signature", "could not bind the exact run request", err)
	}
	spec := input.Run.Spec()
	device, err := d.requireDevice(spec.TargetID, spec.TargetGeneration)
	if err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if device.status.State == domain.TargetGenerationQuarantined {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInvalidState, "cuttlefish.prepare_run", "target", "target generation is quarantined", nil)
	}
	if spec.LeaseID != device.plan.LeaseID {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeForbidden, "cuttlefish.prepare_run", "lease_id", "run is outside this device lease", nil)
	}
	d.mu.Lock()
	if prior, found := d.idempotency[input.IdempotencyKey]; found {
		run, exists := d.runs[prior]
		d.mu.Unlock()
		if !exists || prior != spec.ID.String() {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.prepare_run", "idempotency_key", "was used for another run", nil)
		}
		if run.planSignature.IsZero() || run.planSignature != requestedSignature {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.prepare_run", "idempotency_key", "was reused with a different run plan", nil)
		}
		return runevidence.ClonePrepared(run.prepared), nil
	}
	for _, run := range d.runs {
		if run.scope.TargetID == spec.TargetID && run.scope.Generation == spec.TargetGeneration {
			d.mu.Unlock()
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.prepare_run", "target_generation", "mutable Android authority is single-use per target generation; reset is required", nil)
		}
	}
	d.mu.Unlock()
	persisted, intentFound, generationClaimed, err := loadRunPreparation(device.plan, input)
	if err != nil {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeConflict, "cuttlefish.prepare_run", "durable_intent", "target generation is bound to another exact run preparation", err)
	}
	if !intentFound && !generationClaimed {
		if err := requireGenerationAvailableForRun(device.plan); err != nil {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.prepare_run", "target_generation", "mutable Android authority is single-use per target generation", err)
		}
	}
	if !intentFound {
		persisted, err = d.commitRunPreparationIntent(input, device)
		if err != nil {
			return ports.PreparedTargetRun{}, err
		}
		if err := d.passPrepareCheckpoint(prepareCheckpointIntentCommitted); err != nil {
			return ports.PreparedTargetRun{}, err
		}
	}
	if !generationClaimed || !intentFound {
		if err := d.materializePreparedRun(ctx, input, persisted, device.plan.StateDirectory); err != nil {
			return ports.PreparedTargetRun{}, err
		}
		if err := d.passPrepareCheckpoint(prepareCheckpointMaterialized); err != nil {
			return ports.PreparedTargetRun{}, err
		}
		if err := persistRunGenerationUse(device.plan, input, d.now()); err != nil {
			return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", "generation_use", "durable single-use Android generation gate could not be persisted: "+err.Error(), err)
		}
		if err := d.passPrepareCheckpoint(prepareCheckpointGenerationClaimed); err != nil {
			return ports.PreparedTargetRun{}, err
		}
	}
	return d.commitPreparedRun(input, requestedSignature, device, persisted)
}

func loadRunPreparation(plan VirtualDevicePlan, input ports.TargetRunPlan) (runPlanManifest, bool, bool, error) {
	use, generationClaimed, err := loadGenerationUseManifest(plan)
	if err != nil {
		return runPlanManifest{}, false, false, err
	}
	runDigest, err := persistedRunPlanDigest(persistedRunPlanFrom(input))
	if err != nil {
		return runPlanManifest{}, false, false, err
	}
	if generationClaimed && (use.Reason != generationUseRunPrepared || use.RunID != input.Run.ID().String() || use.RunPlanDigest != runDigest.String()) {
		return runPlanManifest{}, false, true, fmt.Errorf("generation-use gate identifies another run plan")
	}
	directory := filepath.Join(plan.StateDirectory, "runs", input.Run.ID().String())
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return runPlanManifest{}, false, generationClaimed, nil
	}
	if err != nil {
		return runPlanManifest{}, false, generationClaimed, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return runPlanManifest{}, false, generationClaimed, fmt.Errorf("run preparation path is not an exact regular directory")
	}
	manifestPath := filepath.Join(directory, runPlanManifestFilename)
	if _, err := os.Lstat(manifestPath); errors.Is(err, os.ErrNotExist) {
		return runPlanManifest{}, false, generationClaimed, nil
	} else if err != nil {
		return runPlanManifest{}, false, generationClaimed, err
	}
	persisted, err := loadExpectedRunManifest(directory, input)
	if err != nil {
		return runPlanManifest{}, false, generationClaimed, err
	}
	expectedInstance := instanceFromPlan(plan)
	expectedAttachment, err := androidObservationAttachment(plan, expectedInstance.RuntimeID)
	if err != nil {
		return runPlanManifest{}, false, generationClaimed, err
	}
	if persisted.Allocation != expectedInstance.Allocation || persisted.RuntimeID != expectedInstance.RuntimeID || persisted.Prepared.Attachment != expectedAttachment {
		return runPlanManifest{}, false, generationClaimed, fmt.Errorf("run preparation intent identifies another physical runtime")
	}
	return persisted, true, generationClaimed, nil
}

func (d *Driver) commitRunPreparationIntent(input ports.TargetRunPlan, device deviceRecord) (runPlanManifest, error) {
	spec := input.Run.Spec()
	directory := filepath.Join(device.plan.StateDirectory, "runs", spec.ID.String())
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return runPlanManifest{}, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return runPlanManifest{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.prepare_run", "directory", "run intent path is not an exact regular directory", err)
	}
	scope, err := deviceproxy.IssueScope(spec.LeaseID, spec.TargetID, spec.TargetGeneration, spec.ID, device.instance.Allocation.Serial, d.random)
	if err != nil {
		_ = os.RemoveAll(directory)
		return runPlanManifest{}, err
	}
	sourceInstance := device.instance.RuntimeID
	if sourceInstance == "" {
		sourceInstance = device.instance.Allocation.Serial
	}
	attachment, err := androidObservationAttachment(device.plan, sourceInstance)
	if err != nil {
		return runPlanManifest{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.prepare_run", "attachment", "exact Android observation attachment could not be constructed", err)
	}
	prepared := ports.PreparedTargetRun{
		RunID: spec.ID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration,
		MaterializationDigest: spec.MaterializationDigest,
		RequiredCoverage:      append([]string(nil), input.RequiredCoverage...),
		Attachment:            attachment,
		PreparedAt:            runevidence.AtOrAfter(d.now(), spec.CreatedAt),
	}
	if err := persistRunPlanManifest(directory, input, scope, device.instance.Allocation, sourceInstance, prepared, d.now()); err != nil {
		if _, statErr := os.Lstat(filepath.Join(directory, runPlanManifestFilename)); errors.Is(statErr, os.ErrNotExist) {
			_ = os.RemoveAll(directory)
		}
		return runPlanManifest{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", "intent", "exact run preparation intent could not be committed", err)
	}
	persisted, err := loadExpectedRunManifest(directory, input)
	if err != nil {
		return runPlanManifest{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.prepare_run", "intent", "committed run preparation intent could not be reloaded", err)
	}
	return persisted, nil
}

func androidObservationAttachment(plan VirtualDevicePlan, runtimeID string) (ports.ObservationAttachment, error) {
	device, err := ports.NewADBDeviceSelection(plan.ADBServer, plan.Allocation.Serial)
	if err != nil {
		return ports.ObservationAttachment{}, err
	}
	attachment := ports.ObservationAttachment{TargetKind: domain.TargetAndroidVirtualDevice, RuntimeID: runtimeID, ADBDevice: device}
	if err := attachment.Validate(); err != nil {
		return ports.ObservationAttachment{}, err
	}
	return attachment, nil
}

func (d *Driver) materializePreparedRun(ctx context.Context, input ports.TargetRunPlan, persisted runPlanManifest, stateDirectory string) error {
	directory := filepath.Join(stateDirectory, "runs", input.Run.ID().String())
	if err := d.files.PrepareRun(ctx, persisted.Scope, persisted.Allocation); err != nil {
		cause := d.cleanupFailedRunPreparation(persisted.Scope, persisted.Allocation, directory, err)
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", "device_directory", "could not prepare the scoped Android run directory", cause)
	}
	if err := d.materializeRun(ctx, persisted.Scope, persisted.Allocation, input.Material); err != nil {
		return d.cleanupFailedRunPreparation(persisted.Scope, persisted.Allocation, directory, err)
	}
	return nil
}

func (d *Driver) commitPreparedRun(input ports.TargetRunPlan, signature domain.Digest, device deviceRecord, persisted runPlanManifest) (ports.PreparedTargetRun, error) {
	spec := input.Run.Spec()
	d.mu.Lock()
	defer d.mu.Unlock()
	currentDevice, exists := d.targets[deviceKey(spec.TargetID, spec.TargetGeneration)]
	if !exists || currentDevice.status.State == domain.TargetGenerationQuarantined || currentDevice.instance.RuntimeID != device.instance.RuntimeID {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInvalidState, "cuttlefish.prepare_run", "target", "target generation changed while its run preparation was committed", nil)
	}
	d.runs[spec.ID.String()] = &runRecord{
		plan: runevidence.ClonePlan(input), planSignature: signature, scope: persisted.Scope, allocation: persisted.Allocation,
		sourceInstance: persisted.RuntimeID, directory: filepath.Join(device.plan.StateDirectory, "runs", spec.ID.String()), prepared: runevidence.ClonePrepared(persisted.Prepared),
		transports: make(map[*androidTransport]struct{}), scopedWrites: make(map[string]scopedWriteEvidence),
	}
	d.idempotency[input.IdempotencyKey] = spec.ID.String()
	return runevidence.ClonePrepared(persisted.Prepared), nil
}

func (d *Driver) passPrepareCheckpoint(checkpoint prepareCheckpoint) error {
	if d.prepareCheckpoint == nil {
		return nil
	}
	if err := d.prepareCheckpoint(checkpoint); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", "checkpoint", "run preparation was interrupted after "+string(checkpoint), err)
	}
	return nil
}

func (d *Driver) materializeRun(ctx context.Context, scope deviceproxy.Scope, allocation Allocation, material []ports.TargetMaterialPlan) error {
	entries := append([]ports.TargetMaterialPlan(nil), material...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].LogicalPath < entries[j].LogicalPath })
	for index, entry := range entries {
		content, err := entry.Content.Open(ctx)
		if err != nil {
			return domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", fmt.Sprintf("material[%d].content", index), "could not open authorized material bytes", err)
		}
		if content == nil {
			return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.prepare_run", fmt.Sprintf("material[%d].content", index), "content source returned a nil reader", nil)
		}
		spec := entry.Artifact.Spec()
		actual, putErr := d.files.Put(ctx, scope, allocation, DeviceFileWritePlan{
			Area:           DeviceFileMaterial,
			LogicalPath:    entry.LogicalPath,
			Mode:           entry.Mode,
			MaximumBytes:   spec.Size,
			ExpectedDigest: spec.Digest,
			ExpectedSize:   spec.Size,
		}, content)
		closeErr := content.Close()
		if putErr != nil {
			return classifiedDriverFailure("cuttlefish.prepare_run", fmt.Sprintf("material[%d]", index), "authorized material could not be projected exactly", putErr)
		}
		if closeErr != nil {
			return domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", fmt.Sprintf("material[%d].content", index), "authorized material reader could not be closed", closeErr)
		}
		if actual.Digest != spec.Digest || actual.Size != spec.Size {
			return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.prepare_run", fmt.Sprintf("material[%d]", index), "projected device content does not match its artifact identity", nil)
		}
	}
	return nil
}

func (d *Driver) cleanupFailedRunPreparation(scope deviceproxy.Scope, allocation Allocation, directory string, cause error) error {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(cause, d.files.RemoveRun(cleanup, scope, allocation), os.RemoveAll(directory))
}

func (d *Driver) StartRun(ctx context.Context, runID domain.TargetRunID) error {
	if err := ports.RequireDeadline(ctx, "cuttlefish.start_run"); err != nil {
		return err
	}
	for {
		d.mu.Lock()
		run := d.runs[runID.String()]
		if run == nil {
			d.mu.Unlock()
			return domain.NewError(domain.CodeNotFound, "cuttlefish.start_run", "run_id", "run is not prepared", nil)
		}
		if run.stopped || run.quarantined || run.finishing || run.controlPlaneLost {
			d.mu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "run", "run was stopped, quarantined, finishing, or recovered after control-plane loss", nil)
		}
		if run.started {
			d.mu.Unlock()
			return nil
		}
		if run.starting {
			done := run.startDone
			d.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				continue
			}
		}
		readinessContext, cancelReadiness := context.WithCancel(ctx)
		run.starting = true
		run.startDone = make(chan struct{})
		run.startCancel = cancelReadiness
		coverage := requiredExternalRequirements(run.plan.Collectors)
		d.mu.Unlock()

		var readinessErr error
		if len(coverage) > 0 {
			readinessErr = d.collectors.AwaitReady(readinessContext, runID, coverage)
		}
		cancelReadiness()
		if readinessErr == nil {
			readinessErr = ctx.Err()
		}
		d.lifecycleMu.Lock()
		d.mu.Lock()
		current := d.runs[runID.String()]
		if current != run {
			finishRunStartLocked(run)
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeNotFound, "cuttlefish.start_run", "run_id", "run was removed while awaiting collector readiness", nil)
		}
		if current.stopped || current.quarantined || current.finishing || current.controlPlaneLost {
			finishRunStartLocked(current)
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "run", "run stopped or was quarantined while awaiting collector readiness", nil)
		}
		device, exists := d.targets[deviceKey(current.scope.TargetID, current.scope.Generation)]
		if !exists || device.status.State == domain.TargetGenerationQuarantined || !device.status.Ready {
			finishRunStartLocked(current)
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "target", "target generation was quarantined while awaiting collector readiness", nil)
		}
		if readinessErr != nil {
			finishRunStartLocked(current)
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.start_run", "collectors", "required Android collectors are not ready", readinessErr)
		}
		startedAt := runevidence.AtOrAfter(d.now(), current.prepared.PreparedAt)
		payload, encodeErr := json.Marshal(struct {
			RuntimeID string `json:"runtime_id"`
			Serial    string `json:"serial"`
		}{RuntimeID: current.sourceInstance, Serial: current.allocation.Serial})
		if encodeErr != nil {
			finishRunStartLocked(current)
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeInternal, "cuttlefish.start_run", "observation", "could not encode the intrinsic start fact", encodeErr)
		}
		plan := runevidence.ClonePlan(current.plan)
		allocation := current.allocation
		runtimeID := current.sourceInstance
		directory := current.directory
		minimumStartedAt := current.prepared.PreparedAt
		d.mu.Unlock()
		startRecord, commitErr := commitExpectedRunStart(directory, plan, allocation, runtimeID, startedAt, minimumStartedAt)
		if commitErr != nil {
			d.mu.Lock()
			if latest := d.runs[runID.String()]; latest == current {
				finishRunStartLocked(latest)
			}
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeUnavailable, "cuttlefish.start_run", "run_start", "could not durably commit the Android target run start", commitErr)
		}
		startedAt = startRecord.StartedAt
		d.mu.Lock()
		current = d.runs[runID.String()]
		device, exists = d.targets[deviceKey(plan.Run.Spec().TargetID, plan.Run.Spec().TargetGeneration)]
		if current != run || current.stopped || current.quarantined || current.finishing || current.controlPlaneLost || !exists || !device.status.Ready || device.instance.RuntimeID != runtimeID {
			if current == run {
				finishRunStartLocked(current)
			}
			d.mu.Unlock()
			d.lifecycleMu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "run", "run or target ownership changed while durable start was committed", nil)
		}
		deadlineContext, cancelDeadline := context.WithCancel(context.Background())
		current.started = true
		current.startedAt = startedAt
		current.observations = append(current.observations, ports.TargetRunObservation{
			Kind: "target.run.started", ObservedAt: startedAt, Payload: payload,
		})
		current.deadlineCancel = cancelDeadline
		maximumDuration := current.plan.MaximumDuration
		finishRunStartLocked(current)
		d.mu.Unlock()
		d.lifecycleMu.Unlock()
		go d.enforceRunDuration(deadlineContext, runID, maximumDuration)
		return nil
	}
}

func (d *Driver) OpenTransport(ctx context.Context, runID domain.TargetRunID) (ports.TargetTransport, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.open_transport"); err != nil {
		return nil, err
	}
	run, err := d.requireRun(runID)
	if err != nil {
		return nil, err
	}
	if !run.started || run.stopped || run.quarantined || run.finishing || run.controlPlaneLost {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.open_transport", "run", "run is not active", nil)
	}
	device, err := d.requireDevice(run.scope.TargetID, run.scope.Generation)
	if err != nil {
		return nil, err
	}
	if device.status.State == domain.TargetGenerationQuarantined {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.open_transport", "target", "target generation is quarantined", nil)
	}
	transport := &androidTransport{driver: d, gateway: d.gateway, files: d.files, scope: run.scope, allocation: device.instance.Allocation}
	d.mu.Lock()
	current := d.runs[runID.String()]
	currentDevice := d.targets[deviceKey(run.scope.TargetID, run.scope.Generation)]
	if current != nil && current.started && !current.stopped && !current.quarantined && !current.finishing && !current.controlPlaneLost && currentDevice.status.State != domain.TargetGenerationQuarantined && currentDevice.status.Ready {
		current.transports[transport] = struct{}{}
		d.mu.Unlock()
		return transport, nil
	}
	d.mu.Unlock()
	return nil, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.open_transport", "run", "run stopped while opening transport", nil)
}

func (d *Driver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.stop_run"); err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	if !mode.IsValid() {
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.stop_run", "mode", "is not recognized", nil)
	}
	cause := runevidence.CauseCollectorEvidenceUnavailable
	d.mu.Lock()
	current := d.runs[runID.String()]
	if current != nil && !current.started {
		cause = runevidence.CauseNeverStarted
	}
	d.mu.Unlock()
	return d.stopRun(ctx, runID, mode, cause)
}

func (d *Driver) enforceRunDuration(ctx context.Context, runID domain.TargetRunID, maximumDuration time.Duration) {
	timer := time.NewTimer(maximumDuration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	cleanupContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, _ = d.stopRun(cleanupContext, runID, ports.StopForce, runevidence.CauseDurationExceeded)
}

func (d *Driver) stopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode, cause runevidence.Cause) (ports.TargetRunStopReceipt, error) {
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	return d.stopRunLocked(ctx, runID, mode, cause)
}

func (d *Driver) stopRunLocked(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode, cause runevidence.Cause) (ports.TargetRunStopReceipt, error) {
	d.mu.Lock()
	current := d.runs[runID.String()]
	if current == nil {
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeNotFound, "cuttlefish.stop_run", "run_id", "run is not prepared", nil)
	}
	if current.quarantined {
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeInvalidState, "cuttlefish.stop_run", "run", "quarantined run state is preserved for evidence", nil)
	}
	if current.stopped {
		result := runevidence.CloneStopReceipt(*current.receipt)
		d.mu.Unlock()
		return result, nil
	}
	device, found := d.targets[deviceKey(current.scope.TargetID, current.scope.Generation)]
	if !found || device.instance.RuntimeID != current.sourceInstance {
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.stop_run", "target", "exact target generation disappeared before execution could be contained", nil)
	}
	current.finishing = true
	if cause == runevidence.CauseDurationExceeded {
		current.durationExceeded = true
	}
	if current.startCancel != nil {
		current.startCancel()
	}
	if current.deadlineCancel != nil {
		current.deadlineCancel()
		current.deadlineCancel = nil
	}
	transports := make([]*androidTransport, 0, len(current.transports))
	for transport := range current.transports {
		transport.revoke()
		transports = append(transports, transport)
	}
	d.mu.Unlock()

	contained, cleanupErr := d.cleanupRunResources(ctx, current, device.instance, mode, transports)
	if cleanupErr != nil {
		d.mu.Lock()
		if latest := d.runs[runID.String()]; latest == current && !latest.stopped {
			latest.finishing = false
		}
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, cleanupErr
	}
	d.mu.Lock()
	current = d.runs[runID.String()]
	device, found = d.targets[deviceKey(device.plan.TargetID, device.plan.Generation)]
	if current == nil || current.stopped || !found || device.instance.RuntimeID != current.sourceInstance {
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeConflict, "cuttlefish.stop_run", "run", "run or target ownership changed while execution was being contained", nil)
	}
	result, buildErr := d.buildStopReceipt(current, cause, contained.ObservedAt)
	if buildErr != nil {
		current.finishing = false
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, buildErr
	}
	result, contained, buildErr = commitExpectedRunStop(current.directory, current.plan, device.plan, current.allocation, current.sourceInstance, contained, result)
	if buildErr != nil {
		current.finishing = false
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.stop_run", "stop_manifest", "exact stopped-run evidence could not be committed", buildErr)
	}
	current.stopped = true
	current.finishing = false
	current.transports = nil
	containedCopy := contained
	current.containment = &containedCopy
	stored := runevidence.CloneStopReceipt(result)
	current.receipt = &stored
	device.status.Ready = false
	device.status.State = domain.TargetGenerationResettable
	device.status.ObservedAt = contained.ObservedAt
	d.targets[deviceKey(device.plan.TargetID, device.plan.Generation)] = device
	d.mu.Unlock()
	return runevidence.CloneStopReceipt(result), nil
}

func (d *Driver) buildStopReceipt(run *runRecord, cause runevidence.Cause, containedAt time.Time) (ports.TargetRunStopReceipt, error) {
	stoppedAt := runevidence.AtOrAfter(d.now(), containedAt)
	stoppedAt = runevidence.AtOrAfter(stoppedAt, run.prepared.PreparedAt)
	outcome := ports.RunCompleted
	failureKind := ports.TargetRunFailureNone
	terminalKind := "target.run.stopped"
	switch {
	case run.interruptedExecution:
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureTarget
		terminalKind = "target.run.control-plane-failure"
	case !run.started:
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureNeverStarted
		terminalKind = "target.run.never_started"
	case run.durationExceeded || cause == runevidence.CauseDurationExceeded:
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureDurationExceeded
		terminalKind = "target.run.duration_exceeded"
	}
	if run.started || run.interruptedExecution {
		stoppedAt = runevidence.AtOrAfter(stoppedAt, run.startedAt)
	}
	payload, err := json.Marshal(struct {
		FailureKind ports.TargetRunFailureKind `json:"failure_kind,omitempty"`
		RuntimeID   string                     `json:"runtime_id"`
		Serial      string                     `json:"serial"`
	}{FailureKind: failureKind, RuntimeID: run.sourceInstance, Serial: run.allocation.Serial})
	if err != nil {
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeInternal, "cuttlefish.stop_run", "observation", "could not encode the intrinsic stop fact", err)
	}
	observations := append([]ports.TargetRunObservation(nil), run.observations...)
	observations = append(observations, ports.TargetRunObservation{Kind: terminalKind, ObservedAt: stoppedAt, Payload: payload})
	changes, err := d.targetRunChanges(run, stoppedAt)
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	receipt := ports.TargetRunStopReceipt{
		RunID: run.plan.Run.ID(), Outcome: outcome, FailureKind: failureKind,
		StartedAt: run.startedAt, StoppedAt: stoppedAt, Observations: observations, TargetChanges: changes,
	}
	return receipt, receipt.Validate()
}

func requiredExternalRequirements(collectors []ports.CollectorSpec) []ports.ObservationRequirement {
	result := make([]ports.ObservationRequirement, 0, len(collectors))
	for _, collector := range collectors {
		if collector.Requirement.Required && collector.Requirement.SignalFamily != ports.TargetLifecycleSignal {
			result = append(result, collector.Requirement)
		}
	}
	return result
}

func (d *Driver) recordRunFact(runID domain.TargetRunID, observation ports.TargetRunObservation, mutate func(*runRecord)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	run := d.runs[runID.String()]
	if run == nil || !run.started || run.stopped || run.quarantined || run.controlPlaneLost {
		return io.ErrClosedPipe
	}
	minimum := run.startedAt
	if count := len(run.observations); count > 0 && run.observations[count-1].ObservedAt.After(minimum) {
		minimum = run.observations[count-1].ObservedAt
	}
	observation.ObservedAt = runevidence.AtOrAfter(observation.ObservedAt, minimum)
	observation.Payload = append(json.RawMessage(nil), observation.Payload...)
	run.observations = append(run.observations, observation)
	if mutate != nil {
		mutate(run)
	}
	return nil
}

func (d *Driver) cleanupRunResources(ctx context.Context, run *runRecord, instance Instance, mode ports.StopMode, transports []*androidTransport) (BackendQuarantineState, error) {
	var contained BackendQuarantineState
	var containmentErr error
	d.mu.Lock()
	priorContainment := run.containment
	d.mu.Unlock()
	if priorContainment != nil {
		if priorContainment.RuntimeID != instance.RuntimeID || !priorContainment.ExecutionStopped || !priorContainment.NetworkUnreachable || !priorContainment.StatePreserved || priorContainment.ObservedAt.IsZero() {
			containmentErr = domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.stop_run", "execution_containment", "recovered run lacks exact authoritative containment proof", nil)
		} else {
			contained = *priorContainment
		}
	} else if run.controlPlaneLost {
		if run.containment == nil || run.containment.RuntimeID != instance.RuntimeID || !run.containment.ExecutionStopped || !run.containment.NetworkUnreachable || !run.containment.StatePreserved || run.containment.ObservedAt.IsZero() {
			containmentErr = domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.stop_run", "execution_containment", "recovered run lacks exact authoritative containment proof", nil)
		} else {
			contained = *run.containment
		}
	} else {
		contained, containmentErr = d.verifyBackendContainment(ctx, instance, mode)
		if containmentErr != nil {
			containmentErr = classifiedDriverFailure("cuttlefish.stop_run", "execution_containment", "could not prove all guest execution stopped and the exact device became unreachable", containmentErr)
		} else {
			containedCopy := contained
			d.mu.Lock()
			if run.containment == nil {
				run.containment = &containedCopy
			}
			d.mu.Unlock()
		}
	}
	errorsFound := make([]error, 0, len(transports)+1)
	if containmentErr != nil {
		errorsFound = append(errorsFound, containmentErr)
	}
	for _, transport := range transports {
		if err := transport.closeWithContext(ctx); err != nil {
			errorsFound = append(errorsFound, domain.NewError(domain.CodeUnavailable, "cuttlefish.stop_run", "transport_cleanup", "could not drain and revoke a scoped Android transport", err))
		}
	}
	if err := errors.Join(errorsFound...); err != nil {
		return contained, err
	}
	return contained, nil
}

func (d *Driver) verifyBackendContainment(ctx context.Context, instance Instance, mode ports.StopMode) (BackendQuarantineState, error) {
	if !mode.IsValid() {
		return BackendQuarantineState{}, domain.NewError(domain.CodeInvalidArgument, "cuttlefish.execution_containment", "mode", "is not recognized", nil)
	}
	quarantiner, supported := d.backend.(BackendQuarantiner)
	if !supported {
		return BackendQuarantineState{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.execution_containment", "backend", "backend cannot prove execution stop, network unreachability, and state preservation", nil)
	}
	state, err := quarantiner.Quarantine(ctx, instance, mode)
	if err != nil {
		return state, err
	}
	if state.RuntimeID != instance.RuntimeID || !state.ExecutionStopped || !state.NetworkUnreachable || !state.StatePreserved || state.ObservedAt.IsZero() {
		return state, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.execution_containment", "proof", "backend returned incomplete or foreign containment evidence", nil)
	}
	state.ObservedAt = runevidence.AtOrAfter(state.ObservedAt, d.now())
	return state, nil
}

func (d *Driver) targetRunChanges(run *runRecord, sealedAt time.Time) (domain.ChangeSet, error) {
	entries := make([]domain.ChangeEntry, 0, len(run.scopedWrites)+1)
	if run.opaqueMutationReason != "" || run.adbAuthorityIssued {
		reason := run.opaqueMutationReason
		if reason == "" {
			reason = "arbitrary-scoped-adb-authority"
		}
		entry, err := domain.NewChangeEntry(domain.ChangeEntrySpec{
			Kind: domain.ChangeOpaqueDirectory, Path: ".",
			Metadata: map[string]string{
				"reason": reason, "serial": run.allocation.Serial,
				"mutation_coverage": "opaque", "known_scoped_write_count": strconv.Itoa(len(run.scopedWrites)),
			},
		})
		if err != nil {
			return domain.ChangeSet{}, err
		}
		entries = append(entries, entry)
	} else {
		paths := make([]string, 0, len(run.scopedWrites))
		for logicalPath := range run.scopedWrites {
			paths = append(paths, logicalPath)
		}
		sort.Strings(paths)
		for _, logicalPath := range paths {
			write := run.scopedWrites[logicalPath]
			devicePath := strings.TrimPrefix(scopedDevicePath(run.scope, DeviceFileWritable, logicalPath), "/")
			entry, err := domain.NewChangeEntry(domain.ChangeEntrySpec{
				Kind: domain.ChangeAdded, Path: devicePath, AfterDigest: write.file.Digest,
				Metadata: map[string]string{
					"size_bytes": strconv.FormatInt(write.file.Size, 10), "mode": fmt.Sprintf("%#o", write.mode),
					"evidence": "exact-scoped-push",
				},
			})
			if err != nil {
				return domain.ChangeSet{}, err
			}
			entries = append(entries, entry)
		}
	}
	return domain.NewChangeSet(domain.ChangeScopeTarget, entries, domain.InitialRevision, sealedAt)
}

func finishRunStartLocked(run *runRecord) {
	if !run.starting {
		return
	}
	run.starting = false
	run.startCancel = nil
	close(run.startDone)
	run.startDone = nil
}

func (d *Driver) Reset(ctx context.Context, targetID domain.TargetID, reset ports.ResetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.reset"); err != nil {
		return ports.TargetResult{}, err
	}
	if err := reset.Validate(); err != nil {
		return ports.TargetResult{}, err
	}
	// Serialize the destructive transition so retries cannot create or retire
	// the same generation concurrently.
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.mu.Lock()
	if prior, found := d.resetResults[reset.IdempotencyKey]; found {
		d.mu.Unlock()
		if prior.targetID != targetID || prior.plan != reset {
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.reset", "idempotency_key", "was used for a different reset", nil)
		}
		return prior.result, prior.err
	}
	for _, prior := range d.resetResults {
		if prior.targetID == targetID && prior.plan.Previous == reset.Previous {
			d.mu.Unlock()
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.reset", "idempotency_key", "another exact reset request already advanced this previous generation", nil)
		}
	}
	d.mu.Unlock()
	if targetID != reset.Previous.ID {
		return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.reset", "target_id", "does not match reset plan", nil)
	}
	previous, err := d.requireDevice(targetID, reset.Previous.Generation)
	if err != nil {
		return ports.TargetResult{}, err
	}
	if previous.status.State == domain.TargetGenerationQuarantined {
		return ports.TargetResult{}, domain.NewError(domain.CodeInvalidState, "cuttlefish.reset", "target", "quarantined target generation cannot be reset", nil)
	}
	if previous.plan.LeaseID != reset.LeaseID {
		return ports.TargetResult{}, domain.NewError(domain.CodeForbidden, "cuttlefish.reset", "lease_id", "reset is outside this device lease", nil)
	}
	if d.hasUnstoppedRun(targetID, reset.Previous.Generation) {
		return ports.TargetResult{}, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.reset", "run", "all prepared runs must stop before reset", nil)
	}
	if err := d.cleanupStoppedRuns(ctx, targetID, reset.Previous.Generation); err != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "run_cleanup", "stopped run resources could not be cleaned", err)
	}
	if reset.Mode == ports.ResetSnapshot {
		return ports.TargetResult{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.reset", "mode", "snapshot reset cannot produce a separately allocated reachable next generation", nil)
	}
	allocation, err := d.allocator.Reserve(ctx, targetID, reset.NextGeneration)
	if err != nil {
		return ports.TargetResult{}, err
	}
	keep := false
	defer func() {
		if !keep {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = d.allocator.Release(cleanup, allocation)
		}
	}()
	next := previous.plan
	next.Generation = reset.NextGeneration
	next.Name = "world-android-" + targetID.UUID() + "-g" + strconv.FormatUint(uint64(reset.NextGeneration), 10)
	next.StateDirectory = filepath.Join(d.build.TargetRoot, targetID.String(), "generations", strconv.FormatUint(uint64(reset.NextGeneration), 10))
	next.Allocation = allocation
	next.Labels = cloneLabels(previous.plan.Labels)
	next.Labels["world.target-generation"] = strconv.FormatUint(uint64(reset.NextGeneration), 10)
	if err := next.Validate(d.build.TargetRoot, d.build.SystemImageRoot); err != nil {
		return ports.TargetResult{}, err
	}
	transition, err := commitExpectedResetTransition(previous, next, reset, d.now())
	if err != nil {
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "transition_manifest", "exact reset semantics could not be committed before replacement creation", err)
	}
	if err := d.passResetCheckpoint(resetCheckpointTransitionCommitted); err != nil {
		keep = true
		return ports.TargetResult{}, err
	}
	instance, readiness, err := d.createInstance(ctx, next)
	if err != nil {
		if mustRetainAllocation(err) {
			keep = true
		} else if removeErr := os.RemoveAll(next.StateDirectory); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "replacement", "could not create a reachable next device generation", err)
	}
	if err := persistTargetRuntimeManifests(next, instance, readiness, d.now()); err != nil {
		cleanupErr := d.cleanupInstance(instance)
		if cleanupErr != nil {
			keep = true
		}
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "manifest", "replacement target/runtime plan could not be persisted", errors.Join(err, cleanupErr))
	}
	if err := d.passResetCheckpoint(resetCheckpointReplacementManifest); err != nil {
		keep = true
		return ports.TargetResult{}, err
	}
	if err := d.backend.Destroy(ctx, previous.instance); err != nil {
		restoreErr := d.restoreInstance(previous.instance)
		if restoreErr != nil {
			result := readyResetTargetResult(next, instance, d.now())
			status := result.Status
			outcomeErr := domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "retire_previous", "replacement is ready but the previous device could neither retire nor be restored", errors.Join(err, restoreErr))
			persisted, persistErr := d.commitDurableResetOutcome(next, transition, result, outcomeErr)
			if persistErr != nil {
				outcomeErr = domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "outcome_manifest", "authoritative replacement exists but its exact reset outcome could not be persisted", errors.Join(outcomeErr, persistErr))
				persisted = resetOutcome{targetID: targetID, plan: reset, result: result, err: outcomeErr}
			}
			d.commitReset(previous, next, instance, status, persisted)
			// The old allocation is retained because the old process state is
			// uncertain. Next remains the authoritative reachable generation.
			keep = true
			return result, outcomeErr
		}
		rollbackErr := d.cleanupInstance(instance)
		if rollbackErr != nil {
			// Retain the allocation if the replacement could not be
			// destroyed; releasing it could collide with a live instance.
			keep = true
		} else if removeErr := os.RemoveAll(next.StateDirectory); removeErr != nil {
			rollbackErr = removeErr
		}
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "retire_previous", "could not retire the previous device; replacement rollback was attempted", errors.Join(err, restoreErr, rollbackErr))
	}
	if err := d.passResetCheckpoint(resetCheckpointPreviousRetired); err != nil {
		keep = true
		return ports.TargetResult{}, err
	}
	result := readyResetTargetResult(next, instance, d.now())
	status := result.Status
	persisted, persistErr := d.commitDurableResetOutcome(next, transition, result, nil)
	if persistErr != nil {
		outcomeErr := domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "outcome_manifest", "replacement became authoritative but its exact reset outcome could not be persisted", persistErr)
		d.commitReset(previous, next, instance, status, resetOutcome{targetID: targetID, plan: reset, result: result, err: outcomeErr})
		keep = true
		return result, outcomeErr
	}
	if err := d.passResetCheckpoint(resetCheckpointOutcomeCommitted); err != nil {
		keep = true
		return result, err
	}
	d.commitReset(previous, next, instance, status, persisted)
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.allocator.Release(cleanup, previous.plan.Allocation)
	keep = true
	return result, nil
}

func (d *Driver) Quarantine(ctx context.Context, plan ports.TargetQuarantinePlan) (ports.TargetQuarantineEvidence, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.quarantine"); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.mu.Lock()
	if prior, found := d.quarantines[plan.IdempotencyKey]; found {
		d.mu.Unlock()
		if prior.plan != plan {
			return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeConflict, "cuttlefish.quarantine", "idempotency_key", "was used for another quarantine", nil)
		}
		if err := d.drainQuarantinedTransports(ctx, plan.Target); err != nil {
			return prior.evidence, err
		}
		return prior.evidence, nil
	}
	for _, prior := range d.quarantines {
		if prior.plan.Target == plan.Target {
			d.mu.Unlock()
			return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeConflict, "cuttlefish.quarantine", "idempotency_key", "another exact quarantine request already quarantined this target generation", nil)
		}
	}
	d.mu.Unlock()
	device, err := d.requireDevice(plan.Target.ID, plan.Target.Generation)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	state, err := d.verifyBackendContainment(ctx, device.instance, ports.StopForce)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, classifiedDriverFailure("cuttlefish.quarantine", "backend", "backend quarantine failed", err)
	}
	evidence := ports.TargetQuarantineEvidence{
		Target: plan.Target, RuntimeID: state.RuntimeID,
		ExecutionStopped: state.ExecutionStopped, NetworkUnreachable: state.NetworkUnreachable,
		StatePreserved: state.StatePreserved, ObservedAt: state.ObservedAt,
	}
	if err := evidence.Validate(plan.Target); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if state.RuntimeID != device.instance.RuntimeID {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.quarantine", "runtime_id", "backend evidence identifies a different device instance", nil)
	}
	state, err = commitExpectedGenerationQuarantine(device.plan, device.instance, plan, state)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.quarantine", "quarantine_manifest", "durable Android generation quarantine proof could not be persisted: "+err.Error(), err)
	}
	evidence = ports.TargetQuarantineEvidence{
		Target: plan.Target, RuntimeID: state.RuntimeID,
		ExecutionStopped: state.ExecutionStopped, NetworkUnreachable: state.NetworkUnreachable,
		StatePreserved: state.StatePreserved, ObservedAt: state.ObservedAt,
	}
	d.mu.Lock()
	current, found := d.targets[deviceKey(plan.Target.ID, plan.Target.Generation)]
	if !found || current.instance.RuntimeID != device.instance.RuntimeID {
		d.mu.Unlock()
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeConflict, "cuttlefish.quarantine", "target", "target generation changed while quarantine was verified", nil)
	}
	current.status.State = domain.TargetGenerationQuarantined
	current.status.Ready = false
	current.status.ObservedAt = state.ObservedAt
	d.targets[deviceKey(plan.Target.ID, plan.Target.Generation)] = current
	for _, run := range d.runs {
		if run.scope.TargetID != plan.Target.ID || run.scope.Generation != plan.Target.Generation {
			continue
		}
		run.quarantined = true
		if run.startCancel != nil {
			run.startCancel()
		}
		if run.deadlineCancel != nil {
			run.deadlineCancel()
			run.deadlineCancel = nil
		}
	}
	if d.quarantines == nil {
		d.quarantines = make(map[string]quarantineOutcome)
	}
	d.quarantines[plan.IdempotencyKey] = quarantineOutcome{plan: plan, evidence: evidence}
	d.mu.Unlock()
	if err := d.drainQuarantinedTransports(ctx, plan.Target); err != nil {
		return evidence, err
	}
	return evidence, nil
}

func (d *Driver) drainQuarantinedTransports(ctx context.Context, ref ports.TargetRef) error {
	type ownedTransport struct {
		run       *runRecord
		transport *androidTransport
	}
	d.mu.Lock()
	owned := make([]ownedTransport, 0)
	for _, run := range d.runs {
		if run.scope.TargetID != ref.ID || run.scope.Generation != ref.Generation || !run.quarantined {
			continue
		}
		for transport := range run.transports {
			owned = append(owned, ownedTransport{run: run, transport: transport})
		}
	}
	d.mu.Unlock()
	errorsFound := make([]error, 0, len(owned))
	for _, item := range owned {
		if err := item.transport.closeWithContext(ctx); err != nil {
			errorsFound = append(errorsFound, err)
			continue
		}
		d.mu.Lock()
		delete(item.run.transports, item.transport)
		if len(item.run.transports) == 0 {
			item.run.transports = nil
		}
		d.mu.Unlock()
	}
	if err := errors.Join(errorsFound...); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.quarantine", "transport_cleanup", "could not drain every scoped Android transport after containment", err)
	}
	return nil
}

func (d *Driver) commitDurableResetOutcome(next VirtualDevicePlan, transition resetTransitionManifest, result ports.TargetResult, outcomeErr error) (resetOutcome, error) {
	commit := d.commitResetOutcome
	if commit == nil {
		commit = commitExpectedResetOutcome
	}
	return commit(next, transition, result, outcomeErr, d.now())
}

func (d *Driver) passResetCheckpoint(checkpoint resetCheckpoint) error {
	if d.resetCheckpoint == nil {
		return nil
	}
	if err := d.resetCheckpoint(checkpoint); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "checkpoint", "reset was interrupted after "+string(checkpoint), err)
	}
	return nil
}

func readyResetTargetResult(plan VirtualDevicePlan, instance Instance, observedAt time.Time) ports.TargetResult {
	return ports.TargetResult{
		Status: ports.TargetStatus{
			TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetAndroidVirtualDevice,
			State: domain.TargetGenerationReady, Ready: true, RuntimeID: instance.RuntimeID,
			DeviceSerial: instance.Allocation.Serial, ObservedAt: observedAt.UTC(),
		},
		Created: true,
	}
}

func (d *Driver) commitReset(previous deviceRecord, next VirtualDevicePlan, instance Instance, status ports.TargetStatus, outcome resetOutcome) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removeRunsLocked(outcome.targetID, outcome.plan.Previous.Generation)
	delete(d.targets, deviceKey(outcome.targetID, outcome.plan.Previous.Generation))
	delete(d.idempotency, previous.input.IdempotencyKey)
	d.targets[deviceKey(outcome.targetID, outcome.plan.NextGeneration)] = deviceRecord{input: previous.input, plan: next, instance: instance, status: status}
	d.resetResults[outcome.plan.IdempotencyKey] = outcome
}

func (d *Driver) Destroy(ctx context.Context, ref ports.TargetRef) error {
	if err := ports.RequireDeadline(ctx, "cuttlefish.destroy"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	key := deviceKey(ref.ID, ref.Generation)
	d.mu.Lock()
	device, activeFound := d.targets[key]
	cleanupDevice, cleanupFound := d.cleanupOnly[key]
	d.mu.Unlock()
	if activeFound && cleanupFound {
		return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.destroy", "ownership", "generation is both active and cleanup-only", nil)
	}
	if cleanupFound {
		device = cleanupDevice.record
	}
	found := activeFound || cleanupFound
	if activeFound && d.hasUnstoppedRun(ref.ID, ref.Generation) {
		return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.destroy", "run", "all prepared runs must stop before destroy", nil)
	}
	if activeFound {
		if err := d.cleanupStoppedRuns(ctx, ref.ID, ref.Generation); err != nil {
			return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "run_cleanup", "stopped run resources could not be cleaned", err)
		}
	}
	absent := false
	if cleanupFound && !cleanupDevice.runtimePresent {
		if err := d.requireAndroidRuntimeAbsent(ctx, device.instance.RuntimeID); err != nil {
			return err
		}
		absent = true
	} else {
		var err error
		device, absent, err = d.resolveAndroidDestroy(ctx, ref, device, found)
		if err != nil {
			return err
		}
	}
	if absent && device.plan.Allocation.InstanceName == "" {
		return nil
	}
	if !absent {
		if err := d.backend.Destroy(ctx, device.instance); err != nil {
			return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "backend", "device destruction failed", err)
		}
		if err := d.requireAndroidRuntimeAbsent(ctx, device.instance.RuntimeID); err != nil {
			return err
		}
	}
	if err := d.allocator.Release(ctx, device.plan.Allocation); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "allocation", "device endpoint release failed", err)
	}
	if err := os.RemoveAll(device.plan.StateDirectory); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "state_directory", "target state directory cleanup failed", err)
	}
	if found {
		d.mu.Lock()
		if activeFound {
			delete(d.targets, key)
			delete(d.idempotency, device.input.IdempotencyKey)
			d.removeRunsLocked(ref.ID, ref.Generation)
			for resetKey, outcome := range d.resetResults {
				if outcome.result.Status.TargetID == ref.ID && outcome.result.Status.Generation == ref.Generation {
					delete(d.resetResults, resetKey)
				}
			}
			for quarantineKey, outcome := range d.quarantines {
				if outcome.plan.Target == ref {
					delete(d.quarantines, quarantineKey)
				}
			}
		}
		delete(d.cleanupOnly, key)
		d.mu.Unlock()
	}
	return nil
}

func (d *Driver) requireAndroidRuntimeAbsent(ctx context.Context, runtimeID string) error {
	if !safeInstanceName(runtimeID) {
		return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.destroy", "runtime_id", "cleanup authority has an invalid Android runtime identity", nil)
	}
	inventoryBackend, supported := d.backend.(BackendInventory)
	if !supported {
		return domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.destroy", "inventory", "backend cannot prove Android runtime absence", nil)
	}
	runtimeIDs, err := inventoryBackend.ListRuntimeIDs(ctx)
	if err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "inventory", "could not verify Android runtime absence", err)
	}
	inventory, err := exactRuntimeIDSet(runtimeIDs)
	if err != nil {
		return domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.destroy", "inventory", "Android runtime inventory is ambiguous", err)
	}
	if _, remains := inventory[runtimeID]; remains {
		return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.destroy", "absence", "exact Android runtime remains present", nil)
	}
	return nil
}

func (d *Driver) cleanupStoppedRuns(ctx context.Context, id domain.TargetID, generation domain.TargetGeneration) error {
	d.mu.Lock()
	runIDs := make([]domain.TargetRunID, 0)
	for _, run := range d.runs {
		if run.scope.TargetID != id || run.scope.Generation != generation {
			continue
		}
		if !run.stopped {
			d.mu.Unlock()
			return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.run_cleanup", "run", "run is still active", nil)
		}
		runIDs = append(runIDs, run.plan.Run.ID())
	}
	d.mu.Unlock()
	for _, runID := range runIDs {
		if _, err := d.stopRunLocked(ctx, runID, ports.StopForce, runevidence.CauseCollectorEvidenceUnavailable); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) requireDevice(id domain.TargetID, generation domain.TargetGeneration) (deviceRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	record, found := d.targets[deviceKey(id, generation)]
	if !found {
		return deviceRecord{}, domain.NewError(domain.CodeNotFound, "cuttlefish.device", "generation", "device generation is not owned by this driver", nil)
	}
	return record, nil
}

func (d *Driver) requireRun(id domain.TargetRunID) (*runRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	run, found := d.runs[id.String()]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "cuttlefish.run", "run_id", "run is not prepared", nil)
	}
	copied := *run
	return &copied, nil
}

func (d *Driver) hasUnstoppedRun(id domain.TargetID, generation domain.TargetGeneration) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, run := range d.runs {
		if run.scope.TargetID == id && run.scope.Generation == generation && !run.stopped {
			return true
		}
	}
	return false
}

func (d *Driver) removeRunsLocked(id domain.TargetID, generation domain.TargetGeneration) {
	for key, run := range d.runs {
		if run.scope.TargetID != id || run.scope.Generation != generation {
			continue
		}
		delete(d.idempotency, run.plan.IdempotencyKey)
		delete(d.runs, key)
	}
}

func canonicalResetModes(values []ports.ResetMode) (string, error) {
	if len(values) == 0 {
		return "", fmt.Errorf("at least one reset mode is required")
	}
	modes := make([]string, 0, len(values))
	seen := make(map[ports.ResetMode]struct{}, len(values))
	for _, mode := range values {
		if !mode.IsValid() || mode == ports.ResetSnapshot {
			return "", fmt.Errorf("unsupported reset mode %q", mode)
		}
		if _, duplicate := seen[mode]; duplicate {
			return "", fmt.Errorf("duplicate reset mode %q", mode)
		}
		seen[mode] = struct{}{}
		modes = append(modes, string(mode))
	}
	sort.Strings(modes)
	return strings.Join(modes, ","), nil
}

func deviceKey(id domain.TargetID, generation domain.TargetGeneration) string {
	return id.String() + "/" + strconv.FormatUint(uint64(generation), 10)
}
func cloneLabels(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}

var _ ports.TargetDriver = (*Driver)(nil)
