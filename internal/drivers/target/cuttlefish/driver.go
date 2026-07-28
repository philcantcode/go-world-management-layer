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
	build      BuildConfig
	backend    Backend
	allocator  Allocator
	gateway    Gateway
	files      ScopedFileGateway
	collectors CollectorReadiness
	random     io.Reader
	now        func() time.Time

	mu           sync.Mutex
	lifecycleMu  sync.Mutex
	targets      map[string]deviceRecord
	runs         map[string]*runRecord
	idempotency  map[string]string
	resetResults map[string]resetOutcome
	quarantines  map[string]quarantineOutcome
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

type deviceRecord struct {
	input    ports.TargetPlan
	plan     VirtualDevicePlan
	instance Instance
	status   ports.TargetStatus
}

type runRecord struct {
	plan            ports.TargetRunPlan
	scope           deviceproxy.Scope
	allocation      Allocation
	sourceInstance  string
	directory       string
	prepared        ports.PreparedTargetRun
	starting        bool
	startDone       chan struct{}
	startCancel     context.CancelFunc
	started         bool
	startedAt       time.Time
	stopped         bool
	quarantined     bool
	receipt         *ports.TargetRunStopReceipt
	observations    []ports.TargetRunObservation
	deadlineCancel  context.CancelFunc
	cleanupRunning  bool
	cleanupDone     chan struct{}
	cleanupComplete bool
	cleanupErr      error
	transports      map[*androidTransport]struct{}
}

func New(config Config) (*Driver, error) {
	if config.Backend == nil || config.Allocator == nil || config.Gateway == nil || config.Files == nil || config.Collectors == nil {
		return nil, fmt.Errorf("backend, allocator, scoped ADB endpoint and file gateways, and collector gate are required")
	}
	if config.Build.TargetRoot == "" || config.Build.SystemImageRoot == "" || config.Build.BackendVersion == "" || config.Build.RuntimeVersion == "" || config.Build.DeviceConfigDigest.IsZero() {
		return nil, fmt.Errorf("complete virtual-device build configuration is required")
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Driver{build: config.Build, backend: config.Backend, allocator: config.Allocator, gateway: config.Gateway, files: config.Files, collectors: config.Collectors, random: config.Random, now: config.Now, targets: make(map[string]deviceRecord), runs: make(map[string]*runRecord), idempotency: make(map[string]string), resetResults: make(map[string]resetOutcome), quarantines: make(map[string]quarantineOutcome)}, nil
}

func NewDriver(config Config) (*Driver, error) { return New(config) }

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
	virtualConstraints := map[string]string{"backend_version": probe.BackendVersion, "runtime_version": probe.RuntimeVersion}
	if probe.KVMKnown {
		virtualConstraints["kvm"] = strconv.FormatBool(probe.KVM)
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
	evidence := cloneLabels(probe.Evidence)
	evidence["driver"] = backendKind
	return domain.NewCapabilityFingerprint(map[string]domain.Capability{"target.android-virtual": virtual, "target.android-reset": reset, "target.scoped-adb": adb}, evidence)
}

func (d *Driver) Create(ctx context.Context, input ports.TargetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "cuttlefish.create"); err != nil {
		return ports.TargetResult{}, err
	}
	if err := input.Validate(); err != nil {
		return ports.TargetResult{}, err
	}
	spec := input.Generation.Spec()
	key := deviceKey(spec.TargetID, spec.Generation)
	d.mu.Lock()
	if prior, found := d.idempotency[input.IdempotencyKey]; found {
		record, exists := d.targets[prior]
		d.mu.Unlock()
		if !exists || prior != key {
			return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "cuttlefish.create", "idempotency_key", "was used for another device generation", nil)
		}
		return ports.TargetResult{Status: record.status, Created: false}, nil
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
	instance, state, err := d.createInstance(ctx, plan)
	if err != nil {
		if mustRetainAllocation(err) {
			keepAllocation = true
		}
		return ports.TargetResult{}, err
	}
	status := ports.TargetStatus{TargetID: plan.TargetID, Generation: plan.Generation, Kind: domain.TargetAndroidVirtualDevice, State: domain.TargetGenerationReady, Ready: state.Ready(), RuntimeID: instance.RuntimeID, DeviceSerial: instance.Allocation.Serial, ObservedAt: d.now().UTC()}
	d.mu.Lock()
	d.targets[key] = deviceRecord{input: input, plan: plan, instance: instance, status: status}
	d.idempotency[input.IdempotencyKey] = key
	d.mu.Unlock()
	keepAllocation = true
	return ports.TargetResult{Status: status, Created: true}, nil
}

func (d *Driver) createInstance(ctx context.Context, plan VirtualDevicePlan) (Instance, ReadinessState, error) {
	instance, err := d.backend.Create(ctx, plan)
	if err != nil {
		return Instance{}, ReadinessState{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "backend.create", "device creation failed", err)
	}
	if err := d.backend.Start(ctx, instance); err != nil {
		cause := domain.NewError(domain.CodeUnavailable, "cuttlefish.create", "backend.start", "device start failed", err)
		return Instance{}, ReadinessState{}, d.cleanupFailedInstance(instance, cause)
	}
	state, err := d.backend.WaitReady(ctx, instance)
	if err != nil || !state.Ready() {
		cause := domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.create", "readiness", "multi-signal Android readiness was not reached", err)
		return Instance{}, ReadinessState{}, d.cleanupFailedInstance(instance, cause)
	}
	if !instance.Fingerprint.Compatible(plan.Fingerprint) || instance.Allocation.Serial != plan.Allocation.Serial || filepath.Clean(instance.StateDirectory) != filepath.Clean(plan.StateDirectory) || filepath.Clean(instance.SystemImageDirectory) != filepath.Clean(plan.SystemImageDirectory) {
		cause := domain.NewError(domain.CodeIntegrityViolation, "cuttlefish.create", "instance", "backend returned a different reset fingerprint or serial", nil)
		return Instance{}, ReadinessState{}, d.cleanupFailedInstance(instance, cause)
	}
	return instance, state, nil
}

func (d *Driver) cleanupInstance(instance Instance) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return d.backend.Destroy(ctx, instance)
}

type instanceCleanupFailure struct{ cause error }

func (e *instanceCleanupFailure) Error() string { return e.cause.Error() }
func (e *instanceCleanupFailure) Unwrap() error { return e.cause }

func (d *Driver) cleanupFailedInstance(instance Instance, cause error) error {
	if cleanupErr := d.cleanupInstance(instance); cleanupErr != nil {
		return &instanceCleanupFailure{cause: errors.Join(cause, cleanupErr)}
	}
	return cause
}

func mustRetainAllocation(err error) bool {
	var cleanupFailure *instanceCleanupFailure
	return errors.As(err, &cleanupFailure)
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
	if err := ports.RequireDeadline(ctx, "cuttlefish.prepare_run"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := input.Validate(); err != nil {
		return ports.PreparedTargetRun{}, err
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
		return runevidence.ClonePrepared(run.prepared), nil
	}
	d.mu.Unlock()
	directory := filepath.Join(device.plan.StateDirectory, "runs", spec.ID.String())
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	scope, err := deviceproxy.IssueScope(spec.LeaseID, spec.TargetID, spec.TargetGeneration, spec.ID, device.instance.Allocation.Serial, d.random)
	if err != nil {
		_ = os.RemoveAll(directory)
		return ports.PreparedTargetRun{}, err
	}
	if err := d.files.PrepareRun(ctx, scope, device.instance.Allocation); err != nil {
		cause := d.cleanupPreparedRun(scope, device.instance.Allocation, directory, err)
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.prepare_run", "device_directory", "could not prepare the scoped Android run directory", cause)
	}
	if err := d.materializeRun(ctx, scope, device.instance.Allocation, input.Material); err != nil {
		return ports.PreparedTargetRun{}, d.cleanupPreparedRun(scope, device.instance.Allocation, directory, err)
	}
	sourceInstance := device.instance.RuntimeID
	if sourceInstance == "" {
		sourceInstance = device.instance.Allocation.Serial
	}
	preparedAt := runevidence.AtOrAfter(d.now(), spec.CreatedAt)
	prepared := ports.PreparedTargetRun{
		RunID: spec.ID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration,
		MaterializationDigest: spec.MaterializationDigest,
		RequiredCoverage:      append([]string(nil), input.RequiredCoverage...),
		Attachment:            ports.ObservationAttachment{TargetKind: domain.TargetAndroidVirtualDevice, RuntimeID: sourceInstance},
		PreparedAt:            preparedAt,
	}
	d.mu.Lock()
	currentDevice, exists := d.targets[deviceKey(spec.TargetID, spec.TargetGeneration)]
	if !exists || currentDevice.status.State == domain.TargetGenerationQuarantined {
		d.mu.Unlock()
		cause := d.cleanupPreparedRun(scope, device.instance.Allocation, directory, nil)
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeInvalidState, "cuttlefish.prepare_run", "target", "target generation was quarantined while material was prepared", cause)
	}
	d.runs[spec.ID.String()] = &runRecord{plan: runevidence.ClonePlan(input), scope: scope, allocation: device.instance.Allocation, sourceInstance: sourceInstance, directory: directory, prepared: prepared, transports: make(map[*androidTransport]struct{})}
	d.idempotency[input.IdempotencyKey] = spec.ID.String()
	d.mu.Unlock()
	return runevidence.ClonePrepared(prepared), nil
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

func (d *Driver) cleanupPreparedRun(scope deviceproxy.Scope, allocation Allocation, directory string, cause error) error {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deviceErr := d.files.RemoveRun(cleanup, scope, allocation)
	hostErr := os.RemoveAll(directory)
	return errors.Join(cause, deviceErr, hostErr)
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
		if run.stopped || run.quarantined {
			d.mu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "run", "run was stopped or quarantined", nil)
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
		d.mu.Lock()
		current := d.runs[runID.String()]
		if current != run {
			finishRunStartLocked(run)
			d.mu.Unlock()
			return domain.NewError(domain.CodeNotFound, "cuttlefish.start_run", "run_id", "run was removed while awaiting collector readiness", nil)
		}
		if current.stopped || current.quarantined {
			finishRunStartLocked(current)
			d.mu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "run", "run stopped or was quarantined while awaiting collector readiness", nil)
		}
		device := d.targets[deviceKey(current.scope.TargetID, current.scope.Generation)]
		if device.status.State == domain.TargetGenerationQuarantined {
			finishRunStartLocked(current)
			d.mu.Unlock()
			return domain.NewError(domain.CodeInvalidState, "cuttlefish.start_run", "target", "target generation was quarantined while awaiting collector readiness", nil)
		}
		if readinessErr != nil {
			finishRunStartLocked(current)
			d.mu.Unlock()
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
			return domain.NewError(domain.CodeInternal, "cuttlefish.start_run", "observation", "could not encode the intrinsic start fact", encodeErr)
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
	if !run.started || run.stopped || run.quarantined {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.open_transport", "run", "run is not active", nil)
	}
	device, err := d.requireDevice(run.scope.TargetID, run.scope.Generation)
	if err != nil {
		return nil, err
	}
	if device.status.State == domain.TargetGenerationQuarantined {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.open_transport", "target", "target generation is quarantined", nil)
	}
	transport := &androidTransport{gateway: d.gateway, files: d.files, scope: run.scope, allocation: device.instance.Allocation}
	d.mu.Lock()
	current := d.runs[runID.String()]
	currentDevice := d.targets[deviceKey(run.scope.TargetID, run.scope.Generation)]
	if current != nil && current.started && !current.stopped && !current.quarantined && currentDevice.status.State != domain.TargetGenerationQuarantined {
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
	return d.stopRun(ctx, runID, cause)
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
	_, _ = d.stopRun(cleanupContext, runID, runevidence.CauseDurationExceeded)
}

func (d *Driver) stopRun(ctx context.Context, runID domain.TargetRunID, cause runevidence.Cause) (ports.TargetRunStopReceipt, error) {
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
	if !current.stopped {
		result, buildErr := d.buildStopReceipt(current, cause)
		if buildErr != nil {
			d.mu.Unlock()
			return ports.TargetRunStopReceipt{}, buildErr
		}
		current.stopped = true
		stored := cloneStopReceipt(result)
		current.receipt = &stored
	}
	result := cloneStopReceipt(*current.receipt)
	if current.startCancel != nil {
		current.startCancel()
	}
	if current.deadlineCancel != nil {
		current.deadlineCancel()
		current.deadlineCancel = nil
	}
	transports := make([]*androidTransport, 0, len(current.transports))
	for transport := range current.transports {
		transports = append(transports, transport)
	}
	current.transports = nil
	if current.cleanupComplete {
		d.mu.Unlock()
		return result, nil
	}
	if current.cleanupRunning {
		done := current.cleanupDone
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-done:
			d.mu.Lock()
			cleanupErr := current.cleanupErr
			d.mu.Unlock()
			return result, cleanupErr
		}
	}
	current.cleanupRunning = true
	current.cleanupDone = make(chan struct{})
	d.mu.Unlock()

	cleanupErr := d.cleanupRunResources(ctx, current, transports)
	d.mu.Lock()
	current.cleanupRunning = false
	current.cleanupComplete = cleanupErr == nil
	current.cleanupErr = cleanupErr
	close(current.cleanupDone)
	d.mu.Unlock()
	return result, cleanupErr
}

func (d *Driver) buildStopReceipt(run *runRecord, cause runevidence.Cause) (ports.TargetRunStopReceipt, error) {
	stoppedAt := runevidence.AtOrAfter(d.now(), run.prepared.PreparedAt)
	outcome := ports.RunCompleted
	failureKind := ports.TargetRunFailureNone
	terminalKind := "target.run.stopped"
	switch cause {
	case runevidence.CauseNeverStarted:
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureNeverStarted
		terminalKind = "target.run.never_started"
	case runevidence.CauseDurationExceeded:
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureDurationExceeded
		terminalKind = "target.run.duration_exceeded"
	}
	if !run.started {
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureNeverStarted
		terminalKind = "target.run.never_started"
	} else {
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
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, stoppedAt)
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

func cloneStopReceipt(receipt ports.TargetRunStopReceipt) ports.TargetRunStopReceipt {
	receipt.Observations = append([]ports.TargetRunObservation(nil), receipt.Observations...)
	for index := range receipt.Observations {
		receipt.Observations[index].Payload = append([]byte(nil), receipt.Observations[index].Payload...)
	}
	return receipt
}

func (d *Driver) cleanupRunResources(ctx context.Context, run *runRecord, transports []*androidTransport) error {
	errorsFound := make([]error, 0, len(transports)+2)
	for _, transport := range transports {
		if err := transport.Close(); err != nil {
			errorsFound = append(errorsFound, err)
		}
	}
	if err := d.files.RemoveRun(ctx, run.scope, run.allocation); err != nil {
		errorsFound = append(errorsFound, err)
	}
	if run.directory == "" {
		errorsFound = append(errorsFound, fmt.Errorf("prepared run has no host directory"))
	} else if err := os.RemoveAll(run.directory); err != nil {
		errorsFound = append(errorsFound, err)
	}
	return errors.Join(errorsFound...)
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
	instance, _, err := d.createInstance(ctx, next)
	if err != nil {
		if mustRetainAllocation(err) {
			keep = true
		}
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "replacement", "could not create a reachable next device generation", err)
	}
	if err := d.backend.Destroy(ctx, previous.instance); err != nil {
		restoreErr := d.restoreInstance(previous.instance)
		if restoreErr != nil {
			status := ports.TargetStatus{TargetID: targetID, Generation: reset.NextGeneration, Kind: domain.TargetAndroidVirtualDevice, State: domain.TargetGenerationReady, Ready: true, RuntimeID: instance.RuntimeID, DeviceSerial: instance.Allocation.Serial, ObservedAt: d.now().UTC()}
			result := ports.TargetResult{Status: status, Created: true}
			outcomeErr := domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "retire_previous", "replacement is ready but the previous device could neither retire nor be restored", errors.Join(err, restoreErr))
			d.commitReset(previous, next, instance, status, targetID, reset, result, outcomeErr)
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
		}
		return ports.TargetResult{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.reset", "retire_previous", "could not retire the previous device; replacement rollback was attempted", errors.Join(err, restoreErr, rollbackErr))
	}
	status := ports.TargetStatus{TargetID: targetID, Generation: reset.NextGeneration, Kind: domain.TargetAndroidVirtualDevice, State: domain.TargetGenerationReady, Ready: true, RuntimeID: instance.RuntimeID, DeviceSerial: instance.Allocation.Serial, ObservedAt: d.now().UTC()}
	result := ports.TargetResult{Status: status, Created: true}
	d.commitReset(previous, next, instance, status, targetID, reset, result, nil)
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
		return prior.evidence, nil
	}
	d.mu.Unlock()
	device, err := d.requireDevice(plan.Target.ID, plan.Target.Generation)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	quarantiner, supported := d.backend.(BackendQuarantiner)
	if !supported {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeCapabilityUnavailable, "cuttlefish.quarantine", "backend", "backend cannot prove execution stop and network isolation", nil)
	}
	state, err := quarantiner.Quarantine(ctx, device.instance)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeUnavailable, "cuttlefish.quarantine", "backend", "backend quarantine failed", err)
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
	transports := make([]*androidTransport, 0)
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
		for transport := range run.transports {
			transports = append(transports, transport)
		}
		run.transports = nil
	}
	if d.quarantines == nil {
		d.quarantines = make(map[string]quarantineOutcome)
	}
	d.quarantines[plan.IdempotencyKey] = quarantineOutcome{plan: plan, evidence: evidence}
	d.mu.Unlock()
	for _, transport := range transports {
		_ = transport.Close()
	}
	return evidence, nil
}

func (d *Driver) commitReset(previous deviceRecord, next VirtualDevicePlan, instance Instance, status ports.TargetStatus, targetID domain.TargetID, reset ports.ResetPlan, result ports.TargetResult, outcomeErr error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removeRunsLocked(targetID, reset.Previous.Generation)
	delete(d.targets, deviceKey(targetID, reset.Previous.Generation))
	delete(d.idempotency, previous.input.IdempotencyKey)
	d.targets[deviceKey(targetID, reset.NextGeneration)] = deviceRecord{input: previous.input, plan: next, instance: instance, status: status}
	d.resetResults[reset.IdempotencyKey] = resetOutcome{targetID: targetID, plan: reset, result: result, err: outcomeErr}
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
	d.mu.Lock()
	device, found := d.targets[deviceKey(ref.ID, ref.Generation)]
	d.mu.Unlock()
	if !found {
		return nil
	}
	if d.hasUnstoppedRun(ref.ID, ref.Generation) {
		return domain.NewError(domain.CodeFailedPrecondition, "cuttlefish.destroy", "run", "all prepared runs must stop before destroy", nil)
	}
	if err := d.cleanupStoppedRuns(ctx, ref.ID, ref.Generation); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "run_cleanup", "stopped run resources could not be cleaned", err)
	}
	if err := d.backend.Destroy(ctx, device.instance); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "backend", "device destruction failed", err)
	}
	if err := os.RemoveAll(device.plan.StateDirectory); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "state_directory", "target state directory cleanup failed", err)
	}
	if err := d.allocator.Release(ctx, device.plan.Allocation); err != nil {
		return domain.NewError(domain.CodeUnavailable, "cuttlefish.destroy", "allocation", "device endpoint release failed", err)
	}
	d.mu.Lock()
	delete(d.targets, deviceKey(ref.ID, ref.Generation))
	delete(d.idempotency, device.input.IdempotencyKey)
	d.removeRunsLocked(ref.ID, ref.Generation)
	for key, outcome := range d.resetResults {
		if outcome.result.Status.TargetID == ref.ID && outcome.result.Status.Generation == ref.Generation {
			delete(d.resetResults, key)
		}
	}
	for key, outcome := range d.quarantines {
		if outcome.plan.Target == ref {
			delete(d.quarantines, key)
		}
	}
	d.mu.Unlock()
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
		if _, err := d.stopRun(ctx, runID, runevidence.CauseCollectorEvidenceUnavailable); err != nil {
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
