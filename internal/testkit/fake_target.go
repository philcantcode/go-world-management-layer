package testkit

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type fakeTargetRecord struct {
	target     domain.Target
	status     ports.TargetStatus
	template   ports.TargetTemplate
	owner      string
	quarantine *ports.TargetQuarantineEvidence
}

type fakeTargetRun struct {
	plan        ports.TargetRunPlan
	prepared    ports.PreparedTargetRun
	started     bool
	stopped     bool
	quarantined bool
	receipt     ports.TargetRunStopReceipt
	transports  []*FakeTargetTransport
}

// FakeTargetDriver is a deterministic, stateful implementation of TargetDriver.
// Its fault points use the target.<method>.before/after convention. An after
// fault models an ambiguous outcome: retrying with the same idempotency key
// observes the already committed result.
type FakeTargetDriver struct {
	mu                 sync.Mutex
	capabilities       domain.CapabilityFingerprint
	clock              *Clock
	faults             *FaultInjector
	tracker            *OwnershipTracker
	targets            map[ports.TargetRef]fakeTargetRecord
	current            map[domain.TargetID]ports.TargetRef
	createRequests     map[string]string
	createResults      map[string]ports.TargetResult
	resetRequests      map[string]string
	resetResults       map[string]ports.TargetResult
	quarantineRequests map[string]string
	quarantineResults  map[string]ports.TargetQuarantineEvidence
	runRequests        map[string]string
	runs               map[domain.TargetRunID]*fakeTargetRun
	transportNo        uint64
}

func NewFakeTargetDriver(capabilities domain.CapabilityFingerprint, clock *Clock, faults *FaultInjector, tracker *OwnershipTracker) *FakeTargetDriver {
	if clock == nil {
		clock = NewClock(time.Time{})
	}
	if faults == nil {
		faults = NewFaultInjector()
	}
	if tracker == nil {
		tracker = NewOwnershipTracker()
	}
	return &FakeTargetDriver{
		capabilities:       capabilities,
		clock:              clock,
		faults:             faults,
		tracker:            tracker,
		targets:            make(map[ports.TargetRef]fakeTargetRecord),
		current:            make(map[domain.TargetID]ports.TargetRef),
		createRequests:     make(map[string]string),
		createResults:      make(map[string]ports.TargetResult),
		resetRequests:      make(map[string]string),
		resetResults:       make(map[string]ports.TargetResult),
		quarantineRequests: make(map[string]string),
		quarantineResults:  make(map[string]ports.TargetQuarantineEvidence),
		runRequests:        make(map[string]string),
		runs:               make(map[domain.TargetRunID]*fakeTargetRun),
	}
}

func (d *FakeTargetDriver) Probe(ctx context.Context, template ports.TargetTemplate) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := template.Validate(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := d.faults.Check("target.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	return d.capabilities, nil
}

func (d *FakeTargetDriver) Create(ctx context.Context, plan ports.TargetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.create"); err != nil {
		return ports.TargetResult{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetResult{}, err
	}
	if err := d.faults.Check("target.create.before"); err != nil {
		return ports.TargetResult{}, err
	}
	ref := ports.TargetRef{ID: plan.Target.ID(), Generation: plan.Generation.Spec().Generation}
	signature := targetPlanSignature(plan)
	d.mu.Lock()
	defer d.mu.Unlock()
	if previous, found := d.createRequests[plan.IdempotencyKey]; found {
		if previous != signature {
			return ports.TargetResult{}, idempotencyConflict("fake_target.create")
		}
		return d.createResults[plan.IdempotencyKey], nil
	}
	if _, found := d.targets[ref]; found {
		return ports.TargetResult{}, domain.NewError(domain.CodeAlreadyExists, "fake_target.create", "generation", "already exists", nil)
	}
	if err := d.tracker.Acquire("target", targetOwnershipID(ref), plan.LeaseID.String()); err != nil {
		return ports.TargetResult{}, err
	}
	status := ports.TargetStatus{
		TargetID: ref.ID, Generation: ref.Generation, Kind: plan.Target.Kind(),
		State: domain.TargetGenerationReady, Ready: true,
		RuntimeID: "runtime-" + ref.ID.String(), CgroupID: "cgroup-" + ref.ID.String(), ObservedAt: d.clock.Now(),
	}
	if plan.Target.Kind() == domain.TargetAndroidVirtualDevice || plan.Target.Kind() == domain.TargetPhysicalDevice {
		status.DeviceSerial = "device-" + ref.ID.String()
	}
	record := fakeTargetRecord{target: plan.Target, status: status, template: plan.Template, owner: plan.LeaseID.String()}
	result := ports.TargetResult{Status: status, Created: true}
	d.targets[ref] = record
	d.current[ref.ID] = ref
	d.createRequests[plan.IdempotencyKey] = signature
	d.createResults[plan.IdempotencyKey] = result
	if err := d.faults.Check("target.create.after"); err != nil {
		return ports.TargetResult{}, err
	}
	return result, nil
}

func (d *FakeTargetDriver) PrepareRun(ctx context.Context, plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.prepare_run"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	if err := d.faults.Check("target.prepare_run.before"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	spec := plan.Run.Spec()
	signature := targetRunPlanSignature(plan)
	d.mu.Lock()
	defer d.mu.Unlock()
	if previous, found := d.runRequests[plan.IdempotencyKey]; found {
		if previous != signature {
			return ports.PreparedTargetRun{}, idempotencyConflict("fake_target.prepare_run")
		}
		return d.runs[spec.ID].prepared, nil
	}
	ref := ports.TargetRef{ID: spec.TargetID, Generation: spec.TargetGeneration}
	record, found := d.targets[ref]
	if !found || !record.status.Ready {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "fake_target.prepare_run", "target", "generation is not ready", nil)
	}
	if _, found := d.runs[spec.ID]; found {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeAlreadyExists, "fake_target.prepare_run", "target_run_id", "already exists", nil)
	}
	prepared := ports.PreparedTargetRun{
		RunID: spec.ID, TargetID: spec.TargetID, TargetGeneration: spec.TargetGeneration,
		MaterializationDigest: spec.MaterializationDigest,
		RequiredCoverage:      append([]string(nil), plan.RequiredCoverage...),
		Attachment:            ports.ObservationAttachment{TargetKind: record.target.Kind(), RuntimeID: record.status.RuntimeID},
		PreparedAt:            d.clock.Now(),
	}
	d.runs[spec.ID] = &fakeTargetRun{plan: cloneTargetRunPlan(plan), prepared: prepared}
	d.runRequests[plan.IdempotencyKey] = signature
	if err := d.tracker.Acquire("target_run", spec.ID.String(), spec.LeaseID.String()); err != nil {
		delete(d.runs, spec.ID)
		delete(d.runRequests, plan.IdempotencyKey)
		return ports.PreparedTargetRun{}, err
	}
	if err := d.faults.Check("target.prepare_run.after"); err != nil {
		return ports.PreparedTargetRun{}, err
	}
	return prepared, nil
}

func (d *FakeTargetDriver) StartRun(ctx context.Context, runID domain.TargetRunID) error {
	if err := ports.RequireDeadline(ctx, "fake_target.start_run"); err != nil {
		return err
	}
	if runID.IsZero() {
		return domain.NewError(domain.CodeInvalidArgument, "fake_target.start_run", "target_run_id", "must be set", nil)
	}
	if err := d.faults.Check("target.start_run.before"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	run, found := d.runs[runID]
	if !found {
		return domain.NewError(domain.CodeNotFound, "fake_target.start_run", "target_run_id", "not prepared", nil)
	}
	if run.stopped || run.quarantined {
		return domain.NewError(domain.CodeFailedPrecondition, "fake_target.start_run", "target_run_id", "already stopped", nil)
	}
	run.started = true
	return d.faults.Check("target.start_run.after")
}

func (d *FakeTargetDriver) OpenTransport(ctx context.Context, runID domain.TargetRunID) (ports.TargetTransport, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.open_transport"); err != nil {
		return nil, err
	}
	if err := d.faults.Check("target.open_transport"); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	run, found := d.runs[runID]
	if !found || !run.started || run.stopped || run.quarantined {
		return nil, domain.NewError(domain.CodeFailedPrecondition, "fake_target.open_transport", "target_run_id", "run is not active", nil)
	}
	d.transportNo++
	id := fmt.Sprintf("%s/%d", runID, d.transportNo)
	if err := d.tracker.Acquire("target_transport", id, runID.String()); err != nil {
		return nil, err
	}
	transport := newFakeTargetTransport(runID, d.tracker, func() { _ = d.tracker.Release("target_transport", id, runID.String()) })
	run.transports = append(run.transports, transport)
	return transport, nil
}

func (d *FakeTargetDriver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.stop_run"); err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	if !mode.IsValid() {
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeInvalidArgument, "fake_target.stop_run", "mode", "is not recognized", nil)
	}
	if err := d.faults.Check("target.stop_run.before"); err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	run, found := d.runs[runID]
	if !found {
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeNotFound, "fake_target.stop_run", "target_run_id", "not prepared", nil)
	}
	if run.stopped {
		return cloneTargetRunStopReceipt(run.receipt), nil
	}
	for _, transport := range run.transports {
		_ = transport.Close()
	}
	if run.receipt.RunID.IsZero() {
		receipt, err := d.defaultRunReceipt(run)
		if err != nil {
			return ports.TargetRunStopReceipt{}, err
		}
		run.receipt = receipt
	}
	run.stopped = true
	_ = d.tracker.Release("target_run", runID.String(), run.plan.Run.Spec().LeaseID.String())
	if err := d.faults.Check("target.stop_run.after"); err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	return cloneTargetRunStopReceipt(run.receipt), nil
}

func (d *FakeTargetDriver) Quarantine(ctx context.Context, plan ports.TargetQuarantinePlan) (ports.TargetQuarantineEvidence, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.quarantine"); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	if err := d.faults.Check("target.quarantine.before"); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	signature := deterministicPlanSignature(plan)
	d.mu.Lock()
	if prior, found := d.quarantineRequests[plan.IdempotencyKey]; found {
		result := d.quarantineResults[plan.IdempotencyKey]
		d.mu.Unlock()
		if prior != signature {
			return ports.TargetQuarantineEvidence{}, idempotencyConflict("fake_target.quarantine")
		}
		return result, nil
	}
	record, found := d.targets[plan.Target]
	if !found {
		d.mu.Unlock()
		return ports.TargetQuarantineEvidence{}, domain.NewError(domain.CodeNotFound, "fake_target.quarantine", "target", "generation not found", nil)
	}
	var evidence ports.TargetQuarantineEvidence
	if record.quarantine != nil {
		evidence = *record.quarantine
	} else {
		observedAt := d.clock.Now().UTC()
		if observedAt.IsZero() {
			observedAt = time.Unix(1, 0).UTC()
		}
		evidence = ports.TargetQuarantineEvidence{
			Target: plan.Target, RuntimeID: record.status.RuntimeID, ExecutionStopped: true,
			NetworkUnreachable: true, StatePreserved: true, ObservedAt: observedAt,
		}
		record.status.State = domain.TargetGenerationQuarantined
		record.status.Ready = false
		record.status.ObservedAt = observedAt
		record.quarantine = &evidence
		d.targets[plan.Target] = record
		for _, run := range d.runs {
			spec := run.plan.Run.Spec()
			if spec.TargetID != plan.Target.ID || spec.TargetGeneration != plan.Target.Generation {
				continue
			}
			run.quarantined = true
			for _, transport := range run.transports {
				_ = transport.Close()
			}
			run.transports = nil
		}
	}
	d.quarantineRequests[plan.IdempotencyKey] = signature
	d.quarantineResults[plan.IdempotencyKey] = evidence
	d.mu.Unlock()
	if err := d.faults.Check("target.quarantine.after"); err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	return evidence, nil
}

func (d *FakeTargetDriver) Reset(ctx context.Context, targetID domain.TargetID, plan ports.ResetPlan) (ports.TargetResult, error) {
	if err := ports.RequireDeadline(ctx, "fake_target.reset"); err != nil {
		return ports.TargetResult{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.TargetResult{}, err
	}
	if targetID.IsZero() || plan.Previous.ID != targetID {
		return ports.TargetResult{}, domain.NewError(domain.CodeConflict, "fake_target.reset", "target_id", "does not match reset scope", nil)
	}
	if err := d.faults.Check("target.reset.before"); err != nil {
		return ports.TargetResult{}, err
	}
	signature := deterministicPlanSignature(targetID, plan)
	d.mu.Lock()
	defer d.mu.Unlock()
	if previous, found := d.resetRequests[plan.IdempotencyKey]; found {
		if previous != signature {
			return ports.TargetResult{}, idempotencyConflict("fake_target.reset")
		}
		return d.resetResults[plan.IdempotencyKey], nil
	}
	record, found := d.targets[plan.Previous]
	if !found {
		return ports.TargetResult{}, domain.NewError(domain.CodeNotFound, "fake_target.reset", "target", "previous generation not found", nil)
	}
	for _, run := range d.runs {
		spec := run.plan.Run.Spec()
		if spec.TargetID == targetID && !run.stopped {
			return ports.TargetResult{}, domain.NewError(domain.CodeFailedPrecondition, "fake_target.reset", "target", "has an unfinished run", nil)
		}
	}
	nextTarget, err := record.target.AdvanceGeneration(record.target.Revision(), plan.NextGeneration, d.clock.Now())
	if err != nil {
		return ports.TargetResult{}, err
	}
	nextRef := ports.TargetRef{ID: targetID, Generation: plan.NextGeneration}
	if err := d.tracker.Release("target", targetOwnershipID(plan.Previous), record.owner); err != nil {
		return ports.TargetResult{}, err
	}
	if err := d.tracker.Acquire("target", targetOwnershipID(nextRef), record.owner); err != nil {
		_ = d.tracker.Acquire("target", targetOwnershipID(plan.Previous), record.owner)
		return ports.TargetResult{}, err
	}
	delete(d.targets, plan.Previous)
	record.target = nextTarget
	record.status.Generation = plan.NextGeneration
	record.status.State = domain.TargetGenerationReady
	record.status.Ready = true
	record.status.RuntimeID = "runtime-" + targetID.String() + fmt.Sprintf("-%d", plan.NextGeneration)
	record.status.ObservedAt = d.clock.Now()
	d.targets[nextRef] = record
	d.current[targetID] = nextRef
	result := ports.TargetResult{Status: record.status, Created: true}
	d.resetRequests[plan.IdempotencyKey] = signature
	d.resetResults[plan.IdempotencyKey] = result
	if err := d.faults.Check("target.reset.after"); err != nil {
		return ports.TargetResult{}, err
	}
	return result, nil
}

func (d *FakeTargetDriver) Destroy(ctx context.Context, ref ports.TargetRef) error {
	if err := ports.RequireDeadline(ctx, "fake_target.destroy"); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if err := d.faults.Check("target.destroy"); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, found := d.targets[ref]
	if !found {
		return nil
	}
	for _, run := range d.runs {
		spec := run.plan.Run.Spec()
		if spec.TargetID == ref.ID && spec.TargetGeneration == ref.Generation && !run.stopped {
			return domain.NewError(domain.CodeFailedPrecondition, "fake_target.destroy", "target", "has an unfinished run", nil)
		}
	}
	delete(d.targets, ref)
	if d.current[ref.ID] == ref {
		delete(d.current, ref.ID)
	}
	return d.tracker.Release("target", targetOwnershipID(ref), record.owner)
}

func (d *FakeTargetDriver) defaultRunReceipt(run *fakeTargetRun) (ports.TargetRunStopReceipt, error) {
	stoppedAt := d.clock.Now()
	changes, err := domain.NewChangeSet(domain.ChangeScopeTarget, nil, domain.InitialRevision, stoppedAt)
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	outcome := ports.RunCompleted
	failureKind := ports.TargetRunFailureNone
	startedAt := run.prepared.PreparedAt
	observations := []ports.TargetRunObservation{
		{Kind: "target.run.started", ObservedAt: startedAt},
		{Kind: "target.run.stopped", ObservedAt: stoppedAt},
	}
	if !run.started {
		outcome = ports.RunFailed
		failureKind = ports.TargetRunFailureNeverStarted
		startedAt = time.Time{}
		observations = []ports.TargetRunObservation{{Kind: "target.run.never_started", ObservedAt: stoppedAt}}
	}
	receipt := ports.TargetRunStopReceipt{
		RunID: run.plan.Run.ID(), Outcome: outcome, FailureKind: failureKind,
		StartedAt: startedAt, StoppedAt: stoppedAt, Observations: observations, TargetChanges: changes,
	}
	return receipt, receipt.Validate()
}

func cloneTargetRunPlan(plan ports.TargetRunPlan) ports.TargetRunPlan {
	plan.RequiredCoverage = append([]string(nil), plan.RequiredCoverage...)
	plan.Collectors = append([]ports.CollectorSpec(nil), plan.Collectors...)
	plan.Material = append([]ports.TargetMaterialPlan(nil), plan.Material...)
	return plan
}

func cloneTargetRunStopReceipt(receipt ports.TargetRunStopReceipt) ports.TargetRunStopReceipt {
	receipt.Observations = append([]ports.TargetRunObservation(nil), receipt.Observations...)
	for index := range receipt.Observations {
		receipt.Observations[index].Payload = append([]byte(nil), receipt.Observations[index].Payload...)
	}
	entries := receipt.TargetChanges.Entries()
	cloned, err := domain.NewChangeSet(receipt.TargetChanges.Scope(), entries, receipt.TargetChanges.WorkspaceRevision(), receipt.TargetChanges.SealedAt())
	if err == nil {
		receipt.TargetChanges = cloned
	}
	return receipt
}

type targetMaterialRequestIdentity struct {
	Artifact      domain.ArtifactReferenceSpec
	LogicalPath   string
	Mode          uint32
	ContentDigest domain.Digest
	ContentSize   int64
}

func targetPlanSignature(plan ports.TargetPlan) string {
	return deterministicPlanSignature(
		plan.IdempotencyKey,
		plan.LeaseID,
		plan.Target,
		plan.Generation,
		plan.Template,
		plan.PolicyDigest,
		plan.CapabilityFingerprintDigest,
		plan.Resources,
	)
}

func targetRunPlanSignature(plan ports.TargetRunPlan) string {
	material := make([]targetMaterialRequestIdentity, len(plan.Material))
	for index, entry := range plan.Material {
		material[index] = targetMaterialRequestIdentity{
			Artifact:      entry.Artifact.Spec(),
			LogicalPath:   entry.LogicalPath,
			Mode:          entry.Mode,
			ContentDigest: entry.Content.Digest(),
			ContentSize:   entry.Content.Size(),
		}
	}
	return deterministicPlanSignature(
		plan.IdempotencyKey,
		plan.Run,
		plan.RequiredCoverage,
		plan.Collectors,
		material,
		plan.MaximumDuration,
	)
}

// deterministicPlanSignature is intentionally process-local: fake-driver
// request histories are never persisted. Go-syntax formatting preserves every
// aggregate field (including unexported domain state), sorts string-keyed maps,
// and the length framing prevents distinct field sequences from colliding.
func deterministicPlanSignature(fields ...any) string {
	var canonical strings.Builder
	for _, field := range fields {
		value := fmt.Sprintf("%#v", field)
		_, _ = fmt.Fprintf(&canonical, "%d:%s", len(value), value)
	}
	return domain.NewDigest([]byte(canonical.String())).String()
}

func targetOwnershipID(ref ports.TargetRef) string {
	return fmt.Sprintf("%s/%d", ref.ID, ref.Generation)
}

func idempotencyConflict(operation string) error {
	return domain.NewError(domain.CodeConflict, operation, "idempotency_key", "was reused with a different plan", nil)
}

var _ ports.TargetDriver = (*FakeTargetDriver)(nil)
