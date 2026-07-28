// Package process implements a reusable process-backed observer driver. Tool
// binaries and lifecycle probes are injected; no collector command is accepted
// from an untrusted CollectorPlan.
package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const defaultCleanupGrace = 5 * time.Second

const (
	CollectorStdoutRole = ports.CollectorStdoutArtifactRole
	CollectorStderrRole = ports.CollectorStderrArtifactRole
)

type Readiness interface {
	Await(context.Context, ports.CollectorPlan) error
}

type ReadinessFunc func(context.Context, ports.CollectorPlan) error

func (f ReadinessFunc) Await(ctx context.Context, plan ports.CollectorPlan) error {
	return f(ctx, plan)
}

type Adapter struct {
	Name                string
	Version             string
	ConfigurationDigest domain.Digest
	SignalFamily        string
	Placement           domain.CollectorPlacement
	CoverageLevel       domain.CoverageLevel
	Program             string
	Args                []string
	Environment         map[string]string
	VersionArgs         []string
	Readiness           Readiness
}

func (a Adapter) Validate() error {
	if a.Name == "" || a.Version == "" || a.ConfigurationDigest.IsZero() || a.SignalFamily == "" || !a.Placement.IsValid() || !a.CoverageLevel.IsValid() || a.CoverageLevel == domain.CoverageLevelUnknown || a.Program == "" || a.Readiness == nil {
		return fmt.Errorf("observer adapter requires identity, version, configuration digest, signal, placement, concrete coverage, program, and readiness")
	}
	return nil
}

// OutputCapture is an authority-owned, collector-scoped evidence transaction.
// Stdout and Stderr must remain writable until the driver closes them. Finalize
// must be idempotent and may be retried after an error; a successful call must
// return immutable references for the captured streams. Abort must also be
// idempotent and permanently discards a start that never became ready.
type OutputCapture interface {
	Stdout() io.WriteCloser
	Stderr() io.WriteCloser
	Finalize(context.Context) ([]domain.ArtifactReference, error)
	Abort(context.Context) error
}

// OutputFactory opens an evidence transaction under the authority represented
// by the trusted CollectorPlan. Implementations must not derive repository
// destinations from collector process output.
type OutputFactory interface {
	Open(context.Context, ports.CollectorPlan) (OutputCapture, error)
	ReconcileInterruptedRun(context.Context, ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error)
}

type Config struct {
	Runner       command.Runner
	Starter      command.Starter
	Adapters     []Adapter
	Outputs      OutputFactory
	Now          func() time.Time
	CleanupGrace time.Duration
}

type Driver struct {
	runner                 command.Runner
	starter                command.Starter
	adapters               map[string]Adapter
	outputs                OutputFactory
	now                    func() time.Time
	cleanupGrace           time.Duration
	crashCleanupGuaranteed bool

	startMu     sync.Mutex
	mu          sync.Mutex
	records     map[string]*record
	idempotency map[string]string
}

type record struct {
	plan    ports.CollectorPlan
	adapter Adapter
	process command.Process
	capture OutputCapture
	exited  chan struct{}
	done    chan struct{}

	mu                sync.Mutex
	waitErr           error
	stdoutErr         error
	stderrErr         error
	stopRequested     bool
	unexpectedExit    bool
	teardownConfirmed bool
	lifecycleErr      error
	stoppedAt         time.Time
	coverage          domain.CollectorCoverage
	result            *ports.CollectorResult
	resultErr         error
	attempt           *stopAttempt
}

type stopAttempt struct {
	done   chan struct{}
	result ports.CollectorResult
	err    error
}

func New(config Config) (*Driver, error) {
	if config.Runner == nil {
		config.Runner = command.OS{}
	}
	crashCleanupGuaranteed := false
	if config.Starter == nil {
		config.Starter = detachedStarter{}
		crashCleanupGuaranteed = collectorParentDeathSignalGuaranteed()
	}
	if config.Outputs == nil {
		return nil, fmt.Errorf("observer output authority is required")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.CleanupGrace < 0 {
		return nil, fmt.Errorf("observer cleanup grace cannot be negative")
	}
	if config.CleanupGrace == 0 {
		config.CleanupGrace = defaultCleanupGrace
	}
	adapters := make(map[string]Adapter, len(config.Adapters))
	for _, adapter := range config.Adapters {
		if err := adapter.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := adapters[adapter.Name]; duplicate {
			return nil, fmt.Errorf("duplicate observer adapter %q", adapter.Name)
		}
		adapter.Args = append([]string(nil), adapter.Args...)
		adapter.VersionArgs = append([]string(nil), adapter.VersionArgs...)
		adapter.Environment = cloneMap(adapter.Environment)
		adapters[adapter.Name] = adapter
	}
	if len(adapters) == 0 {
		return nil, fmt.Errorf("at least one observer adapter is required")
	}
	return &Driver{
		runner:                 config.Runner,
		starter:                config.Starter,
		adapters:               adapters,
		outputs:                config.Outputs,
		now:                    config.Now,
		cleanupGrace:           config.CleanupGrace,
		crashCleanupGuaranteed: crashCleanupGuaranteed,
		records:                make(map[string]*record),
		idempotency:            make(map[string]string),
	}, nil
}

func NewDriver(config Config) (*Driver, error) { return New(config) }

func (d *Driver) Probe(ctx context.Context, requirement ports.ObservationRequirement) (domain.CapabilityFingerprint, error) {
	if err := ports.RequireDeadline(ctx, "observer.process.probe"); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	if err := requirement.Validate(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	capabilities := make(map[string]domain.Capability)
	evidence := make(map[string]string)
	for _, adapter := range d.adapters {
		if adapter.SignalFamily != requirement.SignalFamily || adapter.Placement != requirement.Placement {
			continue
		}
		result, err := d.runner.Run(ctx, command.Invocation{Program: adapter.Program, Args: adapter.VersionArgs})
		status := domain.CapabilitySupported
		reason := "probe succeeded"
		if err != nil {
			status = domain.CapabilityUnsupported
			reason = err.Error()
		}
		capability, createErr := domain.NewCapability(status, map[string]string{"placement": string(adapter.Placement), "coverage": string(adapter.CoverageLevel)}, map[string]string{"reason": reason})
		if createErr != nil {
			return domain.CapabilityFingerprint{}, createErr
		}
		capabilities["observer."+adapter.Name] = capability
		if len(result.Stdout) > 0 {
			evidence[adapter.Name+".version"] = boundedText(result.Stdout, 512)
		}
	}
	if len(capabilities) == 0 {
		capability, _ := domain.NewCapability(domain.CapabilityUnsupported, nil, map[string]string{"reason": "no configured adapter matches signal and placement"})
		capabilities["observer."+requirement.SignalFamily] = capability
	}
	if len(evidence) == 0 {
		evidence["probe"] = "completed"
	}
	return domain.NewCapabilityFingerprint(capabilities, evidence)
}

func (d *Driver) Start(ctx context.Context, plan ports.CollectorPlan) (ports.Collector, error) {
	const operation = "observer.process.start"
	if err := ports.RequireDeadline(ctx, operation); err != nil {
		return ports.Collector{}, err
	}
	if err := plan.Validate(); err != nil {
		return ports.Collector{}, err
	}
	adapter, found := d.adapters[plan.Adapter]
	if !found {
		return ports.Collector{}, domain.NewError(domain.CodeCapabilityUnavailable, operation, "adapter", "adapter is not configured", nil)
	}
	if adapter.SignalFamily != plan.Requirement.SignalFamily || adapter.Placement != plan.Requirement.Placement || coverageRank(adapter.CoverageLevel) < coverageRank(plan.Requirement.MinimumLevel) {
		return ports.Collector{}, domain.NewError(domain.CodeConflict, operation, "requirement", "adapter does not satisfy signal, placement, or coverage", nil)
	}
	if adapter.Version != plan.Version || adapter.ConfigurationDigest != plan.ConfigurationDigest {
		return ports.Collector{}, domain.NewError(domain.CodeIntegrityViolation, operation, "adapter", "version or configuration digest differs from the authorized plan", nil)
	}

	// Serializing starts prevents duplicate processes while an idempotent start
	// is still crossing the readiness boundary. Other driver operations remain
	// independent.
	d.startMu.Lock()
	defer d.startMu.Unlock()
	if collector, handled, err := d.replayStart(plan); handled {
		return collector, err
	}

	capture, err := d.outputs.Open(ctx, plan)
	if err != nil {
		return ports.Collector{}, domain.NewError(domain.CodeUnavailable, operation, "outputs", "collector evidence transaction could not be opened", err)
	}
	if capture == nil {
		abortErr := d.abortUnstartedCapture(capture)
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeFailedPrecondition, operation, "outputs", "collector evidence transaction returned nil streams", nil),
			abortErr,
		)
	}
	stdout, stderr := capture.Stdout(), capture.Stderr()
	if stdout == nil || stderr == nil {
		abortErr := d.abortUnstartedCapture(capture)
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeFailedPrecondition, operation, "outputs", "collector evidence transaction returned nil streams", nil),
			abortErr,
		)
	}
	invocation := command.Invocation{Program: adapter.Program, Args: append([]string(nil), adapter.Args...), Environment: observerEnvironment(adapter.Environment, plan)}
	process, err := d.starter.Start(ctx, invocation)
	if err != nil {
		cleanupErr := d.abortBeforeSupervision(capture, stdout, stderr)
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeUnavailable, operation, "process", "collector process failed to start", err),
			cleanupErr,
		)
	}
	if process == nil {
		cleanupErr := d.abortBeforeSupervision(capture, stdout, stderr)
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeFailedPrecondition, operation, "process", "collector process returned nil output streams", nil),
			cleanupErr,
		)
	}
	stdoutReader, stderrReader := process.Stdout(), process.Stderr()
	if stdoutReader == nil || stderrReader == nil {
		cleanupErr := d.abortInvalidProcess(process, capture, stdoutReader, stderrReader, stdout, stderr)
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeFailedPrecondition, operation, "process", "collector process returned nil output streams", nil),
			cleanupErr,
		)
	}

	record := &record{
		plan:    plan,
		adapter: adapter,
		process: process,
		capture: capture,
		exited:  make(chan struct{}),
		done:    make(chan struct{}),
	}
	go d.supervise(record, stdoutReader, stderrReader, stdout, stderr)

	if err := adapter.Readiness.Await(ctx, plan); err != nil {
		cleanupErr := d.rollbackStart(record)
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeFailedPrecondition, operation, "readiness", "collector did not become ready", err),
			cleanupErr,
		)
	}
	select {
	case <-record.exited:
		cleanupErr := d.rollbackStart(record)
		record.mu.Lock()
		waitErr := record.waitErr
		record.mu.Unlock()
		return ports.Collector{}, errors.Join(
			domain.NewError(domain.CodeFailedPrecondition, operation, "readiness", "collector exited before readiness was committed", waitErr),
			cleanupErr,
		)
	default:
	}
	coverage, err := newCoverage(plan, adapter.CoverageLevel, domain.CoverageAvailable, time.Time{})
	if err != nil {
		return ports.Collector{}, errors.Join(err, d.rollbackStart(record))
	}
	record.mu.Lock()
	record.coverage = coverage
	record.mu.Unlock()

	key := plan.CollectorID.String()
	d.mu.Lock()
	d.records[key] = record
	d.idempotency[plan.IdempotencyKey] = key
	d.mu.Unlock()
	return collectorFromPlan(plan), nil
}

func (d *Driver) replayStart(plan ports.CollectorPlan) (ports.Collector, bool, error) {
	key := plan.CollectorID.String()
	d.mu.Lock()
	defer d.mu.Unlock()
	if prior, exists := d.idempotency[plan.IdempotencyKey]; exists {
		record, found := d.records[prior]
		if !found || prior != key {
			return ports.Collector{}, true, domain.NewError(domain.CodeConflict, "observer.process.start", "idempotency_key", "was used for another collector", nil)
		}
		return collectorFromPlan(record.plan), true, nil
	}
	if _, exists := d.records[key]; exists {
		return ports.Collector{}, true, domain.NewError(domain.CodeConflict, "observer.process.start", "collector_id", "is already owned by another idempotency key", nil)
	}
	return ports.Collector{}, false, nil
}

func collectorFromPlan(plan ports.CollectorPlan) ports.Collector {
	return ports.Collector{ID: plan.CollectorID, TargetRunID: plan.TargetRunID, SignalFamily: plan.Requirement.SignalFamily, StartedAt: plan.StartedAt}
}

func (d *Driver) supervise(record *record, stdoutReader, stderrReader io.ReadCloser, stdoutWriter, stderrWriter io.WriteCloser) {
	var streams sync.WaitGroup
	streams.Add(2)
	go func() {
		defer streams.Done()
		err := copyAndClose("stdout", stdoutWriter, stdoutReader)
		record.mu.Lock()
		record.stdoutErr = err
		record.mu.Unlock()
	}()
	go func() {
		defer streams.Done()
		err := copyAndClose("stderr", stderrWriter, stderrReader)
		record.mu.Lock()
		record.stderrErr = err
		record.mu.Unlock()
	}()
	waitErr := record.process.Wait()
	// Publish process exit before taking the state lock. Stop can then
	// conservatively classify an exit that raced with its request as early.
	close(record.exited)
	record.mu.Lock()
	record.waitErr = waitErr
	if !record.stopRequested {
		record.unexpectedExit = true
	}
	record.mu.Unlock()
	streams.Wait()
	close(record.done)
}

func copyAndClose(name string, destination io.WriteCloser, source io.ReadCloser) error {
	_, copyErr := io.Copy(destination, source)
	sourceCloseErr := source.Close()
	destinationCloseErr := destination.Close()
	return errors.Join(
		wrapStreamError(name, "copy", copyErr),
		wrapStreamError(name, "source close", sourceCloseErr),
		wrapStreamError(name, "destination close", destinationCloseErr),
	)
}

func wrapStreamError(stream, action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("collector %s %s: %w", stream, action, err)
}

func (d *Driver) Stop(ctx context.Context, id domain.CollectorID) (ports.CollectorResult, error) {
	if err := ports.RequireDeadline(ctx, "observer.process.stop"); err != nil {
		return ports.CollectorResult{}, err
	}
	record, err := d.requireRecord(id)
	if err != nil {
		return ports.CollectorResult{}, err
	}

	record.mu.Lock()
	if record.result != nil {
		result, resultErr := cloneCollectorResult(*record.result), record.resultErr
		record.mu.Unlock()
		return result, resultErr
	}
	if record.attempt != nil {
		attempt := record.attempt
		record.mu.Unlock()
		return awaitStopAttempt(ctx, attempt)
	}
	attempt := &stopAttempt{done: make(chan struct{})}
	record.attempt = attempt
	record.mu.Unlock()

	result, stopErr := d.performStop(ctx, record)
	record.mu.Lock()
	attempt.result = cloneCollectorResult(result)
	attempt.err = stopErr
	record.attempt = nil
	close(attempt.done)
	record.mu.Unlock()
	return cloneCollectorResult(result), stopErr
}

func awaitStopAttempt(ctx context.Context, attempt *stopAttempt) (ports.CollectorResult, error) {
	select {
	case <-ctx.Done():
		return ports.CollectorResult{}, domain.NewError(domain.CodeUnavailable, "observer.process.stop", "wait", "concurrent stop did not finish before the deadline", ctx.Err())
	case <-attempt.done:
		return cloneCollectorResult(attempt.result), attempt.err
	}
}

func (d *Driver) performStop(ctx context.Context, record *record) (ports.CollectorResult, error) {
	if err := d.ensureTeardown(ctx, record); err != nil {
		coverage, coverageErr := d.setFailureCoverage(record, domain.CoverageLevelNone, time.Time{})
		return ports.CollectorResult{CollectorID: record.plan.CollectorID, Coverage: coverage, TeardownConfirmed: false}, errors.Join(err, coverageErr)
	}

	record.mu.Lock()
	stoppedAt := record.stoppedAt
	record.mu.Unlock()
	artifacts, err := record.capture.Finalize(ctx)
	artifactErr := validateArtifacts(artifacts)
	if err == nil && artifactErr != nil {
		err = artifactErr
	}
	if err != nil {
		coverage, coverageErr := d.setFailureCoverage(record, domain.CoverageLevelNone, stoppedAt)
		result := ports.CollectorResult{
			CollectorID:       record.plan.CollectorID,
			Coverage:          coverage,
			Artifacts:         append([]domain.ArtifactReference(nil), artifacts...),
			StoppedAt:         stoppedAt,
			TeardownConfirmed: true,
		}
		// A bounded output authority may publish the retained prefix and also
		// report a permanent truncation error. Preserve those immutable bytes
		// in the failed result; invalid artifact envelopes remain untrusted.
		if artifactErr != nil {
			result.Artifacts = nil
		}
		resultErr := errors.Join(domain.NewError(domain.CodeUnavailable, "observer.process.stop", "outputs", "collector evidence could not be finalized", err), artifactErr, coverageErr)
		if artifactErr == nil && len(result.Artifacts) > 0 && errors.Is(err, ErrCaptureLimit) {
			record.mu.Lock()
			cached := cloneCollectorResult(result)
			record.result = &cached
			record.resultErr = resultErr
			record.mu.Unlock()
		}
		return result, resultErr
	}

	record.mu.Lock()
	permanentErr := record.permanentFailureLocked()
	outputFailure := record.stdoutErr != nil || record.stderrErr != nil
	record.mu.Unlock()
	level, status := record.adapter.CoverageLevel, domain.CoverageAvailable
	if permanentErr != nil {
		level, status = failedCoverageLevel(level), domain.CoverageLost
		if outputFailure {
			level = domain.CoverageLevelNone
		}
	}
	coverage, coverageErr := newCoverage(record.plan, level, status, stoppedAt)
	if coverageErr != nil {
		return ports.CollectorResult{}, coverageErr
	}
	result := ports.CollectorResult{
		CollectorID:       record.plan.CollectorID,
		Coverage:          coverage,
		Artifacts:         append([]domain.ArtifactReference(nil), artifacts...),
		StoppedAt:         stoppedAt,
		TeardownConfirmed: true,
	}
	record.mu.Lock()
	record.coverage = coverage
	cached := cloneCollectorResult(result)
	record.result = &cached
	record.resultErr = permanentErr
	record.mu.Unlock()
	return result, permanentErr
}

func (d *Driver) ensureTeardown(ctx context.Context, record *record) error {
	record.mu.Lock()
	if record.teardownConfirmed {
		record.mu.Unlock()
		return nil
	}
	alreadyExited := channelClosed(record.exited)
	if alreadyExited && !record.stopRequested {
		record.unexpectedExit = true
	}
	record.stopRequested = true
	record.mu.Unlock()

	var lifecycleErr error
	if !alreadyExited {
		if err := record.process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			lifecycleErr = errors.Join(lifecycleErr, fmt.Errorf("signal collector: %w", err))
			if killErr := record.process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				lifecycleErr = errors.Join(lifecycleErr, fmt.Errorf("kill collector after signal failure: %w", killErr))
			}
		}
	}

	confirmed := false
	select {
	case <-record.done:
		confirmed = true
	case <-ctx.Done():
		if err := record.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			lifecycleErr = errors.Join(lifecycleErr, fmt.Errorf("kill collector after stop deadline: %w", err))
		}
		timer := time.NewTimer(d.cleanupGrace)
		select {
		case <-record.done:
			confirmed = true
			timer.Stop()
		case <-timer.C:
		}
	}
	if !confirmed {
		err := domain.NewError(domain.CodeUnavailable, "observer.process.stop", "cleanup", "collector process and output stream teardown could not be confirmed", ctx.Err())
		record.mu.Lock()
		record.lifecycleErr = errors.Join(record.lifecycleErr, err, lifecycleErr)
		record.mu.Unlock()
		return errors.Join(err, lifecycleErr)
	}
	record.mu.Lock()
	record.teardownConfirmed = true
	record.lifecycleErr = errors.Join(record.lifecycleErr, lifecycleErr)
	if record.stoppedAt.IsZero() {
		record.stoppedAt = d.nowNotBefore(record.plan.StartedAt)
	}
	record.mu.Unlock()
	return nil
}

func (r *record) permanentFailureLocked() error {
	result := errors.Join(r.stdoutErr, r.stderrErr, r.lifecycleErr)
	if r.unexpectedExit {
		unexpected := errors.New("collector exited before stop was requested")
		if r.waitErr != nil {
			unexpected = fmt.Errorf("collector exited before stop was requested: %w", r.waitErr)
		}
		result = errors.Join(result, unexpected)
	}
	return result
}

func (d *Driver) setFailureCoverage(record *record, level domain.CoverageLevel, endedAt time.Time) (domain.CollectorCoverage, error) {
	coverage, err := newCoverage(record.plan, level, domain.CoverageLost, endedAt)
	if err != nil {
		return domain.CollectorCoverage{}, err
	}
	record.mu.Lock()
	record.coverage = coverage
	record.mu.Unlock()
	return coverage, nil
}

func (d *Driver) Coverage(ctx context.Context, id domain.CollectorID) (domain.CollectorCoverage, error) {
	if err := ports.RequireDeadline(ctx, "observer.process.coverage"); err != nil {
		return domain.CollectorCoverage{}, err
	}
	record, err := d.requireRecord(id)
	if err != nil {
		return domain.CollectorCoverage{}, err
	}
	if channelClosed(record.exited) {
		record.mu.Lock()
		if !record.stopRequested {
			record.unexpectedExit = true
		}
		unexpected := record.unexpectedExit
		streamFailure := record.stdoutErr != nil || record.stderrErr != nil
		coverage := record.coverage
		endedAt := record.stoppedAt
		if endedAt.IsZero() && unexpected {
			endedAt = d.nowNotBefore(record.plan.StartedAt)
		}
		record.mu.Unlock()
		if unexpected || streamFailure {
			level := failedCoverageLevel(record.adapter.CoverageLevel)
			if streamFailure {
				level = domain.CoverageLevelNone
			}
			coverage, coverageErr := newCoverage(record.plan, level, domain.CoverageLost, endedAt)
			if coverageErr != nil {
				return domain.CollectorCoverage{}, coverageErr
			}
			record.mu.Lock()
			record.coverage = coverage
			record.mu.Unlock()
			return coverage, nil
		}
		return coverage, nil
	}
	record.mu.Lock()
	coverage := record.coverage
	record.mu.Unlock()
	return coverage, nil
}

// InterruptedCollectorCleanupGuaranteed is true only for the built-in Linux
// starter and only for its directly spawned collector process. A custom starter
// is opaque to this driver and therefore cannot be treated as carrying the
// daemon-parent death invariant. Adapters with independently surviving
// descendants require external process-tree containment.
func (d *Driver) InterruptedCollectorCleanupGuaranteed() bool {
	return d.crashCleanupGuaranteed
}

// ReconcileInterruptedCollectors proves that each directly spawned collector
// owned by a previous daemon cannot still be alive. On dedicated Linux hosts
// the kernel delivers SIGKILL when the daemon parent dies; other platforms and
// custom starters fail closed because this process has no authoritative handle
// for the old child. This is not proof that daemonized descendants are dead.
func (d *Driver) ReconcileInterruptedCollectors(ctx context.Context, request ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
	const operation = "observer.process.reconcile_interrupted"
	if err := ports.RequireDeadline(ctx, "observer.process.reconcile_interrupted"); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	if err := request.Validate(); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	if !d.crashCleanupGuaranteed {
		return ports.InterruptedCollectorReconciliationReport{}, domain.NewError(domain.CodeCapabilityUnavailable, operation, "collector_cleanup", "direct collector-process death after controller loss cannot be proven", nil)
	}
	report, err := d.outputs.ReconcileInterruptedRun(ctx, request)
	if err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, domain.NewError(domain.CodeIntegrityViolation, operation, "outputs", "interrupted collector output could not be authoritatively reconciled", err)
	}
	if err := report.ValidateFor(request); err != nil {
		return ports.InterruptedCollectorReconciliationReport{}, err
	}
	return report, nil
}

func (d *Driver) requireRecord(id domain.CollectorID) (*record, error) {
	if id.IsZero() {
		return nil, domain.NewError(domain.CodeInvalidArgument, "observer.process.collector", "collector_id", "must be set", nil)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	record, found := d.records[id.String()]
	if !found {
		return nil, domain.NewError(domain.CodeNotFound, "observer.process.collector", "collector_id", "collector is not owned by this driver", nil)
	}
	return record, nil
}

func (d *Driver) rollbackStart(record *record) error {
	var cleanupErr error
	if !channelClosed(record.exited) {
		if err := record.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("kill failed collector start: %w", err))
		}
	}
	timer := time.NewTimer(d.cleanupGrace)
	select {
	case <-record.done:
		timer.Stop()
		record.mu.Lock()
		cleanupErr = errors.Join(cleanupErr, record.stdoutErr, record.stderrErr)
		record.mu.Unlock()
	case <-timer.C:
		cleanupErr = errors.Join(cleanupErr, errors.New("failed collector start teardown was not confirmed"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.cleanupGrace)
	defer cancel()
	if err := record.capture.Abort(ctx); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("abort failed collector evidence: %w", err))
	}
	return cleanupErr
}

func (d *Driver) abortBeforeSupervision(capture OutputCapture, stdout, stderr io.WriteCloser) error {
	closeErr := errors.Join(wrapStreamError("stdout", "destination close", stdout.Close()), wrapStreamError("stderr", "destination close", stderr.Close()))
	return errors.Join(closeErr, d.abortUnstartedCapture(capture))
}

func (d *Driver) abortInvalidProcess(process command.Process, capture OutputCapture, stdoutReader, stderrReader io.ReadCloser, stdoutWriter, stderrWriter io.WriteCloser) error {
	var cleanupErr error
	if stdoutReader != nil {
		cleanupErr = errors.Join(cleanupErr, wrapStreamError("stdout", "source close", stdoutReader.Close()))
	}
	if stderrReader != nil {
		cleanupErr = errors.Join(cleanupErr, wrapStreamError("stderr", "source close", stderrReader.Close()))
	}
	cleanupErr = errors.Join(cleanupErr, wrapStreamError("stdout", "destination close", stdoutWriter.Close()), wrapStreamError("stderr", "destination close", stderrWriter.Close()))
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("kill invalid collector process: %w", err))
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.Wait() }()
	timer := time.NewTimer(d.cleanupGrace)
	select {
	case <-waitDone:
		timer.Stop()
	case <-timer.C:
		cleanupErr = errors.Join(cleanupErr, errors.New("invalid collector process teardown was not confirmed"))
	}
	return errors.Join(cleanupErr, d.abortUnstartedCapture(capture))
}

func (d *Driver) abortUnstartedCapture(capture OutputCapture) error {
	if capture == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.cleanupGrace)
	defer cancel()
	if err := capture.Abort(ctx); err != nil {
		return fmt.Errorf("abort collector evidence: %w", err)
	}
	return nil
}

func validateArtifacts(artifacts []domain.ArtifactReference) error {
	if len(artifacts) == 0 {
		return errors.New("output authority returned no collector artifacts")
	}
	seen := make(map[string]struct{}, len(artifacts))
	requiredRoles := map[string]bool{CollectorStdoutRole: false, CollectorStderrRole: false}
	for index, artifact := range artifacts {
		spec := artifact.Spec()
		if _, err := domain.NewArtifactReference(spec); err != nil {
			return fmt.Errorf("collector artifact %d is invalid: %w", index, err)
		}
		if _, duplicate := seen[spec.Reference]; duplicate {
			return fmt.Errorf("collector artifact %d duplicates reference %q", index, spec.Reference)
		}
		seen[spec.Reference] = struct{}{}
		if _, required := requiredRoles[spec.Role]; required {
			if requiredRoles[spec.Role] {
				return fmt.Errorf("collector artifacts contain duplicate required role %q", spec.Role)
			}
			requiredRoles[spec.Role] = true
		}
	}
	for role, found := range requiredRoles {
		if !found {
			return fmt.Errorf("output authority omitted required collector artifact role %q", role)
		}
	}
	return nil
}

func (d *Driver) nowNotBefore(startedAt time.Time) time.Time {
	value := d.now().UTC()
	if value.Before(startedAt) {
		return startedAt.UTC()
	}
	return value
}

func cloneCollectorResult(result ports.CollectorResult) ports.CollectorResult {
	result.Artifacts = append([]domain.ArtifactReference(nil), result.Artifacts...)
	return result
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func failedCoverageLevel(level domain.CoverageLevel) domain.CoverageLevel {
	if level == domain.CoverageLevelNone {
		return domain.CoverageLevelNone
	}
	return domain.CoverageLevelPartial
}

func newCoverage(plan ports.CollectorPlan, level domain.CoverageLevel, status domain.CoverageStatus, ended time.Time) (domain.CollectorCoverage, error) {
	started := time.Time{}
	if !ended.IsZero() {
		started = plan.StartedAt
	}
	return domain.NewCollectorCoverage(domain.CollectorCoverageSpec{CollectorID: plan.CollectorID, SignalFamily: plan.Requirement.SignalFamily, Placement: plan.Requirement.Placement, Level: level, Status: status, Required: plan.Requirement.Required, StartedAt: started, EndedAt: ended})
}

func observerEnvironment(base map[string]string, plan ports.CollectorPlan) []string {
	values := cloneMap(base)
	values["WORLD_COLLECTOR_ID"] = plan.CollectorID.String()
	values["WORLD_RESEARCH_SESSION_ID"] = plan.ResearchSessionID.String()
	values["WORLD_LEASE_ID"] = plan.LeaseID.String()
	values["WORLD_AGENT_WORKSPACE_ID"] = plan.AgentWorkspaceID.String()
	values["WORLD_AGENT_GENERATION"] = strconv.FormatUint(uint64(plan.AgentGeneration), 10)
	values["WORLD_TARGET_ID"] = plan.TargetID.String()
	values["WORLD_TARGET_GENERATION"] = strconv.FormatUint(uint64(plan.TargetGeneration), 10)
	values["WORLD_TARGET_RUN_ID"] = plan.TargetRunID.String()
	values["WORLD_TARGET_KIND"] = string(plan.Attachment.TargetKind)
	values["WORLD_TARGET_RUNTIME_ID"] = plan.Attachment.RuntimeID
	values["WORLD_MAXIMUM_BYTES"] = strconv.FormatInt(plan.MaximumBytes, 10)
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+values[name])
	}
	return result
}

func cloneMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}

func coverageRank(level domain.CoverageLevel) int {
	switch level {
	case domain.CoverageLevelComplete:
		return 3
	case domain.CoverageLevelPartial:
		return 2
	case domain.CoverageLevelNone:
		return 1
	}
	return 0
}

func boundedText(value []byte, maximum int) string {
	if len(value) > maximum {
		value = value[:maximum]
	}
	text := string(value)
	if text == "" {
		return "unknown"
	}
	return text
}

var _ ports.ObserverDriver = (*Driver)(nil)
var _ ports.ObserverCrashReconciler = (*Driver)(nil)

// detachedStarter gives the supervisor, rather than the Start RPC context,
// ownership of collector lifetime. Context still bounds admission/spawn; every
// successful process is terminated explicitly by Driver.Stop.
type detachedStarter struct{}

func (detachedStarter) Start(ctx context.Context, invocation command.Invocation) (command.Process, error) {
	if err := invocation.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := exec.Command(invocation.Program, append([]string(nil), invocation.Args...)...)
	configureCollectorParentDeathSignal(cmd)
	cmd.Dir = invocation.Directory
	if invocation.Environment != nil {
		cmd.Env = append([]string(nil), invocation.Environment...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	process := &detachedProcess{cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr}
	if err := ctx.Err(); err != nil {
		_ = process.Kill()
		_ = process.Wait()
		return nil, err
	}
	return process, nil
}

type detachedProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (p *detachedProcess) Stdin() io.WriteCloser         { return p.stdin }
func (p *detachedProcess) Stdout() io.ReadCloser         { return p.stdout }
func (p *detachedProcess) Stderr() io.ReadCloser         { return p.stderr }
func (p *detachedProcess) Wait() error                   { return p.cmd.Wait() }
func (p *detachedProcess) Signal(signal os.Signal) error { return p.cmd.Process.Signal(signal) }
func (p *detachedProcess) Kill() error                   { return p.cmd.Process.Kill() }
