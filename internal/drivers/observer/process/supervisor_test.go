package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestSupervisorCapturesArtifactsAndConfirmsCleanup(t *testing.T) {
	process := newObserverProcess()
	capture := newMemoryCapture()
	driver, invocation := newTestDriver(t, process, capture, nil)
	plan := validCollectorPlan(t)
	ctx, cancel := testContext(t)
	defer cancel()

	collector, err := driver.Start(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if collector.ID != plan.CollectorID || invocation.Program != "/trusted/trace" || !slices.Equal(invocation.Args, []string{"--fixed-config"}) {
		t.Fatalf("collector=%#v invocation=%#v", collector, invocation)
	}
	for _, expected := range []string{"WORLD_COLLECTOR_ID=" + plan.CollectorID.String(), "WORLD_LEASE_ID=" + plan.LeaseID.String(), "WORLD_TARGET_RUN_ID=" + plan.TargetRunID.String()} {
		if !slices.Contains(invocation.Environment, expected) {
			t.Fatalf("missing environment %q in %v", expected, invocation.Environment)
		}
	}
	process.writeStdout(t, "stdout evidence")
	process.writeStderr(t, "stderr evidence")

	coverage, err := driver.Coverage(ctx, plan.CollectorID)
	if err != nil || coverage.Level() != domain.CoverageLevelComplete || coverage.Spec().Status != domain.CoverageAvailable {
		t.Fatalf("coverage=%#v err=%v", coverage, err)
	}
	result, err := driver.Stop(ctx, plan.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if !result.TeardownConfirmed || result.Coverage.Level() != domain.CoverageLevelComplete || result.Coverage.Spec().Status != domain.CoverageAvailable {
		t.Fatalf("stop result = %#v", result)
	}
	if len(result.Artifacts) != 2 || capture.stdoutText() != "stdout evidence" || capture.stderrText() != "stderr evidence" {
		t.Fatalf("artifacts=%#v stdout=%q stderr=%q", result.Artifacts, capture.stdoutText(), capture.stderrText())
	}
	if process.signalCount() != 1 || capture.finalizeCount() != 1 || capture.abortCount() != 0 {
		t.Fatalf("signals=%d finalize=%d abort=%d", process.signalCount(), capture.finalizeCount(), capture.abortCount())
	}

	replay, err := driver.Stop(ctx, plan.CollectorID)
	if err != nil || replay.StoppedAt != result.StoppedAt || len(replay.Artifacts) != 2 || capture.finalizeCount() != 1 {
		t.Fatalf("stop replay = %#v, %v; finalize=%d", replay, err, capture.finalizeCount())
	}
}

func TestSupervisorRejectsMissingOutputAuthority(t *testing.T) {
	if _, err := New(Config{Adapters: testAdapters(nil)}); err == nil || !strings.Contains(err.Error(), "output authority") {
		t.Fatalf("New error = %v", err)
	}
}

func TestCustomStarterCannotClaimInterruptedCollectorCleanup(t *testing.T) {
	process := newObserverProcess()
	driver, _ := newTestDriver(t, process, newMemoryCapture(), nil)
	if driver.InterruptedCollectorCleanupGuaranteed() {
		t.Fatal("custom starter was treated as carrying a parent-death guarantee")
	}
	ctx, cancel := testContext(t)
	defer cancel()
	plan := validCollectorPlan(t)
	request := ports.InterruptedCollectorReconciliation{TargetRunID: plan.TargetRunID, Collectors: []ports.InterruptedCollectorBinding{{Plan: plan, StartCommitted: true}}}
	if _, err := driver.ReconcileInterruptedCollectors(ctx, request); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
		t.Fatalf("custom starter interrupted cleanup error = %v", err)
	}
}

func TestInterruptedCollectorReconciliationDelegatesAndValidatesOutputAuthority(t *testing.T) {
	plan := validCollectorPlan(t)
	request := ports.InterruptedCollectorReconciliation{
		TargetRunID: plan.TargetRunID,
		Collectors:  []ports.InterruptedCollectorBinding{{Plan: plan, StartCommitted: true}},
	}

	t.Run("delegates exact authority", func(t *testing.T) {
		factory := &memoryOutputFactory{
			capture: newMemoryCapture(),
			reconcile: func(received ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
				return ports.InterruptedCollectorReconciliationReport{
					TargetRunID: received.TargetRunID,
					Outputs: []ports.InterruptedCollectorOutput{{
						CollectorID: received.Collectors[0].Plan.CollectorID,
						State:       ports.InterruptedCollectorOutputAborted,
					}},
				}, nil
			},
		}
		driver, err := New(Config{Adapters: testAdapters(nil), Outputs: factory})
		if err != nil {
			t.Fatal(err)
		}
		// The platform guarantee is tested separately on Linux. Force this
		// package-private precondition so this test isolates delegation.
		driver.crashCleanupGuaranteed = true
		ctx, cancel := testContext(t)
		defer cancel()

		report, err := driver.ReconcileInterruptedCollectors(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if len(factory.reconcileRequests) != 1 || !reflect.DeepEqual(factory.reconcileRequests[0], request) {
			t.Fatalf("reconciliation requests = %#v", factory.reconcileRequests)
		}
		if len(report.Outputs) != 1 || report.Outputs[0].CollectorID != plan.CollectorID || report.Outputs[0].State != ports.InterruptedCollectorOutputAborted {
			t.Fatalf("report = %#v", report)
		}
	})

	t.Run("rejects malformed authority report", func(t *testing.T) {
		factory := &memoryOutputFactory{
			capture: newMemoryCapture(),
			reconcile: func(ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
				return ports.InterruptedCollectorReconciliationReport{TargetRunID: request.TargetRunID}, nil
			},
		}
		driver, err := New(Config{Adapters: testAdapters(nil), Outputs: factory})
		if err != nil {
			t.Fatal(err)
		}
		driver.crashCleanupGuaranteed = true
		ctx, cancel := testContext(t)
		defer cancel()

		if _, err := driver.ReconcileInterruptedCollectors(ctx, request); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("malformed reconciliation report error = %v", err)
		}
	})
}

func TestSupervisorRejectsUnconfiguredAdapter(t *testing.T) {
	driver, err := New(Config{Adapters: testAdapters(nil), Outputs: &memoryOutputFactory{capture: newMemoryCapture()}})
	if err != nil {
		t.Fatal(err)
	}
	plan := validCollectorPlan(t)
	plan.Adapter = "caller-controlled-command"
	ctx, cancel := testContext(t)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err == nil {
		t.Fatal("unconfigured observer command accepted")
	}
}

func TestSupervisorReportsEarlyExitAndWaitErrorWithArtifacts(t *testing.T) {
	process := newObserverProcess()
	capture := newMemoryCapture()
	driver, _ := newTestDriver(t, process, capture, nil)
	plan := validCollectorPlan(t)
	ctx, cancel := testContext(t)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	process.writeStdout(t, "partial evidence")
	process.exit(errors.New("exit status 23"))
	waitForLostCoverage(t, ctx, driver, plan.CollectorID)

	result, err := driver.Stop(ctx, plan.CollectorID)
	if err == nil || !strings.Contains(err.Error(), "exit status 23") {
		t.Fatalf("Stop error = %v", err)
	}
	if !result.TeardownConfirmed || result.Coverage.Spec().Status != domain.CoverageLost || result.Coverage.Level() != domain.CoverageLevelPartial || len(result.Artifacts) != 2 {
		t.Fatalf("result = %#v", result)
	}
	if capture.finalizeCount() != 1 {
		t.Fatalf("finalize calls = %d", capture.finalizeCount())
	}
}

func TestSupervisorReportsCopyAndCloseFailuresAsLostEvidence(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*memoryCapture)
		want      string
	}{
		{name: "copy", configure: func(c *memoryCapture) { c.stdout.writeErr = errors.New("sink unavailable") }, want: "sink unavailable"},
		{name: "close", configure: func(c *memoryCapture) { c.stderr.closeErr = errors.New("flush failed") }, want: "flush failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			process := newObserverProcess()
			capture := newMemoryCapture()
			test.configure(capture)
			driver, _ := newTestDriver(t, process, capture, nil)
			plan := validCollectorPlan(t)
			ctx, cancel := testContext(t)
			defer cancel()
			if _, err := driver.Start(ctx, plan); err != nil {
				t.Fatal(err)
			}
			if test.name == "copy" {
				_ = process.tryWriteStdout([]byte("evidence"))
			}
			result, err := driver.Stop(ctx, plan.CollectorID)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Stop error = %v", err)
			}
			if result.Coverage.Spec().Status != domain.CoverageLost || result.Coverage.Level() != domain.CoverageLevelNone || !result.TeardownConfirmed || len(result.Artifacts) != 2 {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestSupervisorRetriesFailedFinalization(t *testing.T) {
	process := newObserverProcess()
	capture := newMemoryCapture()
	capture.finalizeFailures = 1
	driver, _ := newTestDriver(t, process, capture, nil)
	plan := validCollectorPlan(t)
	ctx, cancel := testContext(t)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	process.writeStdout(t, "retryable evidence")

	failed, err := driver.Stop(ctx, plan.CollectorID)
	if err == nil || failed.Coverage.Spec().Status != domain.CoverageLost || failed.Coverage.Level() != domain.CoverageLevelNone || !failed.TeardownConfirmed || len(failed.Artifacts) != 0 {
		t.Fatalf("first Stop = %#v, %v", failed, err)
	}
	result, err := driver.Stop(ctx, plan.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Coverage.Spec().Status != domain.CoverageAvailable || result.Coverage.Level() != domain.CoverageLevelComplete || len(result.Artifacts) != 2 {
		t.Fatalf("retry result = %#v", result)
	}
	if capture.finalizeCount() != 2 || process.signalCount() != 1 {
		t.Fatalf("finalize=%d signal=%d", capture.finalizeCount(), process.signalCount())
	}
}

func TestSupervisorRejectsInvalidFinalizedArtifactsAndRetries(t *testing.T) {
	process := newObserverProcess()
	capture := newMemoryCapture()
	capture.invalidFinalizations = 1
	driver, _ := newTestDriver(t, process, capture, nil)
	plan := validCollectorPlan(t)
	ctx, cancel := testContext(t)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}

	failed, err := driver.Stop(ctx, plan.CollectorID)
	if err == nil || failed.Coverage.Spec().Status != domain.CoverageLost || len(failed.Artifacts) != 0 {
		t.Fatalf("first Stop = %#v, %v", failed, err)
	}
	result, err := driver.Stop(ctx, plan.CollectorID)
	if err != nil || len(result.Artifacts) != 2 || result.Coverage.Spec().Status != domain.CoverageAvailable {
		t.Fatalf("retry Stop = %#v, %v", result, err)
	}
	if capture.finalizeCount() != 2 {
		t.Fatalf("finalize calls = %d", capture.finalizeCount())
	}
}

func TestSupervisorConcurrentAndReplayedStopFinalizeOnce(t *testing.T) {
	process := newObserverProcess()
	capture := newMemoryCapture()
	driver, _ := newTestDriver(t, process, capture, nil)
	plan := validCollectorPlan(t)
	ctx, cancel := testContext(t)
	defer cancel()
	if _, err := driver.Start(ctx, plan); err != nil {
		t.Fatal(err)
	}
	process.writeStderr(t, "concurrent stop")

	const callers = 16
	start := make(chan struct{})
	results := make(chan ports.CollectorResult, callers)
	errorsChannel := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			<-start
			result, err := driver.Stop(ctx, plan.CollectorID)
			results <- result
			errorsChannel <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Errorf("Stop error = %v", err)
		}
	}
	var stoppedAt time.Time
	for result := range results {
		if !result.TeardownConfirmed || len(result.Artifacts) != 2 {
			t.Errorf("result = %#v", result)
		}
		if stoppedAt.IsZero() {
			stoppedAt = result.StoppedAt
		} else if stoppedAt != result.StoppedAt {
			t.Errorf("non-idempotent stopped time: %s != %s", stoppedAt, result.StoppedAt)
		}
	}
	if process.signalCount() != 1 || capture.finalizeCount() != 1 {
		t.Fatalf("signals=%d finalize=%d", process.signalCount(), capture.finalizeCount())
	}
	if _, err := driver.Stop(ctx, plan.CollectorID); err != nil || capture.finalizeCount() != 1 {
		t.Fatalf("replay error=%v finalize=%d", err, capture.finalizeCount())
	}
}

func TestSupervisorReadinessFailureRollsBackAndAbortsEvidence(t *testing.T) {
	process := newObserverProcess()
	capture := newMemoryCapture()
	readinessErr := errors.New("probe socket never appeared")
	driver, _ := newTestDriver(t, process, capture, ReadinessFunc(func(context.Context, ports.CollectorPlan) error { return readinessErr }))
	plan := validCollectorPlan(t)
	ctx, cancel := testContext(t)
	defer cancel()

	if _, err := driver.Start(ctx, plan); err == nil || !errors.Is(err, readinessErr) {
		t.Fatalf("Start error = %v", err)
	}
	if capture.abortCount() != 1 || capture.finalizeCount() != 0 || process.killCount() != 1 {
		t.Fatalf("abort=%d finalize=%d kill=%d", capture.abortCount(), capture.finalizeCount(), process.killCount())
	}
	if _, err := driver.Stop(ctx, plan.CollectorID); err == nil {
		t.Fatal("failed start was retained as a live collector")
	}
}

func TestSupervisorDoesNotClaimTeardownWithoutProof(t *testing.T) {
	process := newObserverProcess()
	process.finishOnSignal = false
	process.finishOnKill = false
	capture := newMemoryCapture()
	driver, _ := newTestDriverWithCleanup(t, process, capture, nil, 10*time.Millisecond)
	plan := validCollectorPlan(t)
	startCtx, startCancel := testContext(t)
	defer startCancel()
	if _, err := driver.Start(startCtx, plan); err != nil {
		t.Fatal(err)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stopCancel()
	result, err := driver.Stop(stopCtx, plan.CollectorID)
	if err == nil || result.TeardownConfirmed || result.Coverage.Spec().Status != domain.CoverageLost || capture.finalizeCount() != 0 {
		t.Fatalf("Stop = %#v, %v; finalize=%d", result, err, capture.finalizeCount())
	}
	process.exit(nil)
}

func newTestDriver(t *testing.T, process *observerProcess, capture *memoryCapture, readiness Readiness) (*Driver, *command.Invocation) {
	t.Helper()
	return newTestDriverWithCleanup(t, process, capture, readiness, time.Second)
}

func newTestDriverWithCleanup(t *testing.T, process *observerProcess, capture *memoryCapture, readiness Readiness, cleanupGrace time.Duration) (*Driver, *command.Invocation) {
	t.Helper()
	if readiness == nil {
		readiness = ReadinessFunc(func(context.Context, ports.CollectorPlan) error { return nil })
	}
	var invocation command.Invocation
	driver, err := New(Config{
		Runner: runnerFunc(func(context.Context, command.Invocation) (command.Result, error) {
			return command.Result{Stdout: []byte("collector-v1")}, nil
		}),
		Starter: starterFunc(func(_ context.Context, value command.Invocation) (command.Process, error) {
			invocation = value
			return process, nil
		}),
		Adapters:     testAdapters(readiness),
		Outputs:      &memoryOutputFactory{capture: capture},
		Now:          func() time.Time { return time.Date(2026, 7, 27, 18, 0, 0, 0, time.UTC) },
		CleanupGrace: cleanupGrace,
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver, &invocation
}

func testAdapters(readiness Readiness) []Adapter {
	if readiness == nil {
		readiness = ReadinessFunc(func(context.Context, ports.CollectorPlan) error { return nil })
	}
	return []Adapter{{
		Name:                "trace",
		Version:             "v1",
		ConfigurationDigest: domain.NewDigest([]byte("config")),
		SignalFamily:        "process",
		Placement:           domain.CollectorPlacementHost,
		CoverageLevel:       domain.CoverageLevelComplete,
		Program:             "/trusted/trace",
		Args:                []string{"--fixed-config"},
		VersionArgs:         []string{"--version"},
		Readiness:           readiness,
	}}
}

func validCollectorPlan(t *testing.T) ports.CollectorPlan {
	t.Helper()
	collector, _ := domain.NewCollectorID()
	session, _ := domain.NewResearchSessionID()
	lease, _ := domain.NewLeaseID()
	agent, _ := domain.NewAgentWorkspaceID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	return ports.CollectorPlan{
		IdempotencyKey:    "collector-key",
		CollectorID:       collector,
		ResearchSessionID: session,
		LeaseID:           lease,
		AgentWorkspaceID:  agent,
		AgentGeneration:   domain.InitialAgentGeneration,
		TargetID:          target,
		TargetGeneration:  domain.InitialTargetGeneration,
		TargetRunID:       run,
		Attachment: ports.ObservationAttachment{
			TargetKind: domain.TargetLinuxContainer,
			RuntimeID:  "runtime-1",
		},
		Requirement: ports.ObservationRequirement{
			SignalFamily: "process",
			Placement:    domain.CollectorPlacementHost,
			MinimumLevel: domain.CoverageLevelComplete,
			Required:     true,
		},
		Adapter:             "trace",
		Version:             "v1",
		ConfigurationDigest: domain.NewDigest([]byte("config")),
		MaximumBytes:        1024,
		StartedAt:           time.Date(2026, 7, 27, 17, 0, 0, 0, time.UTC),
	}
}

func testContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func waitForLostCoverage(t *testing.T, ctx context.Context, driver *Driver, id domain.CollectorID) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		coverage, err := driver.Coverage(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if coverage.Spec().Status == domain.CoverageLost {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("collector exit was not reflected in coverage")
		case <-ticker.C:
		}
	}
}

type runnerFunc func(context.Context, command.Invocation) (command.Result, error)

func (f runnerFunc) Run(ctx context.Context, value command.Invocation) (command.Result, error) {
	return f(ctx, value)
}

type starterFunc func(context.Context, command.Invocation) (command.Process, error)

func (f starterFunc) Start(ctx context.Context, value command.Invocation) (command.Process, error) {
	return f(ctx, value)
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

type observerProcess struct {
	stdin   io.WriteCloser
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter
	done    chan struct{}
	once    sync.Once

	mu             sync.Mutex
	waitErr        error
	signals        int
	kills          int
	finishOnSignal bool
	finishOnKill   bool
}

func newObserverProcess() *observerProcess {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &observerProcess{
		stdin:          nopWriteCloser{Writer: io.Discard},
		stdoutR:        stdoutR,
		stdoutW:        stdoutW,
		stderrR:        stderrR,
		stderrW:        stderrW,
		done:           make(chan struct{}),
		finishOnSignal: true,
		finishOnKill:   true,
	}
}

func (p *observerProcess) Stdin() io.WriteCloser { return p.stdin }
func (p *observerProcess) Stdout() io.ReadCloser { return p.stdoutR }
func (p *observerProcess) Stderr() io.ReadCloser { return p.stderrR }
func (p *observerProcess) Wait() error {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}
func (p *observerProcess) Signal(os.Signal) error {
	p.mu.Lock()
	p.signals++
	finish := p.finishOnSignal
	p.mu.Unlock()
	if finish {
		p.finish()
	}
	return nil
}
func (p *observerProcess) Kill() error {
	p.mu.Lock()
	p.kills++
	finish := p.finishOnKill
	p.mu.Unlock()
	if finish {
		p.finish()
	}
	return nil
}
func (p *observerProcess) exit(err error) {
	p.mu.Lock()
	p.waitErr = err
	p.mu.Unlock()
	p.finish()
}
func (p *observerProcess) finish() {
	p.once.Do(func() {
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		close(p.done)
	})
}
func (p *observerProcess) writeStdout(t *testing.T, value string) {
	t.Helper()
	if err := p.tryWriteStdout([]byte(value)); err != nil {
		t.Fatal(err)
	}
}
func (p *observerProcess) tryWriteStdout(value []byte) error {
	_, err := p.stdoutW.Write(value)
	return err
}
func (p *observerProcess) writeStderr(t *testing.T, value string) {
	t.Helper()
	if _, err := p.stderrW.Write([]byte(value)); err != nil {
		t.Fatal(err)
	}
}
func (p *observerProcess) signalCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.signals
}
func (p *observerProcess) killCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kills
}

type memoryOutputFactory struct {
	capture           *memoryCapture
	reconcile         func(ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error)
	reconcileRequests []ports.InterruptedCollectorReconciliation
}

func (f *memoryOutputFactory) Open(context.Context, ports.CollectorPlan) (OutputCapture, error) {
	return f.capture, nil
}

func (f *memoryOutputFactory) ReconcileInterruptedRun(_ context.Context, request ports.InterruptedCollectorReconciliation) (ports.InterruptedCollectorReconciliationReport, error) {
	f.reconcileRequests = append(f.reconcileRequests, request)
	if f.reconcile != nil {
		return f.reconcile(request)
	}
	report := ports.InterruptedCollectorReconciliationReport{TargetRunID: request.TargetRunID}
	for _, binding := range request.Collectors {
		report.Outputs = append(report.Outputs, ports.InterruptedCollectorOutput{CollectorID: binding.Plan.CollectorID, State: ports.InterruptedCollectorOutputAborted})
	}
	return report, nil
}

type memoryCapture struct {
	stdout *memoryWriter
	stderr *memoryWriter

	mu                   sync.Mutex
	finalizeCalls        int
	finalizeFailures     int
	invalidFinalizations int
	abortCalls           int
}

func newMemoryCapture() *memoryCapture {
	return &memoryCapture{stdout: &memoryWriter{}, stderr: &memoryWriter{}}
}

func (c *memoryCapture) Stdout() io.WriteCloser { return c.stdout }
func (c *memoryCapture) Stderr() io.WriteCloser { return c.stderr }
func (c *memoryCapture) Finalize(context.Context) ([]domain.ArtifactReference, error) {
	c.mu.Lock()
	c.finalizeCalls++
	if c.finalizeFailures > 0 {
		c.finalizeFailures--
		c.mu.Unlock()
		return nil, errors.New("publication temporarily unavailable")
	}
	if c.invalidFinalizations > 0 {
		c.invalidFinalizations--
		c.mu.Unlock()
		return []domain.ArtifactReference{{}}, nil
	}
	c.mu.Unlock()
	stdout, err := artifactFor(CollectorStdoutRole, c.stdout.bytes())
	if err != nil {
		return nil, err
	}
	stderr, err := artifactFor(CollectorStderrRole, c.stderr.bytes())
	if err != nil {
		return nil, err
	}
	return []domain.ArtifactReference{stdout, stderr}, nil
}
func (c *memoryCapture) Abort(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.abortCalls++
	return nil
}
func (c *memoryCapture) stdoutText() string { return string(c.stdout.bytes()) }
func (c *memoryCapture) stderrText() string { return string(c.stderr.bytes()) }
func (c *memoryCapture) finalizeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finalizeCalls
}
func (c *memoryCapture) abortCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.abortCalls
}

func artifactFor(role string, content []byte) (domain.ArtifactReference, error) {
	digest := domain.NewDigest(content)
	return domain.NewArtifactReference(domain.ArtifactReferenceSpec{
		Reference:   fmt.Sprintf("artifact://observer/%s/%s", role, digest.String()),
		Digest:      digest,
		Size:        int64(len(content)),
		Role:        role,
		Sensitivity: domain.SensitivityInternal,
	})
}

type memoryWriter struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	writeErr error
	closeErr error
	closed   bool
}

func (w *memoryWriter) Write(value []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.closed {
		return 0, os.ErrClosed
	}
	return w.buffer.Write(value)
}
func (w *memoryWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return w.closeErr
}
func (w *memoryWriter) bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buffer.Bytes()...)
}
