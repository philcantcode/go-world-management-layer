package orchestration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
)

func TestStartupRunRecoveryReusesEveryExistingStopReservation(t *testing.T) {
	for _, namespace := range []string{"stop_target_run", "start_target_run_rollback", "lease_termination_run"} {
		t.Run(namespace, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			target, run := fixture.readyTargetAndRun()
			service := fixture.service(Config{})
			key, signature := namespace+"/original-key", namespace+"-original-signature"
			reserved, err := service.reserveBundle(context.Background(), target, run, namespace, key, signature)
			if err != nil {
				t.Fatal(err)
			}

			// A new Service has no in-process knowledge of the first attempt; it
			// must recover the exact owner from the durable state journal.
			restarted, err := New(fixture.serviceConfig(Config{}))
			if err != nil {
				t.Fatal(err)
			}
			fallback := fixture.meta("startup-fallback")
			meta, gotNamespace, gotKey, gotSignature, err := restarted.recoveryFinalizationIdentity(target, run, fallback, "startup_run_recovery", "startup-signature")
			if err != nil {
				t.Fatal(err)
			}
			if gotNamespace != namespace || gotKey != key || gotSignature != signature || meta.IdempotencyKey != key {
				t.Fatalf("recovered identity = namespace=%q key=%q signature=%q meta=%q", gotNamespace, gotKey, gotSignature, meta.IdempotencyKey)
			}
			replayed, err := restarted.reserveBundle(context.Background(), target, run, gotNamespace, gotKey, gotSignature)
			if err != nil {
				t.Fatal(err)
			}
			if replayed != reserved {
				t.Fatalf("reservation replay = %#v, want %#v", replayed, reserved)
			}
		})
	}
}

func TestControllerStartupRunRecoveryCompletesEveryExistingStopReservation(t *testing.T) {
	for _, namespace := range []string{"stop_target_run", "start_target_run_rollback", "lease_termination_run"} {
		t.Run(namespace, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			terminalizeFixtureAgent(t, fixture)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)
			run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
				Meta: fixture.meta("reserved-recovery-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
			})
			if err != nil {
				t.Fatal(err)
			}

			latestTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			latestRun, err := targetRun(latestTarget, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			meta := fixture.meta("reserved-recovery-owner")
			key := namespace + "/original-key"
			meta.IdempotencyKey = key
			signature := namespace + "-original-signature"
			injected := errors.New("injected crash after bundle.reserved")
			harness.capabilities.finalizationFaults = &finalizationFaultHooks{afterBundleReserved: failOnce(injected)}
			if _, err := harness.capabilities.stopAndFinalizeRun(
				context.Background(), latestTarget, latestRun, harness.target, ports.StopForce,
				meta, namespace, key, signature, errors.New("original finalization failure"),
			); !errors.Is(err, injected) {
				t.Fatalf("faulted finalization error = %v, want %v", err, injected)
			}
			reservation, found := harness.capabilities.reservations[run.ID]
			if !found || reservation.Namespace != namespace || reservation.IdempotencyKey != key || reservation.Signature != signature {
				t.Fatalf("durable reservation = %#v, found=%t", reservation, found)
			}

			observerRoot := harness.capabilities.observers.stateRoot
			restartedObservers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
				Ledger: fixture.ledger, IDs: fixture.ids, Clock: fixture.clock.Now, StateRoot: observerRoot,
			})
			if err != nil {
				t.Fatal(err)
			}
			targetDriver := &reconciliationTargetDriver{FakeTargetDriver: harness.target}
			targetDriver.recover = func(plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				return harness.target.PrepareRun(ctx, plan)
			}
			agentDriver := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
			finalization, err := application.NewRunFinalizationService(
				fixture.core, inMemoryBundleFinalizer{}, testkit.NewFakeMaterialAuthority(nil, nil),
			)
			if err != nil {
				t.Fatal(err)
			}
			workspaceScope, err := NewCoreWorkspaceResolver(fixture.core)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := New(fixture.serviceConfig(Config{
				Finalization: finalization, Agent: agentDriver,
				Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
				Observers: restartedObservers, Workspace: harness.workspace, WorkspaceScope: workspaceScope, Captures: harness.capture,
			}))
			if err != nil {
				t.Fatal(err)
			}
			controller, err := NewController(ControllerConfig{
				Core: fixture.core, Agent: agentDriver,
				Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: targetDriver},
				Workspace: harness.workspace, Resolver: harness.resolver, Capabilities: restarted, Observers: restartedObservers,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			report, err := controller.ReconcilePhysicalResources(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.RecoveredRuns) != 1 || report.RecoveredRuns[0] != run.ID || targetDriver.recoverCalls != 1 || targetDriver.stopCalls != 1 {
				t.Fatalf("startup recovery report=%#v recover=%d stop=%d", report, targetDriver.recoverCalls, targetDriver.stopCalls)
			}
			latestTarget, err = fixture.core.GetTarget(ctx, target.ID)
			if err != nil {
				t.Fatal(err)
			}
			latestRun, err = targetRun(latestTarget, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if latestRun.State != domain.TargetRunFailed || latestRun.BundleID != reservation.BundleID || len(latestRun.IncidentIDs) == 0 {
				t.Fatalf("recovered terminal run = %#v; reservation = %#v", latestRun, reservation)
			}
			publication, staged := restarted.publications[run.ID]
			if !staged || publication.Reservation != reservation {
				t.Fatalf("recovered publication reservation = %#v, staged=%t; want %#v", publication.Reservation, staged, reservation)
			}
			incident, err := fixture.core.GetIncident(ctx, latestRun.IncidentIDs[0])
			if err != nil {
				t.Fatal(err)
			}
			if incident.State != domain.IncidentEvidenceSealed || incident.ObservationBundleID != reservation.BundleID || !containsIncidentArtifact(incident.Artifacts, publication.Artifact) {
				t.Fatalf("recovered incident was not sealed to the exact bundle artifact: %#v; artifact=%#v", incident, publication.Artifact)
			}
			completion, complete := restarted.completions[run.ID]
			if !complete || completion.BundleID != reservation.BundleID {
				t.Fatalf("recovered completion = %#v, complete=%t", completion, complete)
			}
			parsedRunID, err := domain.ParseTargetRunID(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := restartedObservers.RequireCommitted(parsedRunID); err != nil {
				t.Fatal(err)
			}
			bundle, err := restarted.GetObservationBundle(ctx, &worldv1.GetObservationBundleRequest{TargetRunId: run.ID})
			if err != nil {
				t.Fatal(err)
			}
			if bundle.BundleId != reservation.BundleID || len(bundle.IncidentIds) == 0 || !bundleContainsControlPlaneLoss(bundle) {
				t.Fatalf("recovered public bundle = %#v", bundle)
			}
		})
	}
}

func containsIncidentArtifact(values []application.IncidentArtifactRecord, expected application.IncidentArtifactRecord) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func bundleContainsControlPlaneLoss(bundle *worldv1.ObservationBundle) bool {
	for _, gap := range bundle.Gaps {
		if strings.Contains(gap.Detail, "control-plane loss") {
			return true
		}
	}
	return false
}

func TestServiceRestartRemovesActualAbandonedAtomicWriteStages(t *testing.T) {
	fixture := newIntegrationFixture(t)
	fixture.service(Config{})
	var stagedPaths []string
	for _, directory := range []string{
		filepath.Join(fixture.stateRoot, bundlePublicationDirectory),
		filepath.Join(fixture.stateRoot, "bundles"),
	} {
		staged, err := os.CreateTemp(directory, ".staging-*")
		if err != nil {
			t.Fatal(err)
		}
		stagedPaths = append(stagedPaths, staged.Name())
		if _, err := staged.Write([]byte(`{"partial":`)); err != nil {
			t.Fatal(err)
		}
		if err := staged.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := staged.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := New(fixture.serviceConfig(Config{})); err != nil {
		t.Fatal(err)
	}
	for _, path := range stagedPaths {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("abandoned atomic stage %s survived restart: %v", path, err)
		}
	}
}

func TestRunFinalizationResumesAfterPublicBundleBoundaryFailures(t *testing.T) {
	tests := []struct {
		name        string
		fault       func(*finalizationFaultHooks, error)
		wantIndexed bool
	}{
		{"before-bundle-file-write", func(h *finalizationFaultHooks, err error) { h.afterCoreCommit = failOnce(err) }, false},
		{"before-bundle-index-append", func(h *finalizationFaultHooks, err error) { h.afterBundleFile = failOnce(err) }, false},
		{"after-bundle-index-append", func(h *finalizationFaultHooks, err error) { h.afterBundleIndexed = failOnce(err) }, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newStopFinalizationHarness(t)
			injected := errors.New("injected hard-crash boundary: " + test.name)
			harness.service.finalizationFaults = &finalizationFaultHooks{}
			test.fault(harness.service.finalizationFaults, injected)
			if _, err := harness.service.StopTargetRun(context.Background(), harness.request); !errors.Is(err, injected) {
				t.Fatalf("faulted finalization error = %v, want %v", err, injected)
			}
			latestTarget, err := harness.fixture.core.GetTarget(context.Background(), harness.target.ID)
			if err != nil {
				t.Fatal(err)
			}
			latestRun, err := targetRun(latestTarget, harness.run.ID)
			if err != nil || !latestRun.State.Terminal() || latestRun.BundleID == "" {
				t.Fatalf("Core did not reach the staged terminal boundary: run=%#v err=%v", latestRun, err)
			}
			if _, staged := harness.service.publications[harness.run.ID]; !staged {
				t.Fatal("terminal Core run has no recoverable public-bundle stage")
			}
			if _, indexed := harness.service.bundles[harness.run.ID]; indexed != test.wantIndexed {
				t.Fatalf("indexed = %t, want %t", indexed, test.wantIndexed)
			}
			if err := harness.fixture.ledger.Close(); err != nil {
				t.Fatal(err)
			}

			planDigest, err := TargetRunProvisioningPlanDigest(harness.plan)
			if err != nil {
				t.Fatal(err)
			}
			bundle, latestRun := resumeStagedBundleAfterLedgerRestart(t, harness.fixture, harness.observerRoot, harness.target.ID, harness.run.ID, planDigest)
			if bundle.BundleId != latestRun.BundleID || bundle.TargetRunId != harness.run.ID {
				t.Fatalf("recovered bundle = %#v", bundle)
			}
		})
	}
}

func TestExactStopRetryCompletesEveryPostTerminalBoundary(t *testing.T) {
	tests := []struct {
		name  string
		fault func(*finalizationFaultHooks, error)
	}{
		{"before-bundle-file-write", func(h *finalizationFaultHooks, err error) { h.afterCoreCommit = failOnce(err) }},
		{"before-bundle-index-append", func(h *finalizationFaultHooks, err error) { h.afterBundleFile = failOnce(err) }},
		{"after-bundle-index-append", func(h *finalizationFaultHooks, err error) { h.afterBundleIndexed = failOnce(err) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newStopFinalizationHarness(t)
			injected := errors.New("injected retry boundary: " + test.name)
			harness.service.finalizationFaults = &finalizationFaultHooks{}
			test.fault(harness.service.finalizationFaults, injected)
			if _, err := harness.service.StopTargetRun(context.Background(), harness.request); !errors.Is(err, injected) {
				t.Fatalf("first stop error = %v, want %v", err, injected)
			}
			bundle, err := harness.service.StopTargetRun(context.Background(), harness.request)
			if err != nil {
				t.Fatal(err)
			}
			if bundle.TargetRunId != harness.run.ID {
				t.Fatalf("retry returned bundle %#v", bundle)
			}
			if _, complete := harness.service.completions[harness.run.ID]; !complete {
				t.Fatal("same-process retry did not durably complete observer publication")
			}
		})
	}
}

func TestFailureIncidentSealingResumesAfterCrashImmediatelyAfterCoreCommit(t *testing.T) {
	harness := newStopFinalizationHarness(t)
	harness.service.targets[domain.TargetLinuxContainer] = failedReceiptTargetDriver{TargetDriver: harness.service.targets[domain.TargetLinuxContainer]}
	injected := errors.New("injected crash after terminal run commit")
	harness.service.finalizationFaults = &finalizationFaultHooks{afterCoreCommit: failOnce(injected)}
	if _, err := harness.service.StopTargetRun(context.Background(), harness.request); !errors.Is(err, injected) {
		t.Fatalf("faulted stop error = %v, want %v", err, injected)
	}
	latestTarget, err := harness.fixture.core.GetTarget(context.Background(), harness.target.ID)
	if err != nil {
		t.Fatal(err)
	}
	latestRun, err := targetRun(latestTarget, harness.run.ID)
	if err != nil || latestRun.State != domain.TargetRunFailed || len(latestRun.IncidentIDs) != 1 {
		t.Fatalf("terminal failed run = %#v, %v", latestRun, err)
	}
	before, err := harness.fixture.core.GetIncident(context.Background(), latestRun.IncidentIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if before.State != domain.IncidentOpen {
		t.Fatalf("fault fired after incident sealing: %#v", before)
	}
	publication := harness.service.publications[harness.run.ID]
	if err := harness.fixture.ledger.Close(); err != nil {
		t.Fatal(err)
	}
	planDigest, err := TargetRunProvisioningPlanDigest(harness.plan)
	if err != nil {
		t.Fatal(err)
	}
	resumeStagedBundleAfterLedgerRestart(t, harness.fixture, harness.observerRoot, harness.target.ID, harness.run.ID, planDigest)
	after, err := harness.fixture.core.GetIncident(context.Background(), latestRun.IncidentIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.IncidentEvidenceSealed || after.ObservationBundleID != latestRun.BundleID || !containsIncidentArtifact(after.Artifacts, publication.Artifact) {
		t.Fatalf("recovered incident = %#v; artifact=%#v", after, publication.Artifact)
	}
}

type stopFinalizationHarness struct {
	fixture      *integrationFixture
	target       application.TargetRecord
	run          application.TargetRunRecord
	plan         ports.TargetRunPlan
	observerRoot string
	service      *Service
	request      *worldv1.StopTargetRunRequest
}

type failedReceiptTargetDriver struct {
	ports.TargetDriver
}

func (d failedReceiptTargetDriver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	receipt, err := d.TargetDriver.StopRun(ctx, runID, mode)
	if err != nil {
		return ports.TargetRunStopReceipt{}, err
	}
	receipt.Outcome = ports.RunFailed
	receipt.FailureKind = ports.TargetRunFailureTarget
	receipt.Observations[len(receipt.Observations)-1].Kind = "target.run.failed"
	return receipt, receipt.Validate()
}

func newStopFinalizationHarness(t *testing.T) stopFinalizationHarness {
	t.Helper()
	fixture := newIntegrationFixture(t)
	target, run := fixture.readyTargetAndRun()
	driver := testkit.NewFakeTargetDriver(domain.CapabilityFingerprint{}, fixture.clock, nil, nil)
	plan, prepared := preparePhysicalTarget(t, fixture, driver, target, run)
	observerRoot := filepath.Join(t.TempDir(), "run-observers")
	observers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: fixture.ledger, IDs: fixture.ids, Clock: fixture.clock.Now, StateRoot: observerRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	observerStart, err := bindRunObserverStart(plan, prepared, target)
	if err != nil {
		t.Fatal(err)
	}
	if err := observers.Start(context.Background(), observerStart); err != nil {
		t.Fatal(err)
	}
	finalizer, err := observationbundle.New(filepath.Join(t.TempDir(), "sealed"))
	if err != nil {
		t.Fatal(err)
	}
	finalization, err := application.NewRunFinalizationService(fixture.core, finalizer, testkit.NewFakeMaterialAuthority(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	service := fixture.service(Config{
		Finalization: finalization, Targets: map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver}, Observers: observers,
	})
	request := &worldv1.StopTargetRunRequest{
		Mutation: fixture.wireMeta("faulted-stop"), TargetId: target.ID,
		TargetRunId: run.ID, ExpectedRevision: run.Revision, Reason: "exercise durable publication recovery",
	}
	return stopFinalizationHarness{fixture: fixture, target: target, run: run, plan: plan, observerRoot: observerRoot, service: service, request: request}
}

func TestLeaseTerminationSuppressionLeavesRecoverableBundlePublication(t *testing.T) {
	fixture := newIntegrationFixture(t)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("suppressed-finalization-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	observerRoot := harness.capabilities.observers.stateRoot
	finalizer, err := observationbundle.New(filepath.Join(t.TempDir(), "termination-sealed"))
	if err != nil {
		t.Fatal(err)
	}
	harness.capabilities.finalization, err = application.NewRunFinalizationService(fixture.core, finalizer, testkit.NewFakeMaterialAuthority(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected crash after bundle.indexed")
	harness.capabilities.finalizationFaults = &finalizationFaultHooks{afterBundleIndexed: failOnce(injected)}
	release := application.ReleaseResearchSessionRequest{
		Meta: fixture.meta("suppressed-finalization-release"), LeaseID: view.Lease.ID,
		ExpectedRevision: view.Lease.Revision, Reason: "prove terminal-error suppression remains recoverable",
	}
	if _, err := harness.controller.ReleaseResearchSession(context.Background(), release); err != nil {
		t.Fatalf("lease termination did not suppress the post-terminal bundle-index failure: %v", err)
	}
	assertTerminationState(t, fixture, view.Session.ID, domain.LeaseReleased, application.LeaseTerminationReleased)
	latestTarget, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	latestRun, err := targetRun(latestTarget, run.ID)
	if err != nil || !latestRun.State.Terminal() || latestRun.BundleID == "" {
		t.Fatalf("lease termination run = %#v, %v", latestRun, err)
	}
	if _, staged := harness.capabilities.publications[run.ID]; !staged {
		t.Fatal("suppressed post-terminal error left no recoverable publication")
	}
	if _, indexed := harness.capabilities.bundles[run.ID]; !indexed {
		t.Fatal("suppressed crash happened before bundle.indexed became durable")
	}

	planDigest, err := domain.ParseDigest(latestRun.ProvisioningPlanDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.ledger.Close(); err != nil {
		t.Fatal(err)
	}
	resumeStagedBundleAfterLedgerRestart(t, fixture, observerRoot, target.ID, run.ID, planDigest)
}

func resumeStagedBundleAfterLedgerRestart(t *testing.T, fixture *integrationFixture, observerRoot, targetID, runID string, planDigest domain.Digest) (*worldv1.ObservationBundle, application.TargetRunRecord) {
	t.Helper()
	reopened, _, err := ledger.Open(ledger.Options{Directory: fixture.ledgerPath, SubscriberBuffer: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	fixture.ledger = reopened
	restartedObservers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Ledger: reopened, IDs: fixture.ids, Clock: fixture.clock.Now, StateRoot: observerRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(fixture.serviceConfig(Config{Observers: restartedObservers}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := restarted.ReconcileRunFinalizations(ctx); err != nil {
		t.Fatal(err)
	}
	latestTarget, err := fixture.core.GetTarget(ctx, targetID)
	if err != nil {
		t.Fatal(err)
	}
	latestRun, err := targetRun(latestTarget, runID)
	if err != nil {
		t.Fatal(err)
	}
	published, err := restarted.bundlePublicationComplete(ctx, latestRun)
	if err != nil || !published {
		t.Fatalf("reconciled bundle publication = %t, %v", published, err)
	}
	parsedRunID, err := domain.ParseTargetRunID(runID)
	if err != nil {
		t.Fatal(err)
	}
	binding := PersistedRunObserverBinding{RunID: parsedRunID, PlanDigest: planDigest, State: latestRun.State, BundlePublished: true}
	if err := restartedObservers.ReconcilePersistedRuns(ctx, []PersistedRunObserverBinding{binding}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcileRunFinalizationCompletions(ctx); err != nil {
		t.Fatal(err)
	}
	bundle, err := restarted.GetObservationBundle(ctx, &worldv1.GetObservationBundleRequest{TargetRunId: runID})
	if err != nil {
		t.Fatal(err)
	}
	return bundle, latestRun
}

func failOnce(injected error) func() error {
	fired := false
	return func() error {
		if fired {
			return nil
		}
		fired = true
		return injected
	}
}
