package orchestration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ledger"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/testkit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lifecycleSagaTargetDriver struct {
	*testkit.FakeTargetDriver

	mu              sync.Mutex
	quarantineCalls int
	destroyCalls    int
	destroyed       map[ports.TargetRef]bool
	quarantined     bool
	physicalCalls   []string
}

func newLifecycleSagaTargetDriver(driver *testkit.FakeTargetDriver) *lifecycleSagaTargetDriver {
	return &lifecycleSagaTargetDriver{FakeTargetDriver: driver, destroyed: make(map[ports.TargetRef]bool)}
}

func (d *lifecycleSagaTargetDriver) ReconcileTargets(_ context.Context, request ports.TargetReconciliationRequest) (ports.TargetReconciliationReport, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	report := ports.TargetReconciliationReport{ObservedAt: time.Now().UTC()}
	for _, plan := range allTargetInventoryPlans(request) {
		spec := plan.Generation.Spec()
		ref := ports.TargetRef{ID: spec.TargetID, Generation: spec.Generation}
		observation := ports.TargetReconciliation{Ref: ref}
		if d.destroyed[ref] {
			observation.Classification = ports.PhysicalResourceMissing
			observation.Diagnostic = "authoritative test absence"
		} else {
			observation.Classification = ports.PhysicalResourceAdopted
			observation.RuntimeID = "runtime-" + ref.ID.String()
			observation.PlanMatched = true
		}
		report.Expected = append(report.Expected, observation)
	}
	return report, nil
}

func (d *lifecycleSagaTargetDriver) Quarantine(ctx context.Context, plan ports.TargetQuarantinePlan) (ports.TargetQuarantineEvidence, error) {
	d.mu.Lock()
	d.quarantineCalls++
	d.mu.Unlock()
	evidence, err := d.FakeTargetDriver.Quarantine(ctx, plan)
	if err != nil {
		return ports.TargetQuarantineEvidence{}, err
	}
	d.mu.Lock()
	d.quarantined = true
	d.physicalCalls = append(d.physicalCalls, "quarantine")
	d.mu.Unlock()
	return evidence, nil
}

func (d *lifecycleSagaTargetDriver) StopRun(ctx context.Context, runID domain.TargetRunID, mode ports.StopMode) (ports.TargetRunStopReceipt, error) {
	d.mu.Lock()
	if d.quarantined {
		d.mu.Unlock()
		return ports.TargetRunStopReceipt{}, domain.NewError(domain.CodeFailedPrecondition, "lifecycle_saga.stop_run", "target", "target is already quarantined", nil)
	}
	d.physicalCalls = append(d.physicalCalls, "stop")
	d.mu.Unlock()
	return d.FakeTargetDriver.StopRun(ctx, runID, mode)
}

func (d *lifecycleSagaTargetDriver) Destroy(ctx context.Context, ref ports.TargetRef) error {
	d.mu.Lock()
	d.destroyCalls++
	d.mu.Unlock()
	if err := d.FakeTargetDriver.Destroy(ctx, ref); err != nil {
		return err
	}
	d.mu.Lock()
	d.destroyed[ref] = true
	d.mu.Unlock()
	return nil
}

func (d *lifecycleSagaTargetDriver) RecoverInterruptedRun(ctx context.Context, plan ports.TargetRunPlan) (ports.PreparedTargetRun, error) {
	d.mu.Lock()
	quarantined := d.quarantined
	d.mu.Unlock()
	if quarantined {
		return ports.PreparedTargetRun{}, domain.NewError(domain.CodeFailedPrecondition, "lifecycle_saga.recover_run", "target", "target is already quarantined", nil)
	}
	return d.FakeTargetDriver.PrepareRun(ctx, plan)
}

func (d *lifecycleSagaTargetDriver) counts() (quarantines, destroys int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.quarantineCalls, d.destroyCalls
}

func (d *lifecycleSagaTargetDriver) callOrder() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.physicalCalls...)
}

func reloadLifecycleSaga(t *testing.T, fixture *integrationFixture, harness controllerHarness, driver ports.TargetDriver) (*Service, *Controller) {
	t.Helper()
	previousObservers := harness.controller.observers
	observers, err := NewRunObserverCoordinator(RunObserverCoordinatorConfig{
		Driver: previousObservers.driver, Ledger: previousObservers.ledger, IDs: previousObservers.ids,
		Clock: previousObservers.clock, StateRoot: previousObservers.stateRoot,
		CleanupTimeout: previousObservers.cleanupTimeout, MaxJournalBytes: previousObservers.maxJournalBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	capabilities := fixture.service(Config{
		Finalization: harness.capabilities.finalization, Agent: harness.agent,
		Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver},
		Workspace: harness.workspace, WorkspaceScope: harness.capabilities.workspaceScope,
		Material: harness.capabilities.material, Captures: harness.capture,
		Observers: observers, ActionEvidence: harness.capabilities.actionEvidence,
	})
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		Targets:   map[domain.TargetKind]ports.TargetDriver{domain.TargetLinuxContainer: driver},
		Workspace: harness.workspace, Resolver: harness.resolver, Capabilities: capabilities,
		Observers: observers,
	})
	if err != nil {
		t.Fatal(err)
	}
	return capabilities, controller
}

func reconcileLifecycleSaga(t *testing.T, controller *Controller) PhysicalReconciliationReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, err := controller.ReconcilePhysicalResources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func TestQuarantineTargetRestartConvergesEveryDurableCrashWindow(t *testing.T) {
	tests := []struct {
		name                    string
		install                 func(*targetLifecycleFaultHooks, error)
		wantCallsBeforeRestart  int
		wantCallsAfterRecovery  int
		wantContainedBeforeBoot bool
		wantGenerationBefore    domain.TargetGenerationState
		activeRun               bool
		wantTerminalBeforeBoot  bool
		wantRecoveredRun        bool
	}{
		{
			name: "reservation_before_containment",
			install: func(hooks *targetLifecycleFaultHooks, crash error) {
				hooks.afterQuarantineReserved = func() error { return crash }
			},
			wantCallsAfterRecovery: 1,
			wantGenerationBefore:   domain.TargetGenerationReady,
		},
		{
			name: "containment_before_logical_commit",
			install: func(hooks *targetLifecycleFaultHooks, crash error) {
				hooks.afterQuarantineContained = func() error { return crash }
			},
			wantCallsBeforeRestart:  1,
			wantCallsAfterRecovery:  1,
			wantContainedBeforeBoot: true,
			wantGenerationBefore:    domain.TargetGenerationResettable,
		},
		{
			name: "reserved_interrupted_run_before_admission_barrier",
			install: func(hooks *targetLifecycleFaultHooks, crash error) {
				hooks.afterQuarantineReserved = func() error { return crash }
			},
			wantCallsAfterRecovery: 1,
			wantGenerationBefore:   domain.TargetGenerationReady,
			activeRun:              true,
			wantRecoveredRun:       true,
		},
		{
			name: "finalized_run_and_containment_before_logical_commit",
			install: func(hooks *targetLifecycleFaultHooks, crash error) {
				hooks.afterQuarantineContained = func() error { return crash }
			},
			wantCallsBeforeRestart:  1,
			wantCallsAfterRecovery:  1,
			wantContainedBeforeBoot: true,
			wantGenerationBefore:    domain.TargetGenerationResettable,
			activeRun:               true,
			wantTerminalBeforeBoot:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newIntegrationFixture(t)
			terminalizeFixtureAgent(t, fixture)
			harness := newControllerHarness(t, fixture, nil, nil)
			view := harness.acquire(t, fixture)
			target := harness.createTarget(t, fixture, view)
			driver := newLifecycleSagaTargetDriver(harness.target)
			harness.capabilities.targets[domain.TargetLinuxContainer] = driver
			harness.controller.targets[domain.TargetLinuxContainer] = driver
			var interruptedRunID string
			if test.activeRun {
				run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
					Meta: fixture.meta("quarantine-interrupted-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
				})
				if err != nil {
					t.Fatal(err)
				}
				interruptedRunID = run.ID
				target, err = fixture.core.GetTarget(context.Background(), target.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			crash := errors.New("simulated process loss at " + test.name)
			hooks := &targetLifecycleFaultHooks{}
			test.install(hooks, crash)
			harness.capabilities.lifecycleFaults = hooks
			request := &worldv1.QuarantineTargetRequest{
				Mutation: fixture.wireMeta("quarantine-crash"), TargetId: target.ID,
				ExpectedRevision: target.Revision, Reason: "preserve exact target evidence",
			}
			if _, err := harness.capabilities.QuarantineTarget(context.Background(), request); !errors.Is(err, crash) {
				t.Fatalf("QuarantineTarget() error = %v, want %v", err, crash)
			}
			quarantines, destroys := driver.counts()
			if quarantines != test.wantCallsBeforeRestart || destroys != 0 {
				t.Fatalf("physical calls before restart = quarantine %d destroy %d", quarantines, destroys)
			}
			latest, err := fixture.core.GetTarget(context.Background(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			generation, err := targetGeneration(latest)
			if err != nil || generation.State != test.wantGenerationBefore {
				t.Fatalf("logical state crossed crash boundary: generation=%#v err=%v", generation, err)
			}
			if test.activeRun {
				persistedRun, runErr := targetRun(latest, interruptedRunID)
				if runErr != nil {
					t.Fatal(runErr)
				}
				if test.wantTerminalBeforeBoot {
					if !persistedRun.State.Terminal() || persistedRun.State == domain.TargetRunQuarantined || persistedRun.BundleID == "" || persistedRun.BundleArtifact == "" || persistedRun.BundleDigest == "" {
						t.Fatalf("run was not evidence-finalized before containment: %#v", persistedRun)
					}
					parsedRunID, parseErr := domain.ParseTargetRunID(persistedRun.ID)
					if parseErr != nil {
						t.Fatal(parseErr)
					}
					if commitErr := harness.capabilities.observers.RequireCommitted(parsedRunID); commitErr != nil {
						t.Fatalf("observer finalization was not committed before containment: %v", commitErr)
					}
				} else if persistedRun.State.Terminal() || persistedRun.BundleID != "" {
					t.Fatalf("run crossed the reserved-only crash boundary: %#v", persistedRun)
				}
			}
			reservation, found := harness.capabilities.operationReservation("quarantine_target", target.ID, target.CurrentGeneration)
			if !found || reservation.Quarantine == nil {
				t.Fatal("exact quarantine intent was not durably reserved")
			}
			_, contained := harness.capabilities.targetQuarantineContainment(reservation)
			if contained != test.wantContainedBeforeBoot {
				t.Fatalf("durable containment before restart = %t, want %t", contained, test.wantContainedBeforeBoot)
			}

			capabilities, controller := reloadLifecycleSaga(t, fixture, harness, driver)
			report := reconcileLifecycleSaga(t, controller)
			quarantines, destroys = driver.counts()
			ref := ports.TargetRef{ID: mustTargetID(t, target.ID), Generation: domain.TargetGeneration(target.CurrentGeneration)}
			if quarantines != test.wantCallsAfterRecovery || destroys != 0 || len(report.RecoveredTargetQuarantines) != 1 || report.RecoveredTargetQuarantines[0] != ref {
				t.Fatalf("quarantine recovery mismatch: calls=%d destroys=%d report=%#v", quarantines, destroys, report)
			}
			if test.wantRecoveredRun && (len(report.RecoveredRuns) != 1 || report.RecoveredRuns[0] != interruptedRunID) {
				t.Fatalf("reserved interrupted run was not finalized before containment: report=%#v", report)
			}
			if !test.wantRecoveredRun && len(report.RecoveredRuns) != 0 {
				t.Fatalf("startup repeated already-complete run finalization: report=%#v", report)
			}
			latest, err = fixture.core.GetTarget(context.Background(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			generation, err = targetGeneration(latest)
			if err != nil || generation.State != domain.TargetGenerationQuarantined {
				t.Fatalf("quarantine was not committed: generation=%#v err=%v", generation, err)
			}
			if test.activeRun {
				persistedRun, runErr := targetRun(latest, interruptedRunID)
				if runErr != nil || !persistedRun.State.Terminal() || persistedRun.State == domain.TargetRunQuarantined || persistedRun.BundleID == "" || persistedRun.BundleArtifact == "" || persistedRun.BundleDigest == "" {
					t.Fatalf("recovered quarantine run lacks final evidence: run=%#v err=%v", persistedRun, runErr)
				}
				if got := driver.callOrder(); len(got) != 2 || got[0] != "stop" || got[1] != "quarantine" {
					t.Fatalf("recovered physical call order = %v, want [stop quarantine]", got)
				}
			}
			if _, err := capabilities.QuarantineTarget(context.Background(), request); err != nil {
				t.Fatalf("exact terminal replay failed: %v", err)
			}
			second := reconcileLifecycleSaga(t, controller)
			quarantines, destroys = driver.counts()
			if quarantines != test.wantCallsAfterRecovery || destroys != 0 || len(second.RecoveredTargetQuarantines) != 0 {
				t.Fatalf("completed quarantine replayed physical work: calls=%d destroys=%d report=%#v", quarantines, destroys, second)
			}
		})
	}
}

func TestQuarantineTargetRestartFailsClosedWhenRunCannotBeFinalizedBeforeContainment(t *testing.T) {
	t.Run("durable containment predates bound run finalization", func(t *testing.T) {
		fixture := newIntegrationFixture(t)
		terminalizeFixtureAgent(t, fixture)
		harness := newControllerHarness(t, fixture, nil, nil)
		view := harness.acquire(t, fixture)
		target := harness.createTarget(t, fixture, view)
		driver := newLifecycleSagaTargetDriver(harness.target)
		harness.capabilities.targets[domain.TargetLinuxContainer] = driver
		harness.controller.targets[domain.TargetLinuxContainer] = driver
		run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
			Meta: fixture.meta("contained-live-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
		})
		if err != nil {
			t.Fatal(err)
		}
		target, err = fixture.core.GetTarget(context.Background(), target.ID)
		if err != nil {
			t.Fatal(err)
		}
		reservation := reserveLifecycleSagaQuarantine(t, fixture, harness.capabilities, target, "contained-live-quarantine")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evidence, err := driver.Quarantine(ctx, reservation.Quarantine.Plan)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := harness.capabilities.persistTargetQuarantineContainment(ctx, reservation, evidence, ledger.Identity{
			ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID, TargetGeneration: target.CurrentGeneration,
		}); err != nil {
			t.Fatal(err)
		}

		_, controller := reloadLifecycleSaga(t, fixture, harness, driver)
		if _, err := controller.ReconcilePhysicalResources(ctx); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("ReconcilePhysicalResources() error = %v, want integrity violation", err)
		}
		latest, err := fixture.core.GetTarget(ctx, target.ID)
		if err != nil {
			t.Fatal(err)
		}
		generation, err := targetGeneration(latest)
		persistedRun, runErr := targetRun(latest, run.ID)
		if err != nil || runErr != nil || generation.State != domain.TargetGenerationReady || persistedRun.State != domain.TargetRunRunning || persistedRun.BundleID != "" {
			t.Fatalf("fail-closed state changed: generation=%#v run=%#v errors=%v/%v", generation, persistedRun, err, runErr)
		}
		if got := driver.callOrder(); len(got) != 1 || got[0] != "quarantine" {
			t.Fatalf("startup attempted to reopen a contained run: calls=%v", got)
		}
	})

	t.Run("unbound run blocks physical containment", func(t *testing.T) {
		fixture := newIntegrationFixture(t)
		terminalizeFixtureAgent(t, fixture)
		harness := newControllerHarness(t, fixture, nil, nil)
		view := harness.acquire(t, fixture)
		target := harness.createTarget(t, fixture, view)
		driver := newLifecycleSagaTargetDriver(harness.target)
		harness.capabilities.targets[domain.TargetLinuxContainer] = driver
		harness.controller.targets[domain.TargetLinuxContainer] = driver
		run, err := fixture.core.StartTargetRun(context.Background(), application.StartTargetRunRequest{
			Meta: fixture.meta("unbound-quarantine-run"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
		})
		if err != nil {
			t.Fatal(err)
		}
		target, err = fixture.core.GetTarget(context.Background(), target.ID)
		if err != nil {
			t.Fatal(err)
		}
		crash := errors.New("simulated crash after unbound quarantine reservation")
		harness.capabilities.lifecycleFaults = &targetLifecycleFaultHooks{afterQuarantineReserved: func() error { return crash }}
		if _, err := harness.capabilities.QuarantineTarget(context.Background(), &worldv1.QuarantineTargetRequest{
			Mutation: fixture.wireMeta("unbound-quarantine"), TargetId: target.ID,
			ExpectedRevision: target.Revision, Reason: "contain only after run evidence exists",
		}); !errors.Is(err, crash) {
			t.Fatalf("QuarantineTarget() error = %v, want %v", err, crash)
		}

		_, controller := reloadLifecycleSaga(t, fixture, harness, driver)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := controller.ReconcilePhysicalResources(ctx); !domain.IsCode(err, domain.CodeIntegrityViolation) {
			t.Fatalf("ReconcilePhysicalResources() error = %v, want integrity violation", err)
		}
		quarantines, _ := driver.counts()
		if quarantines != 0 {
			t.Fatalf("startup physically quarantined an unfinalized run: calls=%d", quarantines)
		}
		latest, err := fixture.core.GetTarget(ctx, target.ID)
		if err != nil {
			t.Fatal(err)
		}
		generation, err := targetGeneration(latest)
		persistedRun, runErr := targetRun(latest, run.ID)
		if err != nil || runErr != nil || generation.State != domain.TargetGenerationResettable || persistedRun.State != domain.TargetRunRequested || persistedRun.BundleID != "" {
			t.Fatalf("unbound run did not remain fail-closed: generation=%#v run=%#v errors=%v/%v", generation, persistedRun, err, runErr)
		}
	})
}

func reserveLifecycleSagaQuarantine(t *testing.T, fixture *integrationFixture, service *Service, target application.TargetRecord, key string) operationReservation {
	t.Helper()
	meta := fixture.meta(key)
	reason := "preserve exact target evidence"
	signature, err := requestSignature(struct {
		TargetID string `json:"target_id"`
		Revision uint64 `json:"revision"`
		Reason   string `json:"reason"`
		Policy   string `json:"policy"`
	}{target.ID, target.Revision, reason, meta.AuthorizedPolicyReference})
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := domain.ParseTargetID(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	plan := ports.TargetQuarantinePlan{
		IdempotencyKey: meta.IdempotencyKey,
		Target:         ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(target.CurrentGeneration)},
		Reason:         reason,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reservation, err := service.reserveTargetQuarantine(ctx, target.ID, meta.IdempotencyKey, signature, ledger.Identity{
		ResearchSessionID: target.SessionID, LeaseID: target.LeaseID, TargetID: target.ID, TargetGeneration: target.CurrentGeneration,
	}, targetQuarantineIntent{Plan: plan, CommitMeta: childMeta(meta, "commit", time.Time{})})
	if err != nil {
		t.Fatal(err)
	}
	return reservation
}

func TestDestroyReservationNeverDeletesRunAdmittedBeforeBarrier(t *testing.T) {
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	target := harness.createTarget(t, fixture, view)
	driver := newLifecycleSagaTargetDriver(harness.target)
	harness.capabilities.targets[domain.TargetLinuxContainer] = driver

	reserved := make(chan struct{})
	release := make(chan struct{})
	harness.capabilities.lifecycleFaults = &targetLifecycleFaultHooks{afterDestroyReserved: func() error {
		close(reserved)
		<-release
		return nil
	}}
	request := &worldv1.DestroyTargetRequest{
		Mutation: fixture.wireMeta("destroy-race"), TargetId: target.ID,
		ExpectedRevision: target.Revision, Reason: "destroy only after authoritative finalization",
	}
	destroyResult := make(chan error, 1)
	go func() {
		_, err := harness.capabilities.DestroyTarget(context.Background(), request)
		destroyResult <- err
	}()
	select {
	case <-reserved:
	case <-time.After(5 * time.Second):
		t.Fatal("destroy did not reach its durable reservation boundary")
	}
	run, err := harness.controller.StartTargetRun(context.Background(), application.StartTargetRunRequest{
		Meta: fixture.meta("run-racing-destroy"), TargetID: target.ID, MaterializationDigest: harness.materialDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-destroyResult; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DestroyTarget() error = %v, want FailedPrecondition", err)
	}
	_, destroys := driver.counts()
	if destroys != 0 {
		t.Fatalf("physical destroy ran against a nonterminal run: calls=%d", destroys)
	}
	latest, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := targetGeneration(latest)
	if err != nil || generation.State != domain.TargetGenerationResettable {
		t.Fatalf("destroy admission barrier was not retained: generation=%#v err=%v", generation, err)
	}
	persistedRun, err := targetRun(latest, run.ID)
	if err != nil || persistedRun.State != domain.TargetRunRunning {
		t.Fatalf("racing run was not preserved: run=%#v err=%v", persistedRun, err)
	}

	_, controller := reloadLifecycleSaga(t, fixture, harness, driver)
	first := reconcileLifecycleSaga(t, controller)
	_, destroys = driver.counts()
	if destroys != 0 || len(first.DeferredTargetDestructions) != 1 || len(first.RecoveredRuns) != 1 || first.RecoveredRuns[0] != run.ID {
		t.Fatalf("first restart deleted before run finalization: destroys=%d report=%#v", destroys, first)
	}
	latest, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	persistedRun, err = targetRun(latest, run.ID)
	if err != nil || !persistedRun.State.Terminal() {
		t.Fatalf("startup did not authoritatively finalize deferred run: run=%#v err=%v", persistedRun, err)
	}

	second := reconcileLifecycleSaga(t, controller)
	_, destroys = driver.counts()
	if destroys != 1 || len(second.RecoveredTargetDestructions) != 1 || len(second.DeferredTargetDestructions) != 0 {
		t.Fatalf("second restart did not converge destruction: destroys=%d report=%#v", destroys, second)
	}
	latest, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	generation, err = targetGeneration(latest)
	if err != nil || generation.State != domain.TargetGenerationDestroyed {
		t.Fatalf("destroy was not logically committed: generation=%#v err=%v", generation, err)
	}
}

var _ ports.TargetReconciler = (*lifecycleSagaTargetDriver)(nil)
var _ ports.TargetRunCrashReconciler = (*lifecycleSagaTargetDriver)(nil)
