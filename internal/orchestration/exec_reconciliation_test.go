package orchestration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestControllerReconcilePhysicalResourcesLosesInterruptedExecAfterPhysicalBoundary(t *testing.T) {
	for _, state := range []domain.ExecState{domain.ExecStarting, domain.ExecRunning} {
		t.Run(state.String(), func(t *testing.T) {
			fixture, harness, view, execution := interruptedExecFixture(t, state)
			driver := &recordingExecRecoveryAgentDriver{
				reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
				core:                      fixture.core, execID: execution.ID,
			}
			driver.reconcile = func(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
				latest, err := fixture.core.GetExec(context.Background(), execution.ID)
				if err != nil {
					t.Fatal(err)
				}
				if latest.State != domain.ExecLost || !latest.CleanupConfirmed {
					t.Fatalf("inventory ran before logical loss was committed: %#v", latest)
				}
				return adoptedAgentReport(expected)
			}
			workspaceCalls := &startupRecoveryCallLog{}
			workspace := &startupRecoveryWorkspaceDriver{FakeWorkspaceDriver: harness.workspace, calls: workspaceCalls}
			controller, err := NewController(ControllerConfig{
				Core: fixture.core, Agent: driver, Workspace: workspace, Resolver: harness.resolver,
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			report, err := controller.ReconcilePhysicalResources(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if driver.recoverCalls != 1 || len(driver.recoveredPlans) != 1 {
				t.Fatalf("physical exec recovery calls=%d plans=%d", driver.recoverCalls, len(driver.recoveredPlans))
			}
			if driver.provisionCalls != 0 || len(workspaceCalls.calls) != 0 || strings.Join(driver.events, ",") != "recover,inventory" {
				t.Fatalf("startup mutation order = %v, ordinary provisions=%d workspace calls=%v", driver.events, driver.provisionCalls, workspaceCalls.calls)
			}
			plan := driver.recoveredPlans[0]
			if plan.LeaseID.String() != view.Lease.ID || plan.Generation.Spec().AgentWorkspaceID.String() != view.Agent.ID || uint64(plan.Generation.Spec().Generation) != execution.AgentGeneration {
				t.Fatalf("recovery did not receive exact current plan: %#v", plan)
			}
			if len(report.RecoveredExecs) != 1 || report.RecoveredExecs[0] != execution.ID {
				t.Fatalf("recovery report = %#v", report)
			}
			latest, err := fixture.core.GetExec(context.Background(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			if latest.State != domain.ExecLost || !latest.CleanupConfirmed || latest.Error != interruptedExecError {
				t.Fatalf("interrupted exec = %#v", latest)
			}

			revision := latest.Revision
			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			second, err := controller.ReconcilePhysicalResources(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			latest, err = fixture.core.GetExec(context.Background(), execution.ID)
			if err != nil {
				t.Fatal(err)
			}
			if driver.recoverCalls != 1 || len(second.RecoveredExecs) != 0 || latest.Revision != revision {
				t.Fatalf("terminal exec replayed: calls=%d report=%#v exec=%#v", driver.recoverCalls, second, latest)
			}
		})
	}
}

func TestControllerReconcilePhysicalResourcesReplaysRecoveryAfterPhysicalSuccessBeforeLogicalLoss(t *testing.T) {
	fixture, harness, _, execution := interruptedExecFixture(t, domain.ExecRunning)
	driver := &recordingExecRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		core:                      fixture.core,
		execID:                    execution.ID,
	}
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 3*time.Second)
	driver.afterRecovery = cancelFirst
	controller := newExecRecoveryController(t, fixture, harness, driver)
	_, err := controller.ReconcilePhysicalResources(firstCtx)
	if err == nil {
		t.Fatal("canceled crash-window reconciliation unexpectedly completed")
	}
	latest, loadErr := fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecRunning || driver.recoverCalls != 1 || driver.provisionCalls != 0 || driver.inventoryCalls != 0 {
		t.Fatalf("physical-success crash window = exec %#v recover/provision/inventory %d/%d/%d", latest, driver.recoverCalls, driver.provisionCalls, driver.inventoryCalls)
	}

	driver.afterRecovery = nil
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 3*time.Second)
	report, err := controller.ReconcilePhysicalResources(secondCtx)
	cancelSecond()
	if err != nil {
		t.Fatal(err)
	}
	latest, loadErr = fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecLost || !latest.CleanupConfirmed || driver.recoverCalls != 2 || driver.provisionCalls != 0 || len(report.RecoveredExecs) != 1 {
		t.Fatalf("replayed recovery = exec %#v recover/provision=%d/%d report=%#v", latest, driver.recoverCalls, driver.provisionCalls, report)
	}
}

func TestControllerReconcilePhysicalResourcesFailsClosedWhenExecBoundaryRecoveryFails(t *testing.T) {
	fixture, harness, _, execution := interruptedExecFixture(t, domain.ExecRunning)
	driver := &recordingExecRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		core:                      fixture.core, execID: execution.ID,
		recoverErr: domain.NewError(domain.CodeUnavailable, "test.agent_exec_recovery", "boundary", "injected stop failure", errors.New("still running")),
	}
	controller := newExecRecoveryController(t, fixture, harness, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !strings.Contains(err.Error(), "injected stop failure") {
		t.Fatalf("reconciliation error = %v", err)
	}
	latest, loadErr := fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecRunning || latest.CleanupConfirmed || driver.inventoryCalls != 0 {
		t.Fatalf("failed boundary recovery opened later stages: exec=%#v inventory_calls=%d", latest, driver.inventoryCalls)
	}
}

func TestControllerReconcilePhysicalResourcesRejectsIncompleteExecRecoveryProof(t *testing.T) {
	fixture, harness, _, execution := interruptedExecFixture(t, domain.ExecStarting)
	driver := &recordingExecRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		core:                      fixture.core, execID: execution.ID, invalidProof: true,
	}
	controller := newExecRecoveryController(t, fixture, harness, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !strings.Contains(err.Error(), "execution boundary") {
		t.Fatalf("incomplete proof error = %v", err)
	}
	latest, loadErr := fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecStarting || driver.inventoryCalls != 0 {
		t.Fatalf("incomplete proof was accepted: exec=%#v inventory_calls=%d", latest, driver.inventoryCalls)
	}
}

func TestControllerReconcilePhysicalResourcesRejectsExecRecoveryWithWrongProtocol(t *testing.T) {
	fixture, harness, _, execution := interruptedExecFixture(t, domain.ExecStarting)
	driver := &recordingExecRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		core:                      fixture.core, execID: execution.ID,
		proofMutator: func(proof *ports.AgentExecCrashRecovery) {
			proof.Status.GuestProtocol = uint32(transport.ProtocolVersion) + 1
		},
	}
	controller := newExecRecoveryController(t, fixture, harness, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !strings.Contains(err.Error(), "expected ready agent workspace") {
		t.Fatalf("wrong-protocol proof error = %v", err)
	}
	latest, loadErr := fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecStarting || driver.inventoryCalls != 0 {
		t.Fatalf("wrong-protocol proof was accepted: exec=%#v inventory_calls=%d", latest, driver.inventoryCalls)
	}
}

func TestControllerReconcilePhysicalResourcesAcceptsExecRecoveryWithoutHostVisibleCgroup(t *testing.T) {
	fixture, harness, _, execution := interruptedExecFixture(t, domain.ExecRunning)
	driver := &recordingExecRecoveryAgentDriver{
		reconciliationAgentDriver: &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent},
		core:                      fixture.core,
		execID:                    execution.ID,
		proofMutator: func(proof *ports.AgentExecCrashRecovery) {
			proof.Status.CgroupID = ""
		},
	}
	controller := newExecRecoveryController(t, fixture, harness, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	report, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	latest, loadErr := fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecLost || !latest.CleanupConfirmed || len(report.RecoveredExecs) != 1 {
		t.Fatalf("recovery without host-visible cgroup = exec %#v report=%#v", latest, report)
	}
}

func TestControllerReconcilePhysicalResourcesRequiresExecCrashRecoveryCapability(t *testing.T) {
	fixture, harness, _, execution := interruptedExecFixture(t, domain.ExecStarting)
	base := &reconciliationAgentDriver{FakeAgentWorkspaceDriver: harness.agent}
	driver := agentWithoutExecCrashRecovery{AgentWorkspaceDriver: base, AgentWorkspaceReconciler: base}
	controller := newExecRecoveryController(t, fixture, harness, driver)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err := controller.ReconcilePhysicalResources(ctx)
	cancel()
	if err == nil || !domain.IsCode(err, domain.CodeCapabilityUnavailable) || !strings.Contains(err.Error(), "agent_exec_crash_recovery") {
		t.Fatalf("missing crash recovery capability error = %v", err)
	}
	latest, loadErr := fixture.core.GetExec(context.Background(), execution.ID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if latest.State != domain.ExecStarting {
		t.Fatalf("exec changed without a physical crash recovery capability: %#v", latest)
	}
}

type recordingExecRecoveryAgentDriver struct {
	*reconciliationAgentDriver
	core           *application.Core
	execID         string
	recoverErr     error
	invalidProof   bool
	recoverCalls   int
	provisionCalls int
	inventoryCalls int
	recoveredPlans []ports.AgentWorkspacePlan
	proofMutator   func(*ports.AgentExecCrashRecovery)
	afterRecovery  func()
	events         []string
}

type agentWithoutExecCrashRecovery struct {
	ports.AgentWorkspaceDriver
	ports.AgentWorkspaceReconciler
}

func (d *recordingExecRecoveryAgentDriver) RecoverInterruptedExecs(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentExecCrashRecovery, error) {
	d.recoverCalls++
	d.events = append(d.events, "recover")
	d.recoveredPlans = append(d.recoveredPlans, plan)
	current, err := d.core.GetExec(context.Background(), d.execID)
	if err != nil {
		return ports.AgentExecCrashRecovery{}, err
	}
	if current.State != domain.ExecStarting && current.State != domain.ExecRunning {
		return ports.AgentExecCrashRecovery{}, errors.New("logical exec was finalized before the physical boundary")
	}
	if d.recoverErr != nil {
		return ports.AgentExecCrashRecovery{}, d.recoverErr
	}
	if d.invalidProof {
		return ports.AgentExecCrashRecovery{}, nil
	}
	proof, err := d.FakeAgentWorkspaceDriver.RecoverInterruptedExecs(ctx, plan)
	if err == nil && d.afterRecovery != nil {
		d.afterRecovery()
	}
	if err == nil && d.proofMutator != nil {
		d.proofMutator(&proof)
	}
	return proof, err
}

func (d *recordingExecRecoveryAgentDriver) Provision(ctx context.Context, plan ports.AgentWorkspacePlan) (ports.AgentWorkspaceResult, error) {
	d.provisionCalls++
	d.events = append(d.events, "provision")
	return d.FakeAgentWorkspaceDriver.Provision(ctx, plan)
}

func (d *recordingExecRecoveryAgentDriver) ReconcileAgentWorkspaces(ctx context.Context, request ports.AgentWorkspaceReconciliationRequest) (ports.AgentWorkspaceReconciliationReport, error) {
	d.inventoryCalls++
	d.events = append(d.events, "inventory")
	return d.reconciliationAgentDriver.ReconcileAgentWorkspaces(ctx, request)
}

func interruptedExecFixture(t *testing.T, state domain.ExecState) (*integrationFixture, controllerHarness, application.ResearchSessionView, application.ExecRecord) {
	t.Helper()
	fixture := newIntegrationFixture(t)
	terminalizeFixtureAgent(t, fixture)
	harness := newControllerHarness(t, fixture, nil, nil)
	view := harness.acquire(t, fixture)
	execution, err := fixture.core.CreateExec(context.Background(), application.CreateExecRequest{
		Meta: fixture.meta("startup-interrupted-exec-create"), LeaseID: view.Lease.ID,
		Kind: domain.ExecTool, Executable: "/bin/true", WorkingDirectory: ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err = fixture.core.TransitionExec(context.Background(), application.TransitionExecRequest{
		Meta: fixture.meta("startup-interrupted-exec-starting"), ExecID: execution.ID,
		ExpectedRevision: execution.Revision, State: domain.ExecStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	if state == domain.ExecRunning {
		execution, err = fixture.core.TransitionExec(context.Background(), application.TransitionExecRequest{
			Meta: fixture.meta("startup-interrupted-exec-running"), ExecID: execution.ID,
			ExpectedRevision: execution.Revision, State: domain.ExecRunning,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return fixture, harness, view, execution
}

func newExecRecoveryController(t *testing.T, fixture *integrationFixture, harness controllerHarness, agent ports.AgentWorkspaceDriver) *Controller {
	t.Helper()
	controller, err := NewController(ControllerConfig{
		Core: fixture.core, Agent: agent, Workspace: harness.workspace, Resolver: harness.resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func adoptedAgentReport(expected []ports.AgentWorkspacePlan) ports.AgentWorkspaceReconciliationReport {
	report := ports.AgentWorkspaceReconciliationReport{ObservedAt: time.Now().UTC()}
	for _, plan := range expected {
		spec := plan.Generation.Spec()
		report.Expected = append(report.Expected, ports.AgentWorkspaceReconciliation{
			Ref:         ports.AgentWorkspaceRef{ID: spec.AgentWorkspaceID, Generation: spec.Generation},
			ContainerID: "agent-runtime-" + spec.AgentWorkspaceID.String(), Classification: ports.PhysicalResourceAdopted,
		})
	}
	return report
}

var _ ports.AgentWorkspaceDriver = (*recordingExecRecoveryAgentDriver)(nil)
var _ ports.AgentWorkspaceReconciler = (*recordingExecRecoveryAgentDriver)(nil)
var _ ports.AgentExecCrashReconciler = (*recordingExecRecoveryAgentDriver)(nil)
