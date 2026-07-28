package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

var errInjectedTransactionRollback = errors.New("injected application transaction rollback")

type oneShotStoreFault struct {
	mu     sync.Mutex
	point  string
	err    error
	action func()
}

func (f *oneShotStoreFault) Hit(_ context.Context, point string) error {
	f.mu.Lock()
	if f.point == "" || f.point != point {
		f.mu.Unlock()
		return nil
	}
	err, action := f.err, f.action
	f.point = ""
	f.err = nil
	f.action = nil
	f.mu.Unlock()
	if action != nil {
		action()
	}
	return err
}

func (f *oneShotStoreFault) arm(point string) {
	f.armFailure(point, errInjectedTransactionRollback, nil)
}

func (f *oneShotStoreFault) armCancellation(point string, cancel context.CancelFunc) {
	f.armFailure(point, nil, cancel)
}

func (f *oneShotStoreFault) armFailure(point string, err error, action func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.point = point
	f.err = err
	f.action = action
}

func TestTargetRollbackCannotAliasGenerationRunOrOperationProjection(t *testing.T) {
	faults := &oneShotStoreFault{}
	fixture := newCoreFixtureWithFaults(t, faults)
	view, _ := fixture.acquire(t)
	fixture.readyAgent(t, view.Agent)
	target := fixture.readyTarget(t, view)
	run := fixture.runningRun(t, target)
	operation, err := fixture.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
		Meta: fixture.meta(t, "rollback-operation"), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationShell, CommandDisplay: "inspect suspect process",
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = fixture.core.TransitionTargetOperation(context.Background(), TransitionTargetOperationRequest{
		Meta: fixture.meta(t, "rollback-operation-running"), TargetID: target.ID, OperationID: operation.ID,
		ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	targetID, err := domain.ParseTargetID(before.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := QuarantineTargetRequest{
		Meta: fixture.meta(t, "rollback-quarantine"), TargetID: before.ID, ExpectedRevision: before.Revision,
		Reason: "backend confirmed containment",
		Evidence: ports.TargetQuarantineEvidence{
			Target:    ports.TargetRef{ID: targetID, Generation: domain.TargetGeneration(before.CurrentGeneration)},
			RuntimeID: "runtime-exact", ExecutionStopped: true, NetworkUnreachable: true,
			StatePreserved: true, ObservedAt: fixture.now,
		},
	}

	contained := rollbackThenRetry(t, fixture, faults, func() (TargetRecord, error) {
		return fixture.core.QuarantineTarget(context.Background(), request)
	})
	beforeGeneration, _ := findTargetGeneration(&before, before.CurrentGeneration)
	afterGeneration, _ := findTargetGeneration(&contained, contained.CurrentGeneration)
	beforeRun, _ := findRun(&before, run.ID)
	afterRun, _ := findRun(&contained, run.ID)
	beforeOperation, _ := findOperation(&before, operation.ID)
	afterOperation, _ := findOperation(&contained, operation.ID)
	if afterGeneration.Revision != beforeGeneration.Revision+1 || afterRun.Revision != beforeRun.Revision+1 || afterOperation.Revision != beforeOperation.Revision+1 {
		t.Fatalf("retry revisions = generation %d/%d, run %d/%d, operation %d/%d",
			afterGeneration.Revision, beforeGeneration.Revision, afterRun.Revision, beforeRun.Revision,
			afterOperation.Revision, beforeOperation.Revision)
	}
	if afterGeneration.State != domain.TargetGenerationQuarantined || afterRun.State != domain.TargetRunQuarantined || afterOperation.State != domain.TargetOperationCancelled {
		t.Fatalf("retry states = generation %s, run %s, operation %s", afterGeneration.State, afterRun.State, afterOperation.State)
	}
}

func TestExecRollbackCannotAliasAgentGenerationProjection(t *testing.T) {
	tests := []struct {
		name string
		arm  func(*oneShotStoreFault) (context.Context, error)
	}{
		{
			name: "store transaction",
			arm: func(faults *oneShotStoreFault) (context.Context, error) {
				faults.arm("store.before_commit")
				return context.Background(), errInjectedTransactionRollback
			},
		},
		{
			name: "append control",
			arm: func(faults *oneShotStoreFault) (context.Context, error) {
				ctx, cancel := context.WithCancel(context.Background())
				faults.armCancellation("store.before_handler", cancel)
				return ctx, context.Canceled
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			faults := &oneShotStoreFault{}
			fixture := newCoreFixtureWithFaults(t, faults)
			view, _ := fixture.acquire(t)
			agent := fixture.readyAgent(t, view.Agent)
			execution, err := fixture.core.CreateExec(context.Background(), CreateExecRequest{
				Meta: fixture.meta(t, "rollback-exec"), LeaseID: view.Lease.ID, Kind: domain.ExecTool,
				Executable: "bin/tool", WorkingDirectory: ".",
			})
			if err != nil {
				t.Fatal(err)
			}
			execution, err = fixture.core.TransitionExec(context.Background(), TransitionExecRequest{
				Meta: fixture.meta(t, "rollback-exec-starting"), ExecID: execution.ID,
				ExpectedRevision: execution.Revision, State: domain.ExecStarting,
			})
			if err != nil {
				t.Fatal(err)
			}
			request := TransitionExecRequest{
				Meta: fixture.meta(t, "rollback-exec-running"), ExecID: execution.ID,
				ExpectedRevision: execution.Revision, State: domain.ExecRunning,
			}
			failureContext, failure := test.arm(faults)
			firstAttempt := true
			running := rollbackThenRetryExpecting(t, fixture, failure, func() (ExecRecord, error) {
				ctx := context.Background()
				if firstAttempt {
					firstAttempt = false
					ctx = failureContext
				}
				return fixture.core.TransitionExec(ctx, request)
			})
			view, err = fixture.core.GetResearchSession(context.Background(), view.Session.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeGeneration, _ := findAgentGeneration(&agent, agent.CurrentGeneration)
			afterGeneration, _ := findAgentGeneration(&view.Agent, view.Agent.CurrentGeneration)
			if running.State != domain.ExecRunning || running.Revision != execution.Revision+1 ||
				afterGeneration.State != domain.AgentGenerationRunning || afterGeneration.Revision != beforeGeneration.Revision+1 {
				t.Fatalf("retry result = exec %#v, generation %#v", running, afterGeneration)
			}
		})
	}
}

func TestIncidentLinkRollbackCannotAliasExecOrTargetProjection(t *testing.T) {
	faults := &oneShotStoreFault{}
	fixture := newCoreFixtureWithFaults(t, faults)
	view, _ := fixture.acquire(t)
	agent := fixture.readyAgent(t, view.Agent)
	execution, err := fixture.core.CreateExec(context.Background(), CreateExecRequest{
		Meta: fixture.meta(t, "rollback-incident-exec"), LeaseID: view.Lease.ID,
		Kind: domain.ExecTool, Executable: "bin/tool", WorkingDirectory: ".",
	})
	if err != nil {
		t.Fatal(err)
	}
	target := fixture.readyTarget(t, view)
	run := fixture.runningRun(t, target)
	request := CreateIncidentRequest{
		Meta: fixture.meta(t, "rollback-incident"), Classification: domain.IncidentTargetWorkloadExit,
		SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID,
		AgentGeneration: agent.CurrentGeneration, ExecID: execution.ID, TargetID: target.ID,
		TargetGeneration: target.CurrentGeneration, TargetRunID: run.ID,
		Trigger: "specimen exited", LastKnownState: "running",
		Cause: CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not established", Confidence: 0},
	}
	incident := rollbackThenRetry(t, fixture, faults, func() (IncidentRecord, error) {
		return fixture.core.CreateIncident(context.Background(), request)
	})
	execution, err = fixture.core.GetExec(context.Background(), execution.ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err = fixture.core.GetTarget(context.Background(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	linkedRun, _ := findRun(&target, run.ID)
	if len(execution.IncidentIDs) != 1 || execution.IncidentIDs[0] != incident.ID ||
		len(linkedRun.IncidentIDs) != 1 || linkedRun.IncidentIDs[0] != incident.ID {
		t.Fatalf("retry did not link incident: exec=%v run=%v incident=%s", execution.IncidentIDs, linkedRun.IncidentIDs, incident.ID)
	}
}

func TestRecoveryRollbackCannotAliasGenerationOrIncidentProjection(t *testing.T) {
	t.Run("target", func(t *testing.T) {
		faults := &oneShotStoreFault{}
		fixture := newCoreFixtureWithFaults(t, faults)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		target := fixture.readyTarget(t, view)
		run := fixture.runningRun(t, target)
		bundleID, err := domain.NewObservationBundleID()
		if err != nil {
			t.Fatal(err)
		}
		run, err = fixture.core.FinalizeTargetRun(context.Background(), FinalizeTargetRunRequest{
			Meta: fixture.meta(t, "rollback-target-run"), TargetID: target.ID, RunID: run.ID,
			ExpectedRevision: run.Revision, Failed: true, BundleID: bundleID.String(),
		})
		if err != nil {
			t.Fatal(err)
		}
		incident, err := fixture.core.CreateIncident(context.Background(), CreateIncidentRequest{
			Meta: fixture.meta(t, "rollback-target-incident"), Classification: domain.IncidentLinuxTargetFailure,
			SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID,
			AgentGeneration: agent.CurrentGeneration, TargetID: target.ID, TargetGeneration: target.CurrentGeneration,
			TargetRunID: run.ID, Trigger: "container exited", LastKnownState: "running",
			Cause:     CauseRecord{Kind: domain.CauseUnknown, Summary: "cause not established", Confidence: 0},
			Artifacts: []IncidentArtifactRecord{incidentArtifact("rollback-target")},
		})
		if err != nil {
			t.Fatal(err)
		}
		incident, err = fixture.core.TransitionIncident(context.Background(), TransitionIncidentRequest{
			Meta: fixture.meta(t, "rollback-target-seal"), IncidentID: incident.ID,
			ExpectedRevision: incident.Revision, State: domain.IncidentEvidenceSealed,
		})
		if err != nil {
			t.Fatal(err)
		}
		request := RecoverIncidentRequest{
			Meta: fixture.meta(t, "rollback-target-recovery"), IncidentID: incident.ID,
			ExpectedIncidentRevision: incident.Revision, Resource: RecoveryResourceTarget,
			Strategy: "cold recreate", VisibilityAcknowledgement: "incident delivered",
		}
		outcome := rollbackThenRetry(t, fixture, faults, func() (RecoveryOutcome, error) {
			return fixture.core.RecoverIncident(context.Background(), request)
		})
		if outcome.Target == nil || outcome.Target.CurrentGeneration != target.CurrentGeneration+1 || outcome.Incident.State != domain.IncidentRecovering {
			t.Fatalf("target recovery retry = %#v", outcome)
		}
	})

	t.Run("agent", func(t *testing.T) {
		faults := &oneShotStoreFault{}
		fixture := newCoreFixtureWithFaults(t, faults)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		current, _ := findAgentGeneration(&agent, agent.CurrentGeneration)
		agent, err := fixture.core.TransitionAgentGeneration(context.Background(), TransitionAgentRequest{
			Meta: fixture.meta(t, "rollback-agent-failure"), AgentWorkspaceID: agent.ID,
			Generation: agent.CurrentGeneration, ExpectedRevision: current.Revision, State: domain.AgentGenerationFailed,
		})
		if err != nil {
			t.Fatal(err)
		}
		incident, err := fixture.core.CreateIncident(context.Background(), CreateIncidentRequest{
			Meta: fixture.meta(t, "rollback-agent-incident"), Classification: domain.IncidentAgentWorkspaceFailure,
			SessionID: view.Session.ID, LeaseID: view.Lease.ID, AgentWorkspaceID: agent.ID,
			AgentGeneration: agent.CurrentGeneration, Trigger: "supervisor exited", LastKnownState: "ready",
			Cause:     CauseRecord{Kind: domain.CauseProven, Summary: "supervisor exit observed", Confidence: 1},
			Artifacts: []IncidentArtifactRecord{incidentArtifact("rollback-agent")},
		})
		if err != nil {
			t.Fatal(err)
		}
		incident, err = fixture.core.TransitionIncident(context.Background(), TransitionIncidentRequest{
			Meta: fixture.meta(t, "rollback-agent-seal"), IncidentID: incident.ID,
			ExpectedRevision: incident.Revision, State: domain.IncidentEvidenceSealed,
		})
		if err != nil {
			t.Fatal(err)
		}
		request := RecoverIncidentRequest{
			Meta: fixture.meta(t, "rollback-agent-recovery"), IncidentID: incident.ID,
			ExpectedIncidentRevision: incident.Revision, Resource: RecoveryResourceAgent,
			Strategy: "new provider invocation", VisibilityAcknowledgement: "incident delivered",
		}
		outcome := rollbackThenRetry(t, fixture, faults, func() (RecoveryOutcome, error) {
			return fixture.core.RecoverIncident(context.Background(), request)
		})
		if outcome.Agent == nil || outcome.Lease == nil || outcome.Agent.CurrentGeneration != agent.CurrentGeneration+1 ||
			outcome.Lease.AgentGeneration != outcome.Agent.CurrentGeneration || outcome.Incident.State != domain.IncidentRecovering {
			t.Fatalf("agent recovery retry = %#v", outcome)
		}
	})
}

func applicationProjectionBytes(t *testing.T, core *Core) []byte {
	t.Helper()
	core.mu.Lock()
	defer core.mu.Unlock()
	payload, err := json.Marshal(struct {
		LastSequence int64                           `json:"last_sequence"`
		Sessions     map[string]SessionRecord        `json:"sessions"`
		Leases       map[string]LeaseRecord          `json:"leases"`
		Agents       map[string]AgentWorkspaceRecord `json:"agents"`
		Execs        map[string]ExecRecord           `json:"execs"`
		Targets      map[string]TargetRecord         `json:"targets"`
		Incidents    map[string]IncidentRecord       `json:"incidents"`
	}{core.lastSequence, core.sessions, core.leases, core.agents, core.execs, core.targets, core.incidents})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func rollbackThenRetry[T any](t *testing.T, fixture *coreFixture, faults *oneShotStoreFault, mutate func() (T, error)) T {
	t.Helper()
	faults.arm("store.before_commit")
	return rollbackThenRetryExpecting(t, fixture, errInjectedTransactionRollback, mutate)
}

func rollbackThenRetryExpecting[T any](t *testing.T, fixture *coreFixture, failure error, mutate func() (T, error)) T {
	t.Helper()
	beforeProjection := applicationProjectionBytes(t, fixture.core)
	beforeRecords := controlRecordCount(t, fixture)
	if _, err := mutate(); !errors.Is(err, failure) {
		t.Fatalf("mutation error = %v, want %v", err, failure)
	}
	assertProjectionBytesEqual(t, beforeProjection, applicationProjectionBytes(t, fixture.core))
	if count := controlRecordCount(t, fixture); count != beforeRecords {
		t.Fatalf("rolled-back record count = %d, want %d", count, beforeRecords)
	}
	result, err := mutate()
	if err != nil {
		t.Fatalf("retry mutation: %v", err)
	}
	return result
}

func assertProjectionBytesEqual(t *testing.T, want, got []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("rolled-back transaction mutated projection\nwant: %s\n got: %s", want, got)
	}
}

func controlRecordCount(t *testing.T, fixture *coreFixture) int {
	t.Helper()
	records, err := fixture.store.Records(context.Background(), 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	return len(records)
}
