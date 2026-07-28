package application

import (
	"context"
	"sync"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestQuiesceRejectsExistingExecAndTargetOperation(t *testing.T) {
	t.Run("exec", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		if _, err := fixture.core.CreateExec(context.Background(), CreateExecRequest{
			Meta: fixture.meta(t, "quiesce-exec"), LeaseID: view.Lease.ID,
			Kind: domain.ExecTool, Executable: "bin/tool",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesceRequest(t, fixture, agent, "quiesce-with-exec")); err == nil {
			t.Fatal("quiescing succeeded with a nonterminal exec")
		}
	})

	t.Run("target operation", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		target := fixture.readyTarget(t, view)
		run := fixture.runningRun(t, target)
		if _, err := fixture.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
			Meta: fixture.meta(t, "quiesce-operation"), TargetID: target.ID, RunID: run.ID,
			Kind: domain.TargetOperationShell, CommandDisplay: "write output",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesceRequest(t, fixture, agent, "quiesce-with-operation")); err == nil {
			t.Fatal("quiescing succeeded with a nonterminal target operation")
		}
	})
}

func TestQuiescingClosesTargetWorkAdmission(t *testing.T) {
	t.Run("run", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		target := fixture.readyTarget(t, view)
		if _, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesceRequest(t, fixture, agent, "quiesce-before-run")); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.core.StartTargetRun(context.Background(), StartTargetRunRequest{
			Meta: fixture.meta(t, "run-after-quiesce"), TargetID: target.ID,
			MaterializationDigest: domain.NewDigest([]byte("specimen")).String(),
		}); err == nil {
			t.Fatal("target run started after agent quiescing")
		}
	})

	t.Run("operation", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		target := fixture.readyTarget(t, view)
		run := fixture.runningRun(t, target)
		if _, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesceRequest(t, fixture, agent, "quiesce-before-operation")); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.core.CreateTargetOperation(context.Background(), CreateTargetOperationRequest{
			Meta: fixture.meta(t, "operation-after-quiesce"), TargetID: target.ID, RunID: run.ID,
			Kind: domain.TargetOperationShell, CommandDisplay: "late write",
		}); err == nil {
			t.Fatal("target operation started after agent quiescing")
		}
	})
}

func TestQuiesceAdmissionRacesHaveSingleWinner(t *testing.T) {
	t.Run("exec", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		quiesce := quiesceRequest(t, fixture, agent, "race-quiesce-exec")
		create := CreateExecRequest{
			Meta: fixture.meta(t, "race-create-exec"), LeaseID: view.Lease.ID,
			Kind: domain.ExecTool, Executable: "bin/tool",
		}
		assertExactlyOneMutationSucceeds(t,
			func() error {
				_, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesce)
				return err
			},
			func() error { _, err := fixture.core.CreateExec(context.Background(), create); return err },
		)
	})

	t.Run("target run", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		target := fixture.readyTarget(t, view)
		quiesce := quiesceRequest(t, fixture, agent, "race-quiesce-run")
		start := StartTargetRunRequest{
			Meta: fixture.meta(t, "race-start-run"), TargetID: target.ID,
			MaterializationDigest: domain.NewDigest([]byte("specimen")).String(),
		}
		// Both may succeed only when StartTargetRun linearizes before the
		// quiesce transition. A run itself does not mutate the agent workspace;
		// any later operation on it is rejected by the operation admission gate.
		if succeeded := concurrentMutationSuccesses(
			func() error {
				_, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesce)
				return err
			},
			func() error { _, err := fixture.core.StartTargetRun(context.Background(), start); return err },
		); succeeded < 1 || succeeded > 2 {
			t.Fatalf("successful racing mutations = %d, want one or a start-before-quiesce pair", succeeded)
		}
	})

	t.Run("target operation", func(t *testing.T) {
		fixture := newCoreFixture(t)
		view, _ := fixture.acquire(t)
		agent := fixture.readyAgent(t, view.Agent)
		target := fixture.readyTarget(t, view)
		run := fixture.runningRun(t, target)
		quiesce := quiesceRequest(t, fixture, agent, "race-quiesce-operation")
		create := CreateTargetOperationRequest{
			Meta: fixture.meta(t, "race-create-operation"), TargetID: target.ID, RunID: run.ID,
			Kind: domain.TargetOperationShell, CommandDisplay: "write output",
		}
		assertExactlyOneMutationSucceeds(t,
			func() error {
				_, err := fixture.core.TransitionAgentGeneration(context.Background(), quiesce)
				return err
			},
			func() error { _, err := fixture.core.CreateTargetOperation(context.Background(), create); return err },
		)
	})
}

func quiesceRequest(t *testing.T, fixture *coreFixture, agent AgentWorkspaceRecord, key string) TransitionAgentRequest {
	t.Helper()
	generation := agent.Generations[agent.CurrentGeneration-1]
	return TransitionAgentRequest{
		Meta: fixture.meta(t, key), AgentWorkspaceID: agent.ID,
		Generation: agent.CurrentGeneration, ExpectedRevision: generation.Revision,
		State: domain.AgentGenerationQuiescing,
	}
}

func assertExactlyOneMutationSucceeds(t *testing.T, first, second func() error) {
	t.Helper()
	succeeded := concurrentMutationSuccesses(first, second)
	if succeeded != 1 {
		t.Fatalf("successful racing mutations = %d, want exactly one", succeeded)
	}
}

func concurrentMutationSuccesses(first, second func() error) int {
	start := make(chan struct{})
	errorsByMutation := make(chan error, 2)
	var wait sync.WaitGroup
	for _, mutation := range []func() error{first, second} {
		wait.Add(1)
		go func(mutate func() error) {
			defer wait.Done()
			<-start
			errorsByMutation <- mutate()
		}(mutation)
	}
	close(start)
	wait.Wait()
	close(errorsByMutation)
	succeeded := 0
	for err := range errorsByMutation {
		if err == nil {
			succeeded++
		}
	}
	return succeeded
}
