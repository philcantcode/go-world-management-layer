package agentrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/research"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

type scriptedExecTransport struct {
	mu          sync.Mutex
	sent        []transport.Frame
	frames      chan transport.Frame
	script      []transport.Frame
	start       sync.Once
	closed      chan struct{}
	close       sync.Once
	trigger     transport.Kind
	receiving   chan struct{}
	receiveOnce sync.Once
}

func newScriptedExecTransport(script ...transport.Frame) *scriptedExecTransport {
	return &scriptedExecTransport{
		frames:    make(chan transport.Frame, len(script)),
		script:    script,
		closed:    make(chan struct{}),
		trigger:   transport.KindCloseInput,
		receiving: make(chan struct{}),
	}
}

func (s *scriptedExecTransport) Send(ctx context.Context, kind transport.Kind, data []byte) (transport.Frame, error) {
	if err := ports.RequireDeadline(ctx, "test_transport.send"); err != nil {
		return transport.Frame{}, err
	}
	s.mu.Lock()
	frame := transport.Frame{Sequence: uint64(len(s.sent) + 1), Kind: kind, Data: append([]byte(nil), data...)}
	s.sent = append(s.sent, frame)
	s.mu.Unlock()
	if kind == s.trigger {
		s.start.Do(func() {
			for index, scripted := range s.script {
				scripted.Sequence = uint64(index + 1)
				s.frames <- scripted
			}
		})
	}
	return frame, nil
}

func (s *scriptedExecTransport) Receive(ctx context.Context) (transport.Frame, error) {
	if err := ports.RequireDeadline(ctx, "test_transport.receive"); err != nil {
		return transport.Frame{}, err
	}
	s.receiveOnce.Do(func() { close(s.receiving) })
	select {
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	case <-s.closed:
		return transport.Frame{}, io.EOF
	case frame := <-s.frames:
		return frame, nil
	}
}

func (s *scriptedExecTransport) Close() error {
	s.close.Do(func() { close(s.closed) })
	return nil
}

func (s *scriptedExecTransport) Sent() []transport.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]transport.Frame, len(s.sent))
	copy(result, s.sent)
	return result
}

type execOnlyDriver struct {
	stream ports.ExecTransport
	plan   ports.ExecPlan
}

func (d *execOnlyDriver) Probe(context.Context) (domain.CapabilityFingerprint, error) {
	return domain.CapabilityFingerprint{}, errors.New("not used")
}
func (d *execOnlyDriver) Provision(context.Context, ports.AgentWorkspacePlan) (ports.AgentWorkspaceResult, error) {
	return ports.AgentWorkspaceResult{}, errors.New("not used")
}
func (d *execOnlyDriver) OpenExec(_ context.Context, plan ports.ExecPlan) (ports.ExecTransport, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	d.plan = plan
	return d.stream, nil
}
func (d *execOnlyDriver) Inspect(context.Context, ports.AgentWorkspaceRef) (ports.AgentWorkspaceStatus, error) {
	return ports.AgentWorkspaceStatus{}, errors.New("not used")
}
func (d *execOnlyDriver) Stop(context.Context, ports.AgentWorkspaceRef, ports.StopMode) error {
	return errors.New("not used")
}
func (d *execOnlyDriver) Destroy(context.Context, ports.AgentWorkspaceRef) error {
	return errors.New("not used")
}

type environmentFixture struct {
	core        *application.Core
	environment *Environment
	driver      *execOnlyDriver
	stream      *scriptedExecTransport
	sessionID   string
}

func newEnvironmentFixture(t *testing.T, stream *scriptedExecTransport) *environmentFixture {
	t.Helper()
	controlStore, err := store.Open(context.Background(), store.Options{Path: filepath.Join(t.TempDir(), "world.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	core, err := application.NewCore(context.Background(), application.CoreOptions{Store: controlStore})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	view, err := core.AcquireResearchSession(ctx, application.AcquireRequest{Meta: mutationMeta(t, "acquire", ctx), OwnerSubject: "runner-test", InputViewID: domain.NewInputViewID([]byte("view")).String(), PolicyDigest: domain.NewDigest([]byte("policy")).String(), CapabilityDigest: domain.NewDigest([]byte("capability")).String(), TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	agent := view.Agent
	for _, state := range []domain.AgentGenerationState{domain.AgentGenerationBooting, domain.AgentGenerationReady} {
		generation := agent.Generations[0]
		agent, err = core.TransitionAgentGeneration(ctx, application.TransitionAgentRequest{Meta: mutationMeta(t, "agent-"+state.String(), ctx), AgentWorkspaceID: agent.ID, Generation: 1, ExpectedRevision: generation.Revision, State: state})
		if err != nil {
			t.Fatal(err)
		}
	}
	leaseID, _ := domain.ParseLeaseID(view.Lease.ID)
	agentID, _ := domain.ParseAgentWorkspaceID(agent.ID)
	driver := &execOnlyDriver{stream: stream}
	environment, err := New(Options{Core: core, Driver: driver, LeaseID: leaseID, AgentWorkspaceID: agentID, AgentGeneration: 1, CapabilityDigest: domain.NewDigest([]byte("capability")), AuthorizedPolicyReference: "policy:test"})
	if err != nil {
		t.Fatal(err)
	}
	return &environmentFixture{core: core, environment: environment, driver: driver, stream: stream, sessionID: view.Session.ID}
}

func mutationMeta(t *testing.T, key string, ctx context.Context) application.MutationMeta {
	t.Helper()
	correlationID, err := domain.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	deadline, _ := ctx.Deadline()
	return application.MutationMeta{IdempotencyKey: key, CorrelationID: correlationID.String(), AuthorizedPolicyReference: "policy:test", Deadline: deadline}
}

func requestCorrelation(t *testing.T) string {
	t.Helper()
	id, err := domain.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

func TestExecutePreservesOrderedSeparateStreamsAndInput(t *testing.T) {
	terminal, _ := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: true})
	stream := newScriptedExecTransport(
		transport.Frame{Kind: transport.KindStdout, Data: []byte("one")},
		transport.Frame{Kind: transport.KindStderr, Data: []byte("two")},
		transport.Frame{Kind: transport.KindStdout, Data: []byte("three")},
		transport.Frame{Kind: transport.KindTerminal, Data: terminal},
	)
	fixture := newEnvironmentFixture(t, stream)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ordered []string
	result, err := fixture.environment.Execute(ctx, Request{IdempotencyKey: "provider-run", CorrelationID: requestCorrelation(t), Kind: domain.ExecProvider, Executable: "bin/provider", Argv: []string{"run"}, Stdin: []byte("input bytes"), MaxOutputBytes: 1024, CleanupGrace: time.Second, OnStdout: func(value []byte) error { ordered = append(ordered, "out:"+string(value)); return nil }, OnStderr: func(value []byte) error { ordered = append(ordered, "err:"+string(value)); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 0 || !result.CleanupConfirmed {
		t.Fatalf("result = %#v", result)
	}
	want := []string{"out:one", "err:two", "out:three"}
	if len(ordered) != len(want) {
		t.Fatalf("stream callbacks = %#v", ordered)
	}
	for index := range want {
		if ordered[index] != want[index] {
			t.Fatalf("stream callbacks = %#v", ordered)
		}
	}
	var stdin bytes.Buffer
	sent := stream.Sent()
	for _, frame := range sent {
		if frame.Kind == transport.KindStdin {
			stdin.Write(frame.Data)
		}
	}
	if stdin.String() != "input bytes" || sent[len(sent)-1].Kind != transport.KindCloseInput {
		t.Fatalf("sent frames = %#v", sent)
	}
	view, err := fixture.core.GetResearchSession(context.Background(), fixture.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Execs) != 1 || view.Execs[0].State != domain.ExecCompleted || view.Agent.Generations[0].State != domain.AgentGenerationRunning {
		t.Fatalf("persisted execution = %#v", view)
	}
	if fixture.environment.ID() == "" || fixture.driver.plan.Exec.ID().String() != result.ExecID {
		t.Fatal("environment identity or driver plan was not bound to the exec")
	}
}

func TestExecuteCancellationFinalizesExec(t *testing.T) {
	stream := newScriptedExecTransport()
	fixture := newEnvironmentFixture(t, stream)
	// Keep a generous deadline for control-plane setup; cancel only after the
	// scripted transport is blocked in Receive so cancellation hits exchange.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := fixture.environment.Execute(ctx, Request{
			IdempotencyKey: "cancelled-run", CorrelationID: requestCorrelation(t),
			Kind: domain.ExecTool, Executable: "bin/tool", CleanupGrace: 10 * time.Millisecond,
		})
		errCh <- err
	}()
	select {
	case <-stream.receiving:
	case err := <-errCh:
		t.Fatalf("execution finished before Receive: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for guest Receive before cancellation")
	}
	cancel()
	err := <-errCh
	var executionError *ExecutionError
	if !errors.As(err, &executionError) || executionError.ExecID == "" {
		t.Fatalf("execution error = %T %v", err, err)
	}
	view, getErr := fixture.core.GetResearchSession(context.Background(), fixture.sessionID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if len(view.Execs) != 1 || view.Execs[0].State != domain.ExecCancelled {
		t.Fatalf("cancelled execution was not finalized: %#v", view.Execs)
	}
}

func TestExecuteKeepsGuestLeaseAlive(t *testing.T) {
	terminal, _ := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: true})
	stream := newScriptedExecTransport(transport.Frame{Kind: transport.KindTerminal, Data: terminal})
	stream.trigger = transport.KindHeartbeat
	fixture := newEnvironmentFixture(t, stream)
	fixture.environment.heartbeatInterval = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := fixture.environment.Execute(ctx, Request{IdempotencyKey: "heartbeat-run", CorrelationID: requestCorrelation(t), Kind: domain.ExecTool, Executable: "bin/tool", CleanupGrace: time.Second}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, frame := range stream.Sent() {
		found = found || frame.Kind == transport.KindHeartbeat
	}
	if !found {
		t.Fatal("execution sent no heartbeat")
	}
}

func TestBeginActionRecordsBeginFailureMarker(t *testing.T) {
	// beginAction records a durable marker on Begin failure (fail-open contract).
	evidence, err := research.NewStore(research.StoreOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	env := &Environment{actionEvidence: evidence}
	ctx := context.Background()
	start := research.StartFromCommand("exec_marker-1", research.ActionScopeAgentExec, "bin/tool", nil, ".", time.Now().UTC(), research.ResolveObservationLevel(false, "", false))
	if _, err := evidence.Begin(ctx, start); err != nil {
		t.Fatal(err)
	}
	session, beginErr := env.beginAction(ctx, application.ExecRecord{
		ID: "exec_marker-1", SessionID: "rs", LeaseID: "lease_1",
		AgentWorkspaceID: "aw", AgentGeneration: 1,
		Executable: "bin/tool", WorkingDirectory: ".", CreatedAt: time.Now().UTC(),
	}, Request{IdempotencyKey: "x", CorrelationID: requestCorrelation(t)})
	if beginErr == nil || session != nil {
		t.Fatalf("expected begin conflict, got session=%v err=%v", session, beginErr)
	}
	marker := filepath.Join(evidence.Root(), "actions", "exec_marker-1.begin-failed.json")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected begin-failed marker: %v", err)
	}
}

func TestExecuteFailsOpenOnActionEvidenceBeginFailure(t *testing.T) {
	// Fail-open: when Begin cannot open a session, Execute still succeeds.
	terminal, _ := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: true})
	fixture := newEnvironmentFixture(t, newScriptedExecTransport(transport.Frame{Kind: transport.KindTerminal, Data: terminal}))
	storeRoot := t.TempDir()
	evidence, err := research.NewStore(research.StoreOptions{Root: storeRoot})
	if err != nil {
		t.Fatal(err)
	}
	actionsPath := filepath.Join(storeRoot, "actions")
	if err := os.RemoveAll(actionsPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(actionsPath, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.environment.actionEvidence = evidence

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, executeErr := fixture.environment.Execute(ctx, Request{
		IdempotencyKey: "evidence-begin-failure", CorrelationID: requestCorrelation(t),
		Kind: domain.ExecTool, Executable: "bin/tool", CleanupGrace: time.Second,
	})
	if executeErr != nil {
		t.Fatalf("execution must fail-open on begin failure, got %T %v", executeErr, executeErr)
	}
	if result.ExecID == "" || !result.CleanupConfirmed || result.ExitCode != 0 {
		t.Fatalf("terminal result = %#v", result)
	}
	assertOnlyExecState(t, fixture, domain.ExecCompleted)
}

func TestExecuteFailsOpenOnActionEvidenceSealFailure(t *testing.T) {
	// Fail-open: Seal failure does not fail a successfully completed Execute.
	terminal, _ := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: true})
	stream := newScriptedExecTransport(
		transport.Frame{Kind: transport.KindStdout, Data: []byte("real output")},
		transport.Frame{Kind: transport.KindTerminal, Data: terminal},
	)
	fixture := newEnvironmentFixture(t, stream)
	evidence, err := research.NewStore(research.StoreOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	fixture.environment.actionEvidence = evidence

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, executeErr := fixture.environment.Execute(ctx, Request{
		IdempotencyKey: "evidence-seal-failure", CorrelationID: requestCorrelation(t),
		Kind: domain.ExecTool, Executable: "bin/tool", CleanupGrace: time.Second,
		OnStdout: func([]byte) error {
			entries, readErr := os.ReadDir(filepath.Join(evidence.Root(), "actions"))
			if readErr != nil {
				return readErr
			}
			if len(entries) != 1 {
				return fmt.Errorf("action directories = %d, want 1", len(entries))
			}
			return os.Mkdir(filepath.Join(evidence.Root(), "actions", entries[0].Name(), "stdout.bounded"), 0o700)
		},
	})
	if executeErr != nil {
		t.Fatalf("execution must fail-open on seal failure, got %T %v", executeErr, executeErr)
	}
	if result.ExecID == "" || !result.CleanupConfirmed || result.ExitCode != 0 {
		t.Fatalf("terminal result = %#v", result)
	}
	assertOnlyExecState(t, fixture, domain.ExecCompleted)
}

func assertOnlyExecState(t *testing.T, fixture *environmentFixture, expected domain.ExecState) {
	t.Helper()
	view, err := fixture.core.GetResearchSession(context.Background(), fixture.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Execs) != 1 || view.Execs[0].State != expected {
		t.Fatalf("persisted executions = %#v, want state %s", view.Execs, expected)
	}
}

var _ ports.ExecTransport = (*scriptedExecTransport)(nil)
var _ ports.AgentWorkspaceDriver = (*execOnlyDriver)(nil)
