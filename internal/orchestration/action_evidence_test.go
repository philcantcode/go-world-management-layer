package orchestration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/research"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestBeginAndSealAgentActionProducesBundle(t *testing.T) {
	service, store := newActionEvidenceTestService(t, time.Unix(1_700_000_500, 0).UTC())
	record := application.ExecRecord{
		ID: "exec_orch-1", SessionID: "rs_1", LeaseID: "lease_1",
		AgentWorkspaceID: "aw_1", AgentGeneration: 1,
		Executable: "/usr/bin/nuclei", Argv: []string{"-u", "http://t"},
		WorkingDirectory: ".", CreatedAt: time.Unix(1_700_000_500, 0).UTC(),
	}
	session, err := service.beginAgentAction(context.Background(), record, application.MutationMeta{CorrelationID: "corr_1", IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if session == nil {
		t.Fatal("expected action session")
	}
	session.AppendStdout([]byte("finding"))
	lifecycle := transport.ProcessLifecycle{}
	started := transport.ProcessEvent{Kind: "started", PID: 55, ProcessStartNS: 12, ParentPID: 1}
	if err := lifecycle.Observe(jsonTransportFrame(t, transport.KindProcess, started)); err != nil {
		t.Fatal(err)
	}
	exited := started
	exited.Kind = "exited"
	if err := lifecycle.Observe(jsonTransportFrame(t, transport.KindProcess, exited)); err != nil {
		t.Fatal(err)
	}
	exit := 0
	if err := service.sealActionSession(context.Background(), session, transport.Terminal{ExitCode: exit, CleanupConfirmed: true}, lifecycle); err != nil {
		t.Fatal(err)
	}

	doc, summary, err := store.Get("exec_orch-1")
	if err != nil {
		t.Fatal(err)
	}
	if summary.StimulusClass != research.StimulusWebScanner {
		t.Fatalf("class = %s", summary.StimulusClass)
	}
	if doc.Outcome.ProcessID != 55 || !doc.Outcome.Sealed {
		t.Fatalf("outcome = %#v", doc.Outcome)
	}
	if doc.Outcome.ExitCode == nil || *doc.Outcome.ExitCode != 0 {
		t.Fatalf("exit = %#v", doc.Outcome.ExitCode)
	}
	if summary.ConfidenceFloor.Rank() < research.ConfidenceAttributed.Rank() {
		t.Fatalf("floor = %s", summary.ConfidenceFloor)
	}
	for _, name := range []string{"action.json", "summary.json"} {
		if _, err := os.Stat(filepath.Join(store.Root(), "actions", "exec_orch-1", name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
}

func TestBeginAgentActionRecordsBeginFailureMarker(t *testing.T) {
	store, err := research.NewStore(research.StoreOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{actionEvidence: store, clock: func() time.Time { return time.Unix(1_700_000_700, 0).UTC() }}
	// First begin succeeds and holds the open slot.
	record := application.ExecRecord{
		ID: "exec_begin-fail", SessionID: "rs_1", LeaseID: "lease_1",
		AgentWorkspaceID: "aw_1", AgentGeneration: 1,
		Executable: "curl", WorkingDirectory: ".", CreatedAt: time.Unix(1_700_000_700, 0).UTC(),
	}
	session, err := service.beginAgentAction(context.Background(), record, application.MutationMeta{IdempotencyKey: "k-open"})
	if err != nil || session == nil {
		t.Fatalf("first begin = %v session=%v", err, session)
	}
	// Second begin for same action_id must fail, record marker, and leave session nil.
	session2, err := service.beginAgentAction(context.Background(), record, application.MutationMeta{IdempotencyKey: "k-retry"})
	if err == nil || session2 != nil {
		t.Fatalf("expected begin failure, got session=%v err=%v", session2, err)
	}
	marker := filepath.Join(store.Root(), "actions", record.ID+".begin-failed.json")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("expected begin-failed marker: %v", err)
	}
	// Fail-open contract: callers continue with nil session (err is diagnostic only).
}

func TestSealActionSessionOmitsExitOnTransportFailure(t *testing.T) {
	service, store := newActionEvidenceTestService(t, time.Unix(1_700_000_600, 0).UTC())
	record := application.ExecRecord{
		ID: "exec_orch-fail", SessionID: "rs_1", LeaseID: "lease_1",
		AgentWorkspaceID: "aw_1", AgentGeneration: 1,
		Executable: "curl", WorkingDirectory: ".", CreatedAt: time.Unix(1_700_000_600, 0).UTC(),
	}
	session, err := service.beginAgentAction(context.Background(), record, application.MutationMeta{IdempotencyKey: "k2"})
	if err != nil {
		t.Fatal(err)
	}
	if session == nil {
		t.Fatal("expected session")
	}
	if err := service.sealActionSession(context.Background(), session, transport.Terminal{Error: "stream reset", CleanupConfirmed: false}, transport.ProcessLifecycle{}); err != nil {
		t.Fatal(err)
	}
	doc, _, err := store.Get("exec_orch-fail")
	if err != nil {
		t.Fatal(err)
	}
	if doc.Outcome.ExitCode != nil {
		t.Fatalf("expected nil exit_code, got %#v", doc.Outcome.ExitCode)
	}
	if doc.Outcome.Error != "stream reset" {
		t.Fatalf("error = %q", doc.Outcome.Error)
	}
}

func TestBeginAgentActionReturnsStoreFailure(t *testing.T) {
	service, store := newActionEvidenceTestService(t, time.Now().UTC())
	actionsPath := filepath.Join(store.Root(), "actions")
	if err := os.RemoveAll(actionsPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(actionsPath, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	record := application.ExecRecord{
		ID: "exec_orch-begin-error", SessionID: "rs_1", LeaseID: "lease_1",
		AgentWorkspaceID: "aw_1", AgentGeneration: 1, Executable: "tool", CreatedAt: time.Now().UTC(),
	}
	if _, err := service.beginAgentAction(context.Background(), record, application.MutationMeta{IdempotencyKey: "begin-error"}); err == nil || !strings.Contains(err.Error(), "begin agent action evidence") {
		t.Fatalf("begin error = %v", err)
	}
}

func TestSealActionSessionReturnsStoreFailure(t *testing.T) {
	service, store := newActionEvidenceTestService(t, time.Now().UTC())
	record := application.ExecRecord{
		ID: "exec_orch-seal-error", SessionID: "rs_1", LeaseID: "lease_1",
		AgentWorkspaceID: "aw_1", AgentGeneration: 1, Executable: "tool", CreatedAt: time.Now().UTC(),
	}
	session, err := service.beginAgentAction(context.Background(), record, application.MutationMeta{IdempotencyKey: "seal-error"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(store.Root(), "actions", record.ID, "stdout.bounded"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := service.sealActionSession(context.Background(), session, transport.Terminal{CleanupConfirmed: true}, transport.ProcessLifecycle{}); err == nil || !strings.Contains(err.Error(), "seal action evidence") {
		t.Fatalf("seal error = %v", err)
	}
}

func newActionEvidenceTestService(t *testing.T, now time.Time) (*Service, *research.Store) {
	t.Helper()
	store, err := research.NewStore(research.StoreOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return &Service{actionEvidence: store, clock: func() time.Time { return now }, controlTimeout: time.Second}, store
}
