package researchmcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/research"
)

func TestAPIToolsOverSealedBundle(t *testing.T) {
	store, err := research.NewStore(research.StoreOptions{
		Root:    t.TempDir(),
		Host:    research.FixedHostCollector{Snapshot: research.HostSnapshot{Available: true, ProcessTree: map[string]any{"pid": 7}}},
		Network: research.FixedNetworkCollector{Index: research.NetworkIndex{Available: false, Reason: "pcap not enabled"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	start := research.StartFromCommand("exec_mcp-1", research.ActionScopeAgentExec, "/usr/bin/curl", []string{"https://x", "--token", "secret"}, ".", time.Unix(1_700_000_400, 0).UTC(), research.ResolveObservationLevel(false, "", false))
	start.LeaseID = "lease_mcp"
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	session.AppendStdout([]byte("ok"))
	exit := 0
	if _, err := session.Seal(context.Background(), research.ActionOutcome{
		EndedAt: time.Unix(1_700_000_401, 0).UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 7,
	}); err != nil {
		t.Fatal(err)
	}

	api, err := New(store, Scope{AuthorizedLeases: []string{"lease_mcp"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ToolNames()) < 8 {
		t.Fatalf("tools = %v", ToolNames())
	}

	got, err := api.GetAction("exec_mcp-1")
	if err != nil || got.Summary.StimulusClass != research.StimulusHTTPClient {
		t.Fatalf("get = %#v err=%v", got, err)
	}
	if !got.ArgvRedacted || got.ArgvCount != 3 || got.Action.Start.Argv != nil {
		t.Fatalf("argv should be redacted: %#v", got)
	}
	if got.Action.Start.Executable != "curl" {
		t.Fatalf("executable should be basename only: %q", got.Action.Start.Executable)
	}
	list, err := api.ListActions(ListActionsRequest{LeaseID: "lease_mcp"})
	if err != nil || len(list.Actions) != 1 {
		t.Fatalf("list = %#v err=%v", list, err)
	}
	if _, err := api.ListActions(ListActionsRequest{}); err == nil || !strings.Contains(err.Error(), "lease_id is required") {
		t.Fatalf("unfiltered list = %v", err)
	}
	if _, err := api.ListActions(ListActionsRequest{LeaseID: "lease_other"}); err == nil {
		t.Fatal("expected cross-lease list denial")
	}
	host, err := api.QueryHost("exec_mcp-1")
	if err != nil || !host.Snapshot.Available {
		t.Fatalf("host = %#v err=%v", host, err)
	}
	network, err := api.QueryNetwork("exec_mcp-1")
	if err != nil || network.Index.Available {
		t.Fatalf("network should gap: %#v err=%v", network, err)
	}
	if len(network.Gaps) == 0 {
		t.Fatal("expected network gaps")
	}
	diff, err := api.StateDiff("exec_mcp-1")
	if err != nil || diff.Diff.Available {
		t.Fatalf("state baseline gap = %#v err=%v", diff, err)
	}
	assess, err := api.Assess("exec_mcp-1")
	if err != nil || assess.ConfidenceFloor == "" {
		t.Fatalf("assess = %#v err=%v", assess, err)
	}
	graph, err := api.EvidenceGraph("exec_mcp-1")
	if err != nil || graph.ActionID != "exec_mcp-1" || graph.ProcessID != 7 {
		t.Fatalf("graph = %#v err=%v", graph, err)
	}
	// Replay argv / host cmdline redaction on agent surfaces.
	if len(graph.Replay.Argv) != 0 {
		t.Fatalf("evidence_graph must redact replay argv: %#v", graph.Replay)
	}
	if err := api.EscalateObservation(context.Background(), EscalateRequest{ActionID: "exec_mcp-1", Level: research.ObservationLevelDeep}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("escalate unsupported = %v", err)
	}

	// Handle dispatcher
	value, err := api.Handle(context.Background(), ToolAssess, map[string]string{"action_id": "exec_mcp-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(AssessResult); !ok {
		t.Fatalf("handle assess type %T", value)
	}
	if _, err := api.Handle(context.Background(), "not_a_tool", nil); err == nil {
		t.Fatal("expected unknown tool error")
	}
}

func TestAPIRequiresAuthorizedLeases(t *testing.T) {
	store, err := research.NewStore(research.StoreOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(store, Scope{}); err == nil {
		t.Fatal("expected empty scope rejection")
	}
}

func TestAPIEscalateHook(t *testing.T) {
	store, err := research.NewStore(research.StoreOptions{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	start := research.StartFromCommand("exec_a1", research.ActionScopeAgentExec, "curl", nil, ".", time.Now().UTC(), research.ResolveObservationLevel(false, "", false))
	start.LeaseID = "lease_ok"
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	if _, err := session.Seal(context.Background(), research.ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1}); err != nil {
		t.Fatal(err)
	}
	called := false
	api, err := NewWithOptions(Options{
		Store: store,
		Scope: Scope{AuthorizedLeases: []string{"lease_ok"}},
		Escalate: func(ctx context.Context, actionID string, level research.ObservationLevel, profile string) error {
			called = true
			if actionID != "exec_a1" || level != research.ObservationLevelDeep {
				t.Fatalf("hook args %s %s", actionID, level)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.EscalateObservation(context.Background(), EscalateRequest{ActionID: "exec_a1", Level: research.ObservationLevelDeep}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("escalate hook not called")
	}
	// Cross-lease escalate denied even with hook.
	if err := api.EscalateObservation(context.Background(), EscalateRequest{ActionID: "missing", Level: research.ObservationLevelDeep}); err == nil {
		t.Fatal("expected missing action deny")
	}
}
