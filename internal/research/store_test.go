package research

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStoreSealsActionBundleWithGaps(t *testing.T) {
	// Explicit unavailable collectors isolate gap semantics from the real local
	// collector defaults.
	store := newTestStore(t, StoreOptions{
		Host:    unavailableHostCollector{},
		Network: unavailableNetworkCollector{},
		State:   unavailableStateCollector{},
	})
	start := StartFromCommand("exec_test-action-1", ActionScopeAgentExec, "/usr/bin/curl", []string{"https://example.test"}, ".", time.Unix(1_700_000_100, 0).UTC(), ResolveObservationLevel(false, "", false))
	start.LeaseID = "lease_1"
	start.ExecID = start.ActionID

	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	session.AppendStdout([]byte("HTTP/1.1 200 OK\n"))
	session.AppendStderr([]byte("progress\n"))
	exit := 0
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Unix(1_700_000_101, 0).UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 4242, ProcessStartNS: 99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ActionID != start.ActionID {
		t.Fatalf("summary action = %s", summary.ActionID)
	}
	if summary.StimulusClass != StimulusHTTPClient {
		t.Fatalf("class = %s", summary.StimulusClass)
	}
	if summary.ConfidenceFloor != ConfidenceAttributed {
		// raw + causal (pid) + stimulus; no network semantic → attributed
		t.Fatalf("confidence = %s, want attributed", summary.ConfidenceFloor)
	}
	if summary.EvidenceRoles[RoleStimulus] != RolePresent || summary.EvidenceRoles[RoleRaw] != RolePresent {
		t.Fatalf("roles = %#v", summary.EvidenceRoles)
	}
	if summary.EvidenceRoles[RoleSemantic] != RoleGap {
		t.Fatalf("semantic should be gap when network absent: %#v", summary.EvidenceRoles)
	}
	if len(summary.Gaps) == 0 {
		t.Fatal("expected explicit gaps when collectors absent")
	}

	doc, loaded, err := store.Get(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.Outcome.Sealed || loaded.ConfidenceFloor != summary.ConfidenceFloor {
		t.Fatalf("loaded %#v %#v", doc.Outcome, loaded)
	}
	stdout, err := os.ReadFile(filepath.Join(store.Root(), "actions", start.ActionID, "stdout.bounded"))
	if err != nil || string(stdout) != "HTTP/1.1 200 OK\n" {
		t.Fatalf("stdout = %q err=%v", stdout, err)
	}
	// network index exists with gap semantics
	net, gaps, err := store.QueryNetwork(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if net.Available {
		t.Fatal("network should be unavailable with explicit gap collector")
	}
	if len(gaps) == 0 {
		t.Fatal("network gaps required")
	}
}

func TestStoreFailureStillSealsWithExitCode(t *testing.T) {
	store := newTestStore(t, StoreOptions{})
	start := StartFromCommand("exec_fail-1", ActionScopeAgentExec, "nmap", []string{"-sV", "10.0.0.1"}, ".", time.Unix(1_700_000_200, 0).UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 1
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Unix(1_700_000_201, 0).UTC(), ExitCode: &exit, Error: "scan failed", CleanupConfirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ExitCode == nil || *summary.ExitCode != 1 {
		t.Fatalf("exit = %#v", summary.ExitCode)
	}
	if summary.Error != "scan failed" {
		t.Fatalf("error = %q", summary.Error)
	}
	if summary.StimulusClass != StimulusPortScanner {
		t.Fatalf("class = %s", summary.StimulusClass)
	}
}

func TestStoreConcurrentActionsDistinctIDs(t *testing.T) {
	store := newTestStore(t, StoreOptions{})
	var wg sync.WaitGroup
	ids := []string{"exec_conc-a", "exec_conc-b", "exec_conc-c", "exec_conc-d"}
	errCh := make(chan error, len(ids))
	for _, id := range ids {
		wg.Add(1)
		go func(actionID string) {
			defer wg.Done()
			start := StartFromCommand(actionID, ActionScopeAgentExec, "ffuf", []string{"-u", "http://x/FUZZ"}, ".", time.Now().UTC(), ResolveObservationLevel(false, "", false))
			session, err := store.Begin(context.Background(), start)
			if err != nil {
				errCh <- err
				return
			}
			session.AppendStdout([]byte(actionID))
			exit := 0
			_, err = session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1})
			errCh <- err
		}(id)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	list, err := store.List(ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Actions) != len(ids) {
		t.Fatalf("list len = %d, want %d", len(list.Actions), len(ids))
	}
	seen := map[string]struct{}{}
	for _, item := range list.Actions {
		if _, dup := seen[item.ActionID]; dup {
			t.Fatalf("duplicate action id %s", item.ActionID)
		}
		seen[item.ActionID] = struct{}{}
		if item.StimulusClass != StimulusWebScanner {
			t.Fatalf("%s class = %s", item.ActionID, item.StimulusClass)
		}
	}
}

func TestStoreRejectsReentrantBegin(t *testing.T) {
	store := newTestStore(t, StoreOptions{})
	start := StartFromCommand("exec_reopen-1", ActionScopeAgentExec, "curl", nil, ".", time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background(), start); err == nil {
		t.Fatal("expected re-begin while open to fail")
	}
	exit := 0
	if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background(), start); err == nil {
		t.Fatal("expected re-begin after seal to fail")
	}
}

func TestStoreSealFailureReleasesOpenAndPreservesPartialEvidence(t *testing.T) {
	store := newTestStore(t, StoreOptions{
		// Deep level so Seal attempts after-state writes; force write failure by
		// making the state directory a file after Begin.
		State: FixedStateCollector{
			Before:     StateSnapshot{Available: true, Attributed: true, Entries: map[string]any{"a": 1}},
			After:      StateSnapshot{Available: true, Attributed: true, Entries: map[string]any{"a": 2}},
			DiffResult: StateDiff{Available: true, Attributed: true, Changed: []string{"a"}},
		},
	})
	policy := ResolveObservationLevel(true, "deep", false)
	start := StartFromCommand("exec_abandon-1", ActionScopeAgentExec, "curl", nil, ".", time.Now().UTC(), policy)
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	// Sabotage: replace state/after.json parent by removing write permission is
	// platform-dependent. Instead remove the state directory and replace with a file.
	stateDir := filepath.Join(store.Root(), "actions", start.ActionID, "state")
	if err := os.RemoveAll(stateDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateDir, []byte("not-a-dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	exit := 0
	if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1}); err == nil {
		t.Fatal("expected Seal to fail after state dir sabotage")
	}
	// The stable marker makes the failed seal explicit without deleting partial
	// forensic artifacts.
	abandon := filepath.Join(store.Root(), "actions", start.ActionID, "abandon.json")
	if _, err := os.Stat(abandon); err != nil {
		t.Fatalf("expected abandon marker: %v", err)
	}
	// But state path is a file — RemoveAll on the action dir handles it.
	if _, err := store.Begin(context.Background(), start); err == nil {
		t.Fatal("re-begin must not overwrite partial evidence")
	}
	if _, err := os.Stat(filepath.Join(store.Root(), "actions", start.ActionID, "action.json")); err != nil {
		t.Fatalf("partial action evidence was not preserved: %v", err)
	}
}

func TestStoreDeepLevelStateDiffWithFakeCollector(t *testing.T) {
	store := newTestStore(t, StoreOptions{
		Host: FixedHostCollector{Snapshot: HostSnapshot{
			Available: true, ProcessTree: map[string]any{"pid": 9, "cmd": "curl"},
		}},
		Network: FixedNetworkCollector{Index: NetworkIndex{
			Available: true, Attributed: true, Flows: []map[string]any{{"dst": "10.0.0.2", "port": 80}},
		}},
		State: FixedStateCollector{
			Before:     StateSnapshot{Available: true, Attributed: true, Entries: map[string]any{"/tmp/a": "1"}},
			After:      StateSnapshot{Available: true, Attributed: true, Entries: map[string]any{"/tmp/a": "2"}},
			DiffResult: StateDiff{Available: true, Attributed: true, Changed: []string{"/tmp/a"}},
		},
	})
	policy := ResolveObservationLevel(true, "deep", false)
	start := StartFromCommand("exec_deep-1", ActionScopeAgentExec, "curl", []string{"http://10.0.0.2"}, ".", time.Unix(1_700_000_300, 0).UTC(), policy)
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Unix(1_700_000_301, 0).UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	// stimulus+raw+causal+semantic+state_diff → demonstrated
	if summary.ConfidenceFloor != ConfidenceDemonstrated {
		t.Fatalf("confidence = %s, want demonstrated", summary.ConfidenceFloor)
	}
	diff, gaps, err := store.StateDiff(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if !diff.Available || len(diff.Changed) != 1 {
		t.Fatalf("diff = %#v gaps=%v", diff, gaps)
	}
	graph, err := store.EvidenceGraph(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if graph.ProcessID != 9 || !graph.Network.Available || graph.ConfidenceFloor != ConfidenceDemonstrated {
		t.Fatalf("graph = %#v", graph)
	}
}

func TestStdoutCaptureBound(t *testing.T) {
	store := newTestStore(t, StoreOptions{CaptureBound: 8})
	start := StartFromCommand("exec_bound-1", ActionScopeAgentExec, "wget", nil, ".", time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	session.AppendStdout([]byte("0123456789abcdef"))
	exit := 0
	if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true}); err != nil {
		t.Fatal(err)
	}
	doc, _, err := store.Get(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.Outcome.StdoutTruncated || doc.Outcome.StdoutBytes != 16 {
		t.Fatalf("outcome = %#v", doc.Outcome)
	}
	content, err := os.ReadFile(filepath.Join(store.Root(), "actions", start.ActionID, "stdout.bounded"))
	if err != nil || string(content) != "01234567" {
		t.Fatalf("retained = %q", content)
	}
}

func TestListFiltersByLease(t *testing.T) {
	store := newTestStore(t, StoreOptions{})
	for _, item := range []struct {
		id, lease string
	}{
		{"exec_l1", "lease_a"},
		{"exec_l2", "lease_b"},
	} {
		start := StartFromCommand(item.id, ActionScopeAgentExec, "curl", nil, ".", time.Now().UTC(), ResolveObservationLevel(false, "", false))
		start.LeaseID = item.lease
		session, err := store.Begin(context.Background(), start)
		if err != nil {
			t.Fatal(err)
		}
		exit := 0
		if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1}); err != nil {
			t.Fatal(err)
		}
	}
	list, err := store.List(ListOptions{LeaseID: "lease_a"})
	if err != nil || len(list.Actions) != 1 || list.Actions[0].ActionID != "exec_l1" {
		t.Fatalf("list = %#v err=%v", list, err)
	}
}

func TestSealUnknownExitLeavesNilCode(t *testing.T) {
	store := newTestStore(t, StoreOptions{})
	start := StartFromCommand("exec_nil-exit", ActionScopeAgentExec, "curl", nil, ".", time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	// Pure transport failure: Error set, no ExitCode, no CleanupConfirmed.
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Now().UTC(), Error: "transport disconnected", CleanupConfirmed: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ExitCode != nil {
		t.Fatalf("exit should be nil for unknown process exit, got %#v", summary.ExitCode)
	}
	doc, _, err := store.Get(start.ActionID)
	if err != nil || doc.Outcome.ExitCode != nil {
		t.Fatalf("doc exit = %#v err=%v", doc.Outcome.ExitCode, err)
	}
}

func newTestStore(t *testing.T, options StoreOptions) *Store {
	t.Helper()
	if options.Root == "" {
		options.Root = t.TempDir()
	}
	// Tests default to disabling the capture watchdog unless explicitly set
	// (zero would select the production 30m default and leave timers in flight).
	if options.MaxCaptureDuration == 0 {
		options.MaxCaptureDuration = -1
	}
	store, err := NewStore(options)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type unavailableHostCollector struct{}

func (unavailableHostCollector) Capture(context.Context, ActionStart) (HostSnapshot, error) {
	return HostSnapshot{Available: false, Reason: ReasonHostUnavailable}, nil
}

type unavailableNetworkCollector struct{}

func (unavailableNetworkCollector) Capture(context.Context, ActionStart) (NetworkIndex, error) {
	return NetworkIndex{Available: false, Reason: ReasonNetworkUnavailable}, nil
}

type unavailableStateCollector struct{}

func (unavailableStateCollector) CaptureBefore(context.Context, ActionStart) (StateSnapshot, error) {
	return StateSnapshot{Available: false, Reason: ReasonStateUnavailable}, nil
}

func (unavailableStateCollector) CaptureAfter(context.Context, ActionStart) (StateSnapshot, error) {
	return StateSnapshot{Available: false, Reason: ReasonStateUnavailable}, nil
}

func (unavailableStateCollector) Diff(StateSnapshot, StateSnapshot) StateDiff {
	return StateDiff{Available: false, Reason: ReasonStateUnavailable}
}
