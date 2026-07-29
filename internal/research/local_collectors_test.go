package research

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalHostCollectorCapturesRealProcessAndActionContext(t *testing.T) {
	start := StartFromCommand("exec_local-host", ActionScopeAgentExec, "probe", []string{"--check"}, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(false, "", false))
	start.EnvironmentKeys = []string{"PATH", "WORLD_TEST"}
	snapshot, err := (LocalHostCollector{}).Capture(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Available || snapshot.Scope != "collector_process" {
		t.Fatalf("host snapshot = %#v", snapshot)
	}
	tree, ok := snapshot.ProcessTree.(HostProcessTree)
	if !ok {
		t.Fatalf("process tree type = %T", snapshot.ProcessTree)
	}
	if tree.Collector.PID != os.Getpid() || tree.Collector.ParentPID != os.Getppid() {
		t.Fatalf("collector identity = %#v", tree.Collector)
	}
	if tree.Action.ActionID != start.ActionID || tree.Action.Executable != start.Executable || tree.Action.EnvironmentKeys != 2 {
		t.Fatalf("action identity = %#v", tree.Action)
	}
	if tree.ObservedAt.IsZero() || tree.Host.GOOS == "" || tree.Host.GOARCH == "" || tree.Host.CPUs < 1 {
		t.Fatalf("host identity = %#v observed=%s", tree.Host, tree.ObservedAt)
	}
}

func TestLocalNetworkCollectorCapturesRealInventoryAndSanitizedActionEndpoint(t *testing.T) {
	start := StartFromCommand(
		"exec_local-network", ActionScopeAgentExec, "curl",
		[]string{"https://user:secret@127.0.0.1:8443/private?token=hidden"}, t.TempDir(),
		time.Now().UTC(), ResolveObservationLevel(false, "", false),
	)
	index, err := (LocalNetworkCollector{}).Capture(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if !index.Available || index.Attributed || index.Scope != "collector_host" {
		t.Fatalf("network index = %#v", index)
	}
	observation, ok := index.Flows.(LocalNetworkObservation)
	if !ok {
		t.Fatalf("network observation type = %T", index.Flows)
	}
	if len(observation.Interfaces) == 0 {
		t.Fatal("real local interface inventory was empty")
	}
	if len(observation.ActionEndpoints) != 1 {
		t.Fatalf("action endpoints = %#v", observation.ActionEndpoints)
	}
	endpoint := observation.ActionEndpoints[0]
	if endpoint.Scheme != "https" || endpoint.Host != "127.0.0.1" || endpoint.Port != "8443" {
		t.Fatalf("sanitized endpoint = %#v", endpoint)
	}
	encoded, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), "hidden") || strings.Contains(string(encoded), "/private") {
		t.Fatalf("network index retained URL credentials or payload: %s", encoded)
	}
}

func TestWorkingDirectoryStateCollectorDiffsRealFiles(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "modified.txt"), "before")
	writeTestFile(t, filepath.Join(root, "deleted.txt"), "delete-me")
	collector := WorkingDirectoryStateCollector{
		MaxEntries: 64, MaxDepth: 4, MaxFileContentBytes: 1 << 10,
		MaxTotalContentBytes: 4 << 10, Attributed: true,
	}
	start := StartFromCommand("exec_local-state", ActionScopeAgentExec, "tool", nil, root, time.Now().UTC(), ResolveObservationLevel(true, ProfileDeep, false))
	before, err := collector.CaptureBefore(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(root, "modified.txt"), "after-value")
	writeTestFile(t, filepath.Join(root, "created.txt"), "created")
	if err := os.Remove(filepath.Join(root, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	after, err := collector.CaptureAfter(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	diff := collector.Diff(before, after)
	if !diff.Available || !diff.Attributed || diff.Truncated {
		t.Fatalf("state diff = %#v", diff)
	}
	requireStringPresent(t, diff.Created, "created.txt")
	requireStringPresent(t, diff.Modified, "modified.txt")
	requireStringPresent(t, diff.Deleted, "deleted.txt")
	entry, ok := after.Entries["modified.txt"].(LocalStateEntry)
	if !ok || entry.SHA256 == "" || entry.HashedBytes != int64(len("after-value")) || entry.ContentTruncated {
		t.Fatalf("real file entry = %#v (%T)", after.Entries["modified.txt"], after.Entries["modified.txt"])
	}
}

func TestWorkingDirectoryStateCollectorEnforcesEntryBound(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b", "c", "d"} {
		writeTestFile(t, filepath.Join(root, name), name)
	}
	start := StartFromCommand("exec_local-bound", ActionScopeAgentExec, "tool", nil, root, time.Now().UTC(), ResolveObservationLevel(true, ProfileDeep, false))
	snapshot, err := (WorkingDirectoryStateCollector{MaxEntries: 2, Attributed: true}).CaptureBefore(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Available || !snapshot.Truncated || len(snapshot.Entries) > 2 || snapshot.Reason != ReasonStateSnapshotTruncated {
		t.Fatalf("bounded snapshot = %#v", snapshot)
	}
}

func TestStoreDefaultsPersistRealAmbientCollectors(t *testing.T) {
	// Disable pcap so this test exercises ambient inventory without depending
	// on dumpcap/tcpdump being installed on the developer machine.
	store := newTestStore(t, StoreOptions{
		CollectorOptions: CollectorOptions{DisablePcap: true, DisableSyscall: true},
	})
	start := StartFromCommand("exec_local-defaults", ActionScopeAgentExec, "curl", []string{"https://127.0.0.1:9443/status"}, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	// No ProcessID: host stays ambient pre-start; network remains non-attributed.
	if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true}); err != nil {
		t.Fatal(err)
	}
	host, _, err := store.QueryHost(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	network, gaps, err := store.QueryNetwork(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if !host.Available || host.Scope != "collector_process" || !network.Available || network.Attributed {
		t.Fatalf("default host/network = %#v %#v", host, network)
	}
	if len(gaps) == 0 {
		t.Fatalf("expected network attribution gap, gaps=%#v", gaps)
	}
	found := false
	for _, gap := range gaps {
		if gap.Reason == ReasonNetworkNotAttributed {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ambient attribution gaps = %#v", gaps)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireStringPresent(t *testing.T, values []string, expected string) {
	t.Helper()
	for _, value := range values {
		if value == expected {
			return
		}
	}
	t.Fatalf("%q not found in %#v", expected, values)
}
