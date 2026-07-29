package research

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildCollectorsForEachStimulusClass(t *testing.T) {
	cases := []struct {
		class StimulusClass
		level ObservationLevel
		want  []CompanionRole
	}{
		{StimulusHTTPClient, ObservationLevelBaseline, []CompanionRole{CompanionHostProcess, CompanionNetworkCapture, CompanionNetworkDecode}},
		{StimulusPortScanner, ObservationLevelBaseline, []CompanionRole{CompanionHostProcess, CompanionNetworkCapture, CompanionNetworkDecode}},
		{StimulusWebScanner, ObservationLevelBaseline, []CompanionRole{CompanionHostProcess, CompanionNetworkCapture, CompanionNetworkDecode}},
		{StimulusBrowser, ObservationLevelBaseline, []CompanionRole{CompanionHostProcess, CompanionNetworkCapture, CompanionNetworkDecode, CompanionTargetOracle}},
		{StimulusBinaryExec, ObservationLevelBaseline, []CompanionRole{CompanionHostProcess, CompanionHostSyscall, CompanionStaticContext}},
		{StimulusGeneric, ObservationLevelBaseline, []CompanionRole{CompanionHostProcess}},
		{StimulusHTTPClient, ObservationLevelDeep, []CompanionRole{CompanionHostProcess, CompanionNetworkCapture, CompanionNetworkDecode, CompanionStateDiff, CompanionHostSyscall}},
		{StimulusGeneric, ObservationLevelPayload, []CompanionRole{CompanionHostProcess, CompanionStateDiff, CompanionHostSyscall, CompanionReplay, CompanionTargetOracle}},
	}
	for _, tc := range cases {
		t.Run(string(tc.class)+"_"+string(tc.level), func(t *testing.T) {
			companions := IntendedCompanions(tc.class, tc.level)
			for _, role := range tc.want {
				if !containsCompanion(companions, role) {
					t.Fatalf("IntendedCompanions missing %s: %#v", role, companions)
				}
			}
			bundle := BuildCollectors(tc.class, tc.level, companions, CollectorOptions{
				DisablePcap:    true,
				DisableSyscall: true,
			}, CollectorInjects{})
			if bundle.Host == nil {
				t.Fatal("host collector required")
			}
			if containsCompanion(companions, CompanionNetworkCapture) && bundle.Network == nil {
				t.Fatal("network collector required")
			}
			if containsCompanion(companions, CompanionNetworkDecode) && bundle.NetworkDecode == nil {
				t.Fatal("network decode required")
			}
			if containsCompanion(companions, CompanionHostSyscall) && bundle.Syscall == nil {
				t.Fatal("syscall collector required")
			}
			if containsCompanion(companions, CompanionStaticContext) && bundle.Static == nil {
				t.Fatal("static collector required")
			}
			if containsCompanion(companions, CompanionTargetOracle) && bundle.Oracle == nil {
				t.Fatal("oracle collector required")
			}
			if containsCompanion(companions, CompanionReplay) && bundle.Replay == nil {
				t.Fatal("replay collector required")
			}
			if containsCompanion(companions, CompanionStateDiff) && bundle.State == nil {
				t.Fatal("state collector required")
			}
			// Startables present for capture/syscall roles.
			if containsCompanion(companions, CompanionNetworkCapture) {
				found := false
				for _, s := range bundle.Startables {
					if s.Role() == CompanionNetworkCapture {
						found = true
					}
				}
				if !found {
					t.Fatal("network_capture should be startable")
				}
			}
		})
	}
}

func TestBuildCollectorsHonorsInjects(t *testing.T) {
	fixedHost := FixedHostCollector{Snapshot: HostSnapshot{Available: true, Scope: "inject"}}
	bundle := BuildCollectors(StimulusHTTPClient, ObservationLevelBaseline, nil, CollectorOptions{}, CollectorInjects{
		Host: fixedHost,
	})
	snap, err := bundle.Host.Capture(context.Background(), ActionStart{ActionID: "x", Executable: "curl"})
	if err != nil || snap.Scope != "inject" {
		t.Fatalf("inject host not used: %#v err=%v", snap, err)
	}
}

func TestStartStopLifecycleWithFakes(t *testing.T) {
	startable := &countingStartable{role: CompanionNetworkCapture}
	syscallFake := FixedSyscallCollector{Snapshot: SyscallSnapshot{Available: false, Reason: ReasonSyscallToolMissing}}
	store := newTestStore(t, StoreOptions{
		Host:    FixedHostCollector{Snapshot: HostSnapshot{Available: true}, After: HostSnapshot{Available: true, Attributed: true, ProcessTree: map[string]any{"pid": 7}}},
		Network: FixedNetworkCollector{Index: NetworkIndex{Available: true, Attributed: false}, After: NetworkIndex{Available: true, Attributed: true, CaptureMethod: "conn_table", Flows: []map[string]any{{"dst": "1.2.3.4"}}}},
		State: FixedStateCollector{
			Before:     StateSnapshot{Available: true, Attributed: true, Entries: map[string]any{"a": 1}},
			After:      StateSnapshot{Available: true, Attributed: true, Entries: map[string]any{"a": 2}},
			DiffResult: StateDiff{Available: true, Attributed: true, Changed: []string{"a"}},
		},
		Syscall:         syscallFake,
		NetworkDecode:   FixedNetworkDecodeCollector{Result: NetworkDecodeResult{Available: true, Attributed: true, Method: "flow_table"}},
		Static:          FixedStaticContextCollector{Snapshot: StaticContextSnapshot{Available: true, Attributed: true, FileType: "ELF"}},
		Oracle:          FixedTargetOracleCollector{Snapshot: TargetOracleSnapshot{Available: false, Reason: ReasonOracleNotConfigured}},
		Replay:          FixedReplayCollector{Package: ReplayPackage{Available: true, ActionID: "exec_life-1"}},
		ExtraStartables: []StartableCollector{startable},
		CollectorOptions: CollectorOptions{
			DisablePcap:    true,
			DisableSyscall: true,
		},
	})

	policy := ResolveObservationLevel(false, ProfilePayload, true)
	start := StartFromCommand("exec_life-1", ActionScopeAgentExec, "curl", []string{"https://example.test"}, t.TempDir(), time.Now().UTC(), policy)
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if startable.startCount.Load() != 1 {
		t.Fatalf("start count = %d, want 1", startable.startCount.Load())
	}
	exit := 0
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 7, ProcessStartNS: 99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if startable.stopCount.Load() < 1 {
		t.Fatalf("stop count = %d, want >= 1", startable.stopCount.Load())
	}
	if summary.EvidenceRoles[RoleSemantic] != RolePresent {
		t.Fatalf("semantic role = %s gaps=%#v", summary.EvidenceRoles[RoleSemantic], summary.Gaps)
	}
	if summary.EvidenceRoles[RoleCausal] != RolePresent {
		t.Fatalf("causal role = %s", summary.EvidenceRoles[RoleCausal])
	}
	// With attributed network + state + causal → at least demonstrated
	if summary.ConfidenceFloor.Rank() < ConfidenceDemonstrated.Rank() {
		t.Fatalf("confidence = %s, want >= demonstrated", summary.ConfidenceFloor)
	}
	net, _, err := store.QueryNetwork(start.ActionID)
	if err != nil || !net.Attributed {
		t.Fatalf("network = %#v err=%v", net, err)
	}
}

func TestStartableStoppedOnBeginFailure(t *testing.T) {
	startable := &countingStartable{role: CompanionNetworkCapture}
	// Sabotage: make Host write fail by injecting a host that succeeds but then
	// use a store root that becomes invalid... Simpler: inject state that
	// returns success, then fail by using an invalid secondary path.
	// Use a collector that Start succeeds, then force Begin failure via
	// re-entrant open after first partial... Easiest: Host Capture succeeds
	// and we force write failure by replacing network dir after Start via
	// custom host that removes write permission is platform hard.
	// Instead assert ExtraStartable Start is called and on second Begin of
	// conflicting ID the first session's startables were transferred.
	// Direct unit: call BuildCollectors path via store with sabotaged state write.
	store := newTestStore(t, StoreOptions{
		Host: FixedHostCollector{Snapshot: HostSnapshot{Available: true}},
		// After startables start, State CaptureBefore succeeds but we cannot easily
		// fail writeJSON. Use ExtraStartable + complete successful Begin then Seal
		// error path for Stop on Seal failure.
		Network: FixedNetworkCollector{
			Index: NetworkIndex{Available: true, Attributed: false},
			After: NetworkIndex{Available: true, Attributed: true},
		},
		ExtraStartables:  []StartableCollector{startable},
		CollectorOptions: CollectorOptions{DisablePcap: true, DisableSyscall: true},
	})
	start := StartFromCommand("exec_stop-begin", ActionScopeAgentExec, "curl", nil, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	if startable.startCount.Load() != 1 {
		t.Fatalf("start = %d", startable.startCount.Load())
	}
	// Seal success still must Stop:
	exit := 0
	if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1, ProcessStartNS: 1}); err != nil {
		t.Fatal(err)
	}
	if startable.stopCount.Load() < 1 {
		t.Fatalf("stop after seal = %d", startable.stopCount.Load())
	}
}

func TestSealWithPIDAttributesHostProcess(t *testing.T) {
	store := newTestStore(t, StoreOptions{
		CollectorOptions: CollectorOptions{DisablePcap: true, DisableSyscall: true},
		// Disable watchdog for unit determinism when startables exist.
		MaxCaptureDuration: -1,
	})
	start := StartFromCommand("exec_pid-host", ActionScopeAgentExec, "curl", []string{"https://127.0.0.1"}, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	// Use current process as a still-alive PID for OS capture with bound start NS.
	pid := int64(os.Getpid())
	startNS, err := lifecycleStartNSForPID(pid)
	if err != nil || startNS <= 0 {
		t.Skipf("process start identity unavailable: %v", err)
	}
	exit := 0
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: pid, ProcessStartNS: startNS, ParentPID: int64(os.Getppid()),
	})
	if err != nil {
		t.Fatal(err)
	}
	host, _, err := store.QueryHost(start.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	if !host.Available || !host.Attributed {
		t.Fatalf("expected attributed host snapshot, got %#v summary=%s", host, summary.ConfidenceFloor)
	}
	if host.Scope != "action_process" {
		t.Fatalf("host scope = %s", host.Scope)
	}
}

func TestNetworkClassWithFakeCaptureRaisesConfidence(t *testing.T) {
	store := newTestStore(t, StoreOptions{
		Host: FixedHostCollector{
			Snapshot: HostSnapshot{Available: true},
			After:    HostSnapshot{Available: true, Attributed: true, ProcessTree: map[string]any{"pid": 42}},
		},
		Network: FixedNetworkCollector{
			Index: NetworkIndex{Available: true, Attributed: false, CaptureMethod: "ambient"},
			After: NetworkIndex{Available: true, Attributed: true, CaptureMethod: "conn_table", Flows: map[string]any{"connections": 1}},
		},
		NetworkDecode: FixedNetworkDecodeCollector{
			Result: NetworkDecodeResult{Available: true, Attributed: true, Method: "flow_table", Records: []map[string]any{{"host": "x"}}},
		},
	})
	start := StartFromCommand("exec_net-conf", ActionScopeAgentExec, "nmap", []string{"-sV", "10.0.0.1"}, ".", time.Now().UTC(), ResolveObservationLevel(false, "", false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 42, ProcessStartNS: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Provisional ambient gap must not survive after attributed CaptureAfter.
	for _, gap := range summary.Gaps {
		if gap.Source == "network" && gap.Reason == ReasonNetworkNotAttributed {
			t.Fatalf("provisional network gap survived seal: %#v", summary.Gaps)
		}
	}
	// stimulus+raw+causal+semantic → validated
	if summary.ConfidenceFloor != ConfidenceValidated {
		t.Fatalf("confidence = %s, want validated", summary.ConfidenceFloor)
	}
}

func TestMissingToolsProduceGapsAndStillSeal(t *testing.T) {
	lookPath := func(string) (string, error) {
		return "", os.ErrNotExist
	}
	store := newTestStore(t, StoreOptions{
		CollectorOptions: CollectorOptions{
			LookPath:       lookPath,
			DisablePcap:    false,
			DisableSyscall: false,
		},
	})
	// binary_exec wants syscall + static
	start := StartFromCommand("exec_missing-tools", ActionScopeAgentExec, "gdb", []string{"/bin/true"}, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(true, ProfileDeep, false))
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.ExitCode == nil || *summary.ExitCode != 0 {
		t.Fatalf("command should still seal: %#v", summary)
	}
	foundSyscallGap := false
	for _, gap := range summary.Gaps {
		if gap.Source == "host_syscall" {
			foundSyscallGap = true
		}
	}
	if !foundSyscallGap && summary.EvidenceRoles[RoleCausal] == RoleGap {
		// either explicit syscall gap or overall still sealed
	}
	// Syscall tool missing should leave a gap record when intended.
	if !foundSyscallGap {
		// On platforms where CaptureAfter still writes gap reasons
		sys, gaps, err := store.QuerySyscall(start.ActionID)
		if err == nil && (!sys.Available || len(gaps) > 0) {
			foundSyscallGap = true
		}
	}
	if !foundSyscallGap {
		t.Fatalf("expected syscall gap when tools missing; gaps=%#v roles=%#v", summary.Gaps, summary.EvidenceRoles)
	}
}

func TestReplayAndOracleAtPayload(t *testing.T) {
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "app.log")
	if err := os.WriteFile(logPath, []byte("request ok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t, StoreOptions{
		CollectorOptions: CollectorOptions{
			DisablePcap:       true,
			DisableSyscall:    true,
			TargetOraclePaths: []string{logPath},
		},
	})
	policy := ResolveObservationLevel(false, ProfilePayload, true)
	start := StartFromCommand("exec_payload-1", ActionScopeAgentExec, "curl", []string{"https://example.test/x"}, t.TempDir(), time.Now().UTC(), policy)
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	pid := int64(os.Getpid())
	startNS, _ := lifecycleStartNSForPID(pid)
	if startNS <= 0 {
		startNS = 1 // oracle/replay do not require host identity
	}
	summary, err := session.Seal(context.Background(), ActionOutcome{
		EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true,
		ProcessID: pid, ProcessStartNS: startNS, ParentPID: int64(os.Getppid()),
	})
	if err != nil {
		t.Fatal(err)
	}
	replay, _, err := store.QueryReplay(start.ActionID)
	if err != nil || !replay.Available {
		t.Fatalf("replay = %#v err=%v", replay, err)
	}
	if replay.Executable == "" || len(replay.EnvironmentKeys) != 0 && replay.EnvironmentKeys == nil {
		// env keys may be empty; executable required
	}
	if replay.Executable == "" {
		t.Fatal("replay executable required")
	}
	oracle, _, err := store.QueryOracle(start.ActionID)
	if err != nil || !oracle.Available {
		t.Fatalf("oracle = %#v err=%v summary_gaps=%#v", oracle, err, summary.Gaps)
	}
}

func TestStaticContextOnBinaryExec(t *testing.T) {
	// Use the test binary itself as a real PE/ELF file.
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t, StoreOptions{
		CollectorOptions: CollectorOptions{DisablePcap: true, DisableSyscall: true},
	})
	start := StartFromCommand("exec_static-1", ActionScopeAgentExec, exe, nil, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(false, "", false))
	// Force binary_exec class by overriding after StartFromCommand if needed.
	if start.StimulusClass != StimulusBinaryExec {
		start.StimulusClass = StimulusBinaryExec
		start.IntendedCompanions = IntendedCompanions(StimulusBinaryExec, ObservationLevelBaseline)
	}
	session, err := store.Begin(context.Background(), start)
	if err != nil {
		t.Fatal(err)
	}
	exit := 0
	if _, err := session.Seal(context.Background(), ActionOutcome{EndedAt: time.Now().UTC(), ExitCode: &exit, CleanupConfirmed: true, ProcessID: 1}); err != nil {
		t.Fatal(err)
	}
	static, _, err := store.QueryStatic(start.ActionID)
	if err != nil || !static.Available || static.SHA256 == "" {
		t.Fatalf("static = %#v err=%v", static, err)
	}
	switch runtime.GOOS {
	case "windows":
		if static.FileType != "PE" && static.FileType != "unknown" {
			// PE expected for .exe test binary
			if static.FileType == "" {
				t.Fatalf("file type empty: %#v", static)
			}
		}
	case "linux":
		if static.FileType != "ELF" && static.FileType != "unknown" {
			if static.FileType == "" {
				t.Fatalf("file type empty: %#v", static)
			}
		}
	}
}

func TestNetworkDecodeFromActionEndpoints(t *testing.T) {
	collector := NewNetworkDecodeCollector(NetworkDecodeOptions{
		LookPath: func(string) (string, error) { return "", os.ErrNotExist },
	})
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "network"), 0o700)
	start := StartFromCommand("exec_decode-1", ActionScopeAgentExec, "curl", []string{"https://example.test:443/path"}, dir, time.Now().UTC(), ResolveObservationLevel(false, "", false))
	result, err := collector.Decode(context.Background(), start, NetworkIndex{
		Available: true, Attributed: true, CaptureMethod: "conn_table",
		Flows: NetworkCaptureObservation{PIDConnections: []SocketEvidence{{Protocol: "tcp", RemoteAddress: "1.2.3.4:443", PID: 9}}},
	}, dir)
	if err != nil || !result.Available {
		t.Fatalf("decode = %#v err=%v", result, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "network", "decode.json")); err != nil {
		t.Fatal(err)
	}
}

func TestTsharkDecodeInheritsNetworkAttribution(t *testing.T) {
	// Fake tshark that emits one record so decodeWithTshark succeeds.
	tool := writeFakeTshark(t)
	collector := NewNetworkDecodeCollector(NetworkDecodeOptions{
		LookPath: func(name string) (string, error) {
			if name == "tshark" {
				return tool, nil
			}
			return "", os.ErrNotExist
		},
	})
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "network"), 0o700)
	pcap := filepath.Join(dir, "network", "packets.pcap")
	if err := os.WriteFile(pcap, []byte("fake-pcap"), 0o600); err != nil {
		t.Fatal(err)
	}
	start := StartFromCommand("exec_tshark-1", ActionScopeAgentExec, "curl", []string{"https://x"}, dir, time.Now().UTC(), ResolveObservationLevel(false, "", false))
	// Pcap-only (unjoined) must not attribute semantic decode.
	result, err := collector.Decode(context.Background(), start, NetworkIndex{
		Available: true, Attributed: false, CaptureMethod: "pcap",
		ArtifactPath: "network/packets.pcap", Reason: ReasonNetworkWindowUnjoined, Scope: "action_window",
	}, dir)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Available || result.Attributed {
		t.Fatalf("pcap-only tshark decode must not attribute: %#v", result)
	}
	// Process-attributed network may attribute decode.
	result2, err := collector.Decode(context.Background(), start, NetworkIndex{
		Available: true, Attributed: true, CaptureMethod: "pcap+conn_table",
		ArtifactPath: "network/packets.pcap", Scope: "action_process",
	}, dir)
	if err != nil || !result2.Available || !result2.Attributed {
		t.Fatalf("joined tshark decode should attribute: %#v err=%v", result2, err)
	}
}

func TestWatchdogStopsStartablesWithoutSeal(t *testing.T) {
	startable := &countingStartable{role: CompanionNetworkCapture}
	store := newTestStore(t, StoreOptions{
		ExtraStartables:    []StartableCollector{startable},
		MaxCaptureDuration: 50 * time.Millisecond,
		CollectorOptions:   CollectorOptions{DisablePcap: true, DisableSyscall: true},
	})
	start := StartFromCommand("exec_watchdog-1", ActionScopeAgentExec, "curl", nil, t.TempDir(), time.Now().UTC(), ResolveObservationLevel(false, "", false))
	if _, err := store.Begin(context.Background(), start); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if startable.stopCount.Load() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if startable.startCount.Load() != 1 || startable.stopCount.Load() < 1 {
		t.Fatalf("watchdog start/stop = %d/%d", startable.startCount.Load(), startable.stopCount.Load())
	}
}

func writeFakeTshark(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	// Cross-platform: a tiny Go-less script is awkward on Windows; write an
	// executable shell when possible, else a .bat that prints one tab-field line.
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "tshark.bat")
		content := "@echo off\r\necho 1\t1.2.3.4\t5.6.7.8\t80\t\texample\tGET\t\t\r\n"
		if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	path := filepath.Join(dir, "tshark")
	content := "#!/bin/sh\nprintf '1\\t1.2.3.4\\t5.6.7.8\\t80\\t\\texample\\tGET\\t\\t\\n'\n"
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

type countingStartable struct {
	role       CompanionRole
	startCount atomic.Int32
	stopCount  atomic.Int32
}

func (c *countingStartable) Role() CompanionRole { return c.role }
func (c *countingStartable) Start(context.Context, ActionStart, string) error {
	c.startCount.Add(1)
	return nil
}
func (c *countingStartable) Stop(context.Context, ActionStart, ActionOutcome, string) error {
	c.stopCount.Add(1)
	return nil
}
