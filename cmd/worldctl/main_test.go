package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

func TestCommandCatalogueCoversPublicClientOperations(t *testing.T) {
	rpcCommands := map[string]string{
		"AcquireResearchSession":     "acquire",
		"GetResearchSession":         "get-session",
		"WaitResearchSession":        "wait-session",
		"RenewLease":                 "renew",
		"ReleaseResearchSession":     "release",
		"CreateTarget":               "create-target",
		"GetTarget":                  "get-target",
		"StartTargetRun":             "start-run",
		"WaitTargetRun":              "wait-run",
		"StopTargetRun":              "stop-run",
		"ResetTarget":                "reset",
		"DestroyTarget":              "destroy-target",
		"RequestRecovery":            "recovery",
		"QuarantineTarget":           "quarantine",
		"OpenExec":                   "open-exec",
		"GetLiveSnapshot":            "snapshot",
		"SubscribeObservations":      "watch",
		"SubscribeMetrics":           "metrics",
		"StartCapture":               "start-capture",
		"RequestCapture":             "request-capture",
		"StopCapture":                "stop-capture",
		"GetObservationBundle":       "bundle",
		"GetIncident":                "get-incident",
		"DeclareExport":              "declare-export",
		"PreviewChangeSet":           "preview-export",
		"CommitExport":               "commit-export",
		"TransitionAgentGeneration":  "transition-agent-generation",
		"TransitionTargetGeneration": "transition-target-generation",
		"TransitionTargetRun":        "transition-run",
		"CreateTargetOperation":      "create-operation",
		"TransitionTargetOperation":  "transition-operation",
		"CreateIncident":             "create-incident",
		"TransitionIncident":         "transition-incident",
		"GetExec":                    "get-exec",
		"CreateExec":                 "create-exec",
		"TransitionExec":             "transition-exec",
		"FinalizeExec":               "finalize-exec",
	}
	targetStreamRPCs := map[string]struct{}{
		"OpenTargetExec": {}, "PushTargetFile": {}, "PullTargetFile": {}, "OpenTargetADB": {},
	}
	methods := worldv1.File_world_v1_world_proto.Services().ByName("WorldService").Methods()
	seen := make(map[string]struct{}, methods.Len())
	for index := 0; index < methods.Len(); index++ {
		name := string(methods.Get(index).Name())
		seen[name] = struct{}{}
		if _, targetOnly := targetStreamRPCs[name]; targetOnly {
			continue
		}
		command, ok := rpcCommands[name]
		if !ok {
			t.Errorf("RPC %s has no worldctl command mapping", name)
			continue
		}
		if commands[command] == nil {
			t.Errorf("RPC %s maps to missing command %q", name, command)
		}
	}
	for rpc := range rpcCommands {
		if _, ok := seen[rpc]; !ok {
			t.Errorf("command mapping names unknown RPC %s", rpc)
		}
	}
	if commands["top"] == nil {
		t.Error("missing top snapshot presentation command")
	}
}

func TestParseIncidentPropagatesMetricAndCoverageSnapshots(t *testing.T) {
	directory := t.TempDir()
	metricsPath := filepath.Join(directory, "metrics.json")
	coveragePath := filepath.Join(directory, "coverage.json")
	if err := os.WriteFile(metricsPath, []byte(`[{"subject_id":"subject_1","name":"rss","unit":"bytes","kind":"gauge","availability":"available","cursor":"7","collected_at":"2026-07-27T12:30:00Z"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(coveragePath, []byte(`[{"collector_id":"collector_1","signal_family":"process","placement":"host","level":"full","status":"ready","required":true}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	request, err := parseCreateIncident([]string{
		"-classification", "target_failure", "-session", "session_1", "-trigger", "process exited",
		"-last-state", "running", "-policy", "sha256:policy",
		"-high-water-metrics-file", metricsPath, "-coverage-file", coveragePath,
	}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.HighWaterMetrics) != 1 || request.HighWaterMetrics[0].Name != "rss" || request.HighWaterMetrics[0].Cursor != 7 || request.HighWaterMetrics[0].CollectedAt.AsTime().UTC() != time.Date(2026, time.July, 27, 12, 30, 0, 0, time.UTC) || len(request.Coverage) != 1 || request.Coverage[0].CollectorId != "collector_1" {
		t.Fatalf("snapshot fields were not propagated: %#v", request)
	}
}

func TestIncidentProtoJSONRejectsUnknownNullAndNilMessages(t *testing.T) {
	directory := t.TempDir()
	for name, payload := range map[string]string{
		"unknown":  `[{"collector_id":"collector_1","unexpected":true}]`,
		"null":     `[null]`,
		"rootnull": `null`,
	} {
		path := filepath.Join(directory, name+".json")
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if result, err := readProtoJSONArray(path, func() *worldv1.IncidentCoverage {
			return &worldv1.IncidentCoverage{}
		}); err == nil {
			t.Fatalf("%s input was accepted: %#v", name, result)
		}
	}
	var destination *worldv1.IncidentCoverage
	if result, err := decodeProtoJSON([]byte(`{}`), destination); err == nil {
		t.Fatalf("nil destination was accepted: %#v", result)
	}
}

func TestParseAcquirePropagatesInputViewFields(t *testing.T) {
	request, err := parseAcquire([]string{
		"-policy", "sha256:policy", "-capabilities", "sha256:capabilities",
		"-occurrences", "occ_1,occ_2", "-path-mappings", "occ_1=inputs/sample.bin",
		"-sidecars", "symbols", "-cache-scope", "tenant_1", "-require-zero-copy",
	}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.Mutation == nil || len(request.InputView.ImmutableOccurrenceRefs) != 2 || len(request.InputView.PathMappings) != 1 || !request.InputView.RequireZeroCopy {
		t.Fatalf("input view fields were not propagated: %#v", request)
	}
}

func TestParseRenewLease(t *testing.T) {
	request, err := parseRenewLease([]string{"-lease", "lease_1", "-revision", "4", "-ttl", "2m", "-policy", "sha256:policy"}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.LeaseId != "lease_1" || request.ExpectedRevision != 4 || request.Ttl.AsDuration() != 2*time.Minute {
		t.Fatalf("unexpected request: %#v", request)
	}
}

func TestParseResetTargetPreservesSupportedModeSelection(t *testing.T) {
	request, err := parseResetTarget([]string{
		"-target", "target_1", "-revision", "7", "-mode", "snapshot", "-snapshot-name", "known-good",
		"-recovery-incident", "incident_1", "-policy", "sha256:policy",
	}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.ResetMode != "snapshot" || request.SnapshotName != "known-good" || request.ExpectedRevision != 7 || request.RecoveryIncidentId != "incident_1" {
		t.Fatalf("reset fields were not propagated: %#v", request)
	}

	request, err = parseResetTarget([]string{"-target", "target_1", "-policy", "sha256:policy"}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.ResetMode != "recreate" || request.SnapshotName != "" {
		t.Fatalf("default reset selection = %s/%q, want recreate", request.ResetMode, request.SnapshotName)
	}
}

func TestParseResetTargetRejectsUnknownAndAmbiguousModes(t *testing.T) {
	cases := [][]string{
		{"-target", "target_1", "-mode", "unknown", "-policy", "sha256:policy"},
		{"-target", "target_1", "-mode", "snapshot", "-policy", "sha256:policy"},
		{"-target", "target_1", "-mode", "baseline", "-snapshot-name", "ignored", "-policy", "sha256:policy"},
		{"-target", "target_1", "-policy", "sha256:policy", "unexpected"},
	}
	for index, arguments := range cases {
		if request, err := parseResetTarget(arguments, &bytes.Buffer{}, time.Second); err == nil {
			t.Errorf("case %d was accepted: %#v", index, request)
		}
	}
}

func TestParseQuarantinePreservesDispositionAndMutation(t *testing.T) {
	before := time.Now()
	target, revision, reason, mutation, err := parseTargetDisposition("quarantine", []string{
		"-target", "target_1", "-revision", "9", "-reason", "evidence boundary lost",
		"-policy", "sha256:policy", "-causation", "incident_1",
	}, &bytes.Buffer{}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if target != "target_1" || revision != 9 || reason != "evidence boundary lost" {
		t.Fatalf("quarantine disposition = %q/%d/%q", target, revision, reason)
	}
	assertMutation(t, mutation, "sha256:policy", "incident_1", before, 2*time.Second)
}

func TestMutatingParsersPopulateUniqueBoundedMetadata(t *testing.T) {
	const policy = "sha256:policy"
	timeout := 2 * time.Second
	config := worldcli.OpenConfig{Timeout: timeout}
	tests := []struct {
		name  string
		parse func() (*worldv1.MutationMetadata, error)
	}{
		{name: "acquire", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseAcquire([]string{"-input-view", "iv_1", "-policy", policy, "-capabilities", "sha256:capabilities"}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
		{name: "renew", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseRenewLease([]string{"-lease", "lease_1", "-policy", policy}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
		{name: "create target", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseCreateTarget([]string{"-lease", "lease_1", "-template", "template_1", "-kind", "linux_container", "-policy", policy, "-capabilities", "sha256:capabilities"}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
		{name: "start run", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseStartRun([]string{"-target", "target_1", "-materialization", "sha256:materialization", "-policy", policy}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
		{name: "reset target", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseResetTarget([]string{"-target", "target_1", "-policy", policy}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
		{name: "create exec", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseCreateExec([]string{"-lease", "lease_1", "-executable", "provider-tool", "-policy", policy}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
		{name: "open exec", parse: func() (*worldv1.MutationMetadata, error) {
			options, err := parseOpenExec([]string{"-lease", "lease_1", "-executable", "provider-tool", "-policy", policy}, &bytes.Buffer{}, config)
			if err != nil {
				return nil, err
			}
			return options.start.Mutation, nil
		}},
		{name: "create incident", parse: func() (*worldv1.MutationMetadata, error) {
			request, err := parseCreateIncident([]string{"-classification", "target_failure", "-session", "session_1", "-trigger", "failed", "-last-state", "running", "-policy", policy}, &bytes.Buffer{}, timeout)
			if err != nil {
				return nil, err
			}
			return request.Mutation, nil
		}},
	}
	seenIdempotency := make(map[string]struct{}, len(tests))
	seenCorrelation := make(map[string]struct{}, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := time.Now()
			mutation, err := test.parse()
			if err != nil {
				t.Fatal(err)
			}
			assertMutation(t, mutation, policy, "", before, timeout)
			if _, exists := seenIdempotency[mutation.IdempotencyKey]; exists {
				t.Fatalf("duplicate idempotency key %q", mutation.IdempotencyKey)
			}
			if _, exists := seenCorrelation[mutation.CorrelationId]; exists {
				t.Fatalf("duplicate correlation ID %q", mutation.CorrelationId)
			}
			seenIdempotency[mutation.IdempotencyKey] = struct{}{}
			seenCorrelation[mutation.CorrelationId] = struct{}{}
		})
	}
}

func assertMutation(t *testing.T, mutation *worldv1.MutationMetadata, policy, causation string, before time.Time, timeout time.Duration) {
	t.Helper()
	if mutation == nil || mutation.IdempotencyKey == "" || mutation.CorrelationId == "" || mutation.AuthorizedPolicyReference != policy || mutation.CausationId != causation || mutation.Deadline == nil {
		t.Fatalf("mutation metadata = %#v", mutation)
	}
	if err := mutation.Deadline.CheckValid(); err != nil {
		t.Fatalf("mutation deadline is invalid: %v", err)
	}
	deadline := mutation.Deadline.AsTime()
	if deadline.Before(before.Add(timeout-time.Second)) || deadline.After(time.Now().Add(timeout+time.Second)) {
		t.Fatalf("mutation deadline %v is not bounded by timeout %v", deadline, timeout)
	}
}

func TestParseCreateExecPreservesArgv(t *testing.T) {
	request, err := parseCreateExec([]string{"-lease", "lease_1", "-policy", "sha256:policy", "-executable", "provider-tool", "--", "scan", "--deep"}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Argv) != 2 || request.Argv[0] != "scan" || request.Argv[1] != "--deep" {
		t.Fatalf("argv was not preserved: %#v", request.Argv)
	}
}

func TestParseIncidentPropagatesCausalAndEvidenceFields(t *testing.T) {
	request, err := parseCreateIncident([]string{
		"-classification", "target_failure", "-session", "session_1", "-trigger", "process exited",
		"-last-state", "running", "-cause-kind", "correlated", "-cause-summary", "memory pressure",
		"-cause-method", "cursor_window", "-cause-confidence", "0.75", "-first-cursor", "10", "-last-cursor", "20",
		"-observation-bundle", "bundle_1",
		"-artifact", `{"reference":"artifact_1","digest":"sha256:abc","size":42,"role":"trace","sensitivity":"restricted"}`,
		"-policy", "sha256:policy",
	}, &bytes.Buffer{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.Cause.Method != "cursor_window" || request.Cause.Confidence != .75 || request.ObservationBundleId != "bundle_1" || len(request.Artifacts) != 1 || request.Artifacts[0].Size != 42 {
		t.Fatalf("incident fields were not propagated: %#v", request)
	}
}
