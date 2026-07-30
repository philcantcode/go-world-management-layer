package worldcli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nilExecFrameStream struct{}

func (nilExecFrameStream) Send(*worldv1.ExecFrame) error     { return nil }
func (nilExecFrameStream) Recv() (*worldv1.ExecFrame, error) { return nil, nil }
func (nilExecFrameStream) CloseSend() error                  { return nil }

type coordinatedExecFrameStream struct {
	mu         sync.Mutex
	sent       []*worldv1.ExecFrame
	halfClosed chan struct{}
	closeOnce  sync.Once
	closeErr   error
	received   bool
}

func newCoordinatedExecFrameStream(closeErr error) *coordinatedExecFrameStream {
	return &coordinatedExecFrameStream{halfClosed: make(chan struct{}), closeErr: closeErr}
}

func (stream *coordinatedExecFrameStream) Send(frame *worldv1.ExecFrame) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.sent = append(stream.sent, frame)
	return nil
}

func (stream *coordinatedExecFrameStream) Recv() (*worldv1.ExecFrame, error) {
	<-stream.halfClosed
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.received {
		return nil, io.EOF
	}
	stream.received = true
	return &worldv1.ExecFrame{Outcome: &worldv1.ExecOutcome{Termination: "completed"}}, nil
}

func (stream *coordinatedExecFrameStream) CloseSend() error {
	stream.closeOnce.Do(func() { close(stream.halfClosed) })
	return stream.closeErr
}

type immediateEOFExecFrameStream struct{}

func (immediateEOFExecFrameStream) Send(*worldv1.ExecFrame) error     { return nil }
func (immediateEOFExecFrameStream) Recv() (*worldv1.ExecFrame, error) { return nil, io.EOF }
func (immediateEOFExecFrameStream) CloseSend() error                  { return nil }

type recvErrorExecFrameStream struct{ recvErr error }

func (recvErrorExecFrameStream) Send(*worldv1.ExecFrame) error { return nil }
func (stream recvErrorExecFrameStream) Recv() (*worldv1.ExecFrame, error) {
	return nil, stream.recvErr
}
func (recvErrorExecFrameStream) CloseSend() error { return nil }

type releasedReader struct{ release <-chan struct{} }

func (reader releasedReader) Read([]byte) (int, error) {
	<-reader.release
	return 0, io.EOF
}

func TestParseGlobalLeavesCommandArguments(t *testing.T) {
	config, command, arguments, err := ParseGlobal("test", []string{"-timeout", "7s", "snapshot", "-lease", "lease_1"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if config.Timeout != 7*time.Second || command != "snapshot" {
		t.Fatalf("unexpected parse result: %#v %q", config, command)
	}
	if len(arguments) != 2 || arguments[0] != "-lease" || arguments[1] != "lease_1" {
		t.Fatalf("unexpected command arguments: %#v", arguments)
	}
	if strings.TrimSpace(config.StatePath) == "" || strings.TrimSpace(config.Subject) == "" {
		t.Fatalf("expected local Open defaults: %#v", config)
	}
}

func TestParseGlobalRejectsMissingCommandAndInvalidBounds(t *testing.T) {
	tests := [][]string{
		{},
		{"-timeout", "0s", "snapshot"},
		{"-state", "", "snapshot"},
	}
	for _, arguments := range tests {
		if _, _, _, err := ParseGlobal("test", arguments, &bytes.Buffer{}); err == nil {
			t.Fatalf("ParseGlobal(%#v) succeeded", arguments)
		}
	}
}

func TestWorkspacePath(t *testing.T) {
	for _, valid := range []string{"result.txt", "reports/output.json"} {
		if _, err := WorkspacePath(valid); err != nil {
			t.Errorf("WorkspacePath(%q): %v", valid, err)
		}
	}
	for _, invalid := range []string{"", ".", "../secret", "reports/../../secret", "/host/path", `C:\host\path`, `reports\output.json`, "reports//output.json"} {
		if _, err := WorkspacePath(invalid); err == nil {
			t.Errorf("WorkspacePath(%q) succeeded", invalid)
		}
	}
}

func TestObservationFilterParsesLists(t *testing.T) {
	filter := (ObservationFlags{Lease: " lease_1 ", Targets: "target_1, target_2", SignalFamilies: "cpu,,memory"}).Filter()
	if filter.LeaseId != "lease_1" || len(filter.TargetIds) != 2 || len(filter.SignalFamilies) != 2 {
		t.Fatalf("unexpected filter: %#v", filter)
	}
}

func TestMutationFlagsPreserveCausationAndGenerateIdentities(t *testing.T) {
	flags := NewFlagSet("mutation", &bytes.Buffer{})
	mutation := AddMutationFlags(flags, "")
	if err := flags.Parse([]string{"-policy", "sha256:policy", "-causation", "event_1"}); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	meta, err := mutation.Metadata(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if meta.CausationId != "event_1" || !strings.HasPrefix(meta.IdempotencyKey, "idem_") || meta.CorrelationId == "" || meta.Deadline == nil {
		t.Fatalf("unexpected mutation: %#v", meta)
	}
	deadline := meta.Deadline.AsTime()
	if err := meta.Deadline.CheckValid(); err != nil || deadline.Before(before.Add(900*time.Millisecond)) || deadline.After(time.Now().Add(1100*time.Millisecond)) {
		t.Fatalf("mutation deadline = %v, validation = %v", deadline, err)
	}
	next, err := mutation.Metadata(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if next.IdempotencyKey == meta.IdempotencyKey || next.CorrelationId == meta.CorrelationId {
		t.Fatalf("mutation identities were reused: first=%#v next=%#v", meta, next)
	}
}

func TestExecOutputKeepsControlMetadataOffStdout(t *testing.T) {
	var stdout, stderr bytes.Buffer
	var output ExecOutput
	if err := output.Handle(&stdout, &stderr, []byte("result"), []byte("warning"), &worldv1.ExecOutcome{ExitCode: 7}); err != nil {
		t.Fatal(err)
	}
	err := output.Finish(&stderr, true)
	if stdout.String() != "result" || !strings.Contains(stderr.String(), "warning") || !strings.Contains(stderr.String(), `"exit_code"`) || !strings.Contains(stderr.String(), "7") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if exit, ok := err.(interface{ ExitCode() int }); !ok || exit.ExitCode() != 7 {
		t.Fatalf("exit error = %#v", err)
	}
}

func TestExecOutputRequiresOneSuccessfulTerminalOutcome(t *testing.T) {
	if err := (&ExecOutput{}).Finish(&bytes.Buffer{}, false); err == nil {
		t.Fatal("missing terminal outcome was accepted")
	}
	var output ExecOutput
	if err := output.Handle(&bytes.Buffer{}, &bytes.Buffer{}, nil, nil, &worldv1.ExecOutcome{Termination: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := output.Finish(&bytes.Buffer{}, false); err != nil {
		t.Fatalf("completed outcome failed: %v", err)
	}
	if err := output.Handle(&bytes.Buffer{}, &bytes.Buffer{}, nil, nil, &worldv1.ExecOutcome{Termination: "completed"}); err == nil {
		t.Fatal("duplicate terminal outcome was accepted")
	}
}

func TestPumpBidiCompletesInputAndPropagatesHalfCloseError(t *testing.T) {
	closeErr := errors.New("half-close failed")
	stream := newCoordinatedExecFrameStream(closeErr)
	err := PumpBidi(stream, &worldv1.ExecFrame{Start: &worldv1.ExecStart{}}, strings.NewReader("input"),
		func(data []byte) *worldv1.ExecFrame { return &worldv1.ExecFrame{Stdin: data} },
		func() *worldv1.ExecFrame { return &worldv1.ExecFrame{Heartbeat: true} },
		func(*worldv1.ExecFrame) error { return nil }, PumpBidiOptions{})
	if !errors.Is(err, closeErr) {
		t.Fatalf("PumpBidi error = %v, want %v", err, closeErr)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) != 3 || stream.sent[0].Start == nil || string(stream.sent[1].Stdin) != "input" || !stream.sent[2].Heartbeat {
		t.Fatalf("sent frames = %#v", stream.sent)
	}
}

func TestPumpBidiRejectsServerEOFBeforeClientHalfClose(t *testing.T) {
	release := make(chan struct{})
	err := PumpBidi(immediateEOFExecFrameStream{}, &worldv1.ExecFrame{Start: &worldv1.ExecStart{}}, releasedReader{release: release},
		func(data []byte) *worldv1.ExecFrame { return &worldv1.ExecFrame{Stdin: data} }, nil,
		func(*worldv1.ExecFrame) error { return nil }, PumpBidiOptions{})
	close(release)
	if err == nil || !strings.Contains(err.Error(), "before client input was half-closed") {
		t.Fatalf("premature EOF error = %v", err)
	}
}

func TestPumpBidiAllowsServerEOFBeforeClientHalfCloseWhenConfigured(t *testing.T) {
	release := make(chan struct{})
	err := PumpBidi(immediateEOFExecFrameStream{}, &worldv1.ExecFrame{Start: &worldv1.ExecStart{}}, releasedReader{release: release},
		func(data []byte) *worldv1.ExecFrame { return &worldv1.ExecFrame{Stdin: data} }, nil,
		func(*worldv1.ExecFrame) error { return nil }, PumpBidiOptions{AllowServerEOFBeforeInputHalfClose: true})
	close(release)
	if err != nil {
		t.Fatalf("configured server-first EOF error = %v", err)
	}
}

func TestPumpBidiPropagatesUnavailableBeforeClientHalfCloseWhenEOFIsAllowed(t *testing.T) {
	release := make(chan struct{})
	recvErr := status.Error(codes.Unavailable, "daemon unavailable")
	err := PumpBidi(recvErrorExecFrameStream{recvErr: recvErr}, &worldv1.ExecFrame{Start: &worldv1.ExecStart{}}, releasedReader{release: release},
		func(data []byte) *worldv1.ExecFrame { return &worldv1.ExecFrame{Stdin: data} }, nil,
		func(*worldv1.ExecFrame) error { return nil }, PumpBidiOptions{AllowServerEOFBeforeInputHalfClose: true})
	close(release)
	if !errors.Is(err, recvErr) || status.Code(err) != codes.Unavailable {
		t.Fatalf("PumpBidi error = %v, want propagated Unavailable error %v", err, recvErr)
	}
}

func TestRequireNoArgsAndTerminalValidation(t *testing.T) {
	flags := NewFlagSet("snapshot", &bytes.Buffer{})
	if err := flags.Parse([]string{"unexpected"}); err != nil {
		t.Fatal(err)
	}
	if err := RequireNoArgs(flags); err == nil {
		t.Fatal("unexpected positional argument was accepted")
	}
	terminal, err := Terminal(true, 24, 80, " xterm ")
	if err != nil || terminal.Rows != 24 || terminal.Columns != 80 || terminal.TerminalType != "xterm" {
		t.Fatalf("terminal = %#v, %v", terminal, err)
	}
	for _, dimensions := range [][2]uint{{0, 80}, {24, 0}} {
		if terminal, err := Terminal(true, dimensions[0], dimensions[1], "xterm"); err == nil {
			t.Fatalf("invalid terminal geometry accepted: %#v", terminal)
		}
	}
}

func TestOpenConfigWorldConfigMapsLocalSubject(t *testing.T) {
	cfg := OpenConfig{
		StatePath: "control.db", LedgerDirectory: "ledger", OrchestrationStateRoot: "orch",
		BundleRoot: "bundles", MaterialRoot: "material", Subject: "operator-a", SubjectRole: "operator",
		Timeout: time.Second, AgentDriver: "none", MaterialDriver: "local",
	}
	worldCfg := cfg.WorldConfig()
	if worldCfg.Paths.StatePath != "control.db" || worldCfg.Subject.Name != "operator-a" {
		t.Fatalf("unexpected world config: %#v", worldCfg)
	}
}

func TestEncoderUsesProtobufJSONSemantics(t *testing.T) {
	var output bytes.Buffer
	sample := &worldv1.MetricSample{
		Cursor:      42,
		CollectedAt: timestamppb.New(time.Date(2026, time.July, 27, 12, 30, 0, 0, time.UTC)),
		SampleAge:   durationpb.New(1500 * time.Millisecond),
	}
	if err := Encoder(&output).Encode(sample); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["cursor"] != "42" || decoded["collected_at"] != "2026-07-27T12:30:00Z" || decoded["sample_age"] != "1.500s" {
		t.Fatalf("protobuf JSON = %#v", decoded)
	}
}

func TestEncoderMakesExpiringLeaseAndDurableIntentVisible(t *testing.T) {
	var output bytes.Buffer
	lease := &worldv1.Lease{
		LeaseId: "lease_1", State: "expiring", Revision: 2,
		Termination: &worldv1.LeaseTermination{
			Kind: "expiry", State: "expiring", Reason: "lease lifetime elapsed",
			BeginIdempotencyKey: "expiry/lease_1", BeginRequestDigest: "sha256:begin",
			InitiatedLeaseRevision: 2,
			InitiatedAt:            timestamppb.New(time.Date(2026, time.July, 27, 12, 30, 0, 0, time.UTC)),
		},
	}
	if err := Encoder(&output).Encode(lease); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["state"] != "expiring" {
		t.Fatalf("CLI lease state = %#v, want expiring", decoded["state"])
	}
	termination, ok := decoded["termination"].(map[string]any)
	if !ok {
		t.Fatalf("CLI termination = %#v", decoded["termination"])
	}
	if termination["kind"] != "expiry" || termination["state"] != "expiring" ||
		termination["begin_idempotency_key"] != "expiry/lease_1" || termination["initiated_lease_revision"] != "2" ||
		termination["initiated_at"] != "2026-07-27T12:30:00Z" {
		t.Fatalf("CLI termination = %#v", termination)
	}
	if _, exists := termination["completed_at"]; exists {
		t.Fatalf("in-progress CLI termination included completed_at: %#v", termination)
	}
}

func TestEncoderAndStreamsRejectNilGeneratedMessages(t *testing.T) {
	var output bytes.Buffer
	var sample *worldv1.MetricSample
	if err := Encoder(&output).Encode(sample); err == nil {
		t.Fatal("typed nil protobuf message was encoded")
	}
	if err := EncodeStream(Encoder(&output), func() (*worldv1.MetricSample, error) {
		return nil, nil
	}); err == nil {
		t.Fatal("nil stream message was accepted")
	}
	if err := PumpBidi[worldv1.ExecFrame](nilExecFrameStream{}, nil, strings.NewReader(""), func(data []byte) *worldv1.ExecFrame {
		return &worldv1.ExecFrame{Stdin: data}
	}, nil, func(*worldv1.ExecFrame) error {
		return nil
	}, PumpBidiOptions{}); err == nil {
		t.Fatal("nil start frame was accepted")
	}
	if err := PumpBidi(nilExecFrameStream{}, &worldv1.ExecFrame{}, strings.NewReader(""), func(data []byte) *worldv1.ExecFrame {
		return &worldv1.ExecFrame{Stdin: data}
	}, nil, func(*worldv1.ExecFrame) error {
		return nil
	}, PumpBidiOptions{}); err == nil {
		t.Fatal("nil received frame was accepted")
	}
	if err := WriteTop(&output, &worldv1.LiveSnapshot{Metrics: []*worldv1.MetricSample{nil}}); err == nil {
		t.Fatal("nil snapshot metric was accepted")
	}
}

func TestWriteTopFormatsAndValidatesProtobufDuration(t *testing.T) {
	var output bytes.Buffer
	snapshot := &worldv1.LiveSnapshot{Metrics: []*worldv1.MetricSample{{
		SubjectId: "subject_1", Name: "rss", SampleAge: durationpb.New(1500 * time.Millisecond),
	}}}
	if err := WriteTop(&output, snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "1.5s") {
		t.Fatalf("top output = %q", output.String())
	}
	snapshot.Metrics[0].SampleAge = &durationpb.Duration{Seconds: 315576000001}
	if err := WriteTop(&bytes.Buffer{}, snapshot); err == nil {
		t.Fatal("invalid protobuf sample age was accepted")
	}
}
