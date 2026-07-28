package orchestration

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistedExecAndOperationIdentifiersReturnIntegrityErrors(t *testing.T) {
	execID, _ := domain.NewExecID()
	if _, err := domainExec(application.ExecRecord{ID: execID.String(), LeaseID: "corrupt", AgentWorkspaceID: "corrupt", CreatedAt: time.Now().UTC()}); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("domainExec corruption error = %v", err)
	}
	operationID, _ := domain.NewTargetOperationID()
	if _, err := domainTargetOperation(application.TargetRecord{LeaseID: "corrupt", ID: "corrupt"}, application.TargetOperationRecord{ID: operationID.String(), RunID: "corrupt"}); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("domainTargetOperation corruption error = %v", err)
	}
}

func TestExchangeExecConsumesValidatedProcessLifecycle(t *testing.T) {
	started := transport.ProcessEvent{Kind: "started", PID: 72, ParentPID: 1, ProcessStartNS: 4400}
	exited := started
	exited.Kind = "exited"
	connection := &exchangeExecTransport{frames: []transport.Frame{
		jsonTransportFrame(t, transport.KindProcess, started),
		{Kind: transport.KindStdout, Data: []byte("result")},
		jsonTransportFrame(t, transport.KindProcess, exited),
		jsonTransportFrame(t, transport.KindTerminal, transport.Terminal{CleanupConfirmed: true}),
	}}
	wire := &exchangeExecWire{}
	terminal, err := exchangeExec(context.Background(), connection, wire, 64)
	if err != nil {
		t.Fatal(err)
	}
	if !terminal.CleanupConfirmed || len(wire.outputs) != 1 || wire.outputs[0].kind != transport.KindStdout || string(wire.outputs[0].data) != "result" {
		t.Fatalf("terminal/output = %#v %#v", terminal, wire.outputs)
	}
}

func jsonTransportFrame(t *testing.T, kind transport.Kind, value any) transport.Frame {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return transport.Frame{Kind: kind, Data: encoded}
}

type exchangeExecTransport struct {
	frames []transport.Frame
	index  int
}

func (t *exchangeExecTransport) Send(context.Context, transport.Kind, []byte) (transport.Frame, error) {
	return transport.Frame{}, nil
}
func (t *exchangeExecTransport) Receive(context.Context) (transport.Frame, error) {
	if t.index == len(t.frames) {
		return transport.Frame{}, io.EOF
	}
	frame := t.frames[t.index]
	t.index++
	return frame, nil
}
func (t *exchangeExecTransport) Close() error { return nil }

type exchangeOutput struct {
	kind transport.Kind
	data []byte
}

type exchangeExecWire struct{ outputs []exchangeOutput }

func (w *exchangeExecWire) Context() context.Context    { return context.Background() }
func (w *exchangeExecWire) Receive() (execInput, error) { return execInput{}, io.EOF }
func (w *exchangeExecWire) SendOutput(kind transport.Kind, data []byte) error {
	w.outputs = append(w.outputs, exchangeOutput{kind: kind, data: append([]byte(nil), data...)})
	return nil
}
func (w *exchangeExecWire) SendOutcome(*worldv1.ExecOutcome) error { return nil }

func TestMapTemporaryInputsBindsExactArgvIndexes(t *testing.T) {
	values := []*worldv1.TemporaryInput{
		{NameHint: "prompt.bin", ArgvIndex: 1, Content: []byte{0, 1, 2}, Mode: 0o400},
		{NameHint: "tool.sh", ArgvIndex: 0, Content: []byte("run"), Mode: 0o700},
	}
	mapped, err := mapTemporaryInputs(values, 2, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(mapped) != 2 || mapped[0].ArgvIndex != 1 || mapped[0].NameHint != "prompt.bin" || mapped[0].Mode != 0o400 || mapped[1].ArgvIndex != 0 {
		t.Fatalf("mapped = %#v", mapped)
	}
	values[0].Content[0] = 9
	if mapped[0].Bytes[0] != 0 {
		t.Fatal("mapped temporary bytes alias the public request")
	}
}

func TestMapTemporaryInputsRejectsAmbiguousOrUnsafeBindings(t *testing.T) {
	tests := []struct {
		name   string
		values []*worldv1.TemporaryInput
		argv   int
		limit  int64
		code   codes.Code
	}{
		{"nil input", []*worldv1.TemporaryInput{nil}, 1, 8, codes.InvalidArgument},
		{"unsafe name", []*worldv1.TemporaryInput{{NameHint: "../escape", Content: []byte("x")}}, 1, 8, codes.InvalidArgument},
		{"outside argv", []*worldv1.TemporaryInput{{NameHint: "x", ArgvIndex: 1, Content: []byte("x")}}, 1, 8, codes.InvalidArgument},
		{"duplicate index", []*worldv1.TemporaryInput{{NameHint: "x", Content: []byte("x")}, {NameHint: "y", Content: []byte("y")}}, 2, 8, codes.InvalidArgument},
		{"unsafe mode", []*worldv1.TemporaryInput{{NameHint: "x", Content: []byte("x"), Mode: 0o1000}}, 1, 8, codes.InvalidArgument},
		{"unreadable mode", []*worldv1.TemporaryInput{{NameHint: "x", Content: []byte("x"), Mode: 0o200}}, 1, 8, codes.InvalidArgument},
		{"too large", []*worldv1.TemporaryInput{{NameHint: "x", Content: []byte("too large")}}, 1, 2, codes.ResourceExhausted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := mapTemporaryInputs(test.values, test.argv, test.limit)
			if status.Code(err) != test.code {
				t.Fatalf("code = %s, want %s (err=%v)", status.Code(err), test.code, err)
			}
		})
	}
}

func TestClientStartFramesContainOnlyStart(t *testing.T) {
	tests := []struct {
		name  string
		valid func() error
		mixed func() error
		none  func() error
	}{
		{
			name:  "agent exec",
			valid: func() error { return requireExecStartFrame(&worldv1.ExecFrame{Start: &worldv1.ExecStart{}}) },
			mixed: func() error {
				return requireExecStartFrame(&worldv1.ExecFrame{Start: &worldv1.ExecStart{}, Heartbeat: true})
			},
			none: func() error { return requireExecStartFrame(nil) },
		},
		{
			name: "target exec",
			valid: func() error {
				return requireTargetExecStartFrame(&worldv1.TargetExecFrame{Start: &worldv1.TargetExecStart{}})
			},
			mixed: func() error {
				return requireTargetExecStartFrame(&worldv1.TargetExecFrame{Start: &worldv1.TargetExecStart{}, Stdin: []byte("hidden")})
			},
			none: func() error { return requireTargetExecStartFrame(&worldv1.TargetExecFrame{}) },
		},
		{
			name: "file transfer",
			valid: func() error {
				return requireFileTransferStartFrame(&worldv1.FileTransferFrame{Start: &worldv1.FileTransferStart{}})
			},
			mixed: func() error {
				return requireFileTransferStartFrame(&worldv1.FileTransferFrame{Start: &worldv1.FileTransferStart{}, Complete: true})
			},
			none: func() error { return requireFileTransferStartFrame(&worldv1.FileTransferFrame{}) },
		},
		{
			name:  "ADB",
			valid: func() error { return requireADBStartFrame(&worldv1.ADBFrame{Start: &worldv1.ADBStart{}}) },
			mixed: func() error {
				return requireADBStartFrame(&worldv1.ADBFrame{Start: &worldv1.ADBStart{}, ClientBytes: []byte("hidden")})
			},
			none: func() error { return requireADBStartFrame(nil) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.valid(); err != nil {
				t.Fatalf("start-only frame error = %v", err)
			}
			for label, invoke := range map[string]func() error{"mixed": test.mixed, "missing": test.none} {
				if code := status.Code(invoke()); code != codes.InvalidArgument {
					t.Fatalf("%s frame code = %s, want %s", label, code, codes.InvalidArgument)
				}
			}
		})
	}
}

func TestExecInputFramesContainExactlyOneField(t *testing.T) {
	for _, populated := range []int{0, 2, 4} {
		if code := status.Code(requireExactlyOneExecInput("exec", populated)); code != codes.InvalidArgument {
			t.Fatalf("populated=%d code = %s, want %s", populated, code, codes.InvalidArgument)
		}
	}
	if err := requireExactlyOneExecInput("exec", 1); err != nil {
		t.Fatalf("single input field error = %v", err)
	}
}
