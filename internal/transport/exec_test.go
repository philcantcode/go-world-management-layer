package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/framing"
)

func TestCodecPreservesSeparatedStreamsAndTerminal(t *testing.T) {
	var wire bytes.Buffer
	encoder := NewEncoder(&wire, DefaultMaxFrame)
	if _, err := encoder.Write(KindStdout, []byte("out-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(KindStderr, []byte("err")); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(KindStdout, []byte("-out-2")); err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.WriteJSON(KindTerminal, Terminal{ExitCode: 0, CleanupConfirmed: true}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	terminal, err := ReceiveOutput(context.Background(), NewDecoder(&wire, DefaultMaxFrame), Output{Stdout: &stdout, Stderr: &stderr, MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "out-1-out-2" || stderr.String() != "err" || terminal.ExitCode != 0 {
		t.Fatalf("unexpected streams or terminal: %q %q %#v", stdout.String(), stderr.String(), terminal)
	}
}

func TestProcessLifecycleRequiresOneMatchingPairForSuccessfulTerminal(t *testing.T) {
	started := ProcessEvent{Kind: "started", PID: 41, ParentPID: 1, ProcessStartNS: 9001}
	exited := started
	exited.Kind = "exited"
	var lifecycle ProcessLifecycle
	if err := lifecycle.Observe(processEventFrame(t, started)); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Observe(processEventFrame(t, exited)); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.ValidateTerminal(Terminal{CleanupConfirmed: true}); err != nil {
		t.Fatal(err)
	}
}

func TestProcessLifecycleRejectsMissingDuplicateAndMismatchedEvents(t *testing.T) {
	valid := ProcessEvent{Kind: "started", PID: 41, ParentPID: 1, ProcessStartNS: 9001}
	t.Run("successful terminal without process", func(t *testing.T) {
		if err := (ProcessLifecycle{}).ValidateTerminal(Terminal{CleanupConfirmed: true}); err == nil {
			t.Fatal("successful terminal without lifecycle was accepted")
		}
		if err := (ProcessLifecycle{}).ValidateTerminal(Terminal{CleanupConfirmed: true, Error: "launch failed"}); err != nil {
			t.Fatalf("launch failure without a process was rejected: %v", err)
		}
	})
	t.Run("duplicate start", func(t *testing.T) {
		var lifecycle ProcessLifecycle
		if err := lifecycle.Observe(processEventFrame(t, valid)); err != nil {
			t.Fatal(err)
		}
		if err := lifecycle.Observe(processEventFrame(t, valid)); err == nil {
			t.Fatal("duplicate start was accepted")
		}
	})
	t.Run("mismatched exit", func(t *testing.T) {
		var lifecycle ProcessLifecycle
		if err := lifecycle.Observe(processEventFrame(t, valid)); err != nil {
			t.Fatal(err)
		}
		exited := valid
		exited.Kind = "exited"
		exited.ProcessStartNS++
		if err := lifecycle.Observe(processEventFrame(t, exited)); err == nil {
			t.Fatal("mismatched exit was accepted")
		}
	})
	t.Run("invalid identity", func(t *testing.T) {
		invalid := valid
		invalid.ProcessStartNS = 0
		if err := (&ProcessLifecycle{}).Observe(processEventFrame(t, invalid)); err == nil {
			t.Fatal("invalid process identity was accepted")
		}
	})
}

func processEventFrame(t *testing.T, event ProcessEvent) Frame {
	t.Helper()
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return Frame{Kind: KindProcess, Data: encoded}
}

func TestDecoderRejectsSequenceAndPostTerminal(t *testing.T) {
	var wire bytes.Buffer
	payload := make([]byte, 8)
	payload[7] = 2
	if _, err := framing.Write(&wire, framing.Frame{Version: ProtocolVersion, Flags: uint16(KindStdout), Payload: payload}, DefaultMaxFrame); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDecoder(&wire, DefaultMaxFrame).Read(); !errors.Is(err, ErrSequence) {
		t.Fatalf("error = %v, want sequence", err)
	}
}

func TestExecStartValidation(t *testing.T) {
	valid := ExecStart{ExecID: "exec_1", IdempotencyKey: "key", Executable: "/usr/bin/provider", Argv: []string{"--input", "placeholder"}, WorkingDirectory: "/workspace", TemporaryInputs: []TemporaryInput{{NameHint: "prompt.txt", ArgvIndex: 1, Bytes: []byte("prompt"), Mode: 0o600}}, Deadline: time.Now().Add(time.Minute), MaxOutputBytes: 1024, CleanupGrace: time.Second}
	if err := valid.Validate(1024); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.TemporaryInputs = []TemporaryInput{{NameHint: "../escape", ArgvIndex: 1}}
	if err := invalid.Validate(1024); err == nil {
		t.Fatal("unsafe temporary name accepted")
	}
	for _, key := range []string{" invalid", "invalid ", strings.Repeat("k", domain.MaximumIdempotencyKeyBytes+1)} {
		invalid = valid
		invalid.IdempotencyKey = key
		if err := invalid.Validate(1024); err == nil {
			t.Errorf("malformed idempotency key of %d bytes was accepted", len(key))
		}
	}
}

func TestOutputLimit(t *testing.T) {
	var wire bytes.Buffer
	encoder := NewEncoder(&wire, DefaultMaxFrame)
	_, _ = encoder.Write(KindStdout, []byte("12345"))
	_, err := ReceiveOutput(context.Background(), NewDecoder(&wire, DefaultMaxFrame), Output{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}, MaxBytes: 4})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("error = %v, want output limit", err)
	}
}
