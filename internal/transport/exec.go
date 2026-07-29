// Package transport implements the versioned, bounded byte-transparent exec
// protocol shared by world-guest, agent execution, and scoped target exec. The
// provider streams never contain control or incident records.
package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/framing"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

const (
	ProtocolVersion       uint16 = 1
	DefaultMaxFrame       uint32 = 1 << 20
	DefaultChunkSize             = 32 << 10
	GuestSelfTestArgument        = "--world-guest-self-test"
)

var (
	ErrProtocol       = errors.New("exec protocol violation")
	ErrSequence       = errors.New("exec frame sequence violation")
	ErrTerminal       = errors.New("exec terminal outcome violation")
	ErrOutputLimit    = errors.New("exec output limit exceeded")
	ErrCleanupUnknown = errors.New("exec process cleanup could not be confirmed")
)

type Kind uint16

const (
	KindStart Kind = iota + 1
	KindStdin
	KindCloseInput
	KindStdout
	KindStderr
	KindSignal
	KindResize
	KindHeartbeat
	KindProcess
	KindTerminal
	KindError
)

func (k Kind) valid() bool { return k >= KindStart && k <= KindError }

type Frame struct {
	Sequence uint64
	Kind     Kind
	Data     []byte
}

type Encoder struct {
	mu         sync.Mutex
	w          io.Writer
	maxPayload uint32
	sequence   uint64
}

func NewEncoder(writer io.Writer, maxPayload uint32) *Encoder {
	if maxPayload == 0 {
		maxPayload = DefaultMaxFrame
	}
	return &Encoder{w: writer, maxPayload: maxPayload}
}

func (e *Encoder) Write(kind Kind, data []byte) (Frame, error) {
	if !kind.valid() {
		return Frame{}, fmt.Errorf("%w: unknown kind %d", ErrProtocol, kind)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sequence++
	payload := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(payload[:8], e.sequence)
	copy(payload[8:], data)
	_, err := framing.Write(e.w, framing.Frame{Version: ProtocolVersion, Flags: uint16(kind), Payload: payload}, e.maxPayload)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Sequence: e.sequence, Kind: kind, Data: append([]byte(nil), data...)}, nil
}

func (e *Encoder) WriteJSON(kind Kind, value any) (Frame, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return Frame{}, err
	}
	return e.Write(kind, encoded)
}

type Decoder struct {
	decoder  *framing.Decoder
	expected uint64
	terminal bool
}

func NewDecoder(reader io.Reader, maxPayload uint32) *Decoder {
	if maxPayload == 0 {
		maxPayload = DefaultMaxFrame
	}
	return &Decoder{decoder: framing.NewDecoder(reader, maxPayload), expected: 1}
}

func (d *Decoder) Read() (Frame, error) {
	encoded, _, err := d.decoder.Read()
	if err != nil {
		return Frame{}, err
	}
	if encoded.Version != ProtocolVersion || len(encoded.Payload) < 8 {
		return Frame{}, fmt.Errorf("%w: version %d or short payload", ErrProtocol, encoded.Version)
	}
	kind := Kind(encoded.Flags)
	if !kind.valid() {
		return Frame{}, fmt.Errorf("%w: unknown kind %d", ErrProtocol, kind)
	}
	sequence := binary.BigEndian.Uint64(encoded.Payload[:8])
	if sequence != d.expected {
		return Frame{}, fmt.Errorf("%w: got %d, want %d", ErrSequence, sequence, d.expected)
	}
	if d.terminal {
		return Frame{}, fmt.Errorf("%w: frame after terminal", ErrTerminal)
	}
	d.expected++
	if kind == KindTerminal {
		d.terminal = true
	}
	return Frame{Sequence: sequence, Kind: kind, Data: append([]byte(nil), encoded.Payload[8:]...)}, nil
}

func DecodeJSON[T any](frame Frame) (T, error) {
	var result T
	if err := json.Unmarshal(frame.Data, &result); err != nil {
		return result, fmt.Errorf("decode %d frame: %w", frame.Kind, err)
	}
	return result, nil
}

type TemporaryInput struct {
	NameHint string `json:"name_hint"`
	// ArgvIndex is zero-based within ExecStart.Argv. ExecStart.Argv excludes
	// the executable/argv[0].
	ArgvIndex int    `json:"argv_index"`
	Mode      uint32 `json:"mode"`
	Bytes     []byte `json:"bytes"`
}

type ExecStart struct {
	ExecID         string `json:"exec_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Executable     string `json:"executable"`
	// Argv contains only the arguments after argv[0]. Executable supplies
	// both the program to launch and argv[0].
	Argv             []string          `json:"argv"`
	WorkingDirectory string            `json:"working_directory"`
	Environment      map[string]string `json:"environment,omitempty"`
	TemporaryInputs  []TemporaryInput  `json:"temporary_inputs,omitempty"`
	Terminal         bool              `json:"terminal"`
	Deadline         time.Time         `json:"deadline"`
	MaxOutputBytes   int64             `json:"max_output_bytes"`
	CleanupGrace     time.Duration     `json:"cleanup_grace"`
}

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (s ExecStart) Validate(maxTemporaryBytes int64) error {
	if s.ExecID == "" || !domain.IsCanonicalIdempotencyKey(s.IdempotencyKey) || s.Executable == "" {
		return fmt.Errorf("exec id, idempotency key, and executable are required")
	}
	if strings.IndexByte(s.Executable, 0) >= 0 {
		return fmt.Errorf("executable contains NUL")
	}
	if s.MaxOutputBytes <= 0 || s.CleanupGrace <= 0 {
		return fmt.Errorf("positive output and cleanup limits are required")
	}
	if s.Deadline.IsZero() {
		return fmt.Errorf("deadline is required")
	}
	for index, argument := range s.Argv {
		if strings.IndexByte(argument, 0) >= 0 {
			return fmt.Errorf("argv[%d] contains NUL", index)
		}
	}
	for name, value := range s.Environment {
		if !environmentName.MatchString(name) || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("invalid environment entry %q", name)
		}
	}
	var total int64
	usedIndexes := make(map[int]struct{}, len(s.TemporaryInputs))
	for index, input := range s.TemporaryInputs {
		name, err := safepath.Normalize(input.NameHint)
		if err != nil || strings.Contains(name, "/") {
			return fmt.Errorf("temporary input %d name: %w", index, err)
		}
		if input.ArgvIndex < 0 || input.ArgvIndex >= len(s.Argv) {
			return fmt.Errorf("temporary input %d argv index out of range", index)
		}
		if _, duplicate := usedIndexes[input.ArgvIndex]; duplicate {
			return fmt.Errorf("temporary inputs share argv index %d", input.ArgvIndex)
		}
		usedIndexes[input.ArgvIndex] = struct{}{}
		mode := input.Mode
		if mode == 0 {
			mode = 0o600
		}
		if mode&^uint32(0o777) != 0 || mode&0o400 == 0 {
			return fmt.Errorf("temporary input %d mode must be owner-readable and contain only permission bits", index)
		}
		if int64(len(input.Bytes)) > maxTemporaryBytes-total {
			return fmt.Errorf("temporary inputs exceed %d bytes", maxTemporaryBytes)
		}
		total += int64(len(input.Bytes))
	}
	return nil
}

type Signal struct {
	Name string `json:"name"`
}
type Resize struct {
	Columns uint32 `json:"columns"`
	Rows    uint32 `json:"rows"`
}
type ProcessEvent struct {
	Kind           string `json:"kind"`
	PID            int64  `json:"pid"`
	ProcessStartNS int64  `json:"process_start_ns"`
	ParentPID      int64  `json:"parent_pid,omitempty"`
}

// ProcessLifecycle validates the guest's out-of-band process identity events.
// A successful terminal must be preceded by one matching started/exited pair;
// a launch failure may produce only a terminal carrying an error.
type ProcessLifecycle struct {
	started *ProcessEvent
	exited  bool
}

func (l *ProcessLifecycle) Observe(frame Frame) error {
	if frame.Kind != KindProcess {
		return fmt.Errorf("process lifecycle received frame kind %d", frame.Kind)
	}
	event, err := DecodeJSON[ProcessEvent](frame)
	if err != nil {
		return err
	}
	if event.PID <= 0 || event.ProcessStartNS <= 0 || event.ParentPID < 0 {
		return fmt.Errorf("process lifecycle event has an invalid identity")
	}
	switch event.Kind {
	case "started":
		if l.started != nil || l.exited {
			return fmt.Errorf("process lifecycle has a duplicate or out-of-order start")
		}
		copy := event
		l.started = &copy
	case "exited":
		if l.started == nil || l.exited || event.PID != l.started.PID || event.ProcessStartNS != l.started.ProcessStartNS || event.ParentPID != l.started.ParentPID {
			return fmt.Errorf("process lifecycle has a mismatched or out-of-order exit")
		}
		l.exited = true
	default:
		return fmt.Errorf("process lifecycle event kind %q is invalid", event.Kind)
	}
	return nil
}

func (l ProcessLifecycle) ValidateTerminal(terminal Terminal) error {
	if l.started == nil {
		if terminal.Error == "" {
			return fmt.Errorf("successful terminal has no process lifecycle")
		}
		return nil
	}
	if !l.exited {
		return fmt.Errorf("terminal arrived before the matching process exit")
	}
	return nil
}

// Started returns the observed process start event, if any.
func (l ProcessLifecycle) Started() *ProcessEvent {
	if l.started == nil {
		return nil
	}
	copy := *l.started
	return &copy
}

type Terminal struct {
	ExitCode         int    `json:"exit_code"`
	Signal           string `json:"signal,omitempty"`
	IncidentID       string `json:"incident_id,omitempty"`
	CleanupConfirmed bool   `json:"cleanup_confirmed"`
	Error            string `json:"error,omitempty"`
}

type Output struct {
	Stdout    io.Writer
	Stderr    io.Writer
	OnControl func(Frame) error
	MaxBytes  int64
}

// ReceiveOutput routes only raw stdout/stderr bytes to provider streams.
// Management and lifecycle frames are sent out of band to OnControl.
func ReceiveOutput(ctx context.Context, decoder *Decoder, output Output) (Terminal, error) {
	if output.Stdout == nil || output.Stderr == nil || output.MaxBytes <= 0 {
		return Terminal{}, fmt.Errorf("output writers and positive limit are required")
	}
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return Terminal{}, err
		}
		frame, err := decoder.Read()
		if err != nil {
			return Terminal{}, err
		}
		switch frame.Kind {
		case KindStdout, KindStderr:
			if int64(len(frame.Data)) > output.MaxBytes-written {
				return Terminal{}, ErrOutputLimit
			}
			writer := output.Stdout
			if frame.Kind == KindStderr {
				writer = output.Stderr
			}
			n, err := writer.Write(frame.Data)
			written += int64(n)
			if err != nil {
				return Terminal{}, err
			}
			if n != len(frame.Data) {
				return Terminal{}, io.ErrShortWrite
			}
		case KindTerminal:
			terminal, err := DecodeJSON[Terminal](frame)
			if err != nil {
				return Terminal{}, err
			}
			if !terminal.CleanupConfirmed {
				return terminal, ErrCleanupUnknown
			}
			return terminal, nil
		default:
			if output.OnControl != nil {
				if err := output.OnControl(frame); err != nil {
					return Terminal{}, err
				}
			}
		}
	}
}
