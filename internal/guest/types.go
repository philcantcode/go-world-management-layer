// Package guest implements the provider-neutral world-guest exec supervisor.
package guest

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

var (
	ErrInvalidStart        = errors.New("invalid guest exec start")
	ErrHeartbeatExpired    = errors.New("guest heartbeat lease expired")
	ErrInputLimit          = errors.New("guest stdin limit exceeded")
	ErrTemporaryCleanup    = errors.New("temporary input cleanup unconfirmed")
	ErrUnsupportedTerminal = errors.New("terminal mode is not supported by this guest")
)

const (
	defaultMaxTemporaryBytes = int64(8 << 20)
	defaultMaxStdinBytes     = int64(64 << 20)
	defaultHeartbeatTimeout  = 30 * time.Second
	defaultIOChunkSize       = 32 << 10
)

// Config bounds guest resources and provides injectable process behavior.
type Config struct {
	TemporaryRoot     string
	MaxTemporaryBytes int64
	MaxStdinBytes     int64
	HeartbeatTimeout  time.Duration
	IOChunkSize       int
	Launcher          Launcher
	Now               func() time.Time
}

type ProcessSpec struct {
	Executable string
	// Argv contains only the arguments after argv[0]. Executable supplies
	// both the program to launch and argv[0].
	Argv             []string
	WorkingDirectory string
	Environment      map[string]string
}

type ProcessIdentity struct {
	PID            int64
	ParentPID      int64
	ProcessStartNS int64
}

type ProcessResult struct {
	ExitCode int
	Signal   string
	Err      error
}

// Process owns the launched process tree, not merely its root PID.
type Process interface {
	Identity() ProcessIdentity
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait() ProcessResult
	Signal(name string) error
	Terminate() error
	Kill() error
	ConfirmCleanup(context.Context) (bool, error)
	Close() error
}

type Launcher interface {
	Launch(ProcessSpec) (Process, error)
}

// Supervisor validates starts and owns each process/input lifecycle.
type Supervisor struct {
	config Config
}

func New(config Config) (*Supervisor, error) {
	if config.TemporaryRoot == "" {
		return nil, errors.New("temporary root is required")
	}
	if config.MaxTemporaryBytes == 0 {
		config.MaxTemporaryBytes = defaultMaxTemporaryBytes
	}
	if config.MaxStdinBytes == 0 {
		config.MaxStdinBytes = defaultMaxStdinBytes
	}
	if config.HeartbeatTimeout == 0 {
		config.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if config.IOChunkSize == 0 {
		config.IOChunkSize = defaultIOChunkSize
	}
	if config.Launcher == nil {
		config.Launcher = OSLauncher{}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MaxTemporaryBytes <= 0 || config.MaxStdinBytes <= 0 || config.HeartbeatTimeout <= 0 || config.IOChunkSize <= 0 {
		return nil, errors.New("guest limits must be positive")
	}
	return &Supervisor{config: config}, nil
}

type runEmitter interface {
	Write(kind transport.Kind, data []byte) (transport.Frame, error)
	WriteJSON(kind transport.Kind, value any) (transport.Frame, error)
}
