// Package worldcli contains the narrow, shared mechanics used by the world
// command-line clients. It intentionally owns connection and presentation
// plumbing, not command authority or application policy.
package worldcli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"github.com/philcantcode/go-world-management-layer/world"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const defaultTimeout = 30 * time.Second

// ConnectionConfig is the common authenticated worldd connection surface.
type ConnectionConfig struct {
	UnixSocket    string
	TCPAddress    string
	BearerToken   string
	Timeout       time.Duration
	TLSCA         string
	TLSCert       string
	TLSKey        string
	TLSServerName string
	MaxMessage    int
}

// ParseGlobal parses common flags before the command name. Command-specific
// flags remain untouched in the returned argument slice.
func ParseGlobal(program string, arguments []string, stderr io.Writer) (ConnectionConfig, string, []string, error) {
	defaultSocket, defaultAddress := "/tmp/worldd.sock", ""
	if runtime.GOOS == "windows" {
		defaultSocket, defaultAddress = "", "127.0.0.1:7777"
	}
	flags := NewFlagSet(program, stderr)
	var result ConnectionConfig
	flags.StringVar(&result.UnixSocket, "unix-socket", envOr("WORLD_UNIX_SOCKET", defaultSocket), "worldd Unix socket")
	flags.StringVar(&result.TCPAddress, "address", envOr("WORLD_ADDRESS", defaultAddress), "worldd TCP address")
	flags.StringVar(&result.BearerToken, "token", os.Getenv("WORLD_BEARER_TOKEN"), "local bearer token")
	flags.DurationVar(&result.Timeout, "timeout", defaultTimeout, "command/RPC timeout")
	flags.StringVar(&result.TLSCA, "tls-ca", os.Getenv("WORLD_TLS_CA"), "server CA PEM")
	flags.StringVar(&result.TLSCert, "tls-cert", os.Getenv("WORLD_TLS_CERT"), "optional mTLS client certificate PEM")
	flags.StringVar(&result.TLSKey, "tls-key", os.Getenv("WORLD_TLS_KEY"), "optional mTLS client private key PEM")
	flags.StringVar(&result.TLSServerName, "tls-server-name", os.Getenv("WORLD_TLS_SERVER_NAME"), "expected TLS server name")
	flags.IntVar(&result.MaxMessage, "max-message-bytes", worldv1.DefaultMaxMessageSize, "maximum RPC message bytes")
	if err := flags.Parse(arguments); err != nil {
		return ConnectionConfig{}, "", nil, err
	}
	remaining := flags.Args()
	if len(remaining) == 0 {
		return ConnectionConfig{}, "", nil, UsageError("command is required")
	}
	if result.Timeout <= 0 {
		return ConnectionConfig{}, "", nil, UsageError("timeout must be positive")
	}
	if result.MaxMessage <= 0 {
		return ConnectionConfig{}, "", nil, UsageError("max-message-bytes must be positive")
	}
	if strings.TrimSpace(result.UnixSocket) == "" && strings.TrimSpace(result.TCPAddress) == "" {
		return ConnectionConfig{}, "", nil, UsageError("unix-socket or address is required")
	}
	return result, remaining[0], remaining[1:], nil
}

// Dial constructs the public client using authenticated transport settings.
func Dial(configuration ConnectionConfig) (*world.Client, error) {
	tlsConfig, err := LoadClientTLS(configuration)
	if err != nil {
		return nil, err
	}
	return world.Dial(world.DialOptions{
		UnixSocket:      configuration.UnixSocket,
		TCPAddress:      configuration.TCPAddress,
		BearerToken:     configuration.BearerToken,
		TLSConfig:       tlsConfig,
		MaxMessageBytes: configuration.MaxMessage,
		DefaultTimeout:  configuration.Timeout,
	})
}

// LoadClientTLS accepts either server-authenticated TLS or mTLS. A client
// certificate and key must always be supplied together.
func LoadClientTLS(configuration ConnectionConfig) (*tls.Config, error) {
	anyTLS := configuration.TLSCA != "" || configuration.TLSCert != "" || configuration.TLSKey != "" || configuration.TLSServerName != ""
	if !anyTLS {
		return nil, nil
	}
	if configuration.TLSCA == "" || configuration.TLSServerName == "" {
		return nil, fmt.Errorf("tls-ca and tls-server-name must be configured together")
	}
	if (configuration.TLSCert == "") != (configuration.TLSKey == "") {
		return nil, fmt.Errorf("tls-cert and tls-key must be configured together")
	}
	rootPEM, err := os.ReadFile(configuration.TLSCA)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		return nil, fmt.Errorf("TLS CA contains no certificates")
	}
	result := &tls.Config{RootCAs: roots, ServerName: configuration.TLSServerName, MinVersion: tls.VersionTLS13}
	if configuration.TLSCert != "" {
		certificate, err := tls.LoadX509KeyPair(configuration.TLSCert, configuration.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load mTLS client certificate: %w", err)
		}
		result.Certificates = []tls.Certificate{certificate}
	}
	return result, nil
}

// Context bounds a non-streaming command by the configured timeout.
func Context(configuration ConnectionConfig) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), configuration.Timeout)
}

// Mutation creates required, unique metadata with the command deadline.
func Mutation(policy string, timeout time.Duration) (*worldv1.MutationMetadata, error) {
	if timeout <= 0 {
		return nil, UsageError("timeout must be positive")
	}
	return world.NewMutation(strings.TrimSpace(policy), time.Now().Add(timeout))
}

// Duration converts a native duration at a protobuf boundary and rejects an
// invalid wire representation instead of relying on a later RPC failure.
func Duration(value time.Duration) (*durationpb.Duration, error) {
	result := durationpb.New(value)
	if err := result.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid protobuf duration: %w", err)
	}
	return result, nil
}

// MutationFlags provides the caller-controlled policy and causal link while
// keeping idempotency and correlation identities generated and unique.
type MutationFlags struct {
	Policy    string
	Causation string
}

func AddMutationFlags(flags *flag.FlagSet, defaultPolicy string) *MutationFlags {
	result := &MutationFlags{}
	flags.StringVar(&result.Policy, "policy", defaultPolicy, "authorized policy reference")
	flags.StringVar(&result.Causation, "causation", "", "optional causal event or operation ID")
	return result
}

func (options *MutationFlags) Metadata(timeout time.Duration) (*worldv1.MutationMetadata, error) {
	meta, err := Mutation(options.Policy, timeout)
	if err != nil {
		return nil, err
	}
	meta.CausationId = strings.TrimSpace(options.Causation)
	return meta, nil
}

func NewFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}

// RequireNoArgs rejects positional arguments for flag-only commands. The Go
// flag package otherwise leaves them silently unconsumed.
func RequireNoArgs(flags *flag.FlagSet) error {
	if flags.NArg() == 0 {
		return nil
	}
	return UsageError(fmt.Sprintf("%s accepts no positional arguments", flags.Name()))
}

// Terminal builds validated terminal settings without lossy uint-to-uint32
// conversions. Geometry flags are ignored when terminal mode is disabled.
func Terminal(enabled bool, rows, columns uint, terminalType string) (*worldv1.TerminalSettings, error) {
	if !enabled {
		return nil, nil
	}
	if rows == 0 || uint64(rows) > math.MaxUint32 {
		return nil, UsageError("terminal rows must be between 1 and 4294967295")
	}
	if columns == 0 || uint64(columns) > math.MaxUint32 {
		return nil, UsageError("terminal columns must be between 1 and 4294967295")
	}
	if strings.TrimSpace(terminalType) == "" {
		return nil, UsageError("terminal-type is required when terminal mode is enabled")
	}
	return &worldv1.TerminalSettings{
		Enabled: true, Rows: uint32(rows), Columns: uint32(columns), TerminalType: strings.TrimSpace(terminalType),
	}, nil
}

// JSONEncoder writes generated protobuf messages with protobuf JSON semantics.
// Plain Go values remain supported for the few local CLI status objects that
// are not part of the wire contract.
type JSONEncoder struct {
	output io.Writer
}

func Encoder(output io.Writer) *JSONEncoder {
	return &JSONEncoder{output: output}
}

func (encoder *JSONEncoder) Encode(value any) error {
	if message, ok := value.(proto.Message); ok {
		if !message.ProtoReflect().IsValid() {
			return fmt.Errorf("cannot encode a nil protobuf message")
		}
		payload, err := (protojson.MarshalOptions{UseProtoNames: true, Indent: "  "}).Marshal(message)
		if err != nil {
			return err
		}
		if _, err := encoder.output.Write(payload); err != nil {
			return err
		}
		_, err = io.WriteString(encoder.output, "\n")
		return err
	}
	jsonEncoder := json.NewEncoder(encoder.output)
	jsonEncoder.SetIndent("", "  ")
	return jsonEncoder.Encode(value)
}

func EncodeResult(output *JSONEncoder, value any, err error) error {
	if err != nil {
		return err
	}
	return output.Encode(value)
}

// EncodeStream writes newline-delimited JSON until the server closes a
// read-only stream.
func EncodeStream[Value any](output *JSONEncoder, receive func() (*Value, error)) error {
	for {
		value, err := receive()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("stream returned a nil protobuf message")
		}
		if err := output.Encode(value); err != nil {
			return err
		}
	}
}

// CSV splits a comma-separated flag while ignoring surrounding whitespace and
// empty elements.
func CSV(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

// ObservationFlags is the shared filter surface for snapshot and subscriptions.
type ObservationFlags struct {
	Lease          string
	Targets        string
	Runs           string
	Subjects       string
	SignalFamilies string
	RecordKinds    string
}

// StringValues implements a repeatable string flag without comma ambiguity.
type StringValues []string

func (values *StringValues) String() string { return strings.Join(*values, ",") }

func (values *StringValues) Set(value string) error {
	*values = append(*values, value)
	return nil
}

// ExportPath parses PATH=ROLE, using defaultRole when ROLE is omitted.
func ExportPath(value, defaultRole string) (worldv1.ExportPath, error) {
	path, role, hasRole := strings.Cut(value, "=")
	if !hasRole {
		role = defaultRole
	}
	clean, err := WorkspacePath(path)
	if err != nil {
		return worldv1.ExportPath{}, err
	}
	if strings.TrimSpace(role) == "" {
		return worldv1.ExportPath{}, UsageError("export path role is required")
	}
	return worldv1.ExportPath{WorkspaceRelativePath: clean, Role: strings.TrimSpace(role)}, nil
}

func AddObservationFlags(flags *flag.FlagSet, values *ObservationFlags, defaultLease string) {
	flags.StringVar(&values.Lease, "lease", defaultLease, "lease ID")
	flags.StringVar(&values.Targets, "targets", "", "comma-separated target IDs")
	flags.StringVar(&values.Runs, "runs", "", "comma-separated target run IDs")
	flags.StringVar(&values.Subjects, "subjects", "", "comma-separated subject IDs")
	flags.StringVar(&values.SignalFamilies, "signals", "", "comma-separated signal families")
	flags.StringVar(&values.RecordKinds, "record-kinds", "", "comma-separated observation record kinds")
}

func (values ObservationFlags) Filter() *worldv1.ObservationFilter {
	return &worldv1.ObservationFilter{
		LeaseId:        strings.TrimSpace(values.Lease),
		TargetIds:      CSV(values.Targets),
		TargetRunIds:   CSV(values.Runs),
		SubjectIds:     CSV(values.Subjects),
		SignalFamilies: CSV(values.SignalFamilies),
		RecordKinds:    CSV(values.RecordKinds),
	}
}

// Require reports missing command arguments consistently.
func Require(values ...string) error {
	for index := 0; index+1 < len(values); index += 2 {
		if strings.TrimSpace(values[index+1]) == "" {
			return UsageError(values[index] + " is required")
		}
	}
	return nil
}

// WorkspacePath rejects absolute and parent-traversing paths used by the
// scoped guest helpers. The returned path always uses slash separators for the
// wire contract.
func WorkspacePath(value string) (string, error) {
	return RelativePath("workspace", value)
}

// RelativePath validates a relative path within a named scoped root.
func RelativePath(scope, value string) (string, error) {
	normalized, err := safepath.Normalize(strings.TrimSpace(value))
	if err != nil {
		return "", UsageError(fmt.Sprintf("invalid %s-relative path: %v", scope, err))
	}
	return normalized, nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// Env returns a trimmed environment value used as a scoped-helper default.
func Env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

// UsageError identifies invalid CLI syntax or missing required arguments.
type UsageError string

func (e UsageError) Error() string { return string(e) }
