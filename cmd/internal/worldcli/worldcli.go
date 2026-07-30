// Package worldcli contains the narrow, shared mechanics used by the world
// command-line clients. It intentionally owns local Open/Manager plumbing and
// presentation helpers, not command authority or application policy.
package worldcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
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

// OpenConfig is the common in-process world.Open surface for operator CLIs.
// Remote dial flags (unix-socket, address, token, mTLS) are not supported.
type OpenConfig struct {
	StatePath              string
	LedgerDirectory        string
	OrchestrationStateRoot string
	BundleRoot             string
	MaterialRoot           string
	DeploymentProfile      string
	Subject                string
	SubjectRole            string
	Timeout                time.Duration
	MaxTransferBytes       int64
	MaxExecBytes           int64
	MaxADBBytes            int64
	MaxBundleBytes         int64
	MaxCaptureRecords      int

	// Driver selection mirrors WORLD_* host composition env (logical-only by default).
	AgentDriver     string
	LinuxTarget     string
	AndroidTarget   string
	WorkspaceDriver string
	MaterialDriver  string
	ObserverDriver  string
	CaptureDriver   string

	DockerBinary            string
	AgentWorkspaceRoot      string
	AgentImageRepository    string
	AgentGuestBinary        string
	AgentContainerUser      string
	TargetRoot              string
	TargetImageRepository   string
	TargetAllowPtrace       bool
	AndroidTargetRoot       string
	AndroidSystemImageRoot  string
	AndroidADBBinary        string
	AndroidADBServer        string
	AndroidEmulatorBinary   string
	AndroidSDKRoot          string
	AndroidSDKManagerBinary string
	AndroidAVDManagerBinary string
	AndroidADBBasePort      int
	AndroidBackendVersion   string
	AndroidRuntimeVersion   string
	ObserverOutputRoot      string
	CaptureRoot             string
}

// ParseGlobal parses common local-Open flags before the command name.
// Command-specific flags remain untouched in the returned argument slice.
func ParseGlobal(program string, arguments []string, stderr io.Writer) (OpenConfig, string, []string, error) {
	defaultRoot := envOr("WORLD_ROOT", "world")
	flags := NewFlagSet(program, stderr)
	var result OpenConfig
	flags.StringVar(&result.StatePath, "state", envOr("WORLD_STATE", defaultRoot+"-control.db"), "SQLite control state path")
	flags.StringVar(&result.LedgerDirectory, "ledger-dir", envOr("WORLD_LEDGER_DIR", defaultRoot+"-ledger"), "durable ledger directory")
	flags.StringVar(&result.OrchestrationStateRoot, "orchestration-state-dir", envOr("WORLD_ORCHESTRATION_STATE_DIR", defaultRoot+"-orchestration"), "orchestration durable state root")
	flags.StringVar(&result.BundleRoot, "bundle-dir", envOr("WORLD_BUNDLE_DIR", defaultRoot+"-bundles"), "observation bundle root")
	flags.StringVar(&result.MaterialRoot, "material-dir", envOr("WORLD_MATERIAL_DIR", defaultRoot+"-material"), "material authority root")
	flags.StringVar(&result.DeploymentProfile, "deployment-profile", os.Getenv("WORLD_DEPLOYMENT_PROFILE"), "absolute path to version-3 deployment profile")
	flags.StringVar(&result.Subject, "subject", envOr("WORLD_SUBJECT", envOr("WORLD_BEARER_SUBJECT", "local-operator")), "fixed local policy subject")
	flags.StringVar(&result.SubjectRole, "subject-role", envOr("WORLD_SUBJECT_ROLE", string(world.RoleOperator)), "subject role (operator|internal)")
	flags.DurationVar(&result.Timeout, "timeout", defaultTimeout, "command timeout")
	flags.Int64Var(&result.MaxTransferBytes, "max-transfer-bytes", envInt64("WORLD_MAX_TRANSFER_BYTES", 64<<20), "maximum file transfer bytes")
	flags.Int64Var(&result.MaxExecBytes, "max-exec-bytes", envInt64("WORLD_MAX_EXEC_BYTES", 64<<20), "maximum exec stream bytes")
	flags.Int64Var(&result.MaxADBBytes, "max-adb-bytes", envInt64("WORLD_MAX_ADB_BYTES", 64<<20), "maximum ADB stream bytes")
	flags.Int64Var(&result.MaxBundleBytes, "max-bundle-bytes", envInt64("WORLD_MAX_BUNDLE_BYTES", 64<<20), "maximum observation bundle bytes")
	flags.IntVar(&result.MaxCaptureRecords, "max-capture-records", envInt("WORLD_MAX_CAPTURE_RECORDS", 10000), "maximum capture records")

	flags.StringVar(&result.AgentDriver, "agent-driver", envOr("WORLD_AGENT_DRIVER", "none"), "agent driver (none|docker)")
	flags.StringVar(&result.LinuxTarget, "linux-target-driver", envOr("WORLD_LINUX_TARGET_DRIVER", "none"), "linux target driver (none|docker)")
	flags.StringVar(&result.AndroidTarget, "android-target-driver", envOr("WORLD_ANDROID_TARGET_DRIVER", "none"), "android target driver (none|android-emulator)")
	flags.StringVar(&result.WorkspaceDriver, "workspace-driver", envOr("WORLD_WORKSPACE_DRIVER", "none"), "workspace driver (none|directory)")
	flags.StringVar(&result.MaterialDriver, "material-driver", envOr("WORLD_MATERIAL_DRIVER", "local"), "material driver (local)")
	flags.StringVar(&result.ObserverDriver, "observer-driver", envOr("WORLD_OBSERVER_DRIVER", "none"), "observer driver (none|process)")
	flags.StringVar(&result.CaptureDriver, "capture-driver", envOr("WORLD_CAPTURE_DRIVER", "none"), "capture driver (none|ledger)")

	flags.StringVar(&result.DockerBinary, "docker-binary", envOr("WORLD_DOCKER_BINARY", "docker"), "docker CLI binary")
	flags.StringVar(&result.AgentWorkspaceRoot, "agent-workspace-root", os.Getenv("WORLD_AGENT_WORKSPACE_ROOT"), "agent workspace root")
	flags.StringVar(&result.AgentImageRepository, "agent-image-repository", os.Getenv("WORLD_AGENT_IMAGE_REPOSITORY"), "agent image repository")
	flags.StringVar(&result.AgentGuestBinary, "agent-guest-binary", envOr("WORLD_AGENT_GUEST_BINARY", "/usr/local/bin/world-guest"), "agent guest binary path inside containers")
	flags.StringVar(&result.AgentContainerUser, "agent-container-user", envOr("WORLD_AGENT_CONTAINER_USER", "65532:65532"), "agent container user")
	flags.StringVar(&result.TargetRoot, "target-root", os.Getenv("WORLD_TARGET_ROOT"), "linux target root")
	flags.StringVar(&result.TargetImageRepository, "target-image-repository", os.Getenv("WORLD_TARGET_IMAGE_REPOSITORY"), "linux target image repository")
	flags.BoolVar(&result.TargetAllowPtrace, "target-allow-ptrace", envBool("WORLD_TARGET_ALLOW_PTRACE", false), "allow ptrace on linux targets")
	flags.StringVar(&result.AndroidTargetRoot, "android-target-root", os.Getenv("WORLD_ANDROID_TARGET_ROOT"), "android target root")
	flags.StringVar(&result.AndroidSystemImageRoot, "android-system-image-root", os.Getenv("WORLD_ANDROID_SYSTEM_IMAGE_ROOT"), "android system image root")
	flags.StringVar(&result.AndroidADBBinary, "android-adb-binary", envOr("WORLD_ANDROID_ADB_BINARY", "adb"), "android adb binary")
	flags.StringVar(&result.AndroidADBServer, "android-adb-server", envOr("WORLD_ANDROID_ADB_SERVER", "127.0.0.1:5037"), "android adb server address")
	flags.StringVar(&result.AndroidEmulatorBinary, "android-emulator-binary", envOr("WORLD_ANDROID_EMULATOR_BINARY", "emulator"), "android emulator binary")
	flags.StringVar(&result.AndroidSDKRoot, "android-sdk-root", os.Getenv("WORLD_ANDROID_SDK_ROOT"), "android SDK root")
	flags.StringVar(&result.AndroidSDKManagerBinary, "android-sdkmanager-binary", envOr("WORLD_ANDROID_SDKMANAGER_BINARY", "sdkmanager"), "android sdkmanager binary")
	flags.StringVar(&result.AndroidAVDManagerBinary, "android-avdmanager-binary", envOr("WORLD_ANDROID_AVDMANAGER_BINARY", "avdmanager"), "android avdmanager binary")
	flags.IntVar(&result.AndroidADBBasePort, "android-adb-base-port", envInt("WORLD_ANDROID_ADB_BASE_PORT", 5554), "android ADB base port")
	flags.StringVar(&result.AndroidBackendVersion, "android-backend-version", os.Getenv("WORLD_ANDROID_BACKEND_VERSION"), "android backend version pin")
	flags.StringVar(&result.AndroidRuntimeVersion, "android-runtime-version", os.Getenv("WORLD_ANDROID_RUNTIME_VERSION"), "android runtime version pin")
	flags.StringVar(&result.ObserverOutputRoot, "observer-output-dir", os.Getenv("WORLD_OBSERVER_OUTPUT_DIR"), "observer output root")
	flags.StringVar(&result.CaptureRoot, "capture-dir", os.Getenv("WORLD_CAPTURE_DIR"), "capture root")

	if err := flags.Parse(arguments); err != nil {
		return OpenConfig{}, "", nil, err
	}
	remaining := flags.Args()
	if len(remaining) == 0 {
		return OpenConfig{}, "", nil, UsageError("command is required")
	}
	if result.Timeout <= 0 {
		return OpenConfig{}, "", nil, UsageError("timeout must be positive")
	}
	if err := result.validatePaths(); err != nil {
		return OpenConfig{}, "", nil, err
	}
	return result, remaining[0], remaining[1:], nil
}

func (c OpenConfig) validatePaths() error {
	for name, value := range map[string]string{
		"state":                   c.StatePath,
		"ledger-dir":              c.LedgerDirectory,
		"orchestration-state-dir": c.OrchestrationStateRoot,
		"bundle-dir":              c.BundleRoot,
		"material-dir":            c.MaterialRoot,
		"subject":                 c.Subject,
	} {
		if strings.TrimSpace(value) == "" {
			return UsageError(name + " is required")
		}
	}
	switch world.SubjectRole(strings.TrimSpace(c.SubjectRole)) {
	case "", world.RoleOperator, world.RoleInternal:
	default:
		return UsageError("subject-role must be operator or internal")
	}
	return nil
}

// WorldConfig maps CLI OpenConfig into world.Config.
func (c OpenConfig) WorldConfig() world.Config {
	role := world.SubjectRole(strings.TrimSpace(c.SubjectRole))
	if role == "" {
		role = world.RoleOperator
	}
	return world.Config{
		Paths: world.LocalPaths{
			StatePath:              c.StatePath,
			LedgerDirectory:        c.LedgerDirectory,
			OrchestrationStateRoot: c.OrchestrationStateRoot,
			BundleRoot:             c.BundleRoot,
			MaterialRoot:           c.MaterialRoot,
		},
		Subject: world.Subject{
			Name: strings.TrimSpace(c.Subject),
			Role: role,
		},
		DeploymentProfile: strings.TrimSpace(c.DeploymentProfile),
		DefaultTimeout:    c.Timeout,
		MaxTransferBytes:  c.MaxTransferBytes,
		MaxExecBytes:      c.MaxExecBytes,
		MaxADBBytes:       c.MaxADBBytes,
		MaxBundleBytes:    c.MaxBundleBytes,
		MaxCaptureRecords: c.MaxCaptureRecords,
		Drivers: world.DriverConfig{
			AgentDriver:             c.AgentDriver,
			LinuxTarget:             c.LinuxTarget,
			AndroidTarget:           c.AndroidTarget,
			WorkspaceDriver:         c.WorkspaceDriver,
			MaterialDriver:          c.MaterialDriver,
			ObserverDriver:          c.ObserverDriver,
			CaptureDriver:           c.CaptureDriver,
			DockerBinary:            c.DockerBinary,
			AgentWorkspaceRoot:      c.AgentWorkspaceRoot,
			AgentImageRepository:    c.AgentImageRepository,
			AgentGuestBinary:        c.AgentGuestBinary,
			AgentContainerUser:      c.AgentContainerUser,
			TargetRoot:              c.TargetRoot,
			TargetImageRepository:   c.TargetImageRepository,
			TargetAllowPtrace:       c.TargetAllowPtrace,
			AndroidTargetRoot:       c.AndroidTargetRoot,
			AndroidSystemImageRoot:  c.AndroidSystemImageRoot,
			AndroidADBBinary:        c.AndroidADBBinary,
			AndroidADBServer:        c.AndroidADBServer,
			AndroidEmulatorBinary:   c.AndroidEmulatorBinary,
			AndroidSDKRoot:          c.AndroidSDKRoot,
			AndroidSDKManagerBinary: c.AndroidSDKManagerBinary,
			AndroidAVDManagerBinary: c.AndroidAVDManagerBinary,
			AndroidADBBasePort:      c.AndroidADBBasePort,
			AndroidBackendVersion:   c.AndroidBackendVersion,
			AndroidRuntimeVersion:   c.AndroidRuntimeVersion,
			ObserverOutputRoot:      c.ObserverOutputRoot,
			CaptureRoot:             c.CaptureRoot,
		},
	}
}

// Open constructs an in-process Manager for the configured local state tree.
func Open(configuration OpenConfig) (*world.Manager, error) {
	return OpenContext(context.Background(), configuration)
}

// OpenContext is Open with an explicit parent context (used for cancellation
// during composition/startup reconciliation).
func OpenContext(ctx context.Context, configuration OpenConfig) (*world.Manager, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := EnsureParentDirs(configuration); err != nil {
		return nil, err
	}
	return world.Open(ctx, configuration.WorldConfig())
}

// EnsureParentDirs creates parent directories for Open paths when missing.
// Control state itself is created by the store on Open.
func EnsureParentDirs(configuration OpenConfig) error {
	for _, path := range []string{
		configuration.StatePath,
		filepath.Join(configuration.LedgerDirectory, ".keep"),
		filepath.Join(configuration.OrchestrationStateRoot, ".keep"),
		filepath.Join(configuration.BundleRoot, ".keep"),
		filepath.Join(configuration.MaterialRoot, ".keep"),
	} {
		dir := filepath.Dir(path)
		if dir == "" || dir == "." {
			continue
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create open path parent %q: %w", dir, err)
		}
	}
	return nil
}

// Context bounds a non-streaming command by the configured timeout.
func Context(configuration OpenConfig) (context.Context, context.CancelFunc) {
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

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func envInt64(name string, fallback int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	var parsed int64
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

// Env returns a trimmed environment value used as a scoped-helper default.
func Env(name string) string { return strings.TrimSpace(os.Getenv(name)) }

// UsageError identifies invalid CLI syntax or missing required arguments.
type UsageError string

func (e UsageError) Error() string { return string(e) }
