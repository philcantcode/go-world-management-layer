package daemon

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/cuttlefish"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration"
)

// config is the internal composition configuration shared by OpenHost and
// driver/profile wiring. There is no listen, bearer, mTLS, or dual-mode
// product surface — hosts open via world.Open / OpenHost only.
type config struct {
	statePath              string
	ledgerDirectory        string
	orchestrationStateRoot string
	bundleRoot             string
	materialRoot           string
	maxTransferBytes       int64
	maxExecBytes           int64
	maxADBBytes            int64
	maxBundleBytes         int64
	maxCaptureRecords      int
	allowRemoteADB         bool
	probeTimeout           time.Duration
	controlTimeout         time.Duration
	reconciliationInterval time.Duration
	reconciliationTimeout  time.Duration
	shutdownTimeout        time.Duration
	deploymentProfile      string

	agentDriver          string
	dockerBinary         string
	agentWorkspaceRoot   string
	agentImageRepository string
	agentGuestBinary     string
	agentContainerUser   string

	linuxTargetDriver       string
	targetRoot              string
	targetImageRepository   string
	targetAllowPtrace       bool
	androidTargetDriver     string
	androidTargetRoot       string
	androidSystemImageRoot  string
	androidADBBinary        string
	androidADBServer        string
	androidEmulatorBinary   string
	androidSDKRoot          string
	androidSDKManagerBinary string
	androidAVDManagerBinary string
	androidADBBasePort      int
	androidBackendVersion   string
	androidRuntimeVersion   string
	physicalTargetDriver    string
	observerDriver          string
	observerOutputRoot      string
	captureDriver           string
	captureRoot             string
	workspaceDriver         string
	materialDriver          string
}

func corePolicyResourceInventory(core *application.Core) orchestration.PolicyResourceInventory {
	return func(ctx context.Context) ([]application.ResearchSessionView, error) {
		return core.ListResearchSessions(ctx)
	}
}

func closeRunObservers(observers *orchestration.RunObserverCoordinator, timeout time.Duration) error {
	if observers == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return observers.Close(ctx)
}

func unavailableDriver(flagName, value, detail string) error {
	return fmt.Errorf("%s=%q is unavailable: %s", flagName, value, detail)
}

// defaultConfig returns environment-backed production defaults for a single
// control-state path family (world-*). There is no world-node dual default.
func defaultConfig() (config, error) {
	const prefix = "world"
	allowRemoteADB, err := environmentBool("WORLD_ALLOW_REMOTE_ADB", false)
	if err != nil {
		return config{}, err
	}
	allowPtrace, err := environmentBool("WORLD_TARGET_ALLOW_PTRACE", false)
	if err != nil {
		return config{}, err
	}
	maxCaptureRecords, err := environmentInt("WORLD_MAX_CAPTURE_RECORDS", 10000)
	if err != nil {
		return config{}, err
	}
	androidADBBasePort, err := environmentInt("WORLD_ANDROID_ADB_BASE_PORT", cuttlefish.ManagedEmulatorMinConsolePort)
	if err != nil {
		return config{}, err
	}
	controlTimeout, err := environmentDuration("WORLD_CONTROL_TIMEOUT", 30*time.Second)
	if err != nil {
		return config{}, err
	}
	reconciliationInterval, err := environmentDuration("WORLD_RECONCILIATION_INTERVAL", 30*time.Second)
	if err != nil {
		return config{}, err
	}
	reconciliationTimeout, err := environmentDuration("WORLD_RECONCILIATION_TIMEOUT", 10*time.Second)
	if err != nil {
		return config{}, err
	}
	return config{
		statePath:               envOr("WORLD_STATE", prefix+"-control.db"),
		ledgerDirectory:         envOr("WORLD_LEDGER_DIR", prefix+"-ledger"),
		orchestrationStateRoot:  envOr("WORLD_ORCHESTRATION_STATE_DIR", prefix+"-orchestration"),
		bundleRoot:              envOr("WORLD_BUNDLE_DIR", prefix+"-bundles"),
		materialRoot:            envOr("WORLD_MATERIAL_DIR", prefix+"-material"),
		maxTransferBytes:        64 << 20,
		maxExecBytes:            64 << 20,
		maxADBBytes:             64 << 20,
		maxBundleBytes:          64 << 20,
		maxCaptureRecords:       maxCaptureRecords,
		allowRemoteADB:          allowRemoteADB,
		probeTimeout:            10 * time.Second,
		controlTimeout:          controlTimeout,
		reconciliationInterval:  reconciliationInterval,
		reconciliationTimeout:   reconciliationTimeout,
		shutdownTimeout:         10 * time.Second,
		deploymentProfile:       os.Getenv("WORLD_DEPLOYMENT_PROFILE"),
		agentDriver:             envOr("WORLD_AGENT_DRIVER", "none"),
		dockerBinary:            envOr("WORLD_DOCKER_BINARY", "docker"),
		agentWorkspaceRoot:      os.Getenv("WORLD_AGENT_WORKSPACE_ROOT"),
		agentImageRepository:    os.Getenv("WORLD_AGENT_IMAGE_REPOSITORY"),
		agentGuestBinary:        envOr("WORLD_AGENT_GUEST_BINARY", "/usr/local/bin/world-guest"),
		agentContainerUser:      envOr("WORLD_AGENT_CONTAINER_USER", "65532:65532"),
		linuxTargetDriver:       envOr("WORLD_LINUX_TARGET_DRIVER", "none"),
		targetRoot:              os.Getenv("WORLD_TARGET_ROOT"),
		targetImageRepository:   os.Getenv("WORLD_TARGET_IMAGE_REPOSITORY"),
		targetAllowPtrace:       allowPtrace,
		androidTargetDriver:     envOr("WORLD_ANDROID_TARGET_DRIVER", "none"),
		androidTargetRoot:       os.Getenv("WORLD_ANDROID_TARGET_ROOT"),
		androidSystemImageRoot:  os.Getenv("WORLD_ANDROID_SYSTEM_IMAGE_ROOT"),
		androidADBBinary:        envOr("WORLD_ANDROID_ADB_BINARY", "adb"),
		androidADBServer:        envOr("WORLD_ANDROID_ADB_SERVER", "127.0.0.1:5037"),
		androidEmulatorBinary:   envOr("WORLD_ANDROID_EMULATOR_BINARY", "emulator"),
		androidSDKRoot:          os.Getenv("WORLD_ANDROID_SDK_ROOT"),
		androidSDKManagerBinary: envOr("WORLD_ANDROID_SDKMANAGER_BINARY", "sdkmanager"),
		androidAVDManagerBinary: envOr("WORLD_ANDROID_AVDMANAGER_BINARY", "avdmanager"),
		androidADBBasePort:      androidADBBasePort,
		androidBackendVersion:   os.Getenv("WORLD_ANDROID_BACKEND_VERSION"),
		androidRuntimeVersion:   os.Getenv("WORLD_ANDROID_RUNTIME_VERSION"),
		physicalTargetDriver:    envOr("WORLD_PHYSICAL_TARGET_DRIVER", "none"),
		observerDriver:          envOr("WORLD_OBSERVER_DRIVER", "none"),
		observerOutputRoot:      os.Getenv("WORLD_OBSERVER_OUTPUT_DIR"),
		captureDriver:           envOr("WORLD_CAPTURE_DRIVER", "none"),
		captureRoot:             os.Getenv("WORLD_CAPTURE_DIR"),
		workspaceDriver:         envOr("WORLD_WORKSPACE_DRIVER", "none"),
		materialDriver:          envOr("WORLD_MATERIAL_DRIVER", "local"),
	}, nil
}

func (c config) validate() error {
	return c.validatePathsAndDrivers()
}

func requireChoice(name, value string, allowed ...string) error {
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("%s=%q is invalid; allowed values: %s", name, value, strings.Join(allowed, ", "))
}

func validContainerPath(value string) bool {
	return strings.HasPrefix(value, "/") && path.Clean(value) == value && value != "/"
}

func validNumericContainerUser(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		parsed, err := strconv.ParseUint(part, 10, 32)
		if err != nil || parsed == 0 {
			return false
		}
	}
	return true
}

func envOr(name, fallback string) string {
	if value, found := os.LookupEnv(name); found {
		return value
	}
	return fallback
}

func environmentBool(name string, fallback bool) (bool, error) {
	value, found := os.LookupEnv(name)
	if !found || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return parsed, nil
}

func environmentInt(name string, fallback int) (int, error) {
	value, found := os.LookupEnv(name)
	if !found || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func environmentDuration(name string, fallback time.Duration) (time.Duration, error) {
	value, found := os.LookupEnv(name)
	if !found || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return parsed, nil
}
