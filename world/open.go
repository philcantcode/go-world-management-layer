package world

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/orchestration/daemon"
	"github.com/philcantcode/go-world-management-layer/internal/rpc"
)

// Open acquires exclusive process ownership of Paths.StatePath, composes
// production Core/Service/drivers, runs startup reconciliation, and returns a
// ready Manager. It fails closed on lock contention, policy preflight failure,
// or incomplete physical recovery.
//
// Open is the only supported constructor. There is no remote Dial product.
func Open(ctx context.Context, cfg Config) (*Manager, error) {
	return OpenContext(ctx, cfg)
}

// OpenContext is an alias of Open retained for API clarity at call sites that
// already thread an explicit context.
func OpenContext(ctx context.Context, cfg Config) (*Manager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	host, err := daemon.OpenHost(ctx, hostConfigFrom(cfg))
	if err != nil {
		return nil, err
	}
	manager, err := newManager(host, cfg)
	if err != nil {
		_ = host.Close()
		return nil, err
	}
	return manager, nil
}

func hostConfigFrom(cfg Config) daemon.HostConfig {
	drivers := cfg.Drivers
	materialDriver := strings.TrimSpace(drivers.MaterialDriver)
	if materialDriver == "" {
		materialDriver = "local"
	}
	return daemon.HostConfig{
		StatePath:              cfg.Paths.StatePath,
		LedgerDirectory:        cfg.Paths.LedgerDirectory,
		OrchestrationStateRoot: cfg.Paths.OrchestrationStateRoot,
		BundleRoot:             cfg.Paths.BundleRoot,
		MaterialRoot:           cfg.Paths.MaterialRoot,
		DeploymentProfile:      cfg.DeploymentProfile,
		SubjectName:            strings.TrimSpace(cfg.Subject.Name),

		ControlTimeout:         cfg.ControlTimeout,
		ReconciliationInterval: cfg.ReconciliationInterval,
		ReconciliationTimeout:  cfg.ReconciliationTimeout,
		ShutdownTimeout:        cfg.ShutdownTimeout,
		MaxTransferBytes:       cfg.MaxTransferBytes,
		MaxExecBytes:           cfg.MaxExecBytes,
		MaxADBBytes:            cfg.MaxADBBytes,
		MaxBundleBytes:         cfg.MaxBundleBytes,
		MaxCaptureRecords:      cfg.MaxCaptureRecords,
		AllowRemoteADB:         cfg.AllowRemoteADB,
		ProbeTimeout:           cfg.ProbeTimeout,

		AgentDriver:          drivers.AgentDriver,
		DockerBinary:         drivers.DockerBinary,
		AgentWorkspaceRoot:   drivers.AgentWorkspaceRoot,
		AgentImageRepository: drivers.AgentImageRepository,
		AgentGuestBinary:     drivers.AgentGuestBinary,
		AgentContainerUser:   drivers.AgentContainerUser,

		LinuxTargetDriver:     drivers.LinuxTarget,
		TargetRoot:            drivers.TargetRoot,
		TargetImageRepository: drivers.TargetImageRepository,
		TargetAllowPtrace:     drivers.TargetAllowPtrace,

		AndroidTargetDriver:     drivers.AndroidTarget,
		AndroidTargetRoot:       drivers.AndroidTargetRoot,
		AndroidSystemImageRoot:  drivers.AndroidSystemImageRoot,
		AndroidADBBinary:        drivers.AndroidADBBinary,
		AndroidADBServer:        drivers.AndroidADBServer,
		AndroidEmulatorBinary:   drivers.AndroidEmulatorBinary,
		AndroidSDKRoot:          drivers.AndroidSDKRoot,
		AndroidSDKManagerBinary: drivers.AndroidSDKManagerBinary,
		AndroidAVDManagerBinary: drivers.AndroidAVDManagerBinary,
		AndroidADBBasePort:      drivers.AndroidADBBasePort,
		AndroidBackendVersion:   drivers.AndroidBackendVersion,
		AndroidRuntimeVersion:   drivers.AndroidRuntimeVersion,

		PhysicalTargetDriver: "none",
		ObserverDriver:       drivers.ObserverDriver,
		ObserverOutputRoot:   drivers.ObserverOutputRoot,
		CaptureDriver:        drivers.CaptureDriver,
		CaptureRoot:          drivers.CaptureRoot,
		WorkspaceDriver:      drivers.WorkspaceDriver,
		MaterialDriver:       materialDriver,
	}
}

func newManager(host *daemon.Host, cfg Config) (*Manager, error) {
	if host == nil || host.Production == nil {
		return nil, fmt.Errorf("production host is required")
	}
	subject := strings.TrimSpace(cfg.Subject.Name)
	role := cfg.effectiveRole()
	trusted := map[string]bool{}
	if role == RoleInternal {
		trusted[subject] = true
	}
	facade, err := rpc.NewWorldServer(host.Controller(), rpc.ServerOptions{
		Capabilities:        host.Service(),
		TrustedNodeSubjects: trusted,
	})
	if err != nil {
		return nil, fmt.Errorf("create in-process capability facade: %w", err)
	}
	timeout := cfg.DefaultTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Manager{
		host:           host,
		facade:         facade,
		subject:        Subject{Name: subject, Role: role},
		defaultTimeout: timeout,
		identity:       rpc.Identity{Subject: subject, Method: "local_embed"},
	}, nil
}
