package orchestration

import (
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/observationbundle"
)

// ProductionConfig supplies host-facing dependencies while keeping the
// evidence finalization chain identical for every executable composition.
// Service.Material may be supplied by a repository-aware deployment; when it
// is nil, a local content-addressed authority is created at MaterialRoot.
type ProductionConfig struct {
	Service        Config
	Resolver       ProvisioningResolver
	BundleRoot     string
	MaterialRoot   string
	MaxBundleBytes int64
}

type Production struct {
	Core         *Controller
	Capabilities *Service
}

// NewProduction constructs the complete run-finalization chain before it
// exposes the RPC capability service. This prevents callers from accidentally
// wiring StopRun directly to Core or treating caller-supplied evidence as
// authoritative.
func NewProduction(config ProductionConfig) (*Production, error) {
	if config.Service.Finalization != nil {
		return nil, fmt.Errorf("run finalization is owned by the production constructor")
	}
	if config.Service.PolicyAdmission == nil && config.Resolver != nil {
		admission, ok := config.Resolver.(LeaseOperationPolicyAdmission)
		if !ok {
			return nil, fmt.Errorf("production provisioning resolver must provide capture/export policy admission")
		}
		config.Service.PolicyAdmission = admission
	}
	material := config.Service.Material
	if material == nil {
		if config.MaxBundleBytes <= 0 {
			return nil, fmt.Errorf("positive max bundle bytes are required for local material storage")
		}
		var err error
		material, err = NewBundleAuthority(config.MaterialRoot, config.MaxBundleBytes)
		if err != nil {
			return nil, fmt.Errorf("open bundle material authority: %w", err)
		}
	}
	finalizer, err := observationbundle.New(config.BundleRoot)
	if err != nil {
		return nil, fmt.Errorf("open observation bundle finalizer: %w", err)
	}
	finalization, err := application.NewRunFinalizationService(config.Service.Core, finalizer, material)
	if err != nil {
		return nil, fmt.Errorf("create run finalization service: %w", err)
	}
	config.Service.Material = material
	config.Service.Finalization = finalization
	// Normalize this shared limit before constructing either layer. New also
	// applies service defaults, but it receives Config by value; without this
	// normalization the controller silently fell back to its own timeout while
	// the capability service used a different value.
	config.Service.ControlTimeout = configuredControlTimeout(config.Service.ControlTimeout)
	service, err := New(config.Service)
	if err != nil {
		return nil, fmt.Errorf("create orchestration service: %w", err)
	}
	controller, err := NewController(ControllerConfig{
		Core: config.Service.Core, Agent: config.Service.Agent, Targets: config.Service.Targets,
		Workspace: config.Service.Workspace, Resolver: config.Resolver, Capabilities: service, Observers: config.Service.Observers,
		CleanupTimeout: config.Service.ControlTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create orchestration controller: %w", err)
	}
	return &Production{Core: controller, Capabilities: service}, nil
}
