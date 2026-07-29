package orchestration

import (
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestAndroidPhysicalEnforcementDoesNotRequireContainerUserOrSeccomp(t *testing.T) {
	report := ports.TargetPhysicalPolicyReport{
		Kind:               string(domain.TargetAndroidVirtualDevice),
		InteractionSupport: ports.PhysicalSupportEnforced,
		ResetSupport:       ports.PhysicalSupportEnforced,
		Android: ports.AndroidRuntimePhysicalFacts{
			HardwareAcceleration:        true,
			HardwareAccelerationSupport: ports.PhysicalSupportEnforced,
		},
		Resources: validAndroidPhysicalResources(),
	}
	if err := requireTargetPhysicalEnforcement(report); err != nil {
		t.Fatalf("Android enforcement incorrectly required Linux controls: %v", err)
	}
	report.Android.HardwareAcceleration = false
	if err := requireTargetPhysicalEnforcement(report); err == nil {
		t.Fatal("Android enforcement accepted missing hardware acceleration")
	}
}

func validAndroidPhysicalResources() ports.ContainerResourcePhysicalFacts {
	unsupported := ports.PhysicalLimitFact{Support: ports.PhysicalSupportUnsupported}
	return ports.ContainerResourcePhysicalFacts{
		CPUMilli:           ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
		MemoryBytes:        ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
		WritableStateBytes: ports.PhysicalLimitFact{Support: ports.PhysicalSupportEnforced},
		SwapBytes:          unsupported,
		WorkspaceBytes:     unsupported,
		CaptureBytes:       unsupported,
		Inodes:             unsupported,
		PIDs:               unsupported,
	}
}
