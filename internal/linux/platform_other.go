//go:build !linux

package linux

import (
	"context"
	"runtime"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func PlatformSupport() PlatformResult {
	return PlatformResult{Supported: false, Reason: "Linux node facilities are unsupported on " + runtime.GOOS}
}

func ProbeCapabilities(ctx context.Context, _ ProbePlan) (domain.CapabilityFingerprint, error) {
	if ctx == nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeInvalidArgument, "linux.probe", "context", "must be provided", nil)
	}
	if err := ctx.Err(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	capabilities := make(map[string]domain.Capability)
	for _, name := range []string{"linux.cgroup-v2", "linux.psi", "linux.overlayfs", "linux.kvm", "linux.btf"} {
		capability, _ := domain.NewCapability(domain.CapabilityUnsupported, nil, map[string]string{"goos": runtime.GOOS, "reason": "requires a Linux node"})
		capabilities[name] = capability
	}
	return domain.NewCapabilityFingerprint(capabilities, map[string]string{"goos": runtime.GOOS, "goarch": runtime.GOARCH})
}
