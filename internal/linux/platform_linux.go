//go:build linux

package linux

import (
	"context"
	"os"
	"path/filepath"
	"runtime"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func PlatformSupport() PlatformResult { return PlatformResult{Supported: true, Reason: "linux host"} }

func ProbeCapabilities(ctx context.Context, input ProbePlan) (domain.CapabilityFingerprint, error) {
	if ctx == nil {
		return domain.CapabilityFingerprint{}, domain.NewError(domain.CodeInvalidArgument, "linux.probe", "context", "must be provided", nil)
	}
	if err := ctx.Err(); err != nil {
		return domain.CapabilityFingerprint{}, err
	}
	plan := input.withDefaults()
	capabilities := map[string]domain.Capability{
		"linux.cgroup-v2": pathCapability(filepath.Join(plan.CgroupRoot, "cgroup.controllers")),
		"linux.psi":       combinedPathCapability(filepath.Join(plan.PSIRoot, "cpu"), filepath.Join(plan.PSIRoot, "memory"), filepath.Join(plan.PSIRoot, "io")),
		"linux.overlayfs": pathCapability("/sys/module/overlay"),
		"linux.kvm":       pathCapability(plan.KVMDevice),
		"linux.btf":       pathCapability(plan.BTFPath),
	}
	return domain.NewCapabilityFingerprint(capabilities, map[string]string{"goos": runtime.GOOS, "goarch": runtime.GOARCH})
}

func pathCapability(path string) domain.Capability {
	status := domain.CapabilityUnsupported
	evidence := map[string]string{"path": path, "reason": "not present"}
	if _, err := os.Stat(path); err == nil {
		status = domain.CapabilitySupported
		evidence["reason"] = "present"
	}
	capability, _ := domain.NewCapability(status, nil, evidence)
	return capability
}

func combinedPathCapability(paths ...string) domain.Capability {
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			capability, _ := domain.NewCapability(domain.CapabilityUnsupported, nil, map[string]string{"path": path, "reason": "not present"})
			return capability
		}
	}
	capability, _ := domain.NewCapability(domain.CapabilitySupported, nil, map[string]string{"paths": "cpu,memory,io", "reason": "present"})
	return capability
}
