//go:build !linux

package linux

import (
	"context"
	"runtime"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func ApplyCgroup(ctx context.Context, _ CgroupPlan) (string, error) {
	return "", unsupportedOperation(ctx, "linux.cgroup.apply")
}

func RemoveCgroup(ctx context.Context, _ CgroupPlan) error {
	return unsupportedOperation(ctx, "linux.cgroup.remove")
}

func ReadHostPSI(ctx context.Context, _ ProbePlan, _ string) (PSISample, error) {
	return PSISample{}, unsupportedOperation(ctx, "linux.psi.read")
}

func MountOverlay(ctx context.Context, _ OverlayPlan) error {
	return unsupportedOperation(ctx, "linux.overlay.mount")
}

func UnmountOverlay(ctx context.Context, _ string, _ bool) error {
	return unsupportedOperation(ctx, "linux.overlay.unmount")
}

func unsupportedOperation(ctx context.Context, operation string) error {
	if ctx == nil {
		return domain.NewError(domain.CodeInvalidArgument, operation, "context", "must be provided", nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return domain.NewDetailedError(domain.CodeCapabilityUnavailable, operation, "platform", "requires a Linux node", map[string]string{"goos": runtime.GOOS}, nil)
}
