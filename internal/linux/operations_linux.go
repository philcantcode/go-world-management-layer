//go:build linux

package linux

import (
	"context"
	"fmt"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

func ApplyCgroup(ctx context.Context, plan CgroupPlan) (string, error) {
	if err := requireActiveContext(ctx); err != nil {
		return "", err
	}
	directory, err := plan.Path()
	if err != nil {
		return "", err
	}
	values, err := plan.ControllerValues()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", err
	}
	for _, name := range SortedControllerNames(values) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := os.WriteFile(path.Join(directory, name), []byte(values[name]), 0o600); err != nil {
			return "", fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return directory, nil
}

func RemoveCgroup(ctx context.Context, plan CgroupPlan) error {
	if err := requireActiveContext(ctx); err != nil {
		return err
	}
	directory, err := plan.Path()
	if err != nil {
		return err
	}
	return os.Remove(directory)
}

func ReadHostPSI(ctx context.Context, input ProbePlan, resource string) (PSISample, error) {
	if err := requireActiveContext(ctx); err != nil {
		return PSISample{}, err
	}
	if resource != "cpu" && resource != "memory" && resource != "io" {
		return PSISample{}, fmt.Errorf("PSI resource must be cpu, memory, or io")
	}
	plan := input.withDefaults()
	value, err := os.ReadFile(path.Join(plan.PSIRoot, resource))
	if err != nil {
		return PSISample{}, err
	}
	return ParsePSI(string(value))
}

func MountOverlay(ctx context.Context, plan OverlayPlan) error {
	if err := requireActiveContext(ctx); err != nil {
		return err
	}
	options, err := plan.MountOptions()
	if err != nil {
		return err
	}
	flags := uintptr(0)
	if plan.ReadOnly {
		flags = unix.MS_RDONLY
	}
	return unix.Mount("overlay", plan.MergedDirectory, "overlay", flags, options)
}

func UnmountOverlay(ctx context.Context, mergedDirectory string, detach bool) error {
	if err := requireActiveContext(ctx); err != nil {
		return err
	}
	if !path.IsAbs(mergedDirectory) {
		return fmt.Errorf("absolute merged directory is required")
	}
	flags := 0
	if detach {
		flags = unix.MNT_DETACH
	}
	return unix.Unmount(mergedDirectory, flags)
}

func requireActiveContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("context deadline is required")
	}
	return ctx.Err()
}
