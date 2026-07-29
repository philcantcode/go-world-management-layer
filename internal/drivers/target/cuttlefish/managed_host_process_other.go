//go:build !windows && !linux

package cuttlefish

import (
	"context"
	"fmt"
	"runtime"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

type unsupportedManagedHostProcessAuthority struct{}

func newManagedHostProcessAuthority() managedHostProcessAuthority {
	return unsupportedManagedHostProcessAuthority{}
}

func (unsupportedManagedHostProcessAuthority) ResolveExecutable(string) (string, error) {
	return "", fmt.Errorf("managed emulator host-process authority is unsupported on %s", runtime.GOOS)
}

func (unsupportedManagedHostProcessAuthority) Preflight(string) error {
	return fmt.Errorf("managed emulator host-process authority is unsupported on %s", runtime.GOOS)
}

func (unsupportedManagedHostProcessAuthority) Kind() string { return "unsupported" }

func (unsupportedManagedHostProcessAuthority) ResourcesEnforced() bool { return false }

func (unsupportedManagedHostProcessAuthority) ResourceIdentity(instance Instance) string {
	return managedEmulatorResourceIdentity(instance)
}

func (unsupportedManagedHostProcessAuthority) PreflightResources(context.Context, admission.Resources) error {
	return fmt.Errorf("managed emulator host resource containment is unsupported on %s", runtime.GOOS)
}

func (unsupportedManagedHostProcessAuthority) StartContained(context.Context, command.Starter, command.Invocation, Instance) (command.Process, error) {
	return nil, fmt.Errorf("managed emulator host resource containment is unsupported on %s", runtime.GOOS)
}

func (unsupportedManagedHostProcessAuthority) Open(int, string, string, managedDataStorageBinding, Instance) (managedHostProcess, error) {
	return nil, fmt.Errorf("managed emulator host-process authority is unsupported on %s", runtime.GOOS)
}
