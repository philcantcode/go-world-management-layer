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

func unsupportedManagedAndroidMessage(action string) string {
	return fmt.Sprintf(
		"managed Android emulator %s is unsupported on %s: full managed composition with resource containment requires Windows Job Objects; leave android-target-driver=none on this host",
		action, runtime.GOOS,
	)
}

func (unsupportedManagedHostProcessAuthority) ResolveExecutable(string) (string, error) {
	return "", fmt.Errorf("%s", unsupportedManagedAndroidMessage("host-process identity"))
}

func (unsupportedManagedHostProcessAuthority) Preflight(string) error {
	return fmt.Errorf("%s", unsupportedManagedAndroidMessage("host-process authority preflight"))
}

func (unsupportedManagedHostProcessAuthority) Kind() string { return "unsupported" }

func (unsupportedManagedHostProcessAuthority) ResourcesEnforced() bool { return false }

func (unsupportedManagedHostProcessAuthority) ResourceIdentity(instance Instance) string {
	return managedEmulatorResourceIdentity(instance)
}

func (unsupportedManagedHostProcessAuthority) PreflightResources(context.Context, admission.Resources) error {
	return fmt.Errorf("%s", unsupportedManagedAndroidMessage("CPU/memory resource containment"))
}

func (unsupportedManagedHostProcessAuthority) StartContained(context.Context, command.Starter, command.Invocation, Instance) (command.Process, error) {
	return nil, fmt.Errorf("%s", unsupportedManagedAndroidMessage("contained process start"))
}

func (unsupportedManagedHostProcessAuthority) Open(int, string, string, managedDataStorageBinding, Instance) (managedHostProcess, error) {
	return nil, fmt.Errorf("%s", unsupportedManagedAndroidMessage("process reopen"))
}
