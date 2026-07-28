package cuttlefish

import (
	"context"
	"fmt"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

// runExactSerialADB is the only command-runner bridge for ADB in this driver.
// It guarantees an explicit serial selector and rejects host-global actions.
func runExactSerialADB(ctx context.Context, runner command.Runner, binary, serial string, maximumOutput int64, args ...string) (command.Result, error) {
	if runner == nil || binary == "" || !safeExactADBSerial(serial) {
		return command.Result{}, fmt.Errorf("runner, ADB binary, and safe exact serial are required")
	}
	if len(args) == 0 || hostGlobalADBAction(args[0]) {
		return command.Result{}, fmt.Errorf("host-global or empty ADB action is forbidden")
	}
	adbArgs := make([]string, 0, len(args)+2)
	adbArgs = append(adbArgs, "-s", serial)
	adbArgs = append(adbArgs, args...)
	return runner.Run(ctx, command.Invocation{Program: binary, Args: adbArgs, MaximumOutput: maximumOutput})
}

func safeExactADBSerial(serial string) bool {
	if serial == "" {
		return false
	}
	for _, character := range serial {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' {
			continue
		}
		switch character {
		case '.', '_', '-', ':':
			continue
		default:
			return false
		}
	}
	return true
}

func hostGlobalADBAction(action string) bool {
	switch action {
	case "devices", "track-devices", "version", "host-features", "features", "start-server", "kill-server", "connect", "disconnect", "pair", "mdns", "server", "keygen", "help":
		return true
	default:
		return strings.HasPrefix(action, "host:") || strings.HasPrefix(action, "host-serial:")
	}
}
