package cuttlefish

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const DefaultADBServerEndpoint = "127.0.0.1:5037"

type adbServerEndpoint struct {
	host string
	port string
}

var defaultADBServer = adbServerEndpoint{host: "127.0.0.1", port: "5037"}

// ValidateManagedADBServerEndpoint requires an explicit literal loopback
// address and port. Hostnames are rejected so configuration validation and
// every managed ADB invocation select the same non-DNS endpoint identity.
func ValidateManagedADBServerEndpoint(value string) error {
	_, err := parseADBServerEndpoint(value)
	return err
}

func parseADBServerEndpoint(value string) (adbServerEndpoint, error) {
	endpoint, err := ports.ParseADBServerEndpoint(value)
	if err != nil {
		return adbServerEndpoint{}, err
	}
	return adbServerEndpoint{host: endpoint.Host, port: strconv.Itoa(int(endpoint.Port))}, nil
}

func (s adbServerEndpoint) globalArgs(args ...string) []string {
	result := make([]string, 0, len(args)+4)
	result = append(result, "-H", s.host, "-P", s.port)
	return append(result, args...)
}

// runExactSerialADBAt is the only managed command-runner bridge for ADB. It
// binds both the configured loopback server and exact serial, and rejects
// every action that could replace that device selection.
func runExactSerialADBAt(ctx context.Context, runner command.Runner, binary string, server adbServerEndpoint, serial string, maximumOutput int64, args ...string) (command.Result, error) {
	if runner == nil || binary == "" {
		return command.Result{}, fmt.Errorf("runner and ADB binary are required")
	}
	adbArgs, err := exactSerialADBArguments(server, serial, args...)
	if err != nil {
		return command.Result{}, err
	}
	return runner.Run(ctx, command.Invocation{Program: binary, Args: adbArgs, MaximumOutput: maximumOutput})
}

func exactSerialADBArguments(server adbServerEndpoint, serial string, args ...string) ([]string, error) {
	if err := ports.ValidateExactADBSerial(serial); err != nil {
		return nil, fmt.Errorf("safe exact ADB serial is required")
	}
	if server.host == "" || server.port == "" {
		return nil, fmt.Errorf("exact ADB server endpoint is required")
	}
	if len(args) == 0 || !safeExactADBAction(args[0]) {
		return nil, fmt.Errorf("empty, host-global, or transport-selecting ADB action is forbidden")
	}
	adbArgs := server.globalArgs("-s", serial)
	adbArgs = append(adbArgs, args...)
	return adbArgs, nil
}

func safeExactADBAction(action string) bool {
	return action != "" &&
		!strings.HasPrefix(action, "-") &&
		!hostGlobalADBAction(action) &&
		!transportSelectingADBWaitAction(action)
}

func transportSelectingADBWaitAction(action string) bool {
	for _, transport := range []string{"any", "local", "usb"} {
		prefix := "wait-for-" + transport
		if action == prefix || strings.HasPrefix(action, prefix+"-") {
			return true
		}
	}
	return false
}

func hostGlobalADBAction(action string) bool {
	switch action {
	case "devices", "track-devices", "version", "host-features", "features", "start-server", "kill-server", "connect", "disconnect", "pair", "mdns", "server", "keygen", "help":
		return true
	default:
		return strings.HasPrefix(action, "host:") || strings.HasPrefix(action, "host-serial:")
	}
}
