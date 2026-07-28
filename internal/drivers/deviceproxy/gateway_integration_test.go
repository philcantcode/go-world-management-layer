package deviceproxy

import (
	"bytes"
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func TestGatewayWithRunningSDKEmulator(t *testing.T) {
	if os.Getenv("WORLD_ANDROID_EMULATOR_INTEGRATION") != "1" {
		t.Skip("set WORLD_ANDROID_EMULATOR_INTEGRATION=1 to test through a running SDK emulator")
	}
	serial := os.Getenv("WORLD_ANDROID_EMULATOR_SERIAL")
	if serial == "" {
		serial = "emulator-5554"
	}
	adbBinary := os.Getenv("WORLD_ANDROID_ADB_BINARY")
	if adbBinary == "" {
		adbBinary = "adb"
	}
	leaseID, _ := domain.NewLeaseID()
	targetID, _ := domain.NewTargetID()
	runID, _ := domain.NewTargetRunID()
	scope, err := IssueScope(leaseID, targetID, 1, runID, serial, bytes.NewReader(bytes.Repeat([]byte{9}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := NewGateway(GatewayConfig{MaximumConnectionDuration: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	endpoint, err := gateway.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	host, port, err := net.SplitHostPort(endpoint.Address())
	if err != nil {
		t.Fatal(err)
	}
	runner := command.OS{}
	devices, err := runner.Run(ctx, command.Invocation{Program: adbBinary, Args: []string{"-H", host, "-P", port, "devices"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(devices.Stdout), serial+"\tdevice") {
		t.Fatalf("scoped adb devices output = %q", devices.Stdout)
	}
	qemu, err := runner.Run(ctx, command.Invocation{Program: adbBinary, Args: []string{"-H", host, "-P", port, "-s", serial, "shell", "getprop", "ro.kernel.qemu"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(qemu.Stdout)) != "1" {
		t.Fatalf("scoped emulator response = %q", qemu.Stdout)
	}
}
