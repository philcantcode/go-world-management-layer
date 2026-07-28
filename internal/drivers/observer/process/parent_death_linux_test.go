//go:build linux

package process

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestCollectorParentDeathSignalIsForcedKill(t *testing.T) {
	cmd := exec.Command("true")
	configureCollectorParentDeathSignal(cmd)
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatalf("collector parent-death configuration = %#v", cmd.SysProcAttr)
	}
	if !collectorParentDeathSignalGuaranteed() {
		t.Fatal("Linux parent-death configuration did not advertise its invariant")
	}
}
