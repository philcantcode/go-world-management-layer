package linuxcontainer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/dockercli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func TestRuntimeAuthorityRejectsNonCanonicalDockerIDs(t *testing.T) {
	lease, err := domain.NewLeaseID()
	if err != nil {
		t.Fatal(err)
	}
	target, err := domain.NewTargetID()
	if err != nil {
		t.Fatal(err)
	}
	plan := validLifecycleContainerPlan(t, t.TempDir(), lease, target, 1)
	state := targetStateForPlan("short-id", plan)
	if err := validateRuntimeIdentity(state, plan); err == nil {
		t.Fatal("non-canonical inspected runtime identity was accepted")
	}
	if err := validateRuntimeInventory([]RuntimeState{state}); err == nil {
		t.Fatal("non-canonical inventory runtime identity was accepted")
	}
}

func TestRestartReconcileAdoptsExactReadyTarget(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed(testRuntimeID("runtime-ready"), plan)

	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted || report.Expected[0].RuntimeID != testRuntimeID("runtime-ready") {
		t.Fatalf("reconciliation = %#v", report)
	}
	if runtime.guestCallCount() != 1 {
		t.Fatalf("readiness self-test calls = %d, want 1", runtime.guestCallCount())
	}
	result, err := restarted.Create(targetDeadline(t), input)
	if err != nil || result.Created || !result.Status.Ready || result.Status.RuntimeID != testRuntimeID("runtime-ready") {
		t.Fatalf("adopted create replay = %#v, %v", result, err)
	}
}

func TestCreateResumesProvisioningAcrossPhysicalCrashWindows(t *testing.T) {
	tests := []struct {
		name        string
		seed        bool
		running     bool
		wantCreated bool
	}{
		{name: "before runtime create", wantCreated: true},
		{name: "after runtime create before start", seed: true, running: false},
		{name: "after start before readiness", seed: true, running: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := newInventoryRuntime()
			input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("provisioning-recovery-image")))
			_, restarted, plan := restartTargetDrivers(t, runtime, input)
			if test.seed {
				runtime.seed(testRuntimeID("partial-runtime"), plan)
				runtime.mu.Lock()
				state := runtime.states[testRuntimeID("partial-runtime")]
				state.Running = test.running
				if !test.running {
					state.Status = "created"
				}
				runtime.states[testRuntimeID("partial-runtime")] = state
				runtime.mu.Unlock()
			}

			result, err := restarted.Create(targetDeadline(t), input)
			if err != nil {
				t.Fatal(err)
			}
			if result.Created != test.wantCreated || !result.Status.Ready || result.Status.RuntimeID == "" {
				t.Fatalf("recovered result = %#v, want created=%t", result, test.wantCreated)
			}
			if runtime.guestCallCount() != 1 {
				t.Fatalf("framed readiness calls = %d, want 1", runtime.guestCallCount())
			}
			states, err := runtime.ListContainers(targetDeadline(t))
			if err != nil || len(states) != 1 || !states[0].Running {
				t.Fatalf("runtime inventory after recovery = %#v, %v", states, err)
			}
		})
	}
}

func TestCreateDoesNotRemoveForeignProvisioningCollision(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("provisioning-foreign-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed(testRuntimeID("foreign-runtime"), plan)
	runtime.mu.Lock()
	state := runtime.states[testRuntimeID("foreign-runtime")]
	state.Configuration.Image = "sha256:" + strings.Repeat("f", 64)
	runtime.states[testRuntimeID("foreign-runtime")] = state
	runtime.mu.Unlock()

	if _, err := restarted.Create(targetDeadline(t), input); err == nil {
		t.Fatal("foreign provisioning collision was accepted")
	}
	states, err := runtime.ListContainers(targetDeadline(t))
	if err != nil || len(states) != 1 || states[0].ID != testRuntimeID("foreign-runtime") || runtime.removeCalls() != 0 {
		t.Fatalf("foreign collision was mutated: states=%#v remove_calls=%d err=%v", states, runtime.removeCalls(), err)
	}
}

func TestRestartReconcileDoesNotAdoptWithoutGuestReadiness(t *testing.T) {
	runtime := newInventoryRuntime()
	runtime.guestErr = errors.New("guest protocol unavailable")
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed(testRuntimeID("runtime-unready"), plan)

	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if got := report.Expected[0].Classification; got != ports.PhysicalResourceUncertain {
		t.Fatalf("unready classification = %q, report %#v", got, report)
	}
	if _, err := restarted.requireTarget(plan.TargetID, plan.Generation); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("unready target was adopted: %v", err)
	}
}

func TestRestartReconcileAdoptsDurablyClaimedStoppedTargetAsResettable(t *testing.T) {
	runtime := newInventoryRuntime()
	input, scope := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	if err := prepareTargetDirectories(restarted.build.TargetRoot, plan); err != nil {
		t.Fatal(err)
	}
	runPlan := dockerRunPlan(t, scope, plan.Generation, dockerMaterial(t, "payload.txt", []byte("claimed")), "claimed-run", time.Second)
	if _, _, err := claimTargetGenerationRun(plan.TargetDirectory, runPlan); err != nil {
		t.Fatal(err)
	}
	runtime.seed(testRuntimeID("runtime-stopped"), plan)
	runtime.mu.Lock()
	state := runtime.states[testRuntimeID("runtime-stopped")]
	state.Running = false
	state.Status = dockercli.StoppedStatusExited
	runtime.states[testRuntimeID("runtime-stopped")] = state
	runtime.mu.Unlock()

	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected[0].Classification != ports.PhysicalResourceAdopted || runtime.guestCallCount() != 0 {
		t.Fatalf("stopped claimed reconciliation = %#v, guest calls=%d", report, runtime.guestCallCount())
	}
	record, err := restarted.requireTarget(plan.TargetID, plan.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if record.status.State != domain.TargetGenerationResettable || record.status.Ready {
		t.Fatalf("adopted stopped status = %#v", record.status)
	}
}

func TestRestartReconcileContainsClaimedRunningTargetWithoutGuestExec(t *testing.T) {
	runtime := newInventoryRuntime()
	runtime.guestErr = errors.New("hostile target exhausted guest execution capacity")
	input, scope := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	if err := prepareTargetDirectories(restarted.build.TargetRoot, plan); err != nil {
		t.Fatal(err)
	}
	runPlan := dockerRunPlan(t, scope, plan.Generation, dockerMaterial(t, "payload.txt", []byte("claimed")), "claimed-running-run", time.Second)
	if _, _, err := claimTargetGenerationRun(plan.TargetDirectory, runPlan); err != nil {
		t.Fatal(err)
	}
	runtime.seed(testRuntimeID("runtime-claimed-running"), plan)

	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Expected[0].Classification != ports.PhysicalResourceAdopted || runtime.guestCallCount() != 0 {
		t.Fatalf("claimed running reconciliation = %#v, guest calls=%d", report, runtime.guestCallCount())
	}
	state, err := runtime.Inspect(targetDeadline(t), testRuntimeID("runtime-claimed-running"))
	if err != nil || state.Running {
		t.Fatalf("claimed runtime remained active after reconciliation: %#v, %v", state, err)
	}
	record, err := restarted.requireTarget(plan.TargetID, plan.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if record.status.State != domain.TargetGenerationResettable || record.status.Ready {
		t.Fatalf("contained claimed target status = %#v", record.status)
	}
}

func TestRestartReconcileRejectsForeignTargetCollisions(t *testing.T) {
	tests := map[string]func(*RuntimeState){
		"label mismatch": func(state *RuntimeState) {
			state.Labels["world.capability-digest"] = domain.NewDigest([]byte("foreign-capability")).String()
		},
		"foreign name collision": func(state *RuntimeState) { state.Labels = map[string]string{"owner": "someone-else"} },
		"mount mismatch":         func(state *RuntimeState) { state.Configuration.Mounts[0].Source += "-foreign" },
		"runtime mismatch":       func(state *RuntimeState) { state.Configuration.Runtime = "foreign" },
		"user mismatch":          func(state *RuntimeState) { state.Configuration.User = "65531:65531" },
		"seccomp mismatch": func(state *RuntimeState) {
			state.Configuration.SecurityOptions = []string{dockercli.NoNewPrivilegesOption}
		},
		"swap mismatch":     func(state *RuntimeState) { state.Configuration.MemorySwapBytes++ },
		"security mismatch": func(state *RuntimeState) { state.Configuration.NetworkMode = "host" },
		"restart policy mismatch": func(state *RuntimeState) {
			state.Configuration.RestartPolicy = dockercli.RestartPolicy{Name: "always"}
		},
		"auto remove mismatch": func(state *RuntimeState) { state.Configuration.AutoRemove = true },
		"supplementary group mismatch": func(state *RuntimeState) {
			state.Configuration.GroupAdd = []string{"999"}
		},
		"device request mismatch": func(state *RuntimeState) {
			state.Configuration.DeviceRequests = []dockercli.DeviceRequest{{Driver: "gpu", Count: 1}}
		},
		"network attachment mismatch": func(state *RuntimeState) {
			state.Configuration.NetworkAttachments = []string{"bridge"}
		},
		"working directory mismatch": func(state *RuntimeState) { state.Configuration.WorkingDir = "/tmp" },
		"stdin once mismatch":        func(state *RuntimeState) { state.Configuration.StdinOnce = true },
		"cgroup mismatch":            func(state *RuntimeState) { state.Configuration.Cgroup = "foreign" },
		"configured mount mismatch": func(state *RuntimeState) {
			state.Configuration.ConfiguredMounts[0].BindOptionsKnown = true
			state.Configuration.ConfiguredMounts[0].BindOptions.NonRecursive = true
		},
		"tty mismatch": func(state *RuntimeState) { state.Configuration.TTY = true },
		"environment mismatch": func(state *RuntimeState) {
			state.Configuration.Environment = []string{"UNPLANNED=true"}
		},
		"healthcheck mismatch": func(state *RuntimeState) {
			state.Configuration.HealthcheckKnown = true
			state.Configuration.Healthcheck = dockercli.Healthcheck{Test: []string{"CMD", "false"}}
		},
		"stop signal mismatch": func(state *RuntimeState) { state.Configuration.StopSignal = "SIGKILL" },
		"stop timeout mismatch": func(state *RuntimeState) {
			state.Configuration.StopTimeoutKnown = true
			state.Configuration.StopTimeout = 1
		},
		"memory reservation mismatch": func(state *RuntimeState) { state.Configuration.MemoryReservation = 1 },
		"cpu shares mismatch":         func(state *RuntimeState) { state.Configuration.CPUShares = 1 },
		"cpu quota mismatch":          func(state *RuntimeState) { state.Configuration.CPUQuota = 1 },
		"cpuset mismatch":             func(state *RuntimeState) { state.Configuration.CpusetCPUs = "0" },
		"ulimit mismatch": func(state *RuntimeState) {
			state.Configuration.Ulimits = []dockercli.Ulimit{{Name: "nofile", Soft: 1, Hard: 1}}
		},
		"sysctl mismatch": func(state *RuntimeState) {
			state.Configuration.Sysctls = map[string]string{"kernel.domainname": "foreign"}
		},
		"masked paths mismatch": func(state *RuntimeState) {
			state.Configuration.MaskedPaths = append(state.Configuration.MaskedPaths, "/foreign")
		},
		"readonly paths mismatch": func(state *RuntimeState) {
			state.Configuration.ReadonlyPaths = append(state.Configuration.ReadonlyPaths, "/foreign")
		},
		"shared memory mismatch": func(state *RuntimeState) { state.Configuration.ShmSize++ },
		"log driver mismatch": func(state *RuntimeState) {
			state.Configuration.LogConfig = dockercli.LogConfiguration{Type: "json-file"}
		},
		"volume driver mismatch": func(state *RuntimeState) { state.Configuration.VolumeDriver = "local" },
		"storage option mismatch": func(state *RuntimeState) {
			state.Configuration.StorageOptions = map[string]string{"size": "1G"}
		},
		"paused running state":     func(state *RuntimeState) { state.Paused = true },
		"restarting running state": func(state *RuntimeState) { state.Restarting = true },
		"dead running state":       func(state *RuntimeState) { state.Dead = true },
		"noncanonical live status": func(state *RuntimeState) { state.Status = "paused" },
		"paused stopped state": func(state *RuntimeState) {
			state.Running, state.Paused, state.Status = false, true, "paused"
		},
		"restarting stopped state": func(state *RuntimeState) {
			state.Running, state.Restarting, state.Status = false, true, "restarting"
		},
		"dead stopped state": func(state *RuntimeState) {
			state.Running, state.Dead, state.Status = false, true, "dead"
		},
		"unknown stopped state": func(state *RuntimeState) { state.Running, state.Status = false, "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			runtime := newInventoryRuntime()
			input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
			_, restarted, plan := restartTargetDrivers(t, runtime, input)
			state := targetStateForPlan(testRuntimeID("runtime-foreign"), plan)
			mutate(&state)
			runtime.states[state.ID] = state
			report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
			if err != nil {
				t.Fatal(err)
			}
			if got := report.Expected[0].Classification; got != ports.PhysicalResourceForeign {
				t.Fatalf("classification = %q, report %#v", got, report)
			}
			if _, err := restarted.requireTarget(plan.TargetID, plan.Generation); !domain.IsCode(err, domain.CodeNotFound) {
				t.Fatalf("foreign target was adopted: %v", err)
			}
		})
	}
}

func TestCreateRejectsRuntimeInspectIdentitySubstitution(t *testing.T) {
	runtime := newInventoryRuntime()
	runtime.createID = strings.Repeat("a", 64)
	runtime.inspectID = strings.Repeat("b", 64)
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("identity-substitution-image")))
	_, restarted, _ := restartTargetDrivers(t, runtime, input)

	if _, err := restarted.Create(targetDeadline(t), input); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("substituted Create() error = %v, want integrity violation", err)
	}
	if runtime.guestCallCount() != 0 {
		t.Fatalf("identity-substituted runtime reached guest readiness %d times", runtime.guestCallCount())
	}
}

func TestRestartReconcileReportsTargetOrphansAndDuplicateIdentities(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed(testRuntimeID("runtime-orphan"), plan)

	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Unclaimed) != 1 || report.Unclaimed[0].Classification != ports.PhysicalResourceOrphan {
		t.Fatalf("orphan report = %#v", report)
	}

	runtime.seed(testRuntimeID("runtime-duplicate"), plan)
	report, err = restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil {
		t.Fatal(err)
	}
	if got := report.Expected[0].Classification; got != ports.PhysicalResourceUncertain {
		t.Fatalf("duplicate classification = %q, report %#v", got, report)
	}
}

func TestRestartReconcileMarksTargetMissingOnlyAfterAuthoritativeInventory(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, _ := restartTargetDrivers(t, runtime, input)
	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil || report.Expected[0].Classification != ports.PhysicalResourceMissing {
		t.Fatalf("authoritative empty inventory = %#v, %v", report, err)
	}
	runtime.inventoryErr = errors.New("inventory unavailable")
	report, err = restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err == nil || report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("failed inventory = %#v, %v", report, err)
	}
}

func TestRestartDestroyTargetRequiresAndRemembersProvenAbsence(t *testing.T) {
	runtime := newInventoryRuntime()
	runtime.stickyRemove = true
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	runtime.seed(testRuntimeID("runtime-destroy"), plan)
	ref := ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}

	if err := restarted.Destroy(targetDeadline(t), ref); !domain.IsCode(err, domain.CodeIntegrityViolation) {
		t.Fatalf("unproven Destroy() error = %v", err)
	}
	if calls := runtime.removeCalls(); calls != 0 {
		t.Fatalf("unproven Destroy called Remove %d times", calls)
	}
	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}})
	if err != nil || len(report.Expected) != 1 || !report.Expected[0].PlanMatched {
		t.Fatalf("cleanup-only reconciliation = %#v, %v", report, err)
	}
	if _, err := restarted.requireTarget(plan.TargetID, plan.Generation); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("cleanup-only target became executable: %v", err)
	}
	if err := restarted.Destroy(targetDeadline(t), ref); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("sticky reconciled Destroy() error = %v", err)
	}
	runtime.mu.Lock()
	runtime.stickyRemove = false
	runtime.mu.Unlock()
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatalf("idempotent destroy after authoritative absence = %v", err)
	}
	if calls := runtime.removeCalls(); calls != 2 {
		t.Fatalf("Remove calls = %d, want 2", calls)
	}
}

func TestRestartCleanupOnlyTargetRejectsEveryPlanMismatchWithoutRemove(t *testing.T) {
	tests := map[string]func(*RuntimeState){
		"lease": func(state *RuntimeState) {
			other, _ := dockerTargetFixture(t, domain.NewDigest([]byte("other-image")))
			state.Labels["world.lease"] = other.LeaseID.String()
		},
		"policy": func(state *RuntimeState) {
			state.Labels["world.policy-digest"] = domain.NewDigest([]byte("wrong-policy")).String()
		},
		"plan": func(state *RuntimeState) {
			state.Labels[planDigestLabel] = domain.NewDigest([]byte("wrong-plan")).String()
		},
		"configuration": func(state *RuntimeState) { state.Configuration.MemoryBytes++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			runtime := newInventoryRuntime()
			input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
			_, restarted, plan := restartTargetDrivers(t, runtime, input)
			state := targetStateForPlan(testRuntimeID("runtime-mismatch"), plan)
			mutate(&state)
			runtime.states[state.ID] = state
			report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}})
			if err != nil {
				t.Fatal(err)
			}
			if report.Expected[0].Classification != ports.PhysicalResourceForeign || report.Expected[0].PlanMatched {
				t.Fatalf("mismatch report = %#v", report)
			}
			ref := ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}
			if err := restarted.Destroy(targetDeadline(t), ref); !domain.IsCode(err, domain.CodeIntegrityViolation) {
				t.Fatalf("Destroy mismatch error = %v", err)
			}
			if calls := runtime.removeCalls(); calls != 0 {
				t.Fatalf("mismatch called Remove %d times", calls)
			}
		})
	}
}

func TestRestartDestroyTargetPreservesDirectoryUntilCleanupPlanIsReconciled(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	if err := os.MkdirAll(plan.TargetDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	ref := ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plan.TargetDirectory); err != nil {
		t.Fatalf("unproven absent runtime removed target directory: %v", err)
	}
	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}})
	if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceMissing || !report.Expected[0].CleanupRequired {
		t.Fatalf("missing cleanup reconciliation = %#v, %v", report, err)
	}
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plan.TargetDirectory); !os.IsNotExist(err) {
		t.Fatalf("reconciled cleanup did not remove target directory: %v", err)
	}
	report, err = restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{CleanupOnly: []ports.TargetPlan{input}})
	if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceMissing || report.Expected[0].CleanupRequired {
		t.Fatalf("completed cleanup reconciliation = %#v, %v", report, err)
	}
}

func TestRestartActiveMissingTargetRetainsOnlyDirectoryCleanupAuthority(t *testing.T) {
	runtime := newInventoryRuntime()
	input, _ := dockerTargetFixture(t, domain.NewDigest([]byte("restart-target-image")))
	_, restarted, plan := restartTargetDrivers(t, runtime, input)
	if err := os.MkdirAll(plan.TargetDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	report, err := restarted.ReconcileTargets(targetDeadline(t), ports.TargetReconciliationRequest{Active: []ports.TargetPlan{input}})
	if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceMissing || !report.Expected[0].CleanupRequired {
		t.Fatalf("active missing target reconciliation = %#v, %v", report, err)
	}
	if _, err := restarted.requireTarget(plan.TargetID, plan.Generation); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("missing active target became executable: %v", err)
	}
	ref := ports.TargetRef{ID: plan.TargetID, Generation: plan.Generation}
	if err := restarted.Destroy(targetDeadline(t), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plan.TargetDirectory); !os.IsNotExist(err) {
		t.Fatalf("active plan cleanup authority did not remove target directory: %v", err)
	}
}

// testTargetUser returns an identity the current process can already own.
// Production defaults to 65532:65532 and requires root handoff on Linux; unit
// tests on non-root CI runners use the current uid/gid so ownership is a no-op.
func testTargetUser(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "linux" && os.Geteuid() != 0 {
		return fmt.Sprintf("%d:%d", os.Geteuid(), os.Getegid())
	}
	return defaultTargetUser
}

func restartTargetDrivers(t *testing.T, runtime *inventoryRuntime, input ports.TargetPlan) (*Driver, *Driver, ContainerPlan) {
	t.Helper()
	config := Config{
		Build: BuildConfig{TargetRoot: t.TempDir(), ImageRepository: "example.invalid/target", ContainerUser: testTargetUser(t)}, Runtime: runtime,
		Collectors: CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error { return nil }),
	}
	first, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildContainerPlan(input, first.build)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return first, restarted, plan
}

func targetDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type inventoryRuntime struct {
	mu           sync.Mutex
	states       map[string]RuntimeState
	plans        map[string]ContainerPlan
	removed      int
	stickyRemove bool
	inventoryErr error
	guestErr     error
	guestCalls   int
	createID     string
	inspectID    string
}

func newInventoryRuntime() *inventoryRuntime {
	return &inventoryRuntime{states: make(map[string]RuntimeState), plans: make(map[string]ContainerPlan)}
}

func (r *inventoryRuntime) seed(id string, plan ContainerPlan) {
	r.mu.Lock()
	r.states[id], r.plans[id] = targetStateForPlan(id, plan), plan
	r.mu.Unlock()
}

func (*inventoryRuntime) Probe(context.Context) (RuntimeCapabilities, error) {
	return RuntimeCapabilities{}, nil
}

func (r *inventoryRuntime) Create(_ context.Context, plan ContainerPlan) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, state := range r.states {
		if state.Name == plan.Name {
			return "", errors.New("name already exists")
		}
	}
	id := r.createID
	if id == "" {
		id = testRuntimeID(fmt.Sprintf("runtime-%d", len(r.states)+1))
	}
	state := targetStateForPlan(id, plan)
	state.Running = false
	state.Status = dockercli.StoppedStatusCreated
	r.states[id], r.plans[id] = state, plan
	return id, nil
}

func (r *inventoryRuntime) Start(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = true
	state.Status = "running"
	r.states[id] = state
	return nil
}

func (r *inventoryRuntime) Inspect(_ context.Context, id string) (RuntimeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return RuntimeState{}, errors.New("not found")
	}
	state = cloneRuntimeState(state)
	if r.inspectID != "" {
		state.ID = r.inspectID
	}
	return state, nil
}

func (r *inventoryRuntime) Stop(_ context.Context, id string, _ ports.StopMode) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return errors.New("not found")
	}
	state.Running = false
	state.Status = "exited"
	r.states[id] = state
	return nil
}

func (r *inventoryRuntime) Quarantine(_ context.Context, id string) (RuntimeContainmentEvidence, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, found := r.states[id]
	if !found {
		return RuntimeContainmentEvidence{}, errors.New("not found")
	}
	state.Running = false
	state.Status = "exited"
	r.states[id] = state
	return RuntimeContainmentEvidence{
		RuntimeID: id, ExecutionStopped: true, NetworkUnreachable: true, StatePreserved: true,
		ObservedAt: time.Unix(80, 0).UTC(),
	}, nil
}

func (r *inventoryRuntime) Remove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removed++
	if !r.stickyRemove {
		delete(r.states, id)
		delete(r.plans, id)
	}
	return nil
}

func (r *inventoryRuntime) OpenExec(context.Context, string, ports.TargetExecPlan) (ports.ExecTransport, error) {
	r.mu.Lock()
	r.guestCalls++
	err := r.guestErr
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return successfulTargetReadinessTransport(), nil
}

func (r *inventoryRuntime) ListContainers(context.Context) ([]RuntimeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inventoryErr != nil {
		return nil, r.inventoryErr
	}
	result := make([]RuntimeState, 0, len(r.states))
	for _, state := range r.states {
		result = append(result, cloneRuntimeState(state))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (r *inventoryRuntime) removeCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.removed
}

func (r *inventoryRuntime) guestCallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.guestCalls
}

func targetStateForPlan(id string, plan ContainerPlan) RuntimeState {
	return RuntimeState{ID: id, Name: plan.Name, Running: true, Status: "running", Labels: cloneStrings(plan.Labels), CgroupID: "cgroup/" + id, Configuration: expectedTargetConfiguration(plan)}
}

func cloneRuntimeState(state RuntimeState) RuntimeState {
	state.Labels = cloneStrings(state.Labels)
	state.Configuration.Entrypoint = append([]string(nil), state.Configuration.Entrypoint...)
	state.Configuration.Command = append([]string(nil), state.Configuration.Command...)
	state.Configuration.Mounts = append([]dockercli.Mount(nil), state.Configuration.Mounts...)
	state.Configuration.ConfiguredMounts = append([]dockercli.ConfiguredMount(nil), state.Configuration.ConfiguredMounts...)
	return state
}

var _ Runtime = (*inventoryRuntime)(nil)
var _ RuntimeInventory = (*inventoryRuntime)(nil)
