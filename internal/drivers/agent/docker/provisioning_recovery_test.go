package docker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestProvisionRecoversCrashAfterCreateBeforeStart(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	config, first := newProvisionRecoveryDriver(t, engine, input)
	engine.panicBeforeStart = true
	requireProvisionCrash(t, first, input)
	assertProvisionEngineCounts(t, engine, 1, 0, 0, 0)

	restarted := requireDockerDriver(t, config)
	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err != nil || report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("unprobed stopped reconciliation = %#v, %v", report, err)
	}
	engine.panicBeforeStart = false
	engine.readiness = successfulReadinessFrames(t)
	result, err := restarted.Provision(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Created || !result.Status.Ready || result.Status.State != domain.AgentGenerationReady {
		t.Fatalf("recovered result = %#v", result)
	}
	assertProvisionEngineCounts(t, engine, 1, 1, 1, 0)
	assertReadinessProvenReconciliation(t, restarted, input)
}

func TestProvisionRecoversCrashAfterStartBeforeReadiness(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	config, first := newProvisionRecoveryDriver(t, engine, input)
	engine.panicBeforeReadiness = true
	requireProvisionCrash(t, first, input)
	assertProvisionEngineCounts(t, engine, 1, 1, 1, 0)

	restarted := requireDockerDriver(t, config)
	report, err := restarted.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err != nil || report.Expected[0].Classification != ports.PhysicalResourceUncertain {
		t.Fatalf("unprobed running reconciliation = %#v, %v", report, err)
	}
	engine.panicBeforeReadiness = false
	engine.readiness = successfulReadinessFrames(t)
	result, err := restarted.Provision(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Created || !result.Status.Ready {
		t.Fatalf("recovered result = %#v", result)
	}
	assertProvisionEngineCounts(t, engine, 1, 1, 2, 0)
	assertReadinessProvenReconciliation(t, restarted, input)
}

func TestProvisionCreatesWhenAuthoritativeRetryInventoryIsMissing(t *testing.T) {
	engine := newInventoryEngine()
	engine.readiness = successfulReadinessFrames(t)
	input := testAgentWorkspacePlan(t)
	_, driver := newProvisionRecoveryDriver(t, engine, input)

	result, err := driver.Provision(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || !result.Status.Ready {
		t.Fatalf("missing retry result = %#v", result)
	}
	assertProvisionEngineCounts(t, engine, 1, 1, 1, 0)

	replay, err := driver.Provision(testDeadline(t), input)
	if err != nil || replay.Created || !replay.Status.Ready {
		t.Fatalf("exact replay = %#v, %v", replay, err)
	}
	assertProvisionEngineCounts(t, engine, 1, 1, 2, 0)

	if err := engine.Stop(testDeadline(t), result.Status.ContainerID, ports.StopForce); err != nil {
		t.Fatal(err)
	}
	restarted, err := driver.Provision(testDeadline(t), input)
	if err != nil || restarted.Created || !restarted.Status.Ready {
		t.Fatalf("out-of-band stopped replay = %#v, %v", restarted, err)
	}
	assertProvisionEngineCounts(t, engine, 1, 2, 3, 0)
}

func TestProvisionReadinessFailureRemovesOwnedContainerAndRetryConverges(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	config, _ := newProvisionRecoveryDriver(t, engine, input)
	plan, err := BuildContainerPlan(input, config.Build)
	if err != nil {
		t.Fatal(err)
	}
	engine.seed(testContainerID('a'), plan)
	engine.readiness = nonAuthoritativeReadinessFrames(t)
	driver := requireDockerDriver(t, config)

	if _, err := driver.Provision(testDeadline(t), input); !domain.IsCode(err, domain.CodeFailedPrecondition) {
		t.Fatalf("readiness failure = %v, want failed precondition", err)
	}
	assertProvisionEngineCounts(t, engine, 0, 0, 1, 1)
	if _, err := driver.Inspect(testDeadline(t), ports.AgentWorkspaceRef{ID: plan.AgentWorkspaceID, Generation: plan.Generation}); !domain.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("failed readiness was committed: %v", err)
	}

	engine.readiness = successfulReadinessFrames(t)
	result, err := driver.Provision(testDeadline(t), input)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || !result.Status.Ready {
		t.Fatalf("post-cleanup retry = %#v", result)
	}
	assertProvisionEngineCounts(t, engine, 1, 1, 2, 1)
}

func TestProvisionFailsClosedForForeignAndAmbiguousCrashInventory(t *testing.T) {
	for _, test := range []struct {
		name string
		seed func(*inventoryEngine, ContainerPlan)
	}{
		{
			name: "foreign",
			seed: func(engine *inventoryEngine, plan ContainerPlan) {
				state := agentStateForPlan(testContainerID('b'), plan)
				state.Configuration.Image = "example.invalid/foreign@" + domain.NewDigest([]byte("foreign")).String()
				engine.states[state.ID] = state
			},
		},
		{
			name: "ambiguous",
			seed: func(engine *inventoryEngine, plan ContainerPlan) {
				engine.seed(testContainerID('c'), plan)
				engine.seed(testContainerID('d'), plan)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := newInventoryEngine()
			input := testAgentWorkspacePlan(t)
			config, driver := newProvisionRecoveryDriver(t, engine, input)
			plan, err := BuildContainerPlan(input, config.Build)
			if err != nil {
				t.Fatal(err)
			}
			test.seed(engine, plan)
			if _, err := driver.Provision(testDeadline(t), input); !domain.IsCode(err, domain.CodeIntegrityViolation) {
				t.Fatalf("Provision() error = %v, want integrity violation", err)
			}
			assertProvisionEngineCounts(t, engine, 0, 0, 0, 0)
		})
	}
}

func TestProvisionRequiresAuthoritativeInventoryBeforeMutation(t *testing.T) {
	engine := newInventoryEngine()
	input := testAgentWorkspacePlan(t)
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, input.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	driver := requireDockerDriver(t, Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: engineWithoutInventory{Engine: engine},
	})
	if _, err := driver.Provision(testDeadline(t), input); !domain.IsCode(err, domain.CodeCapabilityUnavailable) {
		t.Fatalf("Provision() error = %v, want capability unavailable", err)
	}
	assertProvisionEngineCounts(t, engine, 0, 0, 0, 0)
}

func newProvisionRecoveryDriver(t *testing.T, engine *inventoryEngine, input ports.AgentWorkspacePlan) (Config, *Driver) {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, input.Workspace.ID().String(), "merged"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{
		Build:  BuildConfig{WorkspaceRoot: root, ImageRepository: "example.invalid/agent", ContainerUser: testGuestUser(t)},
		Engine: engine,
	}
	return config, requireDockerDriver(t, config)
}

func requireDockerDriver(t *testing.T, config Config) *Driver {
	t.Helper()
	driver, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func requireProvisionCrash(t *testing.T, driver *Driver, input ports.AgentWorkspacePlan) {
	t.Helper()
	crashed := false
	func() {
		defer func() { crashed = recover() != nil }()
		_, _ = driver.Provision(testDeadline(t), input)
	}()
	if !crashed {
		t.Fatal("provisioning did not reach the injected crash point")
	}
}

func assertProvisionEngineCounts(t *testing.T, engine *inventoryEngine, created, started, opened, removed int) {
	t.Helper()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.created != created || engine.started != started || engine.opened != opened || engine.removed != removed {
		t.Fatalf("engine calls create/start/readiness/remove = %d/%d/%d/%d, want %d/%d/%d/%d", engine.created, engine.started, engine.opened, engine.removed, created, started, opened, removed)
	}
}

func assertReadinessProvenReconciliation(t *testing.T, driver *Driver, input ports.AgentWorkspacePlan) {
	t.Helper()
	report, err := driver.ReconcileAgentWorkspaces(testDeadline(t), ports.AgentWorkspaceReconciliationRequest{Active: []ports.AgentWorkspacePlan{input}})
	if err != nil || len(report.Expected) != 1 || report.Expected[0].Classification != ports.PhysicalResourceAdopted {
		t.Fatalf("readiness-proven reconciliation = %#v, %v", report, err)
	}
	status, err := driver.Inspect(testDeadline(t), report.Expected[0].Ref)
	if err != nil || !status.Ready || status.GuestProtocol != uint32(transport.ProtocolVersion) {
		t.Fatalf("readiness-proven status = %#v, %v", status, err)
	}
}

func nonAuthoritativeReadinessFrames(t *testing.T) []transport.Frame {
	t.Helper()
	terminal, err := json.Marshal(transport.Terminal{ExitCode: 0, CleanupConfirmed: false})
	if err != nil {
		t.Fatal(err)
	}
	return []transport.Frame{{Sequence: 1, Kind: transport.KindTerminal, Data: terminal}}
}

type engineWithoutInventory struct{ Engine }

var _ Engine = engineWithoutInventory{}
