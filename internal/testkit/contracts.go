package testkit

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const contractTimeout = 5 * time.Second

type AgentWorkspaceDriverContract struct {
	Driver  ports.AgentWorkspaceDriver
	Plan    ports.AgentWorkspacePlan
	Exec    ports.ExecPlan
	Tracker *OwnershipTracker
}

func RunAgentWorkspaceDriverContract(t *testing.T, contract AgentWorkspaceDriverContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Driver.Provision(ctx, contract.Plan)
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	first, err := contract.Driver.Provision(ctx, contract.Plan)
	if err != nil || !first.Status.Ready || !first.Created {
		t.Fatalf("Provision() = %#v, %v", first, err)
	}
	replay, err := contract.Driver.Provision(ctx, contract.Plan)
	if err != nil || replay.Status.AgentWorkspaceID != first.Status.AgentWorkspaceID || replay.Status.Generation != first.Status.Generation {
		t.Fatalf("Provision(replay) = %#v, %v", replay, err)
	}
	if !contract.Exec.Exec.ID().IsZero() {
		exec, err := contract.Driver.OpenExec(ctx, contract.Exec)
		if err != nil {
			t.Fatalf("OpenExec() error = %v", err)
		}
		if err := exec.Close(); err != nil {
			t.Fatalf("Exec.Close() error = %v", err)
		}
	}
	ref := ports.AgentWorkspaceRef{ID: first.Status.AgentWorkspaceID, Generation: first.Status.Generation}
	if _, err := contract.Driver.Inspect(ctx, ref); err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if err := contract.Driver.Stop(ctx, ref, ports.StopGraceful); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := contract.Driver.Stop(ctx, ref, ports.StopGraceful); err != nil {
		t.Fatalf("Stop(replay) error = %v", err)
	}
	if err := contract.Driver.Destroy(ctx, ref); err != nil {
		t.Fatalf("Destroy() error = %v", err)
	}
	if err := contract.Driver.Destroy(ctx, ref); err != nil {
		t.Fatalf("Destroy(replay) error = %v", err)
	}
	requireNoContractLeaks(t, contract.Tracker)
}

type TargetDriverContract struct {
	Driver  ports.TargetDriver
	Create  ports.TargetPlan
	Run     ports.TargetRunPlan
	Tracker *OwnershipTracker
}

func RunTargetDriverContract(t *testing.T, contract TargetDriverContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Driver.Create(ctx, contract.Create)
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	created, err := contract.Driver.Create(ctx, contract.Create)
	if err != nil || !created.Created || !created.Status.Ready {
		t.Fatalf("Create() = %#v, %v", created, err)
	}
	replay, err := contract.Driver.Create(ctx, contract.Create)
	if err != nil || replay.Status.TargetID != created.Status.TargetID || replay.Status.Generation != created.Status.Generation {
		t.Fatalf("Create(replay) = %#v, %v", replay, err)
	}
	prepared, err := contract.Driver.PrepareRun(ctx, contract.Run)
	if err != nil {
		t.Fatalf("PrepareRun() error = %v", err)
	}
	preparedReplay, err := contract.Driver.PrepareRun(ctx, contract.Run)
	if err != nil || preparedReplay.RunID != prepared.RunID {
		t.Fatalf("PrepareRun(replay) = %#v, %v", preparedReplay, err)
	}
	if err := contract.Driver.StartRun(ctx, prepared.RunID); err != nil {
		t.Fatalf("StartRun() error = %v", err)
	}
	if err := contract.Driver.StartRun(ctx, prepared.RunID); err != nil {
		t.Fatalf("StartRun(replay) error = %v", err)
	}
	transport, err := contract.Driver.OpenTransport(ctx, prepared.RunID)
	if err != nil {
		t.Fatalf("OpenTransport() error = %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("TargetTransport.Close() error = %v", err)
	}
	result, err := contract.Driver.StopRun(ctx, prepared.RunID, ports.StopGraceful)
	if err != nil || result.RunID != prepared.RunID || !result.Outcome.IsValid() {
		t.Fatalf("StopRun() = %#v, %v", result, err)
	}
	replayedResult, err := contract.Driver.StopRun(ctx, prepared.RunID, ports.StopGraceful)
	if err != nil || replayedResult.RunID != result.RunID || replayedResult.StoppedAt != result.StoppedAt {
		t.Fatalf("StopRun(replay) = %#v, %v", replayedResult, err)
	}
	ref := ports.TargetRef{ID: created.Status.TargetID, Generation: created.Status.Generation}
	quarantine := ports.TargetQuarantinePlan{IdempotencyKey: domain.DeriveIdempotencyKey(contract.Create.IdempotencyKey, "quarantine"), Target: ref, Reason: "contract containment"}
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Driver.Quarantine(ctx, quarantine)
		return err
	})
	evidence, err := contract.Driver.Quarantine(ctx, quarantine)
	if err != nil || evidence.Validate(ref) != nil {
		t.Fatalf("Quarantine() = %#v, %v", evidence, err)
	}
	replayedEvidence, err := contract.Driver.Quarantine(ctx, quarantine)
	if err != nil || replayedEvidence != evidence {
		t.Fatalf("Quarantine(replay) = %#v, %v; want %#v", replayedEvidence, err, evidence)
	}
	if err := contract.Driver.Destroy(ctx, ref); err != nil {
		t.Fatalf("Destroy() error = %v", err)
	}
	if err := contract.Driver.Destroy(ctx, ref); err != nil {
		t.Fatalf("Destroy(replay) error = %v", err)
	}
	requireNoContractLeaks(t, contract.Tracker)
}

type ObserverDriverContract struct {
	Driver      ports.ObserverDriver
	Requirement ports.ObservationRequirement
	Plan        ports.CollectorPlan
	Tracker     *OwnershipTracker
}

func RunObserverDriverContract(t *testing.T, contract ObserverDriverContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Driver.Start(ctx, contract.Plan)
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	if _, err := contract.Driver.Probe(ctx, contract.Requirement); err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	collector, err := contract.Driver.Start(ctx, contract.Plan)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	replay, err := contract.Driver.Start(ctx, contract.Plan)
	if err != nil || replay.ID != collector.ID {
		t.Fatalf("Start(replay) = %#v, %v", replay, err)
	}
	if _, err := contract.Driver.Coverage(ctx, collector.ID); err != nil {
		t.Fatalf("Coverage() error = %v", err)
	}
	result, err := contract.Driver.Stop(ctx, collector.ID)
	if err != nil || !result.TeardownConfirmed {
		t.Fatalf("Stop() = %#v, %v", result, err)
	}
	replayedResult, err := contract.Driver.Stop(ctx, collector.ID)
	if err != nil || replayedResult.StoppedAt != result.StoppedAt {
		t.Fatalf("Stop(replay) = %#v, %v", replayedResult, err)
	}
	requireNoContractLeaks(t, contract.Tracker)
}

type WorkspaceDriverContract struct {
	Driver  ports.WorkspaceDriver
	Plan    ports.WorkspacePlan
	Tracker *OwnershipTracker
}

func RunWorkspaceDriverContract(t *testing.T, contract WorkspaceDriverContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Driver.Prepare(ctx, contract.Plan)
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	prepared, err := contract.Driver.Prepare(ctx, contract.Plan)
	if err != nil || prepared.State != domain.WorkspaceReady {
		t.Fatalf("Prepare() = %#v, %v", prepared, err)
	}
	if _, err := contract.Driver.Prepare(ctx, contract.Plan); err != nil {
		t.Fatalf("Prepare(replay) error = %v", err)
	}
	if _, err := contract.Driver.Mount(ctx, prepared.WorkspaceID); err != nil {
		t.Fatalf("Mount() error = %v", err)
	}
	if _, err := contract.Driver.Mount(ctx, prepared.WorkspaceID); err != nil {
		t.Fatalf("Mount(replay) error = %v", err)
	}
	if _, err := contract.Driver.Inspect(ctx, prepared.WorkspaceID); err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	preview, err := contract.Driver.Preview(ctx, prepared.WorkspaceID)
	if err != nil || !preview.ChangeSet.WorkspaceRevision().IsValid() {
		t.Fatalf("Preview() = %#v, %v", preview, err)
	}
	replayedPreview, err := contract.Driver.Preview(ctx, prepared.WorkspaceID)
	if err != nil || replayedPreview.ChangeSet.WorkspaceRevision() != preview.ChangeSet.WorkspaceRevision() || replayedPreview.ObservedAt != preview.ObservedAt {
		t.Fatalf("Preview(replay) = %#v, %v", replayedPreview, err)
	}
	sealed, err := contract.Driver.Seal(ctx, prepared.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	replayedSeal, err := contract.Driver.Seal(ctx, prepared.WorkspaceID, preview.ChangeSet.WorkspaceRevision())
	if err != nil || replayedSeal.SealedAt != sealed.SealedAt {
		t.Fatalf("Seal(replay) = %#v, %v", replayedSeal, err)
	}
	if err := contract.Driver.Release(ctx, prepared.WorkspaceID); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := contract.Driver.Release(ctx, prepared.WorkspaceID); err != nil {
		t.Fatalf("Release(replay) error = %v", err)
	}
	requireNoContractLeaks(t, contract.Tracker)
}

type InputCacheContract struct {
	Cache       ports.InputCache
	ContentPlan func() ports.CacheContentPlan
	BuildPlan   ports.InputViewBuildPlan
	Pin         ports.CachePin
	Tracker     *OwnershipTracker
}

func RunInputCacheContract(t *testing.T, contract InputCacheContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Cache.EnsureContent(ctx, contract.ContentPlan())
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	if _, err := contract.Cache.EnsureContent(ctx, contract.ContentPlan()); err != nil {
		t.Fatalf("EnsureContent() error = %v", err)
	}
	if _, err := contract.Cache.EnsureContent(ctx, contract.ContentPlan()); err != nil {
		t.Fatalf("EnsureContent(replay) error = %v", err)
	}
	view, err := contract.Cache.BuildView(ctx, contract.BuildPlan)
	if err != nil || view.InputViewID != contract.BuildPlan.Manifest.ID() {
		t.Fatalf("BuildView() = %#v, %v", view, err)
	}
	if _, err := contract.Cache.BuildView(ctx, contract.BuildPlan); err != nil {
		t.Fatalf("BuildView(replay) error = %v", err)
	}
	if err := contract.Cache.Pin(ctx, contract.Pin); err != nil {
		t.Fatalf("Pin() error = %v", err)
	}
	if err := contract.Cache.Pin(ctx, contract.Pin); err != nil {
		t.Fatalf("Pin(replay) error = %v", err)
	}
	if _, err := contract.Cache.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if err := contract.Cache.Unpin(ctx, contract.Pin); err != nil {
		t.Fatalf("Unpin() error = %v", err)
	}
	if err := contract.Cache.Unpin(ctx, contract.Pin); err != nil {
		t.Fatalf("Unpin(replay) error = %v", err)
	}
	requireNoContractLeaks(t, contract.Tracker)
}

type MaterialAuthorityContract struct {
	Authority  ports.MaterialAuthority
	Input      ports.InputPlan
	Occurrence ports.ArtifactOccurrence
	Output     ports.OutputPlan
}

func RunMaterialAuthorityContract(t *testing.T, contract MaterialAuthorityContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Authority.ResolveInputView(ctx, contract.Input)
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	resolved, err := contract.Authority.ResolveOccurrence(ctx, contract.Input.SecurityScope, contract.Occurrence.Reference)
	if err != nil {
		t.Fatalf("ResolveOccurrence() error = %v", err)
	}
	if resolved != contract.Occurrence {
		t.Fatalf("ResolveOccurrence() = %#v, want %#v", resolved, contract.Occurrence)
	}
	first, err := contract.Authority.ResolveInputView(ctx, contract.Input)
	if err != nil {
		t.Fatalf("ResolveInputView() error = %v", err)
	}
	replay, err := contract.Authority.ResolveInputView(ctx, contract.Input)
	if err != nil || replay.ID() != first.ID() {
		t.Fatalf("ResolveInputView(replay) = %#v, %v", replay, err)
	}
	reader, err := contract.Authority.OpenContent(ctx, contract.Occurrence)
	if err != nil {
		t.Fatalf("OpenContent() error = %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("content read error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("content close error = %v", err)
	}
	if len(contract.Output.Selections) > 0 {
		firstOutputs, err := contract.Authority.CaptureOutputs(ctx, contract.Output)
		if err != nil || len(firstOutputs) == 0 {
			t.Fatalf("CaptureOutputs() = %#v, %v", firstOutputs, err)
		}
		replayedOutputs, err := contract.Authority.CaptureOutputs(ctx, contract.Output)
		if err != nil || len(replayedOutputs) != len(firstOutputs) {
			t.Fatalf("CaptureOutputs(replay) = %#v, %v", replayedOutputs, err)
		}
	}
}

type ResourceControllerContract struct {
	Controller ports.ResourceController
	Reserve    ports.ResourcePlan
	Apply      ports.ResourcePlan
	Tracker    *OwnershipTracker
}

func RunResourceControllerContract(t *testing.T, contract ResourceControllerContract) {
	t.Helper()
	requireDeadlineRejected(t, func(ctx context.Context) error {
		_, err := contract.Controller.Reserve(ctx, contract.Reserve)
		return err
	})
	ctx, cancel := contractContext(t)
	defer cancel()
	if _, err := contract.Controller.Reserve(ctx, contract.Reserve); err != nil {
		t.Fatalf("Reserve() error = %v", err)
	}
	if _, err := contract.Controller.Reserve(ctx, contract.Reserve); err != nil {
		t.Fatalf("Reserve(replay) error = %v", err)
	}
	if _, err := contract.Controller.Apply(ctx, contract.Apply); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if _, err := contract.Controller.Apply(ctx, contract.Apply); err != nil {
		t.Fatalf("Apply(replay) error = %v", err)
	}
	if _, err := contract.Controller.Inspect(ctx, contract.Apply.OwnerID); err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if err := contract.Controller.Release(ctx, contract.Apply.OwnerID); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := contract.Controller.Release(ctx, contract.Apply.OwnerID); err != nil {
		t.Fatalf("Release(replay) error = %v", err)
	}
	requireNoContractLeaks(t, contract.Tracker)
}

func contractContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), contractTimeout)
}

func requireDeadlineRejected(t *testing.T, call func(context.Context) error) {
	t.Helper()
	if err := call(context.Background()); !domain.IsCode(err, domain.CodeInvalidArgument) {
		t.Fatalf("call without deadline error = %v, want invalid_argument", err)
	}
}

func requireNoContractLeaks(t *testing.T, tracker *OwnershipTracker) {
	t.Helper()
	if tracker != nil {
		if err := tracker.RequireNoLeaks(); err != nil {
			t.Fatal(err)
		}
	}
}
