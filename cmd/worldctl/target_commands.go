package main

import (
	"context"
	"io"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/world"
)

func createTarget(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	request, err := parseCreateTarget(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.CreateTarget(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseCreateTarget(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.CreateTargetRequest, error) {
	flags := worldcli.NewFlagSet("create-target", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	template := flags.String("template", "", "policy target template reference")
	kind := flags.String("kind", "", "linux_container, android_virtual_device, or physical_device")
	mutation := worldcli.AddMutationFlags(flags, defaultEnv("WORLD_POLICY_REFERENCE"))
	policy := &mutation.Policy
	capabilities := flags.String("capabilities", "", "sha256 capability digest")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("lease", *lease, "template", *template, "kind", *kind, "policy", *policy, "capabilities", *capabilities); err != nil {
		return nil, err
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.CreateTargetRequest{
		Mutation: meta,
		LeaseId:  *lease,
		Template: &worldv1.TargetTemplate{Reference: *template, Kind: *kind, PolicyDigest: *policy, CapabilityDigest: *capabilities},
	}, nil
}

func getTarget(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	flags := worldcli.NewFlagSet("get-target", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target); err != nil {
		return err
	}
	result, err := manager.GetTarget(ctx, &worldv1.GetTargetRequest{TargetId: *target})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func startRun(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	request, err := parseStartRun(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.StartTargetRun(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseStartRun(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.StartTargetRunRequest, error) {
	flags := worldcli.NewFlagSet("start-run", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	digest := flags.String("materialization", "", "resolved sha256 materialization digest")
	specimens := flags.String("specimens", "", "comma-separated specimen occurrence references")
	fixtures := flags.String("fixtures", "", "comma-separated fixture references")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("target", *target); err != nil {
		return nil, err
	}
	resolved := strings.TrimSpace(*digest) != ""
	unresolved := strings.TrimSpace(*specimens) != "" || strings.TrimSpace(*fixtures) != ""
	if !resolved && !unresolved {
		return nil, worldcli.UsageError("materialization or specimen/fixture references are required")
	}
	if resolved && unresolved {
		return nil, worldcli.UsageError("materialization and unresolved material references are mutually exclusive")
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.StartTargetRunRequest{
		Mutation: meta,
		TargetId: *target,
		RunSpec: &worldv1.TargetRunSpec{
			SpecimenOccurrenceRefs: worldcli.CSV(*specimens),
			FixtureRefs:            worldcli.CSV(*fixtures),
			MaterializationDigest:  *digest,
		},
	}, nil
}

func waitRun(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	flags := worldcli.NewFlagSet("wait-run", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	run := flags.String("run", defaultEnv("WORLD_TARGET_RUN_ID"), "target run ID")
	state := flags.String("state", "running", "desired target run state")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target, "run", *run, "state", *state); err != nil {
		return err
	}
	result, err := manager.WaitTargetRun(ctx, &worldv1.WaitTargetRunRequest{TargetId: *target, TargetRunId: *run, DesiredState: *state})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func stopRun(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	flags := worldcli.NewFlagSet("stop-run", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	run := flags.String("run", defaultEnv("WORLD_TARGET_RUN_ID"), "target run ID")
	revision := flags.Uint64("revision", 0, "expected target run revision")
	reason := flags.String("reason", "requested by operator", "stop reason")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target, "run", *run, "reason", *reason); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.StopTargetRun(ctx, &worldv1.StopTargetRunRequest{Mutation: meta, TargetId: *target, TargetRunId: *run, ExpectedRevision: *revision, Reason: *reason})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func resetTarget(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	request, err := parseResetTarget(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.ResetTarget(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseResetTarget(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.ResetTargetRequest, error) {
	flags := worldcli.NewFlagSet("reset", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	revision := flags.Uint64("revision", 0, "expected target revision")
	mode := flags.String("mode", string(ports.ResetRecreate), "reset mode: baseline, recreate, or snapshot")
	snapshotName := flags.String("snapshot-name", "", "snapshot selector (required only for snapshot mode)")
	recoveryIncident := flags.String("recovery-incident", "", "optional incident ID")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("target", *target, "mode", *mode); err != nil {
		return nil, err
	}
	selectedMode := ports.ResetMode(*mode)
	if err := ports.ValidateResetSelection(selectedMode, *snapshotName); err != nil {
		return nil, worldcli.UsageError(err.Error())
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.ResetTargetRequest{
		Mutation: meta, TargetId: *target, ExpectedRevision: *revision, ResetMode: string(selectedMode),
		RecoveryIncidentId: *recoveryIncident, SnapshotName: *snapshotName,
	}, nil
}

func destroyTarget(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	target, revision, reason, meta, err := parseTargetDisposition("destroy", arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.DestroyTarget(ctx, &worldv1.DestroyTargetRequest{Mutation: meta, TargetId: target, ExpectedRevision: revision, Reason: reason})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func quarantineTarget(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	target, revision, reason, meta, err := parseTargetDisposition("quarantine", arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.QuarantineTarget(ctx, &worldv1.QuarantineTargetRequest{Mutation: meta, TargetId: target, ExpectedRevision: revision, Reason: reason})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseTargetDisposition(name string, arguments []string, stderr io.Writer, timeout time.Duration) (string, uint64, string, *worldv1.MutationMetadata, error) {
	flags := worldcli.NewFlagSet(name, stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	revision := flags.Uint64("revision", 0, "expected target revision")
	reason := flags.String("reason", "requested by operator", name+" reason")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return "", 0, "", nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return "", 0, "", nil, err
	}
	if err := worldcli.Require("target", *target, "reason", *reason); err != nil {
		return "", 0, "", nil, err
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return "", 0, "", nil, err
	}
	return *target, *revision, *reason, meta, nil
}

func requestRecovery(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, configuration worldcli.OpenConfig) error {
	flags := worldcli.NewFlagSet("recovery", stderr)
	incident := flags.String("incident", "", "incident ID")
	revision := flags.Uint64("revision", 0, "expected incident revision")
	mode := flags.String("mode", "", "recovery mode")
	resource := flags.String("resource", "", "optional resource selector")
	strategy := flags.String("strategy", "", "optional recovery strategy")
	acknowledgement := flags.String("visibility-acknowledgement", "", "required visibility acknowledgement when applicable")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("incident", *incident, "mode", *mode); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := manager.RequestRecovery(ctx, &worldv1.RequestRecoveryRequest{
		Mutation:                  meta,
		IncidentId:                *incident,
		ExpectedRevision:          *revision,
		Mode:                      *mode,
		Resource:                  *resource,
		Strategy:                  *strategy,
		VisibilityAcknowledgement: *acknowledgement,
	})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}
