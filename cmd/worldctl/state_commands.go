package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type transitionOptions struct {
	revision uint64
	state    string
	mutation *mutationOptions
}

func addTransitionFlags(flags *flag.FlagSet) *transitionOptions {
	result := &transitionOptions{mutation: addMutationFlags(flags)}
	flags.Uint64Var(&result.revision, "revision", 0, "expected resource revision")
	flags.StringVar(&result.state, "state", "", "destination state")
	return result
}

func (options *transitionOptions) metadata(timeout time.Duration) (*worldv1.MutationMetadata, error) {
	if err := worldcli.Require("state", options.state); err != nil {
		return nil, err
	}
	return options.mutation.Metadata(timeout)
}

func getExec(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("get-exec", stderr)
	execID := flags.String("exec", "", "exec ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("exec", *execID); err != nil {
		return err
	}
	result, err := client.GetExec(ctx, &worldv1.GetExecRequest{ExecId: *execID})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func createExec(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	request, err := parseCreateExec(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.CreateExec(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseCreateExec(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.CreateExecRequest, error) {
	flags := worldcli.NewFlagSet("create-exec", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	kind := flags.String("kind", "provider", "exec kind")
	executable := flags.String("executable", "", "policy-approved provider executable")
	workingDirectory := flags.String("working-directory", "", "workspace-relative working directory")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.Require("lease", *lease, "kind", *kind, "executable", *executable); err != nil {
		return nil, err
	}
	if *workingDirectory != "" {
		clean, err := worldcli.WorkspacePath(*workingDirectory)
		if err != nil {
			return nil, err
		}
		*workingDirectory = clean
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.CreateExecRequest{
		Mutation:                          meta,
		LeaseId:                           *lease,
		Kind:                              *kind,
		ProviderExecutable:                *executable,
		Argv:                              flags.Args(),
		WorkspaceRelativeWorkingDirectory: *workingDirectory,
	}, nil
}

func transitionExec(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("transition-exec", stderr)
	execID := flags.String("exec", "", "exec ID")
	transition := addTransitionFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("exec", *execID); err != nil {
		return err
	}
	meta, err := transition.metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.TransitionExec(ctx, &worldv1.TransitionExecRequest{Mutation: meta, ExecId: *execID, ExpectedRevision: transition.revision, State: transition.state})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func finalizeExec(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("finalize-exec", stderr)
	execID := flags.String("exec", "", "exec ID")
	revision := flags.Uint64("revision", 0, "expected exec revision")
	state := flags.String("state", "", "terminal exec state")
	signal := flags.String("signal", "", "terminating signal")
	incidents := flags.String("incidents", "", "comma-separated incident IDs")
	cleanup := flags.Bool("cleanup-confirmed", false, "confirm exec cleanup")
	errorText := flags.String("error", "", "exec error detail")
	var exitCode optionalInt32
	flags.Var(&exitCode, "exit-code", "optional process exit code")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("exec", *execID, "state", *state); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.FinalizeExec(ctx, &worldv1.FinalizeExecRequest{
		Mutation:         meta,
		ExecId:           *execID,
		ExpectedRevision: *revision,
		State:            *state,
		ExitCode:         exitCode.pointer(),
		Signal:           *signal,
		IncidentIds:      worldcli.CSV(*incidents),
		CleanupConfirmed: *cleanup,
		Error:            *errorText,
	})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func transitionAgentGeneration(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("transition-agent-generation", stderr)
	workspace := flags.String("workspace", "", "agent workspace ID")
	generation := flags.Uint64("generation", 0, "agent generation")
	transition := addTransitionFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("workspace", *workspace); err != nil {
		return err
	}
	meta, err := transition.metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.TransitionAgentGeneration(ctx, &worldv1.TransitionAgentGenerationRequest{Mutation: meta, AgentWorkspaceId: *workspace, Generation: *generation, ExpectedRevision: transition.revision, State: transition.state})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func transitionTargetGeneration(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("transition-target-generation", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	generation := flags.Uint64("generation", 0, "target generation")
	transition := addTransitionFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target); err != nil {
		return err
	}
	meta, err := transition.metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.TransitionTargetGeneration(ctx, &worldv1.TransitionTargetGenerationRequest{Mutation: meta, TargetId: *target, Generation: *generation, ExpectedRevision: transition.revision, State: transition.state})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func transitionTargetRun(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("transition-run", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	run := flags.String("run", defaultEnv("WORLD_TARGET_RUN_ID"), "target run ID")
	transition := addTransitionFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target, "run", *run); err != nil {
		return err
	}
	meta, err := transition.metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.TransitionTargetRun(ctx, &worldv1.TransitionTargetRunRequest{Mutation: meta, TargetId: *target, TargetRunId: *run, ExpectedRevision: transition.revision, State: transition.state})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func createTargetOperation(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("create-operation", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	run := flags.String("run", defaultEnv("WORLD_TARGET_RUN_ID"), "target run ID")
	kind := flags.String("kind", "", "operation kind")
	display := flags.String("command-display", "", "bounded/redacted command display")
	digest := flags.String("content-digest", "", "operation content digest")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target, "run", *run, "kind", *kind); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.CreateTargetOperation(ctx, &worldv1.CreateTargetOperationRequest{Mutation: meta, TargetId: *target, TargetRunId: *run, Kind: *kind, CommandDisplay: *display, ContentDigest: *digest})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func transitionTargetOperation(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("transition-operation", stderr)
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	operation := flags.String("operation", "", "target operation ID")
	transition := addTransitionFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("target", *target, "operation", *operation); err != nil {
		return err
	}
	meta, err := transition.metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.TransitionTargetOperation(ctx, &worldv1.TransitionTargetOperationRequest{Mutation: meta, TargetId: *target, TargetOperationId: *operation, ExpectedRevision: transition.revision, State: transition.state})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func getIncident(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("get-incident", stderr)
	incident := flags.String("incident", "", "incident ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("incident", *incident); err != nil {
		return err
	}
	result, err := client.GetIncident(ctx, &worldv1.GetIncidentRequest{IncidentId: *incident})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func createIncident(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	request, err := parseCreateIncident(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.CreateIncident(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseCreateIncident(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.CreateIncidentRequest, error) {
	flags := worldcli.NewFlagSet("create-incident", stderr)
	classification := flags.String("classification", "", "incident classification")
	session := flags.String("session", defaultEnv("WORLD_SESSION_ID"), "research session ID")
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	workspace := flags.String("workspace", "", "agent workspace ID")
	agentGeneration := flags.Uint64("agent-generation", 0, "agent generation")
	execID := flags.String("exec", "", "exec ID")
	target := flags.String("target", defaultEnv("WORLD_TARGET_ID"), "target ID")
	targetGeneration := flags.Uint64("target-generation", 0, "target generation")
	run := flags.String("run", defaultEnv("WORLD_TARGET_RUN_ID"), "target run ID")
	trigger := flags.String("trigger", "", "trigger description")
	lastState := flags.String("last-state", "", "last known resource state")
	causeKind := flags.String("cause-kind", "unknown", "proven, correlated, or unknown")
	causeSummary := flags.String("cause-summary", "cause is not yet established", "cause summary")
	causeMethod := flags.String("cause-method", "", "cause determination method")
	causeConfidence := flags.Float64("cause-confidence", 0, "cause confidence from 0 to 1")
	firstCursor := flags.Uint64("first-cursor", 0, "first relevant observation cursor")
	lastCursor := flags.Uint64("last-cursor", 0, "last relevant observation cursor")
	observationBundle := flags.String("observation-bundle", "", "sealed observation bundle ID")
	var artifactJSON worldcli.StringValues
	flags.Var(&artifactJSON, "artifact", "repeatable ArtifactReference JSON object")
	artifactsFile := flags.String("artifacts-file", "", "JSON file containing an array of ArtifactReference objects")
	metricsFile := flags.String("high-water-metrics-file", "", "JSON file containing an array of IncidentMetric objects")
	coverageFile := flags.String("coverage-file", "", "JSON file containing an array of IncidentCoverage objects")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := worldcli.Require("classification", *classification, "session", *session, "trigger", *trigger, "last-state", *lastState, "cause-kind", *causeKind, "cause-summary", *causeSummary); err != nil {
		return nil, err
	}
	if *causeConfidence < 0 || *causeConfidence > 1 {
		return nil, worldcli.UsageError("cause-confidence must be between 0 and 1")
	}
	artifacts, err := parseIncidentArtifacts(artifactJSON, *artifactsFile)
	if err != nil {
		return nil, err
	}
	metrics, err := readProtoJSONArray(*metricsFile, func() *worldv1.IncidentMetric {
		return &worldv1.IncidentMetric{}
	})
	if err != nil {
		return nil, fmt.Errorf("high-water metrics: %w", err)
	}
	coverage, err := readProtoJSONArray(*coverageFile, func() *worldv1.IncidentCoverage {
		return &worldv1.IncidentCoverage{}
	})
	if err != nil {
		return nil, fmt.Errorf("coverage: %w", err)
	}
	meta, err := mutation.Metadata(timeout)
	if err != nil {
		return nil, err
	}
	return &worldv1.CreateIncidentRequest{
		Mutation: meta, Classification: *classification, ResearchSessionId: *session, LeaseId: *lease,
		AgentWorkspaceId: *workspace, AgentGeneration: *agentGeneration, ExecId: *execID,
		TargetId: *target, TargetGeneration: *targetGeneration, TargetRunId: *run,
		Trigger: *trigger, LastKnownState: *lastState,
		Cause:            &worldv1.Cause{Kind: *causeKind, Summary: *causeSummary, Method: *causeMethod, Confidence: *causeConfidence},
		HighWaterMetrics: metrics, FirstRelevantCursor: *firstCursor, LastRelevantCursor: *lastCursor,
		Coverage: coverage, ObservationBundleId: *observationBundle, Artifacts: artifacts,
	}, nil
}

func parseIncidentArtifacts(values []string, file string) ([]*worldv1.ArtifactReference, error) {
	result, err := readProtoJSONArray(file, func() *worldv1.ArtifactReference {
		return &worldv1.ArtifactReference{}
	})
	if err != nil {
		return nil, fmt.Errorf("artifacts: %w", err)
	}
	for _, value := range values {
		artifact, err := decodeProtoJSON([]byte(value), &worldv1.ArtifactReference{})
		if err != nil {
			return nil, fmt.Errorf("artifact: %w", err)
		}
		result = append(result, artifact)
	}
	for index, artifact := range result {
		if artifact == nil {
			return nil, fmt.Errorf("artifact %d must not be null", index+1)
		}
		if err := worldcli.Require("artifact reference", artifact.Reference, "artifact digest", artifact.Digest, "artifact role", artifact.Role, "artifact sensitivity", artifact.Sensitivity); err != nil {
			return nil, fmt.Errorf("artifact %d: %w", index+1, err)
		}
	}
	return result, nil
}

func readProtoJSONArray[Message proto.Message](path string, newMessage func() Message) ([]Message, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		return nil, fmt.Errorf("protobuf message list must be a JSON array, not null")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	var elements []json.RawMessage
	if err := decoder.Decode(&elements); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing JSON value")
		}
		return nil, err
	}
	result := make([]Message, 0, len(elements))
	for index, element := range elements {
		message, err := decodeProtoJSON(element, newMessage())
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", index+1, err)
		}
		result = append(result, message)
	}
	return result, nil
}

func decodeProtoJSON[Message proto.Message](payload []byte, message Message) (Message, error) {
	if !message.ProtoReflect().IsValid() {
		return message, fmt.Errorf("protobuf destination is nil")
	}
	if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		return message, fmt.Errorf("protobuf message must not be null")
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, message); err != nil {
		return message, err
	}
	return message, nil
}

func transitionIncident(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("transition-incident", stderr)
	incident := flags.String("incident", "", "incident ID")
	recoveryActions := flags.String("recovery-actions", "", "comma-separated recovery actions")
	acknowledgements := flags.String("visibility-acknowledgements", "", "comma-separated visibility acknowledgements")
	transition := addTransitionFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("incident", *incident); err != nil {
		return err
	}
	meta, err := transition.metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.TransitionIncident(ctx, &worldv1.TransitionIncidentRequest{
		Mutation: meta, IncidentId: *incident, ExpectedRevision: transition.revision, State: transition.state,
		RecoveryActions: worldcli.CSV(*recoveryActions), VisibilityAcknowledgements: worldcli.CSV(*acknowledgements),
	})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}
