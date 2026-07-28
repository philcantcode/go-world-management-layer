package main

import (
	"context"
	"io"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

func startCapture(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("start-capture", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	profile := flags.String("profile", "", "capture profile")
	signals := flags.String("signals", "", "comma-separated signal families")
	duration := flags.Duration("duration", time.Minute, "bounded capture duration")
	byteLimit := flags.Uint64("byte-limit", 64<<20, "maximum captured bytes")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("lease", *lease, "profile", *profile); err != nil {
		return err
	}
	if *duration <= 0 || *byteLimit == 0 {
		return worldcli.UsageError("duration and byte-limit must be positive")
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	wireDuration, err := worldcli.Duration(*duration)
	if err != nil {
		return err
	}
	result, err := client.StartCapture(ctx, &worldv1.StartCaptureRequest{
		Mutation: meta, LeaseId: *lease,
		CaptureSpec: &worldv1.CaptureSpec{Profile: *profile, SignalFamilies: worldcli.CSV(*signals), Duration: wireDuration, ByteLimit: *byteLimit},
	})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func requestCapture(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	request, err := worldcli.ParseRequestCapture(arguments, stderr, configuration.Timeout, defaultEnv("WORLD_LEASE_ID"), defaultEnv("WORLD_POLICY_REFERENCE"))
	if err != nil {
		return err
	}
	result, err := client.RequestCapture(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func stopCapture(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("stop-capture", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	capture := flags.String("capture", "", "capture ID")
	revision := flags.Uint64("revision", 0, "expected capture revision")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("lease", *lease, "capture", *capture); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.StopCapture(ctx, &worldv1.StopCaptureRequest{Mutation: meta, LeaseId: *lease, CaptureId: *capture, ExpectedRevision: *revision})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func declareExport(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	request, err := parseDeclareExport(arguments, stderr, configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.DeclareExport(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func parseDeclareExport(arguments []string, stderr io.Writer, timeout time.Duration) (*worldv1.DeclareExportRequest, error) {
	return worldcli.ParseDeclareExport(arguments, stderr, timeout, defaultEnv("WORLD_LEASE_ID"), defaultEnv("WORLD_POLICY_REFERENCE"))
}

func previewExport(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("preview-export", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("lease", *lease); err != nil {
		return err
	}
	result, err := client.PreviewChangeSet(ctx, &worldv1.PreviewChangeSetRequest{LeaseId: *lease})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}

func commitExport(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	flags := worldcli.NewFlagSet("commit-export", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	exportID := flags.String("export", "", "export ID")
	revision := flags.Uint64("workspace-revision", 0, "expected workspace revision")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return err
	}
	if err := worldcli.Require("lease", *lease, "export", *exportID); err != nil {
		return err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return err
	}
	result, err := client.CommitExport(ctx, &worldv1.CommitExportRequest{Mutation: meta, LeaseId: *lease, ExportId: *exportID, ExpectedWorkspaceRevision: *revision})
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}
