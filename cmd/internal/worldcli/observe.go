package worldcli

import (
	"context"
	"io"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/world"
)

func ParseObservationFilter(name string, arguments []string, stderr io.Writer, defaultLease string) (*worldv1.ObservationFilter, error) {
	flags := NewFlagSet(name, stderr)
	var observation ObservationFlags
	AddObservationFlags(flags, &observation, defaultLease)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := Require("lease", observation.Lease); err != nil {
		return nil, err
	}
	return observation.Filter(), nil
}

func RunSnapshot(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, defaultLease string, table bool) error {
	name := "snapshot"
	if table {
		name = "top"
	}
	filter, err := ParseObservationFilter(name, arguments, stderr, defaultLease)
	if err != nil {
		return err
	}
	result, err := client.GetLiveSnapshot(ctx, &worldv1.GetLiveSnapshotRequest{Filter: filter})
	if err != nil {
		return err
	}
	if table {
		return WriteTop(stdout, result)
	}
	return Encoder(stdout).Encode(result)
}

func RunObservationWatch(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, defaultLease string) error {
	flags := NewFlagSet("watch", stderr)
	var observation ObservationFlags
	AddObservationFlags(flags, &observation, defaultLease)
	after := flags.Uint64("after", 0, "resume after durable cursor")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := RequireNoArgs(flags); err != nil {
		return err
	}
	if err := Require("lease", observation.Lease); err != nil {
		return err
	}
	stream, err := client.SubscribeObservations(ctx, &worldv1.SubscribeObservationsRequest{Filter: observation.Filter(), AfterCursor: *after})
	if err != nil {
		return err
	}
	return EncodeStream(Encoder(stdout), stream.Recv)
}

func RunMetricWatch(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, defaultLease string) error {
	flags := NewFlagSet("metrics", stderr)
	var observation ObservationFlags
	AddObservationFlags(flags, &observation, defaultLease)
	after := flags.Uint64("after", 0, "resume after durable cursor")
	resolution := flags.Duration("resolution", time.Second, "metric sample resolution")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := RequireNoArgs(flags); err != nil {
		return err
	}
	if err := Require("lease", observation.Lease); err != nil {
		return err
	}
	if *resolution <= 0 {
		return UsageError("resolution must be positive")
	}
	wireResolution, err := Duration(*resolution)
	if err != nil {
		return err
	}
	stream, err := client.SubscribeMetrics(ctx, &worldv1.SubscribeMetricsRequest{Filter: observation.Filter(), Resolution: wireResolution, AfterCursor: *after})
	if err != nil {
		return err
	}
	return EncodeStream(Encoder(stdout), stream.Recv)
}

func RunBundle(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, defaultRun string) error {
	flags := NewFlagSet("bundle", stderr)
	run := flags.String("run", defaultRun, "target run ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if err := RequireNoArgs(flags); err != nil {
		return err
	}
	if err := Require("run", *run); err != nil {
		return err
	}
	result, err := client.GetObservationBundle(ctx, &worldv1.GetObservationBundleRequest{TargetRunId: *run})
	return EncodeResult(Encoder(stdout), result, err)
}
