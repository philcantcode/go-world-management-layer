package main

import (
	"context"
	"io"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

func snapshot(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	return worldcli.RunSnapshot(ctx, client, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"), false)
}

func top(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	return worldcli.RunSnapshot(ctx, client, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"), true)
}

func watch(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	return worldcli.RunObservationWatch(ctx, client, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"))
}

func metrics(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	return worldcli.RunMetricWatch(ctx, client, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"))
}

func bundle(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, _ worldcli.ConnectionConfig) error {
	return worldcli.RunBundle(ctx, client, arguments, stdout, stderr, defaultEnv("WORLD_TARGET_RUN_ID"))
}
