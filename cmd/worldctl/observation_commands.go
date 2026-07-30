package main

import (
	"context"
	"io"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

func snapshot(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	return worldcli.RunSnapshot(ctx, manager, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"), false)
}

func top(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	return worldcli.RunSnapshot(ctx, manager, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"), true)
}

func watch(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	return worldcli.RunObservationWatch(ctx, manager, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"))
}

func metrics(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	return worldcli.RunMetricWatch(ctx, manager, arguments, stdout, stderr, defaultEnv("WORLD_LEASE_ID"))
}

func bundle(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer, _ worldcli.OpenConfig) error {
	return worldcli.RunBundle(ctx, manager, arguments, stdout, stderr, defaultEnv("WORLD_TARGET_RUN_ID"))
}
