package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

type observeHandler func(context.Context, *world.Manager, []string, io.Writer, io.Writer) error

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string, stdout, stderr io.Writer) error {
	configuration, command, commandArguments, err := worldcli.ParseGlobal("world-observe", arguments, stderr)
	if err != nil {
		return err
	}
	handler, ok := observeCommands()[command]
	if !ok {
		return worldcli.UsageError(fmt.Sprintf("unknown command %q (available: %s)", command, observeCommandList()))
	}
	manager, err := worldcli.Open(configuration)
	if err != nil {
		return err
	}
	defer manager.Close()
	ctx, cancel := worldcli.Context(configuration)
	defer cancel()
	return handler(ctx, manager, commandArguments, stdout, stderr)
}

func observeCommands() map[string]observeHandler {
	lease := worldcli.Env("WORLD_LEASE_ID")
	run := worldcli.Env("WORLD_TARGET_RUN_ID")
	return map[string]observeHandler{
		"snapshot": func(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer) error {
			return worldcli.RunSnapshot(ctx, manager, arguments, stdout, stderr, lease, false)
		},
		"top": func(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer) error {
			return worldcli.RunSnapshot(ctx, manager, arguments, stdout, stderr, lease, true)
		},
		"watch": func(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer) error {
			return worldcli.RunObservationWatch(ctx, manager, arguments, stdout, stderr, lease)
		},
		"metrics": func(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer) error {
			return worldcli.RunMetricWatch(ctx, manager, arguments, stdout, stderr, lease)
		},
		"bundle": func(ctx context.Context, manager *world.Manager, arguments []string, stdout, stderr io.Writer) error {
			return worldcli.RunBundle(ctx, manager, arguments, stdout, stderr, run)
		},
	}
}

func observeCommandList() string {
	names := make([]string, 0, len(observeCommands()))
	for name := range observeCommands() {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
