package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

type targetHandler func(context.Context, *world.Client, []string, io.Reader, io.Writer, io.Writer, worldcli.ConnectionConfig) error

var targetCommands = map[string]targetHandler{
	"exec":      targetExec,
	"shell":     targetShell,
	"push":      pushTargetFile,
	"pull":      pullTargetFile,
	"adb":       targetADBProxy,
	"adb-proxy": targetADBProxy,
}

type targetScope struct {
	target        string
	run           string
	mutationFlags *worldcli.MutationFlags
}

func addTargetScope(flags *flag.FlagSet) *targetScope {
	result := &targetScope{mutationFlags: worldcli.AddMutationFlags(flags, worldcli.Env("WORLD_POLICY_REFERENCE"))}
	flags.StringVar(&result.target, "target", worldcli.Env("WORLD_TARGET_ID"), "scoped target ID")
	flags.StringVar(&result.run, "run", worldcli.Env("WORLD_TARGET_RUN_ID"), "active target run ID")
	return result
}

func (scope *targetScope) validate() error {
	return worldcli.Require("target", scope.target, "run", scope.run)
}

func (scope *targetScope) mutation(timeout time.Duration) (*worldv1.MutationMetadata, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	return scope.mutationFlags.Metadata(timeout)
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		if exit, ok := err.(interface{ ExitCode() int }); ok {
			os.Exit(exit.ExitCode())
		}
		os.Exit(1)
	}
}

func run(arguments []string, stdin io.Reader, stdout, stderr io.Writer) error {
	configuration, command, commandArguments, err := worldcli.ParseGlobal("world-target", arguments, stderr)
	if err != nil {
		return err
	}
	handler, ok := targetCommands[command]
	if !ok {
		return worldcli.UsageError(fmt.Sprintf("unknown command %q (available: %s)", command, targetCommandList()))
	}
	client, err := worldcli.Dial(configuration)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := worldcli.Context(configuration)
	defer cancel()
	return handler(ctx, client, commandArguments, stdin, stdout, stderr, configuration)
}

func targetCommandList() string {
	names := make([]string, 0, len(targetCommands))
	for name := range targetCommands {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
