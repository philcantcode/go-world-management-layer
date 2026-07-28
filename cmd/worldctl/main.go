package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

type commandHandler func(context.Context, *world.Client, []string, io.Writer, io.Writer, worldcli.ConnectionConfig) error

var commands = map[string]commandHandler{
	"acquire":                      acquire,
	"get":                          getSession,
	"get-session":                  getSession,
	"wait":                         waitSession,
	"wait-session":                 waitSession,
	"renew":                        renewLease,
	"release":                      releaseSession,
	"create-target":                createTarget,
	"get-target":                   getTarget,
	"start-run":                    startRun,
	"wait-run":                     waitRun,
	"stop":                         stopRun,
	"stop-run":                     stopRun,
	"reset":                        resetTarget,
	"destroy":                      destroyTarget,
	"destroy-target":               destroyTarget,
	"quarantine":                   quarantineTarget,
	"recovery":                     requestRecovery,
	"get-exec":                     getExec,
	"create-exec":                  createExec,
	"transition-exec":              transitionExec,
	"finalize-exec":                finalizeExec,
	"open-exec":                    openExec,
	"snapshot":                     snapshot,
	"top":                          top,
	"watch":                        watch,
	"metrics":                      metrics,
	"bundle":                       bundle,
	"start-capture":                startCapture,
	"request-capture":              requestCapture,
	"stop-capture":                 stopCapture,
	"declare-export":               declareExport,
	"preview-export":               previewExport,
	"commit-export":                commitExport,
	"transition-agent-generation":  transitionAgentGeneration,
	"transition-target-generation": transitionTargetGeneration,
	"transition-run":               transitionTargetRun,
	"create-operation":             createTargetOperation,
	"transition-operation":         transitionTargetOperation,
	"get-incident":                 getIncident,
	"create-incident":              createIncident,
	"transition-incident":          transitionIncident,
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		if exit, ok := err.(interface{ ExitCode() int }); ok {
			os.Exit(exit.ExitCode())
		}
		os.Exit(1)
	}
}

func run(arguments []string, stdout, stderr io.Writer) error {
	configuration, command, commandArguments, err := worldcli.ParseGlobal("worldctl", arguments, stderr)
	if err != nil {
		return err
	}
	handler, ok := commands[command]
	if !ok {
		return worldcli.UsageError(fmt.Sprintf("unknown command %q (available: %s)", command, commandList()))
	}
	client, err := worldcli.Dial(configuration)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := worldcli.Context(configuration)
	defer cancel()
	return handler(ctx, client, commandArguments, stdout, stderr, configuration)
}

func commandList() string {
	names := make([]string, 0, len(commands))
	for name := range commands {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Sprint(names)
}
