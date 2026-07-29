package main

import (
	"context"
	"fmt"
	"io"
	"os"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

type targetExecOptions struct {
	start       *worldv1.TargetExecStart
	stdinPath   string
	outcomeJSON bool
}

func targetExec(ctx context.Context, client *world.Client, arguments []string, stdin io.Reader, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	return runTargetExec(ctx, client, arguments, stdin, stdout, stderr, configuration, false)
}

func targetShell(ctx context.Context, client *world.Client, arguments []string, stdin io.Reader, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	return runTargetExec(ctx, client, arguments, stdin, stdout, stderr, configuration, true)
}

func runTargetExec(ctx context.Context, client *world.Client, arguments []string, stdin io.Reader, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig, shell bool) error {
	options, err := parseTargetExec(arguments, stderr, configuration, shell)
	if err != nil {
		return err
	}
	input, closeInput, err := worldcli.OpenInput(options.stdinPath, stdin)
	if err != nil {
		return err
	}
	defer closeInput()
	stream, err := client.OpenTargetExec(ctx)
	if err != nil {
		return err
	}
	var output worldcli.ExecOutput
	err = worldcli.PumpBidi(stream, &worldv1.TargetExecFrame{Start: options.start}, input,
		func(data []byte) *worldv1.TargetExecFrame { return &worldv1.TargetExecFrame{Stdin: data} },
		nil,
		func(frame *worldv1.TargetExecFrame) error {
			return output.Handle(stdout, stderr, frame.Stdout, frame.Stderr, frame.Outcome)
		}, worldcli.PumpBidiOptions{})
	if err != nil {
		return err
	}
	return output.Finish(stderr, options.outcomeJSON)
}

func parseTargetExec(arguments []string, stderr io.Writer, configuration worldcli.ConnectionConfig, shell bool) (*targetExecOptions, error) {
	name := "exec"
	if shell {
		name = "shell"
	}
	flags := worldcli.NewFlagSet(name, stderr)
	scope := addTargetScope(flags)
	workingDirectory := flags.String("working-directory", "", "target-relative working directory")
	stdinPath := flags.String("stdin", "-", "stdin file, or - for this process stdin")
	terminal := flags.Bool("terminal", false, "request a terminal")
	rows := flags.Uint("rows", 24, "terminal rows")
	columns := flags.Uint("columns", 80, "terminal columns")
	terminalType := flags.String("terminal-type", "xterm-256color", "terminal type")
	outcomeJSON := flags.Bool("outcome-json", false, "write the terminal outcome as JSON to stderr")
	scriptText := flags.String("script", "", "exact shell program bytes")
	scriptFile := flags.String("script-file", "", "file containing exact shell program bytes")
	maxScript := flags.Int64("max-script-bytes", 1<<20, "maximum explicit shell program bytes")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if *workingDirectory != "" {
		clean, err := worldcli.RelativePath("target", *workingDirectory)
		if err != nil {
			return nil, err
		}
		*workingDirectory = clean
	}
	meta, err := scope.mutation(configuration.Timeout)
	if err != nil {
		return nil, err
	}
	start := &worldv1.TargetExecStart{Mutation: meta, TargetId: scope.target, TargetRunId: scope.run, TargetRelativeWorkingDirectory: *workingDirectory}
	if shell {
		script, err := resolveShellScript(*scriptText, *scriptFile, flags.Args(), *maxScript)
		if err != nil {
			return nil, err
		}
		start.ExplicitShellBytes = script
	} else {
		if *scriptText != "" || *scriptFile != "" {
			return nil, worldcli.UsageError("script and script-file are valid only for shell")
		}
		start.Argv = flags.Args()
		if len(start.Argv) == 0 {
			return nil, worldcli.UsageError("exec requires an argv after flags")
		}
	}
	start.Terminal, err = worldcli.Terminal(*terminal, *rows, *columns, *terminalType)
	if err != nil {
		return nil, err
	}
	return &targetExecOptions{start: start, stdinPath: *stdinPath, outcomeJSON: *outcomeJSON}, nil
}

func resolveShellScript(text, path string, positional []string, maximum int64) ([]byte, error) {
	provided := 0
	if text != "" {
		provided++
	}
	if path != "" {
		provided++
	}
	if len(positional) > 0 {
		provided++
	}
	if provided != 1 || len(positional) > 1 {
		return nil, worldcli.UsageError("shell requires exactly one of SCRIPT, -script, or -script-file")
	}
	var result []byte
	var err error
	switch {
	case text != "":
		result = []byte(text)
	case path != "":
		result, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read shell script: %w", err)
		}
	default:
		result = []byte(positional[0])
	}
	if maximum <= 0 || int64(len(result)) > maximum {
		return nil, fmt.Errorf("shell script exceeds max-script-bytes (%d)", maximum)
	}
	return result, nil
}
