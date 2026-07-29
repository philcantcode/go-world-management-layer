package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

type execStreamOptions struct {
	start       *worldv1.ExecStart
	stdinPath   string
	outcomeJSON bool
}

func openExec(ctx context.Context, client *world.Client, arguments []string, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	options, err := parseOpenExec(arguments, stderr, configuration)
	if err != nil {
		return err
	}
	input, closeInput, err := worldcli.OpenInput(options.stdinPath, os.Stdin)
	if err != nil {
		return err
	}
	defer closeInput()
	stream, err := client.OpenExec(ctx)
	if err != nil {
		return err
	}
	var output worldcli.ExecOutput
	err = worldcli.PumpBidi(stream, &worldv1.ExecFrame{Start: options.start}, input,
		func(data []byte) *worldv1.ExecFrame { return &worldv1.ExecFrame{Stdin: data} },
		nil,
		func(frame *worldv1.ExecFrame) error {
			return output.Handle(stdout, stderr, frame.Stdout, frame.Stderr, frame.Outcome)
		}, worldcli.PumpBidiOptions{})
	if err != nil {
		return err
	}
	return output.Finish(stderr, options.outcomeJSON)
}

func parseOpenExec(arguments []string, stderr io.Writer, configuration worldcli.ConnectionConfig) (*execStreamOptions, error) {
	flags := worldcli.NewFlagSet("open-exec", stderr)
	lease := flags.String("lease", defaultEnv("WORLD_LEASE_ID"), "lease ID")
	executable := flags.String("executable", "", "policy-approved provider executable")
	workingDirectory := flags.String("working-directory", "", "workspace-relative working directory")
	stdinPath := flags.String("stdin", "-", "stdin file, or - for this process stdin")
	terminal := flags.Bool("terminal", false, "request a terminal")
	rows := flags.Uint("rows", 24, "terminal rows")
	columns := flags.Uint("columns", 80, "terminal columns")
	terminalType := flags.String("terminal-type", "xterm-256color", "terminal type")
	outcomeJSON := flags.Bool("outcome-json", false, "write the terminal outcome as JSON to stderr")
	maxTemporary := flags.Int64("max-temporary-bytes", 8<<20, "maximum total temporary-input bytes")
	var inputs worldcli.StringValues
	flags.Var(&inputs, "temporary-input", "repeatable zero-based ARGV_INDEX:NAME=source-file (replaces that argument after -- with a private temporary path)")
	mutation := addMutationFlags(flags)
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.Require("lease", *lease, "executable", *executable); err != nil {
		return nil, err
	}
	if *maxTemporary < 0 {
		return nil, worldcli.UsageError("max-temporary-bytes cannot be negative")
	}
	if *workingDirectory != "" {
		clean, err := worldcli.WorkspacePath(*workingDirectory)
		if err != nil {
			return nil, err
		}
		*workingDirectory = clean
	}
	temporary, err := readTemporaryInputs(inputs, *maxTemporary)
	if err != nil {
		return nil, err
	}
	argv := flags.Args()
	if err := validateTemporaryInputBindings(temporary, len(argv)); err != nil {
		return nil, err
	}
	meta, err := mutation.Metadata(configuration.Timeout)
	if err != nil {
		return nil, err
	}
	terminalSettings, err := worldcli.Terminal(*terminal, *rows, *columns, *terminalType)
	if err != nil {
		return nil, err
	}
	return &execStreamOptions{
		start: &worldv1.ExecStart{
			Mutation: meta, LeaseId: *lease, ProviderExecutable: *executable, Argv: argv,
			WorkspaceRelativeWorkingDirectory: *workingDirectory, TemporaryInputs: temporary, Terminal: terminalSettings,
		},
		stdinPath: *stdinPath, outcomeJSON: *outcomeJSON,
	}, nil
}

func validateTemporaryInputBindings(values []*worldv1.TemporaryInput, argvCount int) error {
	used := make(map[uint32]struct{}, len(values))
	for _, value := range values {
		if uint64(value.ArgvIndex) >= uint64(argvCount) {
			return worldcli.UsageError(fmt.Sprintf("temporary input argv index %d is outside argv", value.ArgvIndex))
		}
		if _, exists := used[value.ArgvIndex]; exists {
			return worldcli.UsageError(fmt.Sprintf("temporary inputs share argv index %d", value.ArgvIndex))
		}
		used[value.ArgvIndex] = struct{}{}
	}
	return nil
}

func readTemporaryInputs(values []string, maximum int64) ([]*worldv1.TemporaryInput, error) {
	result := make([]*worldv1.TemporaryInput, 0, len(values))
	var total int64
	for _, value := range values {
		binding, source, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(source) == "" {
			return nil, fmt.Errorf("temporary input %q must be ARGV_INDEX:NAME=source-file", value)
		}
		indexValue, name, ok := strings.Cut(binding, ":")
		argvIndex, err := strconv.ParseUint(indexValue, 10, 32)
		if err != nil || !ok || strings.TrimSpace(name) == "" || filepath.Base(name) != name || name == "." || name == ".." {
			return nil, fmt.Errorf("temporary input %q must use a numeric argv index and a plain file name", value)
		}
		content, err := os.ReadFile(source)
		if err != nil {
			return nil, fmt.Errorf("read temporary input %q: %w", source, err)
		}
		total += int64(len(content))
		if total > maximum {
			return nil, fmt.Errorf("temporary inputs exceed max-temporary-bytes (%d)", maximum)
		}
		result = append(result, &worldv1.TemporaryInput{NameHint: name, ArgvIndex: uint32(argvIndex), Content: content, Mode: 0o600})
	}
	return result, nil
}
