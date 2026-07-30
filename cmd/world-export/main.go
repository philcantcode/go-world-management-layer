package main

import (
	"fmt"
	"io"
	"os"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string, stdout, stderr io.Writer) error {
	configuration, command, commandArguments, err := worldcli.ParseGlobal("world-export", arguments, stderr)
	if err != nil {
		return err
	}
	if command != "declare" {
		commandArguments = append([]string{command}, commandArguments...)
	}
	request, err := worldcli.ParseDeclareExport(commandArguments, stderr, configuration.Timeout, worldcli.Env("WORLD_LEASE_ID"), worldcli.Env("WORLD_POLICY_REFERENCE"))
	if err != nil {
		return err
	}
	manager, err := worldcli.Open(configuration)
	if err != nil {
		return err
	}
	defer manager.Close()
	ctx, cancel := worldcli.Context(configuration)
	defer cancel()
	result, err := manager.DeclareExport(ctx, request)
	return worldcli.EncodeResult(worldcli.Encoder(stdout), result, err)
}
