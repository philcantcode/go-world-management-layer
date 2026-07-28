package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
)

type adbProxyOptions struct {
	scope       *targetScope
	listen      string
	connections int
}

func targetADBProxy(ctx context.Context, client *world.Client, arguments []string, _ io.Reader, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	options, err := parseADBProxy(arguments, stderr)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", options.listen)
	if err != nil {
		return fmt.Errorf("listen for scoped ADB: %w", err)
	}
	defer listener.Close()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	if err := worldcli.Encoder(stdout).Encode(map[string]any{"address": listener.Addr().String(), "target_id": options.scope.target, "target_run_id": options.scope.run, "max_connections": options.connections}); err != nil {
		return err
	}
	for handled := 0; handled < options.connections; handled++ {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept scoped ADB client: %w", err)
		}
		err = proxyADBConnection(ctx, client, connection, options.scope, stderr, configuration)
		_ = connection.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func proxyADBConnection(ctx context.Context, client *world.Client, connection net.Conn, scope *targetScope, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	meta, err := scope.mutation(configuration.Timeout)
	if err != nil {
		return err
	}
	stream, err := client.OpenTargetADB(ctx)
	if err != nil {
		return err
	}
	response := adbResponse{connection: connection, stderr: stderr}
	err = worldcli.PumpBidi(stream, &worldv1.ADBFrame{Start: &worldv1.ADBStart{Mutation: meta, TargetId: scope.target, TargetRunId: scope.run}}, connection,
		func(data []byte) *worldv1.ADBFrame { return &worldv1.ADBFrame{ClientBytes: data} },
		func() *worldv1.ADBFrame { return &worldv1.ADBFrame{Complete: true} },
		response.handle)
	if err != nil {
		return err
	}
	return response.finish()
}

type adbResponse struct {
	connection net.Conn
	stderr     io.Writer
	serial     string
	complete   bool
}

func (response *adbResponse) handle(frame *worldv1.ADBFrame) error {
	if response.complete {
		return fmt.Errorf("ADB server returned a frame after completion")
	}
	if frame.Start != nil || len(frame.ClientBytes) != 0 {
		return fmt.Errorf("ADB server returned a client-only field")
	}
	if frame.AssignedSerial == "" && len(frame.ServerBytes) == 0 && !frame.Complete {
		return fmt.Errorf("ADB server returned an empty frame")
	}
	if frame.AssignedSerial != "" {
		if response.serial != "" && response.serial != frame.AssignedSerial {
			return fmt.Errorf("ADB server changed assigned serial from %q to %q", response.serial, frame.AssignedSerial)
		}
		if response.serial == "" {
			response.serial = frame.AssignedSerial
			if _, err := fmt.Fprintf(response.stderr, "assigned ADB serial: %s\n", response.serial); err != nil {
				return err
			}
		}
	}
	if len(frame.ServerBytes) > 0 {
		if _, err := response.connection.Write(frame.ServerBytes); err != nil {
			return err
		}
	}
	response.complete = frame.Complete
	return nil
}

func (response *adbResponse) finish() error {
	if response.serial == "" {
		return fmt.Errorf("ADB stream closed without an assigned serial")
	}
	if !response.complete {
		return fmt.Errorf("ADB stream closed without completion")
	}
	return nil
}

func parseADBProxy(arguments []string, stderr io.Writer) (*adbProxyOptions, error) {
	flags := worldcli.NewFlagSet("adb-proxy", stderr)
	scope := addTargetScope(flags)
	listen := flags.String("listen", "127.0.0.1:0", "loopback TCP listen address")
	connections := flags.Int("connections", 1, "maximum sequential ADB client connections (1-16)")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	if err := worldcli.RequireNoArgs(flags); err != nil {
		return nil, err
	}
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if err := requireLoopback(*listen); err != nil {
		return nil, err
	}
	if *connections < 1 || *connections > 16 {
		return nil, worldcli.UsageError("connections must be between 1 and 16")
	}
	return &adbProxyOptions{scope: scope, listen: *listen, connections: *connections}, nil
}

func requireLoopback(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return worldcli.UsageError("listen must be a host:port address")
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return worldcli.UsageError("ADB proxy may listen only on a loopback address")
	}
	return nil
}
