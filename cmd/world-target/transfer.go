package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
	"github.com/philcantcode/go-world-management-layer/world"
	"google.golang.org/protobuf/proto"
)

type pushOptions struct {
	scope             *targetScope
	workspaceSource   string
	targetDestination string
	mode              uint32
}

func pushTargetFile(ctx context.Context, client *world.Client, arguments []string, _ io.Reader, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	options, err := parsePush(arguments, stderr)
	if err != nil {
		return err
	}
	meta, err := options.scope.mutation(configuration.Timeout)
	if err != nil {
		return err
	}
	stream, err := client.PushTargetFile(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(options.start(meta)); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	operation, err := receiveWorkspaceTransfer(stream.Recv, "push")
	if err != nil {
		return err
	}
	return worldcli.Encoder(stdout).Encode(operation)
}

func (options *pushOptions) start(meta *worldv1.MutationMetadata) *worldv1.FileTransferFrame {
	return &worldv1.FileTransferFrame{Start: &worldv1.FileTransferStart{
		Mutation: meta, TargetId: options.scope.target, TargetRunId: options.scope.run,
		WorkspaceRelativePath: options.workspaceSource, TargetRelativePath: options.targetDestination, Mode: options.mode,
	}}
}

func parsePush(arguments []string, stderr io.Writer) (*pushOptions, error) {
	flags := worldcli.NewFlagSet("push", stderr)
	scope := addTargetScope(flags)
	source := flags.String("source", "", "workspace-relative source file")
	destination := flags.String("destination", "", "target-relative destination file")
	mode := flags.Uint("mode", 0o600, "target file permission bits")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	remaining := flags.Args()
	if *source == "" && *destination == "" && len(remaining) == 2 {
		*source, *destination = remaining[0], remaining[1]
		remaining = nil
	}
	if len(remaining) != 0 {
		return nil, worldcli.UsageError("push accepts SOURCE DESTINATION")
	}
	if err := scope.validate(); err != nil {
		return nil, err
	}
	cleanSource, err := worldcli.WorkspacePath(*source)
	if err != nil {
		return nil, err
	}
	cleanDestination, err := worldcli.RelativePath("target", *destination)
	if err != nil {
		return nil, err
	}
	if *mode == 0 || *mode > 0o777 {
		return nil, worldcli.UsageError("mode must contain non-zero user/group/other permission bits")
	}
	return &pushOptions{scope: scope, workspaceSource: cleanSource, targetDestination: cleanDestination, mode: uint32(*mode)}, nil
}

type pullOptions struct {
	scope                *targetScope
	targetSource         string
	workspaceDestination string
}

func pullTargetFile(ctx context.Context, client *world.Client, arguments []string, _ io.Reader, stdout, stderr io.Writer, configuration worldcli.ConnectionConfig) error {
	options, err := parsePull(arguments, stderr)
	if err != nil {
		return err
	}
	meta, err := options.scope.mutation(configuration.Timeout)
	if err != nil {
		return err
	}
	stream, err := client.PullTargetFile(ctx, options.request(meta))
	if err != nil {
		return err
	}
	operation, err := receiveWorkspaceTransfer(stream.Recv, "pull")
	if err != nil {
		return err
	}
	return worldcli.Encoder(stdout).Encode(operation)
}

func (options *pullOptions) request(meta *worldv1.MutationMetadata) *worldv1.PullTargetFileRequest {
	return &worldv1.PullTargetFileRequest{
		Mutation: meta, TargetId: options.scope.target, TargetRunId: options.scope.run,
		TargetRelativePath: options.targetSource, WorkspaceRelativePath: options.workspaceDestination,
	}
}

func parsePull(arguments []string, stderr io.Writer) (*pullOptions, error) {
	flags := worldcli.NewFlagSet("pull", stderr)
	scope := addTargetScope(flags)
	source := flags.String("source", "", "target-relative source file")
	destination := flags.String("destination", "", "workspace-relative destination file")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}
	remaining := flags.Args()
	if *source == "" && *destination == "" && len(remaining) == 2 {
		*source, *destination = remaining[0], remaining[1]
		remaining = nil
	}
	if len(remaining) != 0 {
		return nil, worldcli.UsageError("pull accepts SOURCE DESTINATION")
	}
	if err := scope.validate(); err != nil {
		return nil, err
	}
	cleanSource, err := worldcli.RelativePath("target", *source)
	if err != nil {
		return nil, err
	}
	cleanDestination, err := worldcli.WorkspacePath(*destination)
	if err != nil {
		return nil, err
	}
	return &pullOptions{scope: scope, targetSource: cleanSource, workspaceDestination: cleanDestination}, nil
}

type transferCompletion struct {
	complete  bool
	digest    string
	operation *worldv1.TargetOperation
}

func receiveWorkspaceTransfer(receive func() (*worldv1.FileTransferFrame, error), operationName string) (*worldv1.TargetOperation, error) {
	var result transferCompletion
	for {
		frame, err := receive()
		if errors.Is(err, io.EOF) {
			return result.finish(operationName)
		}
		if err != nil {
			return nil, err
		}
		if err := result.accept(frame); err != nil {
			return nil, fmt.Errorf("%s protocol: %w", operationName, err)
		}
	}
}

func (result *transferCompletion) accept(frame *worldv1.FileTransferFrame) error {
	if frame == nil {
		return fmt.Errorf("server returned a nil transfer frame")
	}
	if result.complete {
		return fmt.Errorf("server returned a frame after transfer completion")
	}
	if frame.Start != nil || len(frame.Data) != 0 {
		return fmt.Errorf("workspace-backed transfer returned a start or data payload")
	}
	if frame.Digest != "" && !frame.Complete {
		return fmt.Errorf("server returned a digest before transfer completion")
	}
	if frame.Operation != nil {
		if result.operation != nil {
			return fmt.Errorf("server returned more than one target operation acknowledgement")
		}
		result.operation = proto.Clone(frame.Operation).(*worldv1.TargetOperation)
	}
	if frame.Complete {
		if frame.Digest == "" {
			return fmt.Errorf("completion frame omitted the content digest")
		}
		result.complete = true
		result.digest = frame.Digest
	}
	if frame.Operation == nil && !frame.Complete {
		return fmt.Errorf("server returned an empty transfer frame")
	}
	return nil
}

func (result *transferCompletion) finish(operationName string) (*worldv1.TargetOperation, error) {
	if !result.complete {
		return nil, fmt.Errorf("%s stream closed without transfer completion", operationName)
	}
	if result.digest == "" {
		return nil, fmt.Errorf("%s completed without a content digest", operationName)
	}
	if result.operation == nil {
		return nil, fmt.Errorf("%s completed without a target operation acknowledgement", operationName)
	}
	return result.operation, nil
}
