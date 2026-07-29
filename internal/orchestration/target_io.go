package orchestration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/safepath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) PushTargetFile(stream worldv1.WorldService_PushTargetFileServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := requireFileTransferStartFrame(first); err != nil {
		return err
	}
	start := first.Start
	ctx, cancel, meta, err := mutationContext(stream.Context(), start.Mutation)
	if err != nil {
		return err
	}
	defer cancel()
	target, run, driver, err := s.scopedTarget(ctx, start.TargetId, start.TargetRunId, meta.AuthorizedPolicyReference)
	if err != nil {
		return err
	}
	if run.State != domain.TargetRunRunning {
		return status.Errorf(codes.FailedPrecondition, "target run is %s, not running", run.State)
	}
	path, err := normalizeTargetPath(start.TargetRelativePath)
	if err != nil {
		return err
	}
	mode, err := normalizeTransferMode(start.Mode)
	if err != nil {
		return err
	}
	var content []byte
	var declaredDigest string
	if start.WorkspaceRelativePath != "" {
		workspacePath, normalizeErr := normalizeWorkspacePath(start.WorkspaceRelativePath)
		if normalizeErr != nil {
			return normalizeErr
		}
		root, rootErr := s.requireRunWorkspace(ctx, target, run)
		if rootErr != nil {
			return rootErr
		}
		source, sourceErr := newWorkspaceContentSource(root, workspacePath, s.maxTransferBytes)
		if sourceErr != nil {
			return sourceErr
		}
		content = bytes.Clone(source.content)
		if endErr := requireFileTransferEnd(stream); endErr != nil {
			return endErr
		}
	} else {
		content, declaredDigest, err = s.receivePushContent(stream)
		if err != nil {
			return err
		}
	}
	digest := domain.NewDigest(content)
	if declaredDigest != "" && declaredDigest != digest.String() {
		return status.Error(codes.DataLoss, "declared transfer digest does not match streamed bytes")
	}
	operation, err := s.core.CreateTargetOperation(ctx, application.CreateTargetOperationRequest{
		Meta: childMeta(meta, "operation-create", deadline(ctx)), TargetID: target.ID, RunID: run.ID,
		Kind: domain.TargetOperationPush, CommandDisplay: "push " + path, ContentDigest: digest.String(),
	})
	if err != nil {
		return err
	}
	if operation.State.Terminal() {
		return stream.Send(&worldv1.FileTransferFrame{Complete: true, Digest: digest.String(), Operation: mapTargetOperation(operation)})
	}
	model, err := domainTargetOperation(target, operation)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	connection, err := driver.OpenTransport(ctx, model.Spec().TargetRunID)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	defer connection.Close()
	running, err := s.core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{Meta: childMeta(meta, "operation-running", deadline(ctx)), TargetID: target.ID, OperationID: operation.ID, ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning})
	if err != nil {
		return errors.Join(err, connection.Close())
	}
	result, err := connection.PushFile(ctx, ports.TargetTransferPlan{Operation: model, RelativePath: path, Mode: mode, MaximumBytes: s.maxTransferBytes}, bytes.NewReader(content))
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, errors.Join(err, connection.Close()))
	}
	if result.OperationID != model.ID() || result.Bytes != int64(len(content)) || result.Digest != digest {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, status.Error(codes.DataLoss, "target driver transfer result does not match streamed content"))
	}
	if err := connection.Close(); err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, err)
	}
	completed, err := s.transitionOperation(ctx, meta, target.ID, running, domain.TargetOperationCompleted, "operation-completed")
	if err != nil {
		return err
	}
	return stream.Send(&worldv1.FileTransferFrame{Offset: uint64(len(content)), Digest: digest.String(), Complete: true, Operation: mapTargetOperation(completed)})
}

func (s *Service) PullTargetFile(request *worldv1.PullTargetFileRequest, stream worldv1.WorldService_PullTargetFileServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	ctx, cancel, meta, err := mutationContext(stream.Context(), request.Mutation)
	if err != nil {
		return err
	}
	defer cancel()
	target, run, driver, err := s.scopedTarget(ctx, request.TargetId, request.TargetRunId, meta.AuthorizedPolicyReference)
	if err != nil {
		return err
	}
	if run.State != domain.TargetRunRunning {
		return status.Errorf(codes.FailedPrecondition, "target run is %s, not running", run.State)
	}
	path, err := normalizeTargetPath(request.TargetRelativePath)
	if err != nil {
		return err
	}
	var workspaceRoot, workspacePath string
	if request.WorkspaceRelativePath != "" {
		workspacePath, err = normalizeWorkspacePath(request.WorkspaceRelativePath)
		if err != nil {
			return err
		}
		workspaceRoot, err = s.requireRunWorkspace(ctx, target, run)
		if err != nil {
			return err
		}
	}
	operation, err := s.core.CreateTargetOperation(ctx, application.CreateTargetOperationRequest{Meta: childMeta(meta, "operation-create", deadline(ctx)), TargetID: target.ID, RunID: run.ID, Kind: domain.TargetOperationPull, CommandDisplay: "pull " + path})
	if err != nil {
		return err
	}
	model, err := domainTargetOperation(target, operation)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	connection, err := driver.OpenTransport(ctx, model.Spec().TargetRunID)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	defer connection.Close()
	running, err := s.core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{Meta: childMeta(meta, "operation-running", deadline(ctx)), TargetID: target.ID, OperationID: operation.ID, ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning})
	if err != nil {
		return errors.Join(err, connection.Close())
	}
	reader, err := connection.PullFile(ctx, ports.TargetTransferPlan{Operation: model, RelativePath: path, MaximumBytes: s.maxTransferBytes})
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, errors.Join(err, connection.Close()))
	}
	defer reader.Close()
	hash := sha256.New()
	var workspaceContent bytes.Buffer
	buffer := make([]byte, 32<<10)
	var offset int64
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			if int64(count) > s.maxTransferBytes-offset {
				return s.finalizeOperationFailure(ctx, meta, target.ID, running, status.Error(codes.ResourceExhausted, "target pull exceeded the configured byte limit"))
			}
			chunk := append([]byte(nil), buffer[:count]...)
			_, _ = hash.Write(chunk)
			if workspacePath != "" {
				_, _ = workspaceContent.Write(chunk)
			} else {
				if err := stream.Send(&worldv1.FileTransferFrame{Data: chunk, Offset: uint64(offset)}); err != nil {
					return s.finalizeOperationFailure(ctx, meta, target.ID, running, err)
				}
			}
			offset += int64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return s.finalizeOperationFailure(ctx, meta, target.ID, running, readErr)
		}
	}
	if err := errors.Join(reader.Close(), connection.Close()); err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, err)
	}
	if workspacePath != "" {
		writeErr := safepath.WriteRegularAtomic(workspaceRoot, workspacePath, 0o600, func(writer io.Writer) error {
			_, err := writeAll(writer, workspaceContent.Bytes())
			return err
		})
		if writeErr != nil {
			return s.finalizeOperationFailure(ctx, meta, target.ID, running, writeErr)
		}
	}
	completed, err := s.transitionOperation(ctx, meta, target.ID, running, domain.TargetOperationCompleted, "operation-completed")
	if err != nil {
		return err
	}
	digest := "sha256:" + fmt.Sprintf("%x", hash.Sum(nil))
	return stream.Send(&worldv1.FileTransferFrame{Offset: uint64(offset), Digest: digest, Complete: true, Operation: mapTargetOperation(completed)})
}

func (s *Service) requireRunWorkspace(ctx context.Context, target application.TargetRecord, run application.TargetRunRecord) (string, error) {
	scope, err := s.requireWorkspaceScope(ctx, target.LeaseID)
	if err != nil {
		return "", err
	}
	if scope.AgentWorkspaceID.String() != run.AgentWorkspaceID || uint64(scope.AgentGeneration) != run.AgentGeneration {
		return "", status.Error(codes.FailedPrecondition, "target run is not bound to the lease's current agent workspace generation")
	}
	if err := requireWritableWorkspaceScope(scope); err != nil {
		return "", err
	}
	handle, err := s.workspace.Inspect(ctx, scope.WorkspaceID)
	if err != nil {
		return "", err
	}
	if handle.WorkspaceID != scope.WorkspaceID || handle.State != domain.WorkspaceMounted || strings.TrimSpace(handle.MergedPath) == "" {
		return "", status.Error(codes.FailedPrecondition, "agent workspace is not mounted and writable for this target run")
	}
	return handle.MergedPath, nil
}

func (s *Service) OpenTargetADB(stream worldv1.WorldService_OpenTargetADBServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if err := requireADBStartFrame(first); err != nil {
		return err
	}
	start := first.Start
	ctx, cancel, meta, err := mutationContext(stream.Context(), start.Mutation)
	if err != nil {
		return err
	}
	defer cancel()
	target, run, driver, err := s.scopedTarget(ctx, start.TargetId, start.TargetRunId, meta.AuthorizedPolicyReference)
	if err != nil {
		return err
	}
	if run.State != domain.TargetRunRunning {
		return status.Errorf(codes.FailedPrecondition, "target run is %s, not running", run.State)
	}
	operation, err := s.core.CreateTargetOperation(ctx, application.CreateTargetOperationRequest{Meta: childMeta(meta, "operation-create", deadline(ctx)), TargetID: target.ID, RunID: run.ID, Kind: domain.TargetOperationADBService, CommandDisplay: "scoped ADB tunnel"})
	if err != nil {
		return err
	}
	runID, err := requireStoredID("orchestration.open_target_adb", "target_run_id", run.ID, domain.ParseTargetRunID)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	connection, err := driver.OpenTransport(ctx, runID)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	defer connection.Close()
	endpoint, err := connection.OpenADB(ctx)
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, errors.Join(err, connection.Close()))
	}
	defer endpoint.Close()
	if err := s.validateADBAddress(endpoint.Address()); err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	tunnel, err := s.dialer.DialContext(ctx, "tcp", endpoint.Address())
	if err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, operation, err)
	}
	defer tunnel.Close()
	running, err := s.core.TransitionTargetOperation(ctx, application.TransitionTargetOperationRequest{Meta: childMeta(meta, "operation-running", deadline(ctx)), TargetID: target.ID, OperationID: operation.ID, ExpectedRevision: operation.Revision, State: domain.TargetOperationRunning})
	if err != nil {
		return errors.Join(err, closeTargetADBResources(tunnel, endpoint, connection))
	}
	if err := stream.Send(&worldv1.ADBFrame{AssignedSerial: endpoint.Serial()}); err != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, err)
	}
	exchangeErr := s.exchangeADB(ctx, tunnel, stream)
	closeErr := closeTargetADBResources(tunnel, endpoint, connection)
	if exchangeErr != nil || closeErr != nil {
		return s.finalizeOperationFailure(ctx, meta, target.ID, running, errors.Join(exchangeErr, closeErr))
	}
	if _, err := s.transitionOperation(ctx, meta, target.ID, running, domain.TargetOperationCompleted, "operation-completed"); err != nil {
		return err
	}
	return stream.Send(&worldv1.ADBFrame{Complete: true})
}

func closeTargetADBResources(resources ...io.Closer) error {
	var closeErrors []error
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		if err := resource.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func (s *Service) receivePushContent(stream worldv1.WorldService_PushTargetFileServer) ([]byte, string, error) {
	content := make([]byte, 0)
	var declaredDigest string
	for {
		frame, err := stream.Recv()
		if err != nil {
			return nil, "", err
		}
		if frame == nil || frame.Start != nil || frame.Operation != nil {
			return nil, "", status.Error(codes.InvalidArgument, "file data frame contains a repeated start or server-only operation")
		}
		if frame.Offset != uint64(len(content)) {
			return nil, "", status.Errorf(codes.InvalidArgument, "file transfer offset %d is not contiguous after %d bytes", frame.Offset, len(content))
		}
		if int64(len(frame.Data)) > s.maxTransferBytes-int64(len(content)) {
			return nil, "", status.Error(codes.ResourceExhausted, "target push exceeded the configured byte limit")
		}
		content = append(content, frame.Data...)
		if frame.Complete {
			declaredDigest = frame.Digest
			if declaredDigest != "" {
				if _, err := domain.ParseDigest(declaredDigest); err != nil {
					return nil, "", status.Error(codes.InvalidArgument, "transfer digest is invalid")
				}
			}
			if err := requireFileTransferEnd(stream); err != nil {
				return nil, "", err
			}
			return content, declaredDigest, nil
		}
		if frame.Digest != "" {
			return nil, "", status.Error(codes.InvalidArgument, "digest is allowed only on the complete frame")
		}
	}
}

func requireFileTransferEnd(stream worldv1.WorldService_PushTargetFileServer) error {
	if _, err := stream.Recv(); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return status.Error(codes.InvalidArgument, "file transfer contains data after its declared end")
}

func (s *Service) exchangeADB(ctx context.Context, connection net.Conn, stream worldv1.WorldService_OpenTargetADBServer) error {
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-child.Done()
		_ = connection.Close()
	}()
	inputErrors := make(chan error, 1)
	go func() {
		var total int64
		for {
			frame, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				inputErrors <- closeWrite(connection)
				return
			}
			if err != nil {
				inputErrors <- err
				cancel()
				return
			}
			if frame == nil || frame.Start != nil || len(frame.ServerBytes) > 0 || frame.AssignedSerial != "" {
				inputErrors <- status.Error(codes.InvalidArgument, "ADB client frame contains a repeated start or server-only field")
				cancel()
				return
			}
			if int64(len(frame.ClientBytes)) > s.maxADBBytes-total {
				inputErrors <- status.Error(codes.ResourceExhausted, "ADB client byte limit exceeded")
				cancel()
				return
			}
			total += int64(len(frame.ClientBytes))
			if len(frame.ClientBytes) > 0 {
				if _, err := writeAll(connection, frame.ClientBytes); err != nil {
					inputErrors <- err
					cancel()
					return
				}
			}
			if frame.Complete {
				inputErrors <- closeWrite(connection)
				return
			}
		}
	}()
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		count, err := connection.Read(buffer)
		if count > 0 {
			if int64(count) > s.maxADBBytes-total {
				return status.Error(codes.ResourceExhausted, "ADB server byte limit exceeded")
			}
			total += int64(count)
			if sendErr := stream.Send(&worldv1.ADBFrame{ServerBytes: append([]byte(nil), buffer[:count]...)}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			select {
			case inputErr := <-inputErrors:
				return inputErr
			default:
				return nil
			}
		}
		if err != nil {
			select {
			case inputErr := <-inputErrors:
				if inputErr != nil {
					return inputErr
				}
			default:
			}
			return err
		}
	}
}

func (s *Service) validateADBAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return status.Error(codes.FailedPrecondition, "target driver returned an invalid scoped ADB endpoint")
	}
	if s.allowRemoteADB || strings.EqualFold(host, "localhost") {
		return nil
	}
	parsed := net.ParseIP(host)
	if parsed == nil || !parsed.IsLoopback() {
		return status.Error(codes.PermissionDenied, "non-loopback ADB endpoints require explicit allow_remote_adb configuration")
	}
	return nil
}

func normalizeTargetPath(value string) (string, error) {
	return normalizeScopedPath("target_relative_path", value)
}

func requireFileTransferStartFrame(frame *worldv1.FileTransferFrame) error {
	return requireStartOnly("file transfer", frame != nil && frame.Start != nil, frame != nil && (len(frame.Data) > 0 || frame.Offset != 0 || frame.Digest != "" || frame.Complete || frame.Operation != nil))
}

func requireADBStartFrame(frame *worldv1.ADBFrame) error {
	return requireStartOnly("ADB", frame != nil && frame.Start != nil, frame != nil && (len(frame.ClientBytes) > 0 || len(frame.ServerBytes) > 0 || frame.AssignedSerial != "" || frame.Complete))
}

func normalizeWorkspacePath(value string) (string, error) {
	return normalizeScopedPath("workspace_relative_path", value)
}

func normalizeScopedPath(field, value string) (string, error) {
	normalized, err := safepath.Normalize(value)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "unsafe %s: %v", field, err)
	}
	return normalized, nil
}

func normalizeTransferMode(value uint32) (uint32, error) {
	if value == 0 {
		return 0o600, nil
	}
	if value&^uint32(0o777) != 0 {
		return 0, status.Error(codes.InvalidArgument, "mode may contain only user/group/other permission bits")
	}
	return value, nil
}

func writeAll(writer io.Writer, content []byte) (int, error) {
	written := 0
	for written < len(content) {
		count, err := writer.Write(content[written:])
		written += count
		if err != nil {
			return written, err
		}
		if count == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func closeWrite(connection net.Conn) error {
	if value, ok := connection.(interface{ CloseWrite() error }); ok {
		return value.CloseWrite()
	}
	return nil
}
