// Package world is the stable Go client for the versioned world.v1 contract.
package world

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	ResearchSessionView = worldv1.ResearchSessionView
	Lease               = worldv1.Lease
	Target              = worldv1.Target
	TargetRun           = worldv1.TargetRun
	Incident            = worldv1.Incident
	ObservationBundle   = worldv1.ObservationBundle
)

type DialOptions struct {
	UnixSocket      string
	TCPAddress      string
	BearerToken     string
	TLSConfig       *tls.Config
	Dialer          func(context.Context, string) (net.Conn, error)
	MaxMessageBytes int
	DefaultTimeout  time.Duration
}

type Client struct {
	rpc            worldv1.WorldServiceClient
	connection     *grpc.ClientConn
	defaultTimeout time.Duration
}

func Dial(options DialOptions) (*Client, error) {
	if options.BearerToken != "" && options.TLSConfig == nil && options.Dialer == nil && options.TCPAddress != "" && !isLoopbackAddress(options.TCPAddress) {
		return nil, fmt.Errorf("refusing to send a local bearer over non-loopback plaintext TCP")
	}
	target, err := dialTarget(options)
	if err != nil {
		return nil, err
	}
	maxBytes := options.MaxMessageBytes
	if maxBytes <= 0 {
		maxBytes = worldv1.DefaultMaxMessageSize
	}
	transportCredentials := credentials.TransportCredentials(insecure.NewCredentials())
	if options.TLSConfig != nil {
		transportCredentials = credentials.NewTLS(options.TLSConfig.Clone())
	}
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxBytes),
			grpc.MaxCallSendMsgSize(maxBytes),
		),
	}
	if options.BearerToken != "" {
		dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(bearerUnary(options.BearerToken)), grpc.WithChainStreamInterceptor(bearerStream(options.BearerToken)))
	}
	if options.Dialer != nil {
		dialOptions = append(dialOptions, grpc.WithContextDialer(options.Dialer))
	}
	connection, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return nil, err
	}
	client := NewClient(worldv1.NewWorldServiceClient(connection), options.DefaultTimeout)
	client.connection = connection
	return client, nil
}

func DialContext(ctx context.Context, options DialOptions) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return Dial(options)
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	parsed := net.ParseIP(host)
	return parsed != nil && parsed.IsLoopback()
}

func dialTarget(options DialOptions) (string, error) {
	if options.Dialer != nil {
		return "passthrough:///world", nil
	}
	if address := strings.TrimSpace(options.TCPAddress); address != "" {
		return "dns:///" + address, nil
	}
	if socket := strings.TrimSpace(options.UnixSocket); socket != "" {
		return "unix://" + socket, nil
	}
	return "", fmt.Errorf("unix_socket or tcp_address is required")
}

// NewClient wraps any world.v1 client, which is useful with bufconn and test
// doubles. Every returned unary value is deep-copied before it crosses this
// boundary, so callers cannot mutate transport-owned or cached state.
func NewClient(rpc worldv1.WorldServiceClient, defaultTimeout ...time.Duration) *Client {
	timeout := 30 * time.Second
	if len(defaultTimeout) > 0 && defaultTimeout[0] > 0 {
		timeout = defaultTimeout[0]
	}
	return &Client{rpc: rpc, defaultTimeout: timeout}
}

func (c *Client) Close() error {
	if c == nil || c.connection == nil {
		return nil
	}
	return c.connection.Close()
}

func bearerUnary(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		return invoke(withBearer(ctx, token), method, request, reply, connection, options...)
	}
}

func bearerStream(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, description *grpc.StreamDesc, connection *grpc.ClientConn, method string, streamer grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withBearer(ctx, token), description, connection, method, options...)
	}
}

func withBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func (c *Client) contextFor(ctx context.Context, mutation *worldv1.MutationMetadata) (context.Context, context.CancelFunc, error) {
	deadline := time.Now().Add(c.defaultTimeout)
	if current, ok := ctx.Deadline(); ok && current.Before(deadline) {
		deadline = current
	}
	if mutation != nil && mutation.Deadline != nil {
		if err := mutation.Deadline.CheckValid(); err != nil {
			return nil, nil, fmt.Errorf("mutation deadline: %w", err)
		}
		declared := mutation.Deadline.AsTime()
		if declared.Before(deadline) {
			deadline = declared
		}
	}
	bound, cancel := context.WithDeadline(ctx, deadline)
	return bound, cancel, nil
}

func unary[Response any](ctx context.Context, client *Client, mutation *worldv1.MutationMetadata, invoke func(context.Context) (*Response, error)) (*Response, error) {
	bound, cancel, err := client.contextFor(ctx, mutation)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := invoke(bound)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("world RPC returned a nil response")
	}
	return defensiveCopy(response)
}

func defensiveCopy[Value any](value *Value) (*Value, error) {
	if value == nil {
		return nil, nil
	}
	message, ok := any(value).(proto.Message)
	if !ok {
		return nil, fmt.Errorf("copy world response: %T is not a protobuf message", value)
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("copy world response: %w", err)
	}
	copy := new(Value)
	copyMessage, ok := any(copy).(proto.Message)
	if !ok {
		return nil, fmt.Errorf("copy world response: %T is not a protobuf message", copy)
	}
	if err := proto.Unmarshal(payload, copyMessage); err != nil {
		return nil, fmt.Errorf("copy world response: %w", err)
	}
	return copy, nil
}

func (c *Client) AcquireResearchSession(ctx context.Context, request *worldv1.AcquireResearchSessionRequest) (*worldv1.AcquireResearchSessionResponse, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.AcquireResearchSessionRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.AcquireResearchSessionResponse, error) {
		return c.rpc.AcquireResearchSession(bound, request)
	})
}
func (c *Client) GetResearchSession(ctx context.Context, request *worldv1.GetResearchSessionRequest) (*worldv1.ResearchSessionView, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.ResearchSessionView, error) {
		return c.rpc.GetResearchSession(bound, request)
	})
}
func (c *Client) WaitResearchSession(ctx context.Context, request *worldv1.WaitResearchSessionRequest) (*worldv1.ResearchSessionView, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.ResearchSessionView, error) {
		return c.rpc.WaitResearchSession(bound, request)
	})
}
func (c *Client) RenewLease(ctx context.Context, request *worldv1.RenewLeaseRequest) (*worldv1.Lease, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.RenewLeaseRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Lease, error) { return c.rpc.RenewLease(bound, request) })
}
func (c *Client) ReleaseResearchSession(ctx context.Context, request *worldv1.ReleaseResearchSessionRequest) (*worldv1.ReleaseOutcome, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.ReleaseResearchSessionRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.ReleaseOutcome, error) {
		return c.rpc.ReleaseResearchSession(bound, request)
	})
}
func (c *Client) CreateTarget(ctx context.Context, request *worldv1.CreateTargetRequest) (*worldv1.Target, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.CreateTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) { return c.rpc.CreateTarget(bound, request) })
}
func (c *Client) GetTarget(ctx context.Context, request *worldv1.GetTargetRequest) (*worldv1.Target, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.Target, error) { return c.rpc.GetTarget(bound, request) })
}
func (c *Client) StartTargetRun(ctx context.Context, request *worldv1.StartTargetRunRequest) (*worldv1.TargetRun, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.StartTargetRunRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetRun, error) { return c.rpc.StartTargetRun(bound, request) })
}
func (c *Client) WaitTargetRun(ctx context.Context, request *worldv1.WaitTargetRunRequest) (*worldv1.TargetRun, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.TargetRun, error) { return c.rpc.WaitTargetRun(bound, request) })
}
func (c *Client) ResetTarget(ctx context.Context, request *worldv1.ResetTargetRequest) (*worldv1.Target, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.ResetTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) { return c.rpc.ResetTarget(bound, request) })
}
func (c *Client) RequestRecovery(ctx context.Context, request *worldv1.RequestRecoveryRequest) (*worldv1.RecoveredResource, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.RequestRecoveryRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.RecoveredResource, error) {
		return c.rpc.RequestRecovery(bound, request)
	})
}
func (c *Client) StopTargetRun(ctx context.Context, request *worldv1.StopTargetRunRequest) (*worldv1.ObservationBundle, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.StopTargetRunRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.ObservationBundle, error) {
		return c.rpc.StopTargetRun(bound, request)
	})
}
func (c *Client) DestroyTarget(ctx context.Context, request *worldv1.DestroyTargetRequest) (*worldv1.Outcome, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.DestroyTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Outcome, error) {
		return c.rpc.DestroyTarget(bound, request)
	})
}
func (c *Client) QuarantineTarget(ctx context.Context, request *worldv1.QuarantineTargetRequest) (*worldv1.Target, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.QuarantineTargetRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) {
		return c.rpc.QuarantineTarget(bound, request)
	})
}
func (c *Client) GetIncident(ctx context.Context, request *worldv1.GetIncidentRequest) (*worldv1.Incident, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.Incident, error) { return c.rpc.GetIncident(bound, request) })
}
func (c *Client) CreateIncident(ctx context.Context, request *worldv1.CreateIncidentRequest) (*worldv1.Incident, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.CreateIncidentRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Incident, error) { return c.rpc.CreateIncident(bound, request) })
}
func (c *Client) GetExec(ctx context.Context, request *worldv1.GetExecRequest) (*worldv1.Exec, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.Exec, error) { return c.rpc.GetExec(bound, request) })
}
func (c *Client) CreateExec(ctx context.Context, request *worldv1.CreateExecRequest) (*worldv1.Exec, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.CreateExecRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Exec, error) {
		return c.rpc.CreateExec(bound, request)
	})
}
func (c *Client) TransitionExec(ctx context.Context, request *worldv1.TransitionExecRequest) (*worldv1.Exec, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.TransitionExecRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Exec, error) {
		return c.rpc.TransitionExec(bound, request)
	})
}
func (c *Client) FinalizeExec(ctx context.Context, request *worldv1.FinalizeExecRequest) (*worldv1.Exec, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.FinalizeExecRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Exec, error) {
		return c.rpc.FinalizeExec(bound, request)
	})
}
func (c *Client) GetLiveSnapshot(ctx context.Context, request *worldv1.GetLiveSnapshotRequest) (*worldv1.LiveSnapshot, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.LiveSnapshot, error) {
		return c.rpc.GetLiveSnapshot(bound, request)
	})
}
func (c *Client) GetObservationBundle(ctx context.Context, request *worldv1.GetObservationBundleRequest) (*worldv1.ObservationBundle, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.ObservationBundle, error) {
		return c.rpc.GetObservationBundle(bound, request)
	})
}
func (c *Client) StartCapture(ctx context.Context, request *worldv1.StartCaptureRequest) (*worldv1.Capture, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.StartCaptureRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Capture, error) {
		return c.rpc.StartCapture(bound, request)
	})
}
func (c *Client) RequestCapture(ctx context.Context, request *worldv1.RequestCaptureRequest) (*worldv1.Capture, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.RequestCaptureRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Capture, error) {
		return c.rpc.RequestCapture(bound, request)
	})
}
func (c *Client) StopCapture(ctx context.Context, request *worldv1.StopCaptureRequest) (*worldv1.Capture, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.StopCaptureRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Capture, error) {
		return c.rpc.StopCapture(bound, request)
	})
}
func (c *Client) DeclareExport(ctx context.Context, request *worldv1.DeclareExportRequest) (*worldv1.Export, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.DeclareExportRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Export, error) {
		return c.rpc.DeclareExport(bound, request)
	})
}
func (c *Client) PreviewChangeSet(ctx context.Context, request *worldv1.PreviewChangeSetRequest) (*worldv1.ChangeSet, error) {
	return unary(ctx, c, nil, func(bound context.Context) (*worldv1.ChangeSet, error) {
		return c.rpc.PreviewChangeSet(bound, request)
	})
}
func (c *Client) CommitExport(ctx context.Context, request *worldv1.CommitExportRequest) (*worldv1.Export, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.CommitExportRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Export, error) {
		return c.rpc.CommitExport(bound, request)
	})
}
func (c *Client) TransitionAgentGeneration(ctx context.Context, request *worldv1.TransitionAgentGenerationRequest) (*worldv1.AgentWorkspace, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.TransitionAgentGenerationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.AgentWorkspace, error) {
		return c.rpc.TransitionAgentGeneration(bound, request)
	})
}
func (c *Client) TransitionTargetGeneration(ctx context.Context, request *worldv1.TransitionTargetGenerationRequest) (*worldv1.Target, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.TransitionTargetGenerationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Target, error) {
		return c.rpc.TransitionTargetGeneration(bound, request)
	})
}
func (c *Client) TransitionTargetRun(ctx context.Context, request *worldv1.TransitionTargetRunRequest) (*worldv1.TargetRun, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.TransitionTargetRunRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetRun, error) {
		return c.rpc.TransitionTargetRun(bound, request)
	})
}
func (c *Client) CreateTargetOperation(ctx context.Context, request *worldv1.CreateTargetOperationRequest) (*worldv1.TargetOperation, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.CreateTargetOperationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetOperation, error) {
		return c.rpc.CreateTargetOperation(bound, request)
	})
}
func (c *Client) TransitionTargetOperation(ctx context.Context, request *worldv1.TransitionTargetOperationRequest) (*worldv1.TargetOperation, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.TransitionTargetOperationRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.TargetOperation, error) {
		return c.rpc.TransitionTargetOperation(bound, request)
	})
}
func (c *Client) TransitionIncident(ctx context.Context, request *worldv1.TransitionIncidentRequest) (*worldv1.Incident, error) {
	return unary(ctx, c, mutationOf(request, func(v *worldv1.TransitionIncidentRequest) *worldv1.MutationMetadata { return v.Mutation }), func(bound context.Context) (*worldv1.Incident, error) {
		return c.rpc.TransitionIncident(bound, request)
	})
}

func mutationOf[Request any](request *Request, get func(*Request) *worldv1.MutationMetadata) *worldv1.MutationMetadata {
	if request == nil {
		return nil
	}
	return get(request)
}

func (c *Client) SubscribeObservations(ctx context.Context, request *worldv1.SubscribeObservationsRequest) (worldv1.WorldService_SubscribeObservationsClient, error) {
	return c.rpc.SubscribeObservations(ctx, request)
}

func (c *Client) SubscribeMetrics(ctx context.Context, request *worldv1.SubscribeMetricsRequest) (worldv1.WorldService_SubscribeMetricsClient, error) {
	return c.rpc.SubscribeMetrics(ctx, request)
}

func (c *Client) OpenExec(ctx context.Context) (worldv1.WorldService_OpenExecClient, error) {
	return c.rpc.OpenExec(ctx)
}

func (c *Client) OpenTargetExec(ctx context.Context) (worldv1.WorldService_OpenTargetExecClient, error) {
	return c.rpc.OpenTargetExec(ctx)
}

func (c *Client) PushTargetFile(ctx context.Context) (worldv1.WorldService_PushTargetFileClient, error) {
	return c.rpc.PushTargetFile(ctx)
}

func (c *Client) PullTargetFile(ctx context.Context, request *worldv1.PullTargetFileRequest) (worldv1.WorldService_PullTargetFileClient, error) {
	return c.rpc.PullTargetFile(ctx, request)
}

func (c *Client) OpenTargetADB(ctx context.Context) (worldv1.WorldService_OpenTargetADBClient, error) {
	return c.rpc.OpenTargetADB(ctx)
}

// NewMutation creates unique idempotency and correlation identities and an
// explicit absolute deadline for a public mutation.
func NewMutation(authorizedPolicyReference string, deadline time.Time) (*worldv1.MutationMetadata, error) {
	if strings.TrimSpace(authorizedPolicyReference) == "" || deadline.IsZero() {
		return nil, fmt.Errorf("authorized policy reference and deadline are required")
	}
	correlation, err := domain.NewCorrelationID()
	if err != nil {
		return nil, err
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, err
	}
	protobufDeadline := timestamppb.New(deadline.UTC())
	if err := protobufDeadline.CheckValid(); err != nil {
		return nil, fmt.Errorf("mutation deadline: %w", err)
	}
	return &worldv1.MutationMetadata{IdempotencyKey: "idem_" + hex.EncodeToString(entropy[:]), CorrelationId: correlation.String(), AuthorizedPolicyReference: authorizedPolicyReference, Deadline: protobufDeadline}, nil
}
