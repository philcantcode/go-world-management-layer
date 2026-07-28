package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type protobufBoundaryCore struct {
	Core
	acquireCalls int
}

func (s *protobufBoundaryCore) Authorize(context.Context, application.AuthorizationRequest) error {
	return nil
}

func (s *protobufBoundaryCore) AcquireResearchSession(context.Context, application.AcquireRequest) (application.ResearchSessionView, error) {
	s.acquireCalls++
	return application.ResearchSessionView{}, nil
}

type protobufBoundaryCapabilities struct {
	worldv1.UnimplementedWorldServiceServer
	captureCalls int
	metricsCalls int
	exportCalls  int
}

func (s *protobufBoundaryCapabilities) StartCapture(context.Context, *worldv1.StartCaptureRequest) (*worldv1.Capture, error) {
	s.captureCalls++
	return &worldv1.Capture{CaptureId: "capture"}, nil
}

func (s *protobufBoundaryCapabilities) SubscribeMetrics(*worldv1.SubscribeMetricsRequest, worldv1.WorldService_SubscribeMetricsServer) error {
	s.metricsCalls++
	return nil
}

func (s *protobufBoundaryCapabilities) DeclareExport(context.Context, *worldv1.DeclareExportRequest) (*worldv1.Export, error) {
	s.exportCalls++
	return &worldv1.Export{ExportId: "export"}, nil
}

type protobufMetricsStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *protobufMetricsStream) Context() context.Context { return s.ctx }
func (*protobufMetricsStream) Send(*worldv1.MetricSample) error {
	return nil
}

func TestNativeWellKnownTypesRejectMalformedValues(t *testing.T) {
	invalidTimestamp := &timestamppb.Timestamp{Seconds: 253402300800}
	_, err := nativeTimestamp(invalidTimestamp, "observed_at", true)
	requireDomainFieldError(t, err, domain.CodeInvalidArgument, "observed_at")
	_, err = nativeTimestamp(nil, "observed_at", true)
	requireDomainFieldError(t, err, domain.CodeInvalidArgument, "observed_at")

	invalidDuration := &durationpb.Duration{Seconds: 1, Nanos: -1}
	_, err = nativeDuration(invalidDuration, "resolution", true)
	requireDomainFieldError(t, err, domain.CodeInvalidArgument, "resolution")
	validButTooLargeForNative := &durationpb.Duration{Seconds: 10_000_000_000}
	_, err = nativeDuration(validButTooLargeForNative, "resolution", true)
	requireDomainFieldError(t, err, domain.CodeInvalidArgument, "resolution")
	if value, err := nativeDuration(nil, "resolution", false); err != nil || value != 0 {
		t.Fatalf("optional duration = %s, %v", value, err)
	}
}

func TestRPCRejectsMalformedWellKnownTypesBeforeDelegation(t *testing.T) {
	core := &protobufBoundaryCore{}
	capabilities := &protobufBoundaryCapabilities{}
	server, err := NewWorldServer(core, ServerOptions{Capabilities: capabilities})
	if err != nil {
		t.Fatal(err)
	}
	stream := &protobufMetricsStream{ctx: testRPCContext()}

	invalidDuration := &durationpb.Duration{Seconds: 1, Nanos: -1}
	if err := server.SubscribeMetrics(&worldv1.SubscribeMetricsRequest{
		Filter: &worldv1.ObservationFilter{LeaseId: "lease"}, Resolution: invalidDuration,
	}, stream); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid metrics resolution code = %s, want %s (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	if capabilities.metricsCalls != 0 {
		t.Fatal("invalid metrics resolution reached capability service")
	}

	if _, err := server.StartCapture(testRPCContext(), &worldv1.StartCaptureRequest{
		Mutation: testWireMutation(), LeaseId: "lease", CaptureSpec: &worldv1.CaptureSpec{Duration: invalidDuration},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid capture duration code = %s, want %s (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	invalidDeadline := testWireMutation()
	invalidDeadline.Deadline = &timestamppb.Timestamp{Seconds: 253402300800}
	if _, err := server.StartCapture(testRPCContext(), &worldv1.StartCaptureRequest{
		Mutation: invalidDeadline, LeaseId: "lease", CaptureSpec: &worldv1.CaptureSpec{Duration: durationpb.New(time.Second)},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid mutation deadline code = %s, want %s (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	if capabilities.captureCalls != 0 {
		t.Fatal("malformed capture request reached capability service")
	}

	if _, err := server.StartCapture(testRPCContext(), &worldv1.StartCaptureRequest{
		Mutation: testWireMutation(), LeaseId: "lease", CaptureSpec: &worldv1.CaptureSpec{Duration: durationpb.New(time.Second)},
	}); err != nil {
		t.Fatalf("valid capture: %v", err)
	}
	if err := server.SubscribeMetrics(&worldv1.SubscribeMetricsRequest{
		Filter: &worldv1.ObservationFilter{LeaseId: "lease"}, Resolution: durationpb.New(time.Second),
	}, stream); err != nil {
		t.Fatalf("valid metrics subscription: %v", err)
	}
	if capabilities.captureCalls != 1 || capabilities.metricsCalls != 1 {
		t.Fatalf("valid delegation calls = capture:%d metrics:%d", capabilities.captureCalls, capabilities.metricsCalls)
	}
}

func TestRPCRejectsNilRepeatedMessagesBeforeDelegation(t *testing.T) {
	core := &protobufBoundaryCore{}
	capabilities := &protobufBoundaryCapabilities{}
	server, err := NewWorldServer(core, ServerOptions{Capabilities: capabilities})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := server.AcquireResearchSession(testRPCContext(), &worldv1.AcquireResearchSessionRequest{
		Mutation: testWireMutation(),
		InputView: &worldv1.InputViewSpec{
			FrozenSelectionRef: "selection",
			PathMappings:       []*worldv1.PathMapping{nil},
		},
		PolicyDigest: "sha256:policy",
		Ttl:          durationpb.New(time.Minute),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil path mapping code = %s, want %s (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	if core.acquireCalls != 0 {
		t.Fatal("nil path mapping reached application core")
	}

	if _, err := server.DeclareExport(testRPCContext(), &worldv1.DeclareExportRequest{
		Mutation: testWireMutation(), LeaseId: "lease", Paths: []*worldv1.ExportPath{nil},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil export path code = %s, want %s (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	if capabilities.exportCalls != 0 {
		t.Fatal("nil export path reached capability service")
	}

	_, err = applicationIncidentMetrics([]*worldv1.IncidentMetric{nil})
	requireDomainFieldError(t, err, domain.CodeInvalidArgument, "high_water_metrics[0]")
}

func requireDomainFieldError(t *testing.T, err error, code domain.ErrorCode, field string) {
	t.Helper()
	var typed *domain.Error
	if !errors.As(err, &typed) || typed.Code() != code || typed.Field() != field {
		t.Fatalf("error = %v, want domain code %s field %q", err, code, field)
	}
}
