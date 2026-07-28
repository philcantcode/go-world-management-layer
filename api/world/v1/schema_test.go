package worldv1

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGeneratedSchemaContract(t *testing.T) {
	file := File_world_v1_world_proto
	if got, want := file.Path(), "world/v1/world.proto"; got != want {
		t.Fatalf("descriptor path = %q, want %q", got, want)
	}
	service := file.Services().ByName("WorldService")
	if service == nil {
		t.Fatal("WorldService descriptor is missing")
	}
	if got, want := service.Methods().Len(), 41; got != want {
		t.Fatalf("WorldService method count = %d, want %d", got, want)
	}
	reset := file.Messages().ByName("ResetTargetRequest")
	if reset == nil {
		t.Fatal("ResetTargetRequest descriptor is missing")
	}
	requireField(t, reset, "reset_mode", 4, protoreflect.StringKind, "")
	requireField(t, reset, "snapshot_name", 6, protoreflect.StringKind, "")
	mutation := file.Messages().ByName("MutationMetadata")
	requireField(t, mutation, "deadline", 5, protoreflect.MessageKind, "google.protobuf.Timestamp")
	lease := file.Messages().ByName("Lease")
	requireField(t, lease, "termination", 13, protoreflect.MessageKind, "world.v1.LeaseTermination")
	termination := file.Messages().ByName("LeaseTermination")
	requireField(t, termination, "kind", 1, protoreflect.StringKind, "")
	requireField(t, termination, "state", 2, protoreflect.StringKind, "")
	requireField(t, termination, "reason", 3, protoreflect.StringKind, "")
	requireField(t, termination, "begin_idempotency_key", 4, protoreflect.StringKind, "")
	requireField(t, termination, "begin_request_digest", 5, protoreflect.StringKind, "")
	requireField(t, termination, "initiated_lease_revision", 6, protoreflect.Uint64Kind, "")
	requireField(t, termination, "initiated_at", 7, protoreflect.MessageKind, "google.protobuf.Timestamp")
	requireField(t, termination, "complete_idempotency_key", 8, protoreflect.StringKind, "")
	requireField(t, termination, "complete_request_digest", 9, protoreflect.StringKind, "")
	requireField(t, termination, "completed_at", 10, protoreflect.MessageKind, "google.protobuf.Timestamp")
}

func TestLeaseTerminationRoundTripsBinaryAndProtoJSON(t *testing.T) {
	initiated := timestamppb.New(time.Date(2026, time.July, 27, 12, 30, 0, 0, time.UTC))
	completed := timestamppb.New(time.Date(2026, time.July, 27, 12, 31, 0, 0, time.UTC))
	want := &Lease{
		LeaseId: "lease_1", State: "expired", Revision: 3,
		Termination: &LeaseTermination{
			Kind: "expiry", State: "expired", Reason: "lease lifetime elapsed",
			BeginIdempotencyKey: "expiry/lease_1", BeginRequestDigest: "sha256:begin",
			InitiatedLeaseRevision: 2, InitiatedAt: initiated,
			CompleteIdempotencyKey: "termination/lease_1", CompleteRequestDigest: "sha256:complete",
			CompletedAt: completed,
		},
	}
	binary, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var binaryRoundTrip Lease
	if err := proto.Unmarshal(binary, &binaryRoundTrip); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &binaryRoundTrip) {
		t.Fatalf("binary round trip = %v, want %v", &binaryRoundTrip, want)
	}
	jsonPayload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var jsonRoundTrip Lease
	if err := protojson.Unmarshal(jsonPayload, &jsonRoundTrip); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &jsonRoundTrip) {
		t.Fatalf("protojson round trip = %v, want %v", &jsonRoundTrip, want)
	}
}

func requireField(t *testing.T, message protoreflect.MessageDescriptor, name protoreflect.Name, number protoreflect.FieldNumber, kind protoreflect.Kind, messageType protoreflect.FullName) {
	t.Helper()
	if message == nil {
		t.Fatalf("message containing %s is missing", name)
	}
	field := message.Fields().ByName(name)
	if field == nil {
		t.Fatalf("%s.%s is missing", message.Name(), name)
	}
	if field.Number() != number || field.Kind() != kind {
		t.Fatalf("%s.%s = field %d/%s, want %d/%s", message.Name(), name, field.Number(), field.Kind(), number, kind)
	}
	if messageType != "" && (field.Message() == nil || field.Message().FullName() != messageType) {
		t.Fatalf("%s.%s message type = %v, want %s", message.Name(), name, field.Message(), messageType)
	}
}

func TestGeneratedMessagesRoundTripBinaryAndProtoJSON(t *testing.T) {
	deadline := timestamppb.New(time.Date(2026, time.July, 27, 12, 30, 0, 123, time.UTC))
	want := &ResetTargetRequest{
		Mutation: &MutationMetadata{IdempotencyKey: "idem", CorrelationId: "corr", Deadline: deadline},
		TargetId: "target_1", ExpectedRevision: 9, ResetMode: ResetModeSnapshot, SnapshotName: "known-good",
	}
	binary, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var binaryRoundTrip ResetTargetRequest
	if err := proto.Unmarshal(binary, &binaryRoundTrip); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &binaryRoundTrip) {
		t.Fatalf("binary round trip = %v, want %v", &binaryRoundTrip, want)
	}
	jsonPayload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var jsonRoundTrip ResetTargetRequest
	if err := protojson.Unmarshal(jsonPayload, &jsonRoundTrip); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, &jsonRoundTrip) {
		t.Fatalf("protojson round trip = %v, want %v", &jsonRoundTrip, want)
	}
}

type generatedTransportServer struct {
	UnimplementedWorldServiceServer
}

func (*generatedTransportServer) GetResearchSession(_ context.Context, request *GetResearchSessionRequest) (*ResearchSessionView, error) {
	return &ResearchSessionView{Session: &ResearchSession{ResearchSessionId: request.ResearchSessionId}}, nil
}

func TestGeneratedClientAndServerUseDefaultProtobufTransport(t *testing.T) {
	listener := bufconn.Listen(DefaultMaxMessageSize)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(DefaultMaxMessageSize), grpc.MaxSendMsgSize(DefaultMaxMessageSize))
	RegisterWorldServiceServer(server, &generatedTransportServer{})
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	go func() { _ = server.Serve(listener) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(DefaultMaxMessageSize), grpc.MaxCallSendMsgSize(DefaultMaxMessageSize)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := NewWorldServiceClient(connection)
	view, err := client.GetResearchSession(ctx, &GetResearchSessionRequest{ResearchSessionId: "session_1"})
	if err != nil {
		t.Fatal(err)
	}
	if view.GetSession().GetResearchSessionId() != "session_1" {
		t.Fatalf("default protobuf response = %v", view)
	}

	_, err = client.GetResearchSession(ctx, &GetResearchSessionRequest{ResearchSessionId: string(make([]byte, DefaultMaxMessageSize))})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized protobuf request code = %s, want %s (err=%v)", status.Code(err), codes.ResourceExhausted, err)
	}
}
