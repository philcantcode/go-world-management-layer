package rpc

import (
	"testing"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCBoundaryRejectsMixedStartFrames(t *testing.T) {
	tests := []struct {
		name  string
		valid func() error
		mixed func() error
	}{
		{"agent exec", func() error { return requireExecStartFrame(&worldv1.ExecFrame{Start: &worldv1.ExecStart{}}) }, func() error {
			return requireExecStartFrame(&worldv1.ExecFrame{Start: &worldv1.ExecStart{}, Outcome: &worldv1.ExecOutcome{}})
		}},
		{"target exec", func() error {
			return requireTargetExecStartFrame(&worldv1.TargetExecFrame{Start: &worldv1.TargetExecStart{}})
		}, func() error {
			return requireTargetExecStartFrame(&worldv1.TargetExecFrame{Start: &worldv1.TargetExecStart{}, Signal: "TERM"})
		}},
		{"file transfer", func() error {
			return requireFileTransferStartFrame(&worldv1.FileTransferFrame{Start: &worldv1.FileTransferStart{}})
		}, func() error {
			return requireFileTransferStartFrame(&worldv1.FileTransferFrame{Start: &worldv1.FileTransferStart{}, Offset: 1})
		}},
		{"ADB", func() error { return requireADBStartFrame(&worldv1.ADBFrame{Start: &worldv1.ADBStart{}}) }, func() error {
			return requireADBStartFrame(&worldv1.ADBFrame{Start: &worldv1.ADBStart{}, ServerBytes: []byte("server-only")})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.valid(); err != nil {
				t.Fatalf("valid start error = %v", err)
			}
			if code := status.Code(test.mixed()); code != codes.InvalidArgument {
				t.Fatalf("mixed frame code = %s, want %s", code, codes.InvalidArgument)
			}
		})
	}
	if code := status.Code(requireExecStartFrame(nil)); code != codes.InvalidArgument {
		t.Fatalf("nil frame code = %s, want %s", code, codes.InvalidArgument)
	}
}
