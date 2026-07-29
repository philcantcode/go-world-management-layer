package orchestration

import (
	"errors"
	"net"
	"testing"
)

type targetIOErrorCloser struct {
	err error
}

func (closer targetIOErrorCloser) Close() error {
	return closer.err
}

func TestCloseTargetADBResourcesAcceptsAlreadyClosedAndPreservesOtherFailures(t *testing.T) {
	closeFailure := errors.New("close failed")
	err := closeTargetADBResources(
		targetIOErrorCloser{},
		targetIOErrorCloser{err: net.ErrClosed},
		targetIOErrorCloser{err: closeFailure},
	)
	if !errors.Is(err, closeFailure) {
		t.Fatalf("close error = %v, want %v", err, closeFailure)
	}
	if errors.Is(err, net.ErrClosed) {
		t.Fatalf("already-closed transport leaked into cleanup failure: %v", err)
	}
	if err := closeTargetADBResources(nil, targetIOErrorCloser{}, targetIOErrorCloser{err: net.ErrClosed}); err != nil {
		t.Fatalf("successful/idempotent cleanup error = %v", err)
	}
	local, peer := net.Pipe()
	defer peer.Close()
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closeTargetADBResources(local); err != nil {
		t.Fatalf("repeated real connection close error = %v", err)
	}
}
