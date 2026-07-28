package deviceproxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestGatewaySynthesizesScopedHostQueriesAndForwardsExactDevice(t *testing.T) {
	upstream := newFakeADBUpstream(t)
	defer upstream.Close()
	gateway, err := NewGateway(GatewayConfig{
		UpstreamAddress: upstream.Address(), MaximumConnectionDuration: 2 * time.Second,
		MaximumStreamBytes: 1 << 20, Features: []string{"shell_v2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := gatewayTestScope(t, "emulator-5554")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := gateway.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()

	devices := gatewayRequest(t, endpoint.Address(), "host:devices-l")
	if !strings.Contains(string(devices), scope.Serial+"\tdevice") || strings.Contains(string(devices), "other-device") {
		t.Fatalf("scoped devices response = %q", devices)
	}
	if services := upstream.Services(); len(services) != 0 {
		t.Fatalf("synthetic host query reached upstream: %v", services)
	}

	output := gatewayRequest(t, endpoint.Address(), "shell:echo scoped")
	if string(output) != "scoped-output" {
		t.Fatalf("device output = %q", output)
	}
	services := upstream.Services()
	want := []string{"host:transport:" + scope.Serial, "shell:echo scoped"}
	if len(services) != len(want) || services[0] != want[0] || services[1] != want[1] {
		t.Fatalf("upstream services = %v, want %v", services, want)
	}
}

func TestGatewaySupportsExplicitAssignedTransportAndDeniesAuthorityEscape(t *testing.T) {
	upstream := newFakeADBUpstream(t)
	defer upstream.Close()
	gateway, err := NewGateway(GatewayConfig{UpstreamAddress: upstream.Address(), MaximumConnectionDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	scope := gatewayTestScope(t, "emulator-5554")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := gateway.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()

	connection := dialGateway(t, endpoint.Address())
	if err := writeADBRequest(connection, "host:transport:"+scope.Serial); err != nil {
		t.Fatal(err)
	}
	if okay, _, err := readADBStatus(connection); err != nil || !okay {
		t.Fatalf("transport status = %v, %v", okay, err)
	}
	if err := writeADBRequest(connection, "shell:echo scoped"); err != nil {
		t.Fatal(err)
	}
	if okay, _, err := readADBStatus(connection); err != nil || !okay {
		t.Fatalf("service status = %v, %v", okay, err)
	}
	output, err := io.ReadAll(connection)
	if err != nil || string(output) != "scoped-output" {
		t.Fatalf("selected service output = %q, %v", output, err)
	}
	_ = connection.Close()

	connection = dialGateway(t, endpoint.Address())
	if err := writeADBRequest(connection, "host:tport:serial:"+scope.Serial); err != nil {
		t.Fatal(err)
	}
	if okay, _, err := readADBStatus(connection); err != nil || !okay {
		t.Fatalf("modern transport status = %v, %v", okay, err)
	}
	var transportID [8]byte
	if _, err := io.ReadFull(connection, transportID[:]); err != nil || binary.LittleEndian.Uint64(transportID[:]) != 1 {
		t.Fatalf("modern transport id = %x, %v", transportID, err)
	}
	if err := writeADBRequest(connection, "shell:echo scoped"); err != nil {
		t.Fatal(err)
	}
	if okay, _, err := readADBStatus(connection); err != nil || !okay {
		t.Fatalf("modern service status = %v, %v", okay, err)
	}
	if output, err := io.ReadAll(connection); err != nil || string(output) != "scoped-output" {
		t.Fatalf("modern selected service output = %q, %v", output, err)
	}
	_ = connection.Close()

	for _, service := range []string{"host:kill", "host:transport:other-device", "host:tport:serial:other-device", "host:tport:any", "host:transport-id:2", "host-serial:other-device:shell:id"} {
		connection := dialGateway(t, endpoint.Address())
		if err := writeADBRequest(connection, service); err != nil {
			t.Fatal(err)
		}
		okay, _, err := readADBStatus(connection)
		_ = connection.Close()
		if err != nil || okay {
			t.Fatalf("escape service %q status = %v, %v", service, okay, err)
		}
	}
}

func TestGatewayForwardsNULFramedModernDeviceServiceAfterExactSelection(t *testing.T) {
	upstream := newFakeADBUpstream(t)
	defer upstream.Close()
	gateway, err := NewGateway(GatewayConfig{UpstreamAddress: upstream.Address(), MaximumConnectionDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	scope := gatewayTestScope(t, "emulator-5554")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := gateway.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	service := "abb_exec:package\x00install-create\x00-S\x00123"
	if output := gatewayRequest(t, endpoint.Address(), service); len(output) != 0 {
		t.Fatalf("unexpected modern device service output %q", output)
	}
	services := upstream.Services()
	want := []string{"host:transport:" + scope.Serial, service}
	if len(services) != len(want) || services[0] != want[0] || services[1] != want[1] {
		t.Fatalf("upstream services = %#v, want %#v", services, want)
	}
}

func TestGatewayCloseRevokesListenerAndActiveStreams(t *testing.T) {
	upstream := newFakeADBUpstream(t)
	defer upstream.Close()
	gateway, err := NewGateway(GatewayConfig{UpstreamAddress: upstream.Address(), MaximumConnectionDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	scope := gatewayTestScope(t, "emulator-5554")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := gateway.Open(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	connection := dialGateway(t, endpoint.Address())
	if err := writeADBRequest(connection, "shell:hold"); err != nil {
		t.Fatal(err)
	}
	if okay, _, err := readADBStatus(connection); err != nil || !okay {
		t.Fatalf("hold status = %v, %v", okay, err)
	}
	closed := make(chan error, 1)
	go func() { closed <- endpoint.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("endpoint close did not revoke active streams")
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("revoked client stream remained readable")
	}
	_ = connection.Close()
	if connection, err := net.DialTimeout("tcp", endpoint.Address(), 50*time.Millisecond); err == nil {
		_ = connection.Close()
		t.Fatal("revoked endpoint still accepted connections")
	}
}

func TestGatewayRejectsNonLoopbackAndMalformedFrames(t *testing.T) {
	if _, err := NewGateway(GatewayConfig{ListenAddress: "0.0.0.0:0"}); err == nil {
		t.Fatal("non-loopback listener was accepted")
	}
	upstream := newFakeADBUpstream(t)
	defer upstream.Close()
	gateway, err := NewGateway(GatewayConfig{UpstreamAddress: upstream.Address(), MaximumConnectionDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := gateway.Open(ctx, gatewayTestScope(t, "emulator-5554"))
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	connection := dialGateway(t, endpoint.Address())
	_, _ = connection.Write([]byte("GGGG"))
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadAll(connection); err == nil {
		// EOF is also a successful ReadAll result; either way the server must
		// close without forwarding the malformed frame.
	}
	_ = connection.Close()
	if len(upstream.Services()) != 0 {
		t.Fatal("malformed frame reached upstream")
	}
}

func gatewayRequest(t *testing.T, address, service string) []byte {
	t.Helper()
	connection := dialGateway(t, address)
	defer connection.Close()
	if err := writeADBRequest(connection, service); err != nil {
		t.Fatal(err)
	}
	okay, payload, err := readADBStatus(connection)
	if err != nil || !okay {
		t.Fatalf("gateway status = %v/%q, %v", okay, payload, err)
	}
	if strings.HasPrefix(service, "host:") {
		length, err := readADBLength(connection)
		if err != nil {
			t.Fatal(err)
		}
		result := make([]byte, length)
		if _, err := io.ReadFull(connection, result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	result, err := io.ReadAll(connection)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func dialGateway(t *testing.T, address string) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return connection
}

func gatewayTestScope(t *testing.T, serial string) Scope {
	t.Helper()
	leaseID, _ := domain.NewLeaseID()
	targetID, _ := domain.NewTargetID()
	runID, _ := domain.NewTargetRunID()
	scope, err := IssueScope(leaseID, targetID, 1, runID, serial, bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

type fakeADBUpstream struct {
	listener net.Listener
	wait     sync.WaitGroup
	mu       sync.Mutex
	services []string
}

func newFakeADBUpstream(t *testing.T) *fakeADBUpstream {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeADBUpstream{listener: listener}
	server.wait.Add(1)
	go server.accept()
	return server
}

func (s *fakeADBUpstream) Address() string { return s.listener.Addr().String() }

func (s *fakeADBUpstream) Services() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.services...)
}

func (s *fakeADBUpstream) Close() {
	_ = s.listener.Close()
	s.wait.Wait()
}

func (s *fakeADBUpstream) accept() {
	defer s.wait.Done()
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wait.Add(1)
		go s.serve(connection)
	}
}

func (s *fakeADBUpstream) serve(connection net.Conn) {
	defer s.wait.Done()
	defer connection.Close()
	for {
		service, err := readADBRequest(connection)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.services = append(s.services, service)
		s.mu.Unlock()
		if strings.HasPrefix(service, "host:transport:") {
			if strings.TrimPrefix(service, "host:transport:") != "emulator-5554" {
				_ = writeADBFailure(connection, "unknown serial")
				return
			}
			_, _ = io.WriteString(connection, "OKAY")
			continue
		}
		_, _ = io.WriteString(connection, "OKAY")
		switch service {
		case "shell:echo scoped":
			_, _ = io.WriteString(connection, "scoped-output")
			return
		case "shell:hold":
			_, _ = io.Copy(io.Discard, connection)
			return
		default:
			return
		}
	}
}
