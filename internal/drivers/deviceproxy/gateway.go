package deviceproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	adbHeaderBytes              = 4
	defaultMaximumConnections   = 8
	defaultConnectionDuration   = 10 * time.Minute
	defaultDialTimeout          = 5 * time.Second
	defaultMaximumStreamBytes   = int64(1 << 30)
	defaultADBServerVersion     = "0029"
	defaultGatewayListenAddress = "127.0.0.1:0"
	defaultADBUpstreamAddress   = "127.0.0.1:5037"
)

type ContextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type GatewayConfig struct {
	ListenAddress             string
	UpstreamAddress           string
	Dialer                    ContextDialer
	MaximumConnections        int
	MaximumConnectionDuration time.Duration
	DialTimeout               time.Duration
	MaximumStreamBytes        int64
	ServerVersion             string
	Features                  []string
}

// Gateway exposes a standard ADB-server socket on loopback. Possession of the
// returned endpoint is the local capability; every accepted service is still
// authorized against the endpoint's immutable Scope before reaching upstream.
type Gateway struct{ config GatewayConfig }

func NewGateway(config GatewayConfig) (*Gateway, error) {
	if config.ListenAddress == "" {
		config.ListenAddress = defaultGatewayListenAddress
	}
	if config.UpstreamAddress == "" {
		config.UpstreamAddress = defaultADBUpstreamAddress
	}
	if err := requireLoopbackTCPAddress(config.ListenAddress, true); err != nil {
		return nil, fmt.Errorf("listen address: %w", err)
	}
	if err := requireLoopbackTCPAddress(config.UpstreamAddress, false); err != nil {
		return nil, fmt.Errorf("upstream address: %w", err)
	}
	if config.MaximumConnections == 0 {
		config.MaximumConnections = defaultMaximumConnections
	}
	if config.MaximumConnections < 1 || config.MaximumConnections > 1024 {
		return nil, fmt.Errorf("maximum connections must be between 1 and 1024")
	}
	if config.MaximumConnectionDuration == 0 {
		config.MaximumConnectionDuration = defaultConnectionDuration
	}
	if config.MaximumConnectionDuration <= 0 {
		return nil, fmt.Errorf("maximum connection duration must be positive")
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = defaultDialTimeout
	}
	if config.DialTimeout <= 0 {
		return nil, fmt.Errorf("dial timeout must be positive")
	}
	if config.MaximumStreamBytes == 0 {
		config.MaximumStreamBytes = defaultMaximumStreamBytes
	}
	if config.MaximumStreamBytes <= 0 || config.MaximumStreamBytes == math.MaxInt64 {
		return nil, fmt.Errorf("maximum stream bytes must be positive and leave room for an overflow sentinel")
	}
	if config.ServerVersion == "" {
		config.ServerVersion = defaultADBServerVersion
	}
	if !isHexHeader(config.ServerVersion) {
		return nil, fmt.Errorf("server version must be four hexadecimal digits")
	}
	for _, feature := range config.Features {
		if feature == "" || strings.ContainsAny(feature, ",\x00\r\n\t ") {
			return nil, fmt.Errorf("ADB features must be non-blank delimiter-free tokens")
		}
	}
	if len(strings.Join(config.Features, ",")) > MaximumServiceBytes {
		return nil, fmt.Errorf("combined ADB features exceed the bounded response frame")
	}
	if config.Dialer == nil {
		config.Dialer = &net.Dialer{Timeout: config.DialTimeout}
	}
	config.Features = append([]string(nil), config.Features...)
	return &Gateway{config: config}, nil
}

func (g *Gateway) Open(ctx context.Context, scope Scope) (*Endpoint, error) {
	if ctx == nil {
		return nil, fmt.Errorf("open context is required")
	}
	if _, found := ctx.Deadline(); !found {
		return nil, fmt.Errorf("open context deadline is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", g.config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for scoped ADB: %w", err)
	}
	serverContext, cancel := context.WithCancel(context.Background())
	server := &endpointServer{
		config: g.config, scope: scope, listener: listener, context: serverContext, cancel: cancel,
		connections: make(map[net.Conn]struct{}), capacity: make(chan struct{}, g.config.MaximumConnections),
	}
	authorizer, err := NewAuthorizer(scope, func() bool { return serverContext.Err() == nil })
	if err != nil {
		_ = listener.Close()
		cancel()
		return nil, err
	}
	server.authorizer = authorizer
	server.wait.Add(1)
	go server.accept()
	return NewEndpoint(scope.Serial, listener.Addr().String(), server.close), nil
}

type endpointServer struct {
	config     GatewayConfig
	scope      Scope
	listener   net.Listener
	context    context.Context
	cancel     context.CancelFunc
	authorizer *Authorizer
	capacity   chan struct{}

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	wait        sync.WaitGroup
	closeOnce   sync.Once
	closeErr    error
}

func (s *endpointServer) accept() {
	defer s.wait.Done()
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		select {
		case s.capacity <- struct{}{}:
		default:
			_ = connection.Close()
			continue
		}
		s.mu.Lock()
		if s.context.Err() != nil {
			s.mu.Unlock()
			<-s.capacity
			_ = connection.Close()
			return
		}
		s.connections[connection] = struct{}{}
		s.mu.Unlock()
		s.wait.Add(1)
		go s.serve(connection)
	}
}

func (s *endpointServer) serve(client net.Conn) {
	defer s.wait.Done()
	defer func() {
		s.mu.Lock()
		delete(s.connections, client)
		s.mu.Unlock()
		<-s.capacity
		_ = client.Close()
	}()
	_ = client.SetDeadline(time.Now().Add(s.config.MaximumConnectionDuration))
	service, err := readADBRequest(client)
	if err != nil {
		return
	}
	decision, err := s.authorizer.Authorize(Request{Credential: s.scope.Credential, Service: service})
	if err != nil {
		_ = writeADBFailure(client, "service denied by scoped gateway")
		return
	}
	switch decision.Action {
	case SynthesizeHost:
		_ = s.synthesize(client, decision.Service)
	case SelectDevice:
		_ = s.serveSelected(client, decision)
	case ForwardDevice:
		_ = s.serveForward(client, decision)
	default:
		_ = writeADBFailure(client, "unsupported scoped gateway action")
	}
}

func (s *endpointServer) serveSelected(client net.Conn, decision Decision) error {
	upstream, err := s.dialUpstream()
	if err != nil {
		return writeADBFailure(client, "upstream ADB server unavailable")
	}
	defer s.releaseUpstream(upstream)
	if err := writeADBRequest(upstream, "host:transport:"+decision.Serial); err != nil {
		return err
	}
	okay, payload, err := readADBStatus(upstream)
	if err != nil {
		return err
	}
	if err := writeADBStatus(client, okay, payload); err != nil || !okay {
		return err
	}
	if strings.HasPrefix(decision.Service, "host:tport:serial:") {
		var transportID [8]byte
		binary.LittleEndian.PutUint64(transportID[:], 1)
		if _, err := client.Write(transportID[:]); err != nil {
			return err
		}
	}
	service, err := readADBRequest(client)
	if err != nil {
		return err
	}
	next, err := s.authorizer.Authorize(Request{Credential: s.scope.Credential, Serial: s.scope.Serial, Service: service})
	if err != nil || next.Action != ForwardDevice || strings.HasPrefix(next.Service, "host-serial:") {
		return writeADBFailure(client, "selected transport accepts only device services")
	}
	if err := writeADBRequest(upstream, next.Service); err != nil {
		return err
	}
	return s.relayResponseAndStream(client, upstream)
}

func (s *endpointServer) serveForward(client net.Conn, decision Decision) error {
	upstream, err := s.dialUpstream()
	if err != nil {
		return writeADBFailure(client, "upstream ADB server unavailable")
	}
	defer s.releaseUpstream(upstream)
	if strings.HasPrefix(decision.Service, "host-serial:") {
		if err := writeADBRequest(upstream, decision.Service); err != nil {
			return err
		}
		return s.relayResponseAndStream(client, upstream)
	}
	if err := writeADBRequest(upstream, "host:transport:"+decision.Serial); err != nil {
		return err
	}
	okay, payload, err := readADBStatus(upstream)
	if err != nil {
		return err
	}
	if !okay {
		return writeADBStatus(client, false, payload)
	}
	if err := writeADBRequest(upstream, decision.Service); err != nil {
		return err
	}
	return s.relayResponseAndStream(client, upstream)
}

func (s *endpointServer) relayResponseAndStream(client, upstream net.Conn) error {
	okay, payload, err := readADBStatus(upstream)
	if err != nil {
		return err
	}
	if err := writeADBStatus(client, okay, payload); err != nil || !okay {
		return err
	}
	return relayBounded(client, upstream, s.config.MaximumStreamBytes)
}

func (s *endpointServer) synthesize(client net.Conn, service string) error {
	if err := writeADBStatus(client, true, nil); err != nil {
		return err
	}
	var payload string
	switch service {
	case "host:version":
		payload = s.config.ServerVersion
	case "host:features":
		payload = strings.Join(s.config.Features, ",")
	case "host:devices", "host:track-devices":
		payload = s.scope.Serial + "\tdevice\n"
	case "host:devices-l", "host:track-devices-l":
		payload = s.scope.Serial + "\tdevice product:scoped model:scoped device:scoped transport_id:1\n"
	default:
		return fmt.Errorf("unsupported synthetic host service %q", service)
	}
	if err := writeADBPayload(client, []byte(payload)); err != nil {
		return err
	}
	if service != "host:track-devices" && service != "host:track-devices-l" {
		return nil
	}
	disconnected := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, client)
		close(disconnected)
	}()
	select {
	case <-s.context.Done():
	case <-disconnected:
	case <-time.After(s.config.MaximumConnectionDuration):
	}
	return nil
}

func (s *endpointServer) dialUpstream() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(s.context, s.config.DialTimeout)
	defer cancel()
	connection, err := s.config.Dialer.DialContext(ctx, "tcp", s.config.UpstreamAddress)
	if err != nil {
		return nil, err
	}
	_ = connection.SetDeadline(time.Now().Add(s.config.MaximumConnectionDuration))
	s.mu.Lock()
	if s.context.Err() != nil {
		s.mu.Unlock()
		_ = connection.Close()
		return nil, s.context.Err()
	}
	s.connections[connection] = struct{}{}
	s.mu.Unlock()
	return connection, nil
}

func (s *endpointServer) releaseUpstream(connection net.Conn) {
	s.mu.Lock()
	delete(s.connections, connection)
	s.mu.Unlock()
	_ = connection.Close()
}

func (s *endpointServer) close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.listener.Close()
		s.mu.Lock()
		connections := make([]net.Conn, 0, len(s.connections))
		for connection := range s.connections {
			connections = append(connections, connection)
		}
		s.mu.Unlock()
		for _, connection := range connections {
			_ = connection.Close()
		}
		s.wait.Wait()
		if errors.Is(s.closeErr, net.ErrClosed) {
			s.closeErr = nil
		}
	})
	return s.closeErr
}

func readADBRequest(reader io.Reader) (string, error) {
	length, err := readADBLength(reader)
	if err != nil {
		return "", err
	}
	if length == 0 || length > MaximumServiceBytes {
		return "", fmt.Errorf("ADB service length %d is outside the bounded frame", length)
	}
	value := make([]byte, length)
	if _, err := io.ReadFull(reader, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func writeADBRequest(writer io.Writer, service string) error {
	if service == "" || len(service) > MaximumServiceBytes {
		return fmt.Errorf("ADB service is empty or too large")
	}
	if _, err := fmt.Fprintf(writer, "%04X", len(service)); err != nil {
		return err
	}
	_, err := io.WriteString(writer, service)
	return err
}

func readADBLength(reader io.Reader) (int, error) {
	var header [adbHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, err
	}
	if !isHexHeader(string(header[:])) {
		return 0, fmt.Errorf("invalid ADB frame length %q", header)
	}
	value, err := strconv.ParseUint(string(header[:]), 16, 16)
	return int(value), err
}

func writeADBPayload(writer io.Writer, payload []byte) error {
	if len(payload) > MaximumServiceBytes {
		return fmt.Errorf("ADB payload is too large")
	}
	if _, err := fmt.Fprintf(writer, "%04X", len(payload)); err != nil {
		return err
	}
	_, err := writer.Write(payload)
	return err
}

func readADBStatus(reader io.Reader) (bool, []byte, error) {
	var status [adbHeaderBytes]byte
	if _, err := io.ReadFull(reader, status[:]); err != nil {
		return false, nil, err
	}
	switch string(status[:]) {
	case "OKAY":
		return true, nil, nil
	case "FAIL":
		length, err := readADBLength(reader)
		if err != nil || length > MaximumServiceBytes {
			return false, nil, fmt.Errorf("invalid ADB failure payload: %w", err)
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return false, nil, err
		}
		return false, payload, nil
	default:
		return false, nil, fmt.Errorf("invalid ADB status %q", status)
	}
}

func writeADBStatus(writer io.Writer, okay bool, payload []byte) error {
	if okay {
		_, err := io.WriteString(writer, "OKAY")
		return err
	}
	if _, err := io.WriteString(writer, "FAIL"); err != nil {
		return err
	}
	return writeADBPayload(writer, payload)
}

func writeADBFailure(writer io.Writer, message string) error {
	return writeADBStatus(writer, false, []byte(message))
}

func relayBounded(client, upstream net.Conn, maximum int64) error {
	type copyResult struct{ err error }
	results := make(chan copyResult, 2)
	copyOne := func(destination, source net.Conn) {
		limited := &io.LimitedReader{R: source, N: maximum + 1}
		copied, err := io.Copy(destination, limited)
		if copied > maximum {
			err = fmt.Errorf("ADB stream exceeded %d bytes", maximum)
		}
		if closer, ok := destination.(interface{ CloseWrite() error }); ok {
			_ = closer.CloseWrite()
		}
		results <- copyResult{err: err}
	}
	go copyOne(upstream, client)
	go copyOne(client, upstream)
	first := <-results
	second := <-results
	return errors.Join(first.err, second.err)
}

func requireLoopbackTCPAddress(address string, allowZero bool) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("address must use an explicit loopback host")
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 0 || port > 65535 || (!allowZero && port == 0) {
		return fmt.Errorf("address has an invalid port")
	}
	return nil
}

func isHexHeader(value string) bool {
	if len(value) != adbHeaderBytes {
		return false
	}
	for _, character := range value {
		if character >= '0' && character <= '9' || character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F' {
			continue
		}
		return false
	}
	return true
}
