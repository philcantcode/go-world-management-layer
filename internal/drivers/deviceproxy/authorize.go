// Package deviceproxy defines the authorization boundary for a one-device ADB
// gateway. It authorizes ADB protocol service strings, not shell semantics:
// arbitrary services inside the assigned device remain opaque and permitted.
package deviceproxy

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// ADB service requests use a four-hex-digit length prefix, so 0xffff is the
// largest service that can be represented by the wire protocol.
const MaximumServiceBytes = 0xffff

type Scope struct {
	LeaseID    domain.LeaseID
	TargetID   domain.TargetID
	Generation domain.TargetGeneration
	RunID      domain.TargetRunID
	Serial     string
	Credential string
}

func IssueScope(leaseID domain.LeaseID, targetID domain.TargetID, generation domain.TargetGeneration, runID domain.TargetRunID, serial string, random io.Reader) (Scope, error) {
	if leaseID.IsZero() || targetID.IsZero() || !generation.IsValid() || runID.IsZero() || serial == "" {
		return Scope{}, domain.NewError(domain.CodeInvalidArgument, "adb_scope.issue", "scope", "lease, target, generation, run, and serial are required", nil)
	}
	if !safeADBSerial(serial) {
		return Scope{}, domain.NewError(domain.CodeInvalidArgument, "adb_scope.issue", "serial", "contains an unsafe delimiter", nil)
	}
	if random == nil {
		random = rand.Reader
	}
	secret := make([]byte, 32)
	if _, err := io.ReadFull(random, secret); err != nil {
		return Scope{}, domain.NewError(domain.CodeUnavailable, "adb_scope.issue", "credential", "could not generate a run credential", err)
	}
	return Scope{LeaseID: leaseID, TargetID: targetID, Generation: generation, RunID: runID, Serial: serial, Credential: base64.RawURLEncoding.EncodeToString(secret)}, nil
}

func (s Scope) Validate() error {
	if s.LeaseID.IsZero() || s.TargetID.IsZero() || !s.Generation.IsValid() || s.RunID.IsZero() || !safeADBSerial(s.Serial) || s.Credential == "" {
		return domain.NewError(domain.CodeInvalidArgument, "adb_scope.validate", "scope", "scope is incomplete", nil)
	}
	return nil
}

func safeADBSerial(serial string) bool {
	if serial == "" {
		return false
	}
	for _, value := range serial {
		if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' {
			continue
		}
		switch value {
		case '.', '_', '-', ':':
			continue
		default:
			return false
		}
	}
	return true
}

type Action string

const (
	ForwardDevice  Action = "forward_device"
	SelectDevice   Action = "select_device"
	SynthesizeHost Action = "synthesize_host"
)

type Request struct {
	Credential string
	Serial     string
	Service    string
}

type Decision struct {
	Action  Action
	Serial  string
	Service string
}

type Authorizer struct {
	scope  Scope
	active func() bool
}

func NewAuthorizer(scope Scope, active func() bool) (*Authorizer, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if active == nil {
		active = func() bool { return true }
	}
	return &Authorizer{scope: scope, active: active}, nil
}

func (a *Authorizer) Authorize(request Request) (Decision, error) {
	if !a.active() {
		return Decision{}, domain.NewError(domain.CodeFailedPrecondition, "adb.authorize", "run", "target run is not active", nil)
	}
	if subtle.ConstantTimeCompare([]byte(request.Credential), []byte(a.scope.Credential)) != 1 {
		return Decision{}, domain.NewError(domain.CodeUnauthorized, "adb.authorize", "credential", "run credential is invalid", nil)
	}
	if request.Serial != "" && request.Serial != a.scope.Serial {
		return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "serial", "another device serial cannot be selected", nil)
	}
	if request.Service == "" || len(request.Service) > MaximumServiceBytes {
		return Decision{}, domain.NewError(domain.CodeInvalidArgument, "adb.authorize", "service", "service is empty or too large", nil)
	}
	return a.authorizeService(request.Service)
}

func (a *Authorizer) authorizeService(service string) (Decision, error) {
	// Modern device services such as abb_exec carry NUL-separated arguments
	// inside the ADB length-framed service. Those bytes are opaque guest
	// authority and are safe only after the exact device has been selected.
	// Host queries and selectors remain delimiter-free so a NUL can never make
	// the gateway and upstream ADB server disagree about the selected device.
	hasNUL := strings.IndexByte(service, 0) >= 0
	// These host queries are answered by the gateway itself. They are never
	// forwarded to the host ADB server, so devices/version outside the scope
	// cannot be observed.
	switch service {
	case "host:version", "host:features", "host:devices", "host:devices-l", "host:track-devices", "host:track-devices-l":
		return Decision{Action: SynthesizeHost, Serial: a.scope.Serial, Service: service}, nil
	}
	const transportPrefix = "host:transport:"
	if strings.HasPrefix(service, transportPrefix) {
		if hasNUL {
			return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "transport selector contains an unsafe delimiter", nil)
		}
		serial := strings.TrimPrefix(service, transportPrefix)
		if serial != a.scope.Serial {
			return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "transport selects another or ambiguous device", nil)
		}
		return Decision{Action: SelectDevice, Serial: a.scope.Serial, Service: service}, nil
	}
	const transportPortPrefix = "host:tport:serial:"
	if strings.HasPrefix(service, transportPortPrefix) {
		if hasNUL {
			return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "transport selector contains an unsafe delimiter", nil)
		}
		serial := strings.TrimPrefix(service, transportPortPrefix)
		if serial != a.scope.Serial {
			return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "transport selects another or ambiguous device", nil)
		}
		return Decision{Action: SelectDevice, Serial: a.scope.Serial, Service: service}, nil
	}
	if service == "host:transport-id:1" {
		return Decision{Action: SelectDevice, Serial: a.scope.Serial, Service: service}, nil
	}
	const serialPrefix = "host-serial:"
	if strings.HasPrefix(service, serialPrefix) {
		remainder := strings.TrimPrefix(service, serialPrefix)
		assignedPrefix := a.scope.Serial + ":"
		if !strings.HasPrefix(remainder, assignedPrefix) {
			return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "host-serial service selects another device", nil)
		}
		deviceService := strings.TrimPrefix(remainder, assignedPrefix)
		if deviceService == "" {
			return Decision{}, domain.NewError(domain.CodeInvalidArgument, "adb.authorize", "service", "host-serial service is malformed", nil)
		}
		if isHostGlobal(deviceService) {
			return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "host-global authority is forbidden", nil)
		}
		return Decision{Action: ForwardDevice, Serial: a.scope.Serial, Service: service}, nil
	}
	if isHostGlobal(service) || strings.HasPrefix(service, "host-usb:") || strings.HasPrefix(service, "host-local:") {
		return Decision{}, domain.NewError(domain.CodeForbidden, "adb.authorize", "service", "host-global ADB service is forbidden", nil)
	}
	// All remaining bytes are an already-selected device service. Shell, sync,
	// install, forward/reverse, root/remount, reboot, and future services pass
	// through unchanged.
	return Decision{Action: ForwardDevice, Serial: a.scope.Serial, Service: service}, nil
}

func isHostGlobal(service string) bool {
	if !strings.HasPrefix(service, "host:") {
		return false
	}
	return true
}

func (d Decision) Validate() error {
	if d.Serial == "" || d.Service == "" || (d.Action != ForwardDevice && d.Action != SelectDevice && d.Action != SynthesizeHost) {
		return fmt.Errorf("invalid ADB authorization decision")
	}
	return nil
}
