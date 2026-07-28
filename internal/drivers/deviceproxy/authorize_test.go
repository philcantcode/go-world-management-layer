package deviceproxy

import (
	"bytes"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestAuthorizerDeniesHostAndOtherSerialWhilePreservingDeviceService(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	scope, err := IssueScope(lease, target, 3, run, "device-serial", bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(scope, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	service := "shell:sh -c 'printf «opaque;$()\u00bb'"
	decision, err := authorizer.Authorize(Request{Credential: scope.Credential, Serial: scope.Serial, Service: service})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != ForwardDevice || decision.Service != service || decision.Serial != scope.Serial {
		t.Fatalf("device service changed: %#v", decision)
	}
	abbService := "abb_exec:package\x00install-create\x00-S\x00123"
	abbDecision, err := authorizer.Authorize(Request{Credential: scope.Credential, Serial: scope.Serial, Service: abbService})
	if err != nil || abbDecision.Action != ForwardDevice || abbDecision.Service != abbService {
		t.Fatalf("NUL-framed modern device service changed or rejected: %#v %v", abbDecision, err)
	}
	for name, request := range map[string]Request{
		"wrong credential":     {Credential: "wrong", Service: service},
		"other request serial": {Credential: scope.Credential, Serial: "other", Service: service},
		"other transport":      {Credential: scope.Credential, Service: "host:transport:other"},
		"other tport":          {Credential: scope.Credential, Service: "host:tport:serial:other"},
		"ambiguous tport":      {Credential: scope.Credential, Service: "host:tport:any"},
		"other transport id":   {Credential: scope.Credential, Service: "host:transport-id:2"},
		"other host serial":    {Credential: scope.Credential, Service: "host-serial:other:forward:tcp:1;tcp:2"},
		"kill server":          {Credential: scope.Credential, Service: "host:kill"},
		"connect host":         {Credential: scope.Credential, Service: "host:connect:10.0.0.1:5555"},
		"NUL transport suffix": {Credential: scope.Credential, Service: "host:transport:" + scope.Serial + "\x00other"},
		"NUL tport suffix":     {Credential: scope.Credential, Service: "host:tport:serial:" + scope.Serial + "\x00other"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := authorizer.Authorize(request); err == nil {
				t.Fatal("authority escape accepted")
			}
		})
	}
	selected, err := authorizer.Authorize(Request{Credential: scope.Credential, Service: "host:transport:" + scope.Serial})
	if err != nil || selected.Action != SelectDevice {
		t.Fatalf("assigned transport rejected: %#v %v", selected, err)
	}
	for _, service := range []string{"host:tport:serial:" + scope.Serial, "host:transport-id:1"} {
		selected, err := authorizer.Authorize(Request{Credential: scope.Credential, Service: service})
		if err != nil || selected.Action != SelectDevice {
			t.Fatalf("assigned modern transport %q rejected: %#v %v", service, selected, err)
		}
	}
	forward, err := authorizer.Authorize(Request{Credential: scope.Credential, Service: "host-serial:" + scope.Serial + ":forward:tcp:27042;tcp:27042"})
	if err != nil || forward.Service == "" {
		t.Fatalf("assigned forward rejected: %#v %v", forward, err)
	}
	synthetic, err := authorizer.Authorize(Request{Credential: scope.Credential, Service: "host:devices-l"})
	if err != nil || synthetic.Action != SynthesizeHost {
		t.Fatalf("scoped listing not synthesized: %#v %v", synthetic, err)
	}
}

func TestAuthorizerRejectsInactiveRun(t *testing.T) {
	lease, _ := domain.NewLeaseID()
	target, _ := domain.NewTargetID()
	run, _ := domain.NewTargetRunID()
	scope, _ := IssueScope(lease, target, 1, run, "serial", bytes.NewReader(bytes.Repeat([]byte{1}, 32)))
	authorizer, _ := NewAuthorizer(scope, func() bool { return false })
	if _, err := authorizer.Authorize(Request{Credential: scope.Credential, Service: "reboot:"}); err == nil {
		t.Fatal("inactive run authorized")
	}
}
