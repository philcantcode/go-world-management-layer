package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

func TestParseNamedCaptureRequest(t *testing.T) {
	request, err := worldcli.ParseRequestCapture([]string{"-lease", "lease_1", "-policy", "sha256:policy", "network-trace"}, &bytes.Buffer{}, time.Second, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if request.LeaseId != "lease_1" || request.NamedProfile != "network-trace" || request.Mutation == nil {
		t.Fatalf("unexpected request: %#v", request)
	}
}

func TestCaptureRequestRejectsCollectorArguments(t *testing.T) {
	_, err := worldcli.ParseRequestCapture([]string{"-lease", "lease_1", "-policy", "sha256:policy", "network-trace", "--tcpdump-arg"}, &bytes.Buffer{}, time.Second, "", "")
	if err == nil {
		t.Fatal("expected extra argument rejection")
	}
}
