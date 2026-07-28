package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

func TestParseExportPathsAndRoles(t *testing.T) {
	request, err := worldcli.ParseDeclareExport([]string{"-lease", "lease_1", "-policy", "sha256:policy", "-path", "reports/result.json=report", "logs/run.txt"}, &bytes.Buffer{}, time.Second, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(request.Paths) != 2 || request.Paths[0].Role != "report" || request.Paths[1].Role != "result" {
		t.Fatalf("unexpected paths: %#v", request.Paths)
	}
}

func TestParseExportRejectsWorkspaceEscape(t *testing.T) {
	_, err := worldcli.ParseDeclareExport([]string{"-lease", "lease_1", "-policy", "sha256:policy", "../secret"}, &bytes.Buffer{}, time.Second, "", "")
	if err == nil {
		t.Fatal("expected workspace escape rejection")
	}
}
