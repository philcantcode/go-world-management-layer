package main

import (
	"bytes"
	"testing"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

func TestObserveCommandCatalogue(t *testing.T) {
	for _, name := range []string{"snapshot", "watch", "metrics", "top", "bundle"} {
		if observeCommands()[name] == nil {
			t.Fatalf("missing %q command", name)
		}
	}
}

func TestObservationParserDoesNotRequireDaemon(t *testing.T) {
	filter, err := worldcli.ParseObservationFilter("snapshot", []string{"-lease", "lease_1", "-targets", "target_1,target_2"}, &bytes.Buffer{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if filter.LeaseId != "lease_1" || len(filter.TargetIds) != 2 {
		t.Fatalf("unexpected filter: %#v", filter)
	}
}
