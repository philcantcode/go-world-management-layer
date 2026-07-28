package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func TestRunSelfTestIsExactAndReportsProtocol(t *testing.T) {
	var output bytes.Buffer
	handled, err := runSelfTest([]string{transport.GuestSelfTestArgument}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if !handled || output.String() != fmt.Sprintf("world-guest protocol=%d\n", transport.ProtocolVersion) {
		t.Fatalf("self-test handled=%t output=%q", handled, output.String())
	}
	for _, arguments := range [][]string{nil, {transport.GuestSelfTestArgument, "extra"}, {"--help"}} {
		handled, err = runSelfTest(arguments, &output)
		if err != nil || handled {
			t.Fatalf("arguments %#v handled=%t err=%v", arguments, handled, err)
		}
	}
}
