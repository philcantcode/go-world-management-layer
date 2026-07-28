package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/guest"
	"github.com/philcantcode/go-world-management-layer/internal/transport"
)

func main() {
	if handled, err := runSelfTest(os.Args[1:], os.Stdout); handled {
		if err != nil {
			fatal(err)
		}
		return
	}
	temporaryRoot := flag.String("temporary-root", os.TempDir()+string(os.PathSeparator)+"world-guest", "private root for materialized exec inputs")
	maxTemporary := flag.Int64("max-temporary-bytes", 8<<20, "maximum temporary-input bytes per exec")
	maxStdin := flag.Int64("max-stdin-bytes", 64<<20, "maximum stdin bytes per exec")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 30*time.Second, "maximum interval without a host heartbeat")
	maxFrame := flag.Uint("max-frame-bytes", uint(transport.DefaultMaxFrame), "maximum framed protocol payload")
	flag.Parse()

	supervisor, err := guest.New(guest.Config{
		TemporaryRoot:     *temporaryRoot,
		MaxTemporaryBytes: *maxTemporary,
		MaxStdinBytes:     *maxStdin,
		HeartbeatTimeout:  *heartbeatTimeout,
	})
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = supervisor.Serve(ctx, os.Stdin, os.Stdout, uint32(*maxFrame)); err != nil {
		fatal(err)
	}
}

func runSelfTest(arguments []string, output io.Writer) (bool, error) {
	if len(arguments) != 1 || arguments[0] != transport.GuestSelfTestArgument {
		return false, nil
	}
	_, err := fmt.Fprintf(output, "world-guest protocol=%d\n", transport.ProtocolVersion)
	return true, err
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
