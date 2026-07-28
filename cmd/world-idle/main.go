// world-idle is the inert PID used to keep a disposable target container
// alive between explicitly scoped target operations. It has no command,
// network, filesystem, or runtime-control surface.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
