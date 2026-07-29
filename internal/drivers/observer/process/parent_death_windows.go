//go:build windows

package process

import (
	"context"
	"sync"
	"time"
)

var (
	windowsCollectorPreflightOnce sync.Once
	windowsCollectorPreflightErr  error
)

// collectorParentDeathSignalGuaranteed is true only after this daemon process
// has successfully created an atomically assigned Job member and observed the
// sole-handle kill-on-close behavior on the real host. If the daemon itself is
// already job-contained, this same launch also exercises the required nested
// Job boundary.
func collectorParentDeathSignalGuaranteed() bool {
	windowsCollectorPreflightOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		windowsCollectorPreflightErr = preflightWindowsCollectorJob(ctx)
	})
	return windowsCollectorPreflightErr == nil
}
