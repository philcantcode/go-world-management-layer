//go:build !linux && !windows

package research

import "fmt"

func lifecycleStartNSForPID(pid int64) (int64, error) {
	return 0, fmt.Errorf("unsupported platform")
}
