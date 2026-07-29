//go:build windows

package research

import "context"

func lifecycleStartNSForPID(pid int64) (int64, error) {
	_, ns, err := windowsProcessCreationNS(context.Background(), pid)
	return ns, err
}
