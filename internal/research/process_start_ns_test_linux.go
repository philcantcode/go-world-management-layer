//go:build linux

package research

func lifecycleStartNSForPID(pid int64) (int64, error) {
	ticks, _, err := readLinuxProcStatIdentity(pid)
	if err != nil {
		return 0, err
	}
	return linuxProcStartUnixNS(ticks)
}
