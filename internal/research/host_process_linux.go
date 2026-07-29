//go:build linux

package research

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func captureProcessEvidence(ctx context.Context, pid, parentHint int64, identity processIdentityStatus) (ProcessEvidence, []SocketEvidence, []string, error) {
	if err := ctx.Err(); err != nil {
		return ProcessEvidence{}, nil, nil, err
	}
	warnings := make([]string, 0, 4)
	evidence := ProcessEvidence{PID: pid, ParentPID: parentHint, Alive: identity.Alive, StartTime: identity.KernelStartTime}
	if !identity.Alive {
		evidence.Status = "not_found"
		return evidence, nil, append(warnings, "process_exited_before_capture"), nil
	}
	procDir := filepath.Join("/proc", strconv.FormatInt(pid, 10))
	if ex, err := os.Readlink(filepath.Join(procDir, "exe")); err == nil {
		evidence.Executable = boundText(ex, maximumEvidenceTextBytes)
	} else {
		warnings = append(warnings, "exe_unavailable")
	}
	if cwd, err := os.Readlink(filepath.Join(procDir, "cwd")); err == nil {
		evidence.WorkingDirectory = boundText(cwd, maximumEvidenceTextBytes)
	} else {
		warnings = append(warnings, "cwd_unavailable")
	}
	// Command line is forensic-class (may contain secrets). MCP redacts on query.
	if cmdline, err := os.ReadFile(filepath.Join(procDir, "cmdline")); err == nil {
		evidence.CommandLine = boundText(strings.ReplaceAll(string(cmdline), "\x00", " "), maximumEvidenceTextBytes)
	}
	if status, err := os.ReadFile(filepath.Join(procDir, "status")); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "State:") {
				evidence.Status = boundText(strings.TrimSpace(strings.TrimPrefix(line, "State:")), maximumEvidenceTextBytes)
			}
			if strings.HasPrefix(line, "PPid:") {
				if ppid, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")), 10, 64); err == nil {
					evidence.ParentPID = ppid
				}
			}
		}
	}
	evidence.Children = listChildPIDs(pid, 64)
	sockets, sockWarnings := captureLinuxSocketsForPID(pid)
	warnings = append(warnings, sockWarnings...)
	return evidence, sockets, warnings, nil
}

func listChildPIDs(parent int64, limit int) []int64 {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	children := make([]int64, 0)
	for _, entry := range entries {
		if len(children) >= limit {
			break
		}
		if !entry.IsDir() {
			continue
		}
		childPID, err := strconv.ParseInt(entry.Name(), 10, 64)
		if err != nil {
			continue
		}
		statusPath := filepath.Join("/proc", entry.Name(), "status")
		content, err := os.ReadFile(statusPath)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(content), "\n") {
			if strings.HasPrefix(line, "PPid:") {
				ppid, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")), 10, 64)
				if err == nil && ppid == parent {
					children = append(children, childPID)
				}
				break
			}
		}
	}
	return children
}

func captureLinuxSocketsForPID(pid int64) ([]SocketEvidence, []string) {
	warnings := make([]string, 0)
	inodes := linuxPIDSocketInodes(pid)
	if len(inodes) == 0 {
		warnings = append(warnings, "socket_inodes_unavailable")
	}
	sockets := make([]SocketEvidence, 0)
	for _, table := range []struct {
		path     string
		protocol string
	}{
		{"/proc/net/tcp", "tcp"},
		{"/proc/net/tcp6", "tcp6"},
		{"/proc/net/udp", "udp"},
		{"/proc/net/udp6", "udp6"},
	} {
		entries, err := parseLinuxProcNet(table.path, table.protocol)
		if err != nil {
			warnings = append(warnings, "proc_net_unavailable")
			continue
		}
		for _, entry := range entries {
			if len(inodes) > 0 {
				if _, ok := inodes[entry.inode]; !ok {
					continue
				}
			} else {
				continue
			}
			entry.PID = pid
			sockets = append(sockets, entry.SocketEvidence)
			if len(sockets) >= 256 {
				return sockets, append(warnings, "socket_list_truncated")
			}
		}
	}
	return sockets, warnings
}

func linuxPIDSocketInodes(pid int64) map[uint64]struct{} {
	fdDir := filepath.Join("/proc", strconv.FormatInt(pid, 10), "fd")
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return nil
	}
	inodes := make(map[uint64]struct{})
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join(fdDir, entry.Name()))
		if err != nil {
			continue
		}
		if !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
		inode, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			continue
		}
		inodes[inode] = struct{}{}
	}
	return inodes
}

type linuxNetEntry struct {
	SocketEvidence
	inode uint64
}

func parseLinuxProcNet(path, protocol string) ([]linuxNetEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, scanner.Err()
	}
	result := make([]linuxNetEntry, 0)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 {
			continue
		}
		local, err := parseLinuxAddr(fields[1])
		if err != nil {
			continue
		}
		remote, err := parseLinuxAddr(fields[2])
		if err != nil {
			continue
		}
		inode, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			continue
		}
		state := linuxTCPState(fields[3])
		result = append(result, linuxNetEntry{
			SocketEvidence: SocketEvidence{
				Protocol: protocol, LocalAddress: local, RemoteAddress: remote, State: state,
			},
			inode: inode,
		})
		if len(result) >= 4096 {
			break
		}
	}
	return result, scanner.Err()
}

func parseLinuxAddr(value string) (string, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("bad addr")
	}
	port64, err := strconv.ParseUint(parts[1], 16, 16)
	if err != nil {
		return "", err
	}
	ipHex := parts[0]
	if len(ipHex) == 8 {
		raw, err := strconv.ParseUint(ipHex, 16, 32)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d.%d.%d.%d:%d",
			byte(raw), byte(raw>>8), byte(raw>>16), byte(raw>>24), port64), nil
	}
	if len(ipHex) == 32 {
		return fmt.Sprintf("[%s]:%d", formatLinuxIPv6(ipHex), port64), nil
	}
	return value, nil
}

func formatLinuxIPv6(hex string) string {
	// /proc/net/tcp6 stores four little-endian 32-bit words (16 bytes).
	if len(hex) != 32 {
		return strings.ToLower(hex)
	}
	raw := make([]byte, 16)
	for word := 0; word < 4; word++ {
		chunk := hex[word*8 : word*8+8]
		val, err := strconv.ParseUint(chunk, 16, 32)
		if err != nil {
			return strings.ToLower(hex)
		}
		raw[word*4+0] = byte(val)
		raw[word*4+1] = byte(val >> 8)
		raw[word*4+2] = byte(val >> 16)
		raw[word*4+3] = byte(val >> 24)
	}
	return net.IP(raw).String()
}

func linuxTCPState(code string) string {
	switch strings.ToUpper(code) {
	case "01":
		return "ESTABLISHED"
	case "02":
		return "SYN_SENT"
	case "03":
		return "SYN_RECV"
	case "04":
		return "FIN_WAIT1"
	case "05":
		return "FIN_WAIT2"
	case "06":
		return "TIME_WAIT"
	case "07":
		return "CLOSE"
	case "08":
		return "CLOSE_WAIT"
	case "09":
		return "LAST_ACK"
	case "0A":
		return "LISTEN"
	case "0B":
		return "CLOSING"
	default:
		return code
	}
}
