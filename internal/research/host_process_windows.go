//go:build windows

package research

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
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
	name, alive, err := windowsProcessName(ctx, pid)
	if err != nil {
		warnings = append(warnings, "process_query_failed")
	}
	evidence.Alive = alive
	if name != "" {
		evidence.Executable = boundText(name, maximumEvidenceTextBytes)
	}
	if !alive {
		evidence.Status = "not_found"
		return evidence, nil, append(warnings, "process_exited_before_capture"), nil
	}
	evidence.Status = "running"
	sockets, sockWarnings := captureWindowsSocketsForPID(ctx, pid)
	warnings = append(warnings, sockWarnings...)
	return evidence, sockets, warnings, nil
}

func windowsProcessName(ctx context.Context, pid int64) (string, bool, error) {
	cmd := exec.CommandContext(ctx, "tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/FO", "CSV", "/NH")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	out, err := cmd.Output()
	if err != nil {
		return "", false, err
	}
	line := strings.TrimSpace(string(out))
	if line == "" || strings.HasPrefix(strings.ToLower(line), "info:") {
		return "", false, nil
	}
	// "name.exe","1234","Session","1","1,234 K"
	fields := parseCSVLine(line)
	if len(fields) == 0 {
		return "", true, nil
	}
	return fields[0], true, nil
}

func captureWindowsSocketsForPID(ctx context.Context, pid int64) ([]SocketEvidence, []string) {
	warnings := make([]string, 0)
	cmd := exec.CommandContext(ctx, "netstat", "-ano")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	out, err := cmd.Output()
	if err != nil {
		return nil, []string{"netstat_unavailable"}
	}
	sockets := make([]SocketEvidence, 0)
	scanner := bufio.NewScanner(strings.NewReader(string(out)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 {
			continue
		}
		proto := strings.ToLower(fields[0])
		if proto != "tcp" && proto != "tcpv6" && proto != "udp" && proto != "udpv6" {
			continue
		}
		// TCP: Proto Local Foreign State PID
		// UDP: Proto Local Foreign PID
		var local, remote, state string
		var pidField string
		if strings.HasPrefix(proto, "tcp") {
			if len(fields) < 5 {
				continue
			}
			local, remote, state, pidField = fields[1], fields[2], fields[3], fields[4]
		} else {
			if len(fields) < 4 {
				continue
			}
			local, remote, state, pidField = fields[1], fields[2], "", fields[len(fields)-1]
		}
		owner, err := strconv.ParseInt(pidField, 10, 64)
		if err != nil || owner != pid {
			continue
		}
		sockets = append(sockets, SocketEvidence{
			Protocol: boundText(proto, 16), LocalAddress: boundText(local, maximumEvidenceTextBytes),
			RemoteAddress: boundText(remote, maximumEvidenceTextBytes), State: boundText(state, 32), PID: pid,
		})
		if len(sockets) >= 256 {
			warnings = append(warnings, "socket_list_truncated")
			break
		}
	}
	return sockets, warnings
}

func parseCSVLine(line string) []string {
	fields := make([]string, 0)
	var current strings.Builder
	inQuotes := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch {
		case ch == '"':
			inQuotes = !inQuotes
		case ch == ',' && !inQuotes:
			fields = append(fields, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	fields = append(fields, current.String())
	return fields
}
