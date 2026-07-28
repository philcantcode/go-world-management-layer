package dockercli

import (
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type PhysicalCapabilities struct {
	OSType          string
	SecurityOptions []string
	Runtimes        []string
	CPUCFSQuota     bool
	MemoryLimit     bool
	SwapLimit       bool
	PIDsLimit       bool
}

type PhysicalSupportAssessment struct {
	Container ports.PhysicalSupport
	Seccomp   ports.PhysicalSupport
	CPU       ports.PhysicalSupport
	Memory    ports.PhysicalSupport
	Swap      ports.PhysicalSupport
	PIDs      ports.PhysicalSupport
}

func AssessPhysicalSupport(capabilities PhysicalCapabilities, runtime string) PhysicalSupportAssessment {
	platform := linuxPlatformSupport(capabilities.OSType)
	runtimeSupport := availableRuntimeSupport(platform, capabilities.Runtimes, runtime)
	container := combinePhysicalSupport(platform, runtimeSupport)
	security := container
	if runtime != RuncRuntime && security == ports.PhysicalSupportEnforced {
		security = ports.PhysicalSupportUnknown
	}
	seccomp := combinePhysicalSupport(security, boolPhysicalSupport(SupportsSecurityOption(capabilities.SecurityOptions, "seccomp")))
	return PhysicalSupportAssessment{
		Container: container,
		Seccomp:   seccomp,
		CPU:       combinePhysicalSupport(container, boolPhysicalSupport(capabilities.CPUCFSQuota)),
		Memory:    combinePhysicalSupport(container, boolPhysicalSupport(capabilities.MemoryLimit)),
		Swap:      combinePhysicalSupport(container, boolPhysicalSupport(capabilities.MemoryLimit && capabilities.SwapLimit)),
		PIDs:      combinePhysicalSupport(container, boolPhysicalSupport(capabilities.PIDsLimit)),
	}
}

type PhysicalResourceValues struct {
	CPUMilli           int64
	MemoryBytes        int64
	SwapBytes          int64
	WorkspaceBytes     int64
	WritableStateBytes int64
	CaptureBytes       int64
	Inodes             int64
	PIDs               int64
}

func PhysicalResourceFacts(support PhysicalSupportAssessment, values PhysicalResourceValues) ports.ContainerResourcePhysicalFacts {
	return ports.ContainerResourcePhysicalFacts{
		CPUMilli:           physicalLimit(values.CPUMilli, support.CPU, "Docker did not report CPU CFS quota support"),
		MemoryBytes:        physicalLimit(values.MemoryBytes, support.Memory, "Docker did not report memory-limit support"),
		SwapBytes:          physicalLimit(values.SwapBytes, support.Swap, "Docker did not report memory-and-swap limit support"),
		WorkspaceBytes:     physicalLimit(values.WorkspaceBytes, ports.PhysicalSupportUnsupported, "Docker does not quota bytes in a host bind mount"),
		WritableStateBytes: physicalLimit(values.WritableStateBytes, ports.PhysicalSupportUnsupported, "Docker does not quota bytes in a host bind mount"),
		CaptureBytes:       physicalLimit(values.CaptureBytes, ports.PhysicalSupportUnsupported, "capture storage is outside the container resource controller"),
		Inodes:             physicalLimit(values.Inodes, ports.PhysicalSupportUnsupported, "Docker does not quota inodes in a host bind mount"),
		PIDs:               physicalLimit(values.PIDs, support.PIDs, "Docker did not report PID-limit support"),
	}
}

func linuxPlatformSupport(osType string) ports.PhysicalSupport {
	if strings.TrimSpace(osType) == "" {
		return ports.PhysicalSupportUnknown
	}
	if strings.EqualFold(osType, "linux") {
		return ports.PhysicalSupportEnforced
	}
	return ports.PhysicalSupportUnsupported
}

func availableRuntimeSupport(platform ports.PhysicalSupport, runtimes []string, runtime string) ports.PhysicalSupport {
	if platform != ports.PhysicalSupportEnforced {
		return platform
	}
	if runtime == "" || len(runtimes) == 0 {
		return ports.PhysicalSupportUnknown
	}
	if SupportsRuntime(runtimes, runtime) {
		return ports.PhysicalSupportEnforced
	}
	return ports.PhysicalSupportUnsupported
}

func boolPhysicalSupport(value bool) ports.PhysicalSupport {
	if value {
		return ports.PhysicalSupportEnforced
	}
	return ports.PhysicalSupportUnsupported
}

func combinePhysicalSupport(values ...ports.PhysicalSupport) ports.PhysicalSupport {
	result := ports.PhysicalSupportEnforced
	for _, value := range values {
		if value == ports.PhysicalSupportUnknown {
			return ports.PhysicalSupportUnknown
		}
		if value != ports.PhysicalSupportEnforced {
			result = ports.PhysicalSupportUnsupported
		}
	}
	return result
}

func physicalLimit(value int64, support ports.PhysicalSupport, unsupportedDetail string) ports.PhysicalLimitFact {
	detail := ""
	if support == ports.PhysicalSupportUnsupported {
		detail = unsupportedDetail
	} else if support == ports.PhysicalSupportUnknown {
		detail = "Docker could not prove this resource controller"
	}
	return ports.PhysicalLimitFact{Value: value, Support: support, Detail: detail}
}
