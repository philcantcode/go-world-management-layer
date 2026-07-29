package research

import (
	"os/exec"
)

// CollectorOptions configures class-aware collector construction and capability
// probes. All fields are optional; zero values select safe defaults.
type CollectorOptions struct {
	// LookPath resolves external tools. Defaults to exec.LookPath.
	LookPath func(file string) (string, error)
	// TargetOraclePaths are optional absolute or action-relative log paths to
	// tail for target-side confirmation. Empty means oracle records a gap.
	TargetOraclePaths []string
	// MaxPcapBytes bounds packet capture file size (default 16 MiB).
	MaxPcapBytes int64
	// MaxSyscallBytes bounds syscall trace retention (default 4 MiB).
	MaxSyscallBytes int64
	// MaxOracleBytes bounds oracle log retention (default 1 MiB).
	MaxOracleBytes int64
	// MaxStaticFileBytes bounds static binary hashing (default 32 MiB).
	MaxStaticFileBytes int64
	// StateAttributed marks working-directory state as action-attributed when
	// the collector and action share a filesystem namespace (default true for
	// registry-built state collectors).
	StateAttributed *bool
	// DisablePcap skips dumpcap/tcpdump even when present (tests).
	DisablePcap bool
	// DisableSyscall skips strace/platform syscall tools even when present.
	DisableSyscall bool
}

// CollectorInjects allows tests and specialized hosts to override individual
// companion collectors while still using the class-aware registry for others.
type CollectorInjects struct {
	Host          HostCollector
	Network       NetworkCollector
	State         StateCollector
	Syscall       SyscallCollector
	Static        StaticContextCollector
	Oracle        TargetOracleCollector
	Replay        ReplayCollector
	NetworkDecode NetworkDecodeCollector
	// ExtraStartables are appended after registry-built startables (tests).
	ExtraStartables []StartableCollector
}

// ActionCollectors is the per-action companion set selected from stimulus class,
// observation level, and intended companion roles.
type ActionCollectors struct {
	Host          HostCollector
	Network       NetworkCollector
	State         StateCollector
	Syscall       SyscallCollector
	Static        StaticContextCollector
	Oracle        TargetOracleCollector
	Replay        ReplayCollector
	NetworkDecode NetworkDecodeCollector
	// Startables are started at Begin and stopped at Seal (fail-open).
	Startables []StartableCollector
	// Roles lists companions the registry attempted to attach.
	Roles []CompanionRole
}

// BuildCollectors selects collectors for the given class/level/companions.
// Injected collectors replace the corresponding default implementation.
// Missing OS tools produce collectors that record explicit gaps rather than
// panicking or inventing evidence.
func BuildCollectors(class StimulusClass, level ObservationLevel, companions []CompanionRole, opts CollectorOptions, inject CollectorInjects) ActionCollectors {
	if len(companions) == 0 {
		companions = IntendedCompanions(class, level)
	}
	lookPath := opts.LookPath
	if lookPath == nil {
		lookPath = exec.LookPath
	}
	stateAttributed := true
	if opts.StateAttributed != nil {
		stateAttributed = *opts.StateAttributed
	}

	result := ActionCollectors{Roles: append([]CompanionRole(nil), companions...)}
	wanted := companionSet(companions)

	// host_process: always prefer injected host; else PID-aware host collector.
	if inject.Host != nil {
		result.Host = inject.Host
	} else if wanted[CompanionHostProcess] {
		result.Host = NewHostProcessCollector()
	} else {
		result.Host = LocalHostCollector{}
	}

	// network_capture (+ ambient inventory fallback).
	if inject.Network != nil {
		result.Network = inject.Network
	} else if wanted[CompanionNetworkCapture] {
		capture := NewNetworkCaptureCollector(NetworkCaptureOptions{
			LookPath:     lookPath,
			MaxPcapBytes: opts.MaxPcapBytes,
			DisablePcap:  opts.DisablePcap,
		})
		result.Network = capture
		result.Startables = append(result.Startables, capture)
	} else {
		result.Network = LocalNetworkCollector{}
	}

	// network_decode
	if inject.NetworkDecode != nil {
		result.NetworkDecode = inject.NetworkDecode
	} else if wanted[CompanionNetworkDecode] {
		result.NetworkDecode = NewNetworkDecodeCollector(NetworkDecodeOptions{LookPath: lookPath})
	}

	// state_diff
	if inject.State != nil {
		result.State = inject.State
	} else if wanted[CompanionStateDiff] || level.AtLeast(ObservationLevelDeep) {
		result.State = WorkingDirectoryStateCollector{Attributed: stateAttributed}
	} else {
		result.State = WorkingDirectoryStateCollector{Attributed: false}
	}

	// host_syscall
	if inject.Syscall != nil {
		result.Syscall = inject.Syscall
	} else if wanted[CompanionHostSyscall] {
		syscallCollector := NewHostSyscallCollector(HostSyscallOptions{
			LookPath:        lookPath,
			MaxSyscallBytes: opts.MaxSyscallBytes,
			Disable:         opts.DisableSyscall,
		})
		result.Syscall = syscallCollector
		if startable, ok := syscallCollector.(StartableCollector); ok {
			result.Startables = append(result.Startables, startable)
		}
	}

	// static_context
	if inject.Static != nil {
		result.Static = inject.Static
	} else if wanted[CompanionStaticContext] {
		result.Static = NewStaticContextCollector(StaticContextOptions{
			LookPath:           lookPath,
			MaxStaticFileBytes: opts.MaxStaticFileBytes,
		})
	}

	// target_oracle
	if inject.Oracle != nil {
		result.Oracle = inject.Oracle
	} else if wanted[CompanionTargetOracle] {
		result.Oracle = NewTargetOracleCollector(TargetOracleOptions{
			Paths:          append([]string(nil), opts.TargetOraclePaths...),
			MaxOracleBytes: opts.MaxOracleBytes,
		})
	}

	// replay
	if inject.Replay != nil {
		result.Replay = inject.Replay
	} else if wanted[CompanionReplay] {
		result.Replay = NewReplayCollector()
	}

	if len(inject.ExtraStartables) > 0 {
		result.Startables = append(result.Startables, inject.ExtraStartables...)
	}

	return result
}

// HasRole reports whether the companion was selected for this action.
func (c ActionCollectors) HasRole(role CompanionRole) bool {
	for _, existing := range c.Roles {
		if existing == role {
			return true
		}
	}
	return false
}

func companionSet(companions []CompanionRole) map[CompanionRole]bool {
	set := make(map[CompanionRole]bool, len(companions))
	for _, role := range companions {
		set[role] = true
	}
	return set
}

// DefaultCaptureBounds used when options omit size limits.
const (
	defaultMaxPcapBytes       = int64(16 << 20)
	defaultMaxSyscallBytes    = int64(4 << 20)
	defaultMaxOracleBytes     = int64(1 << 20)
	defaultMaxStaticFileBytes = int64(32 << 20)
	maximumMaxPcapBytes       = int64(256 << 20)
	maximumMaxSyscallBytes    = int64(64 << 20)
	maximumMaxOracleBytes     = int64(16 << 20)
	maximumMaxStaticFileBytes = int64(256 << 20)
)
