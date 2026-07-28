// Package linux contains typed plans for Linux-only node mechanisms. The plan
// types compile everywhere; platform probes explicitly report unsupported
// capabilities rather than pretending to provide equivalent behavior.
package linux

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
)

type PlatformResult struct {
	Supported bool
	Reason    string
}

type ProbePlan struct {
	CgroupRoot string
	PSIRoot    string
	KVMDevice  string
	BTFPath    string
}

func DefaultProbePlan() ProbePlan {
	return ProbePlan{CgroupRoot: "/sys/fs/cgroup", PSIRoot: "/proc/pressure", KVMDevice: "/dev/kvm", BTFPath: "/sys/kernel/btf/vmlinux"}
}

func (p ProbePlan) withDefaults() ProbePlan {
	defaults := DefaultProbePlan()
	if p.CgroupRoot == "" {
		p.CgroupRoot = defaults.CgroupRoot
	}
	if p.PSIRoot == "" {
		p.PSIRoot = defaults.PSIRoot
	}
	if p.KVMDevice == "" {
		p.KVMDevice = defaults.KVMDevice
	}
	if p.BTFPath == "" {
		p.BTFPath = defaults.BTFPath
	}
	return p
}

var ownerName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type CgroupPlan struct {
	Root            string
	Parent          string
	Owner           string
	Resources       admission.Resources
	MemoryHighBytes int64
	CPUPeriodMicros int64
}

func (p CgroupPlan) Validate() error {
	if !path.IsAbs(p.Root) || !ownerName.MatchString(p.Owner) {
		return fmt.Errorf("absolute cgroup root and safe owner are required")
	}
	if p.Parent != "" && !ownerName.MatchString(p.Parent) {
		return fmt.Errorf("cgroup parent is unsafe")
	}
	if err := p.Resources.Validate(); err != nil {
		return err
	}
	if p.MemoryHighBytes < 0 {
		return fmt.Errorf("memory.high cannot be negative")
	}
	if p.Resources.MemoryBytes > 0 && p.MemoryHighBytes > p.Resources.MemoryBytes {
		return fmt.Errorf("memory.high exceeds memory.max")
	}
	if p.CPUPeriodMicros < 0 {
		return fmt.Errorf("CPU period cannot be negative")
	}
	return nil
}

func (p CgroupPlan) Path() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	if p.Parent == "" {
		return path.Join(p.Root, p.Owner), nil
	}
	return path.Join(p.Root, p.Parent, p.Owner), nil
}

// ControllerValues returns exact cgroup-v2 file values. Callers can inspect or
// audit this plan before a privileged helper writes anything.
func (p CgroupPlan) ControllerValues() (map[string]string, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	values := make(map[string]string)
	if p.Resources.MemoryBytes > 0 {
		values["memory.max"] = strconv.FormatInt(p.Resources.MemoryBytes, 10)
	}
	if p.MemoryHighBytes > 0 {
		values["memory.high"] = strconv.FormatInt(p.MemoryHighBytes, 10)
	}
	if p.Resources.PIDs > 0 {
		values["pids.max"] = strconv.FormatInt(p.Resources.PIDs, 10)
	}
	if p.Resources.CPUMilli > 0 {
		period := p.CPUPeriodMicros
		if period == 0 {
			period = 100000
		}
		quota := p.Resources.CPUMilli * period / 1000
		if quota < 1000 {
			quota = 1000
		}
		values["cpu.max"] = strconv.FormatInt(quota, 10) + " " + strconv.FormatInt(period, 10)
	}
	return values, nil
}

type OverlayPlan struct {
	LowerDirectories []string
	UpperDirectory   string
	WorkDirectory    string
	MergedDirectory  string
	ReadOnly         bool
	ExtraOptions     []string
}

func (p OverlayPlan) Validate() error {
	if len(p.LowerDirectories) == 0 || p.MergedDirectory == "" {
		return fmt.Errorf("lower and merged directories are required")
	}
	if !p.ReadOnly && (p.UpperDirectory == "" || p.WorkDirectory == "") {
		return fmt.Errorf("writable overlay requires upper and work directories")
	}
	all := append(append([]string(nil), p.LowerDirectories...), p.MergedDirectory)
	if !p.ReadOnly {
		all = append(all, p.UpperDirectory, p.WorkDirectory)
	}
	seen := make(map[string]struct{}, len(all))
	for _, value := range all {
		if !path.IsAbs(value) || strings.ContainsAny(value, "\x00:\n,") {
			return fmt.Errorf("overlay path must be absolute and mount-option safe")
		}
		clean := path.Clean(value)
		if _, duplicate := seen[clean]; duplicate {
			return fmt.Errorf("overlay directories must be distinct")
		}
		seen[clean] = struct{}{}
	}
	if p.ReadOnly && (p.UpperDirectory != "" || p.WorkDirectory != "") {
		return fmt.Errorf("read-only overlay cannot have upper/work directories")
	}
	for _, option := range p.ExtraOptions {
		if option == "" || strings.ContainsAny(option, "\x00\n,") || strings.HasPrefix(option, "lowerdir=") || strings.HasPrefix(option, "upperdir=") || strings.HasPrefix(option, "workdir=") {
			return fmt.Errorf("unsafe or duplicate overlay option")
		}
	}
	return nil
}

func (p OverlayPlan) MountOptions() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	lowers := append([]string(nil), p.LowerDirectories...)
	// OverlayFS resolves lower layers right-to-left; retain caller order rather
	// than sorting it because it is semantic.
	options := []string{"lowerdir=" + strings.Join(lowers, ":")}
	if !p.ReadOnly {
		options = append(options, "upperdir="+p.UpperDirectory, "workdir="+p.WorkDirectory)
	}
	options = append(options, p.ExtraOptions...)
	return strings.Join(options, ","), nil
}

func SortedControllerNames(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for name := range values {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}
