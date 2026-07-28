// Package admission provides deterministic capacity admission, fair queueing,
// and the fixed pressure-shedding decision order. It emits decisions only;
// platform resource controllers execute and report their outcomes separately.
package admission

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

var (
	ErrInvalidResources = errors.New("invalid resource vector")
	ErrRequestOverLimit = errors.New("resource request exceeds its hard limit")
	ErrInsufficient     = errors.New("insufficient allocatable capacity")
	ErrPressure         = errors.New("host pressure prevents admission")
)

type Resources struct {
	CPUMilli     int64            `json:"cpu_milli"`
	MemoryBytes  int64            `json:"memory_bytes"`
	SwapBytes    int64            `json:"swap_bytes"`
	StorageBytes int64            `json:"storage_bytes"`
	CaptureBytes int64            `json:"capture_bytes"`
	Inodes       int64            `json:"inodes"`
	PIDs         int64            `json:"pids"`
	Devices      map[string]int64 `json:"devices,omitempty"`
}

func (r Resources) Validate() error {
	values := map[string]int64{"cpu_milli": r.CPUMilli, "memory_bytes": r.MemoryBytes, "swap_bytes": r.SwapBytes, "storage_bytes": r.StorageBytes, "capture_bytes": r.CaptureBytes, "inodes": r.Inodes, "pids": r.PIDs}
	for name, value := range values {
		if value < 0 {
			return fmt.Errorf("%w: %s is negative", ErrInvalidResources, name)
		}
	}
	for name, value := range r.Devices {
		if name == "" || value < 0 {
			return fmt.Errorf("%w: invalid device %q", ErrInvalidResources, name)
		}
	}
	return nil
}

func (r Resources) Clone() Resources {
	result := r
	result.Devices = make(map[string]int64, len(r.Devices))
	for name, value := range r.Devices {
		result.Devices[name] = value
	}
	return result
}

func (r Resources) Add(other Resources) (Resources, error) {
	if err := r.Validate(); err != nil {
		return Resources{}, err
	}
	if err := other.Validate(); err != nil {
		return Resources{}, err
	}
	result := r.Clone()
	for _, pair := range []struct {
		destination *int64
		value       int64
	}{
		{&result.CPUMilli, other.CPUMilli}, {&result.MemoryBytes, other.MemoryBytes}, {&result.SwapBytes, other.SwapBytes}, {&result.StorageBytes, other.StorageBytes},
		{&result.CaptureBytes, other.CaptureBytes}, {&result.Inodes, other.Inodes}, {&result.PIDs, other.PIDs},
	} {
		if pair.value > math.MaxInt64-*pair.destination {
			return Resources{}, fmt.Errorf("%w: overflow", ErrInvalidResources)
		}
		*pair.destination += pair.value
	}
	for name, value := range other.Devices {
		if value > math.MaxInt64-result.Devices[name] {
			return Resources{}, fmt.Errorf("%w: device overflow", ErrInvalidResources)
		}
		result.Devices[name] += value
	}
	return result, nil
}

func (r Resources) Subtract(other Resources) (Resources, error) {
	if !other.FitsWithin(r) {
		return Resources{}, ErrInsufficient
	}
	result := r.Clone()
	result.CPUMilli -= other.CPUMilli
	result.MemoryBytes -= other.MemoryBytes
	result.SwapBytes -= other.SwapBytes
	result.StorageBytes -= other.StorageBytes
	result.CaptureBytes -= other.CaptureBytes
	result.Inodes -= other.Inodes
	result.PIDs -= other.PIDs
	for name, value := range other.Devices {
		result.Devices[name] -= value
	}
	return result, nil
}

func (r Resources) FitsWithin(limit Resources) bool {
	if r.Validate() != nil || limit.Validate() != nil {
		return false
	}
	if r.CPUMilli > limit.CPUMilli || r.MemoryBytes > limit.MemoryBytes || r.SwapBytes > limit.SwapBytes || r.StorageBytes > limit.StorageBytes || r.CaptureBytes > limit.CaptureBytes || r.Inodes > limit.Inodes || r.PIDs > limit.PIDs {
		return false
	}
	for name, value := range r.Devices {
		if value > limit.Devices[name] {
			return false
		}
	}
	return true
}

func (r Resources) IsZero() bool {
	if r.CPUMilli != 0 || r.MemoryBytes != 0 || r.SwapBytes != 0 || r.StorageBytes != 0 || r.CaptureBytes != 0 || r.Inodes != 0 || r.PIDs != 0 {
		return false
	}
	for _, value := range r.Devices {
		if value != 0 {
			return false
		}
	}
	return true
}

type PSI struct {
	Current float64 `json:"current"`
	Trend   float64 `json:"trend"`
}

func (p PSI) Validate() error {
	if math.IsNaN(p.Current) || math.IsInf(p.Current, 0) || p.Current < 0 || p.Current > 1 || math.IsNaN(p.Trend) || math.IsInf(p.Trend, 0) {
		return fmt.Errorf("invalid PSI")
	}
	return nil
}

type Pressure struct {
	MemoryFull        PSI     `json:"memory_full"`
	IOFull            PSI     `json:"io_full"`
	CPUFull           PSI     `json:"cpu_full"`
	FreeDiskPercent   float64 `json:"free_disk_percent"`
	FreeInodesPercent float64 `json:"free_inodes_percent"`
}

type PressureThresholds struct {
	MemoryPSIFull            float64 `json:"memory_psi_full"`
	IOPSIFull                float64 `json:"io_psi_full"`
	CPUPSIFull               float64 `json:"cpu_psi_full"`
	MinimumFreeDiskPercent   float64 `json:"minimum_free_disk_percent"`
	MinimumFreeInodesPercent float64 `json:"minimum_free_inodes_percent"`
}

func (p Pressure) Validate() error {
	for name, value := range map[string]PSI{"memory_full": p.MemoryFull, "io_full": p.IOFull, "cpu_full": p.CPUFull} {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("invalid %s pressure: %w", name, err)
		}
	}
	for name, value := range map[string]float64{"free_disk_percent": p.FreeDiskPercent, "free_inodes_percent": p.FreeInodesPercent} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return fmt.Errorf("invalid %s pressure", name)
		}
	}
	return nil
}

// Validate permits zero to disable an individual pressure signal. Policy
// compilation is responsible for filling non-zero defaults where desired.
func (t PressureThresholds) Validate() error {
	for name, value := range map[string]float64{"memory_psi_full": t.MemoryPSIFull, "io_psi_full": t.IOPSIFull, "cpu_psi_full": t.CPUPSIFull} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
			return fmt.Errorf("invalid %s threshold", name)
		}
	}
	for name, value := range map[string]float64{"minimum_free_disk_percent": t.MinimumFreeDiskPercent, "minimum_free_inodes_percent": t.MinimumFreeInodesPercent} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
			return fmt.Errorf("invalid %s threshold", name)
		}
	}
	return nil
}

func (p Pressure) unsafe(thresholds PressureThresholds) []string {
	reasons := make([]string, 0)
	if thresholds.MemoryPSIFull > 0 && (p.MemoryFull.Current >= thresholds.MemoryPSIFull || p.MemoryFull.Current+p.MemoryFull.Trend >= thresholds.MemoryPSIFull) {
		reasons = append(reasons, "memory PSI")
	}
	if thresholds.IOPSIFull > 0 && (p.IOFull.Current >= thresholds.IOPSIFull || p.IOFull.Current+p.IOFull.Trend >= thresholds.IOPSIFull) {
		reasons = append(reasons, "I/O PSI")
	}
	if thresholds.CPUPSIFull > 0 && (p.CPUFull.Current >= thresholds.CPUPSIFull || p.CPUFull.Current+p.CPUFull.Trend >= thresholds.CPUPSIFull) {
		reasons = append(reasons, "CPU PSI")
	}
	if thresholds.MinimumFreeDiskPercent > 0 && p.FreeDiskPercent <= thresholds.MinimumFreeDiskPercent {
		reasons = append(reasons, "free disk")
	}
	if thresholds.MinimumFreeInodesPercent > 0 && p.FreeInodesPercent <= thresholds.MinimumFreeInodesPercent {
		reasons = append(reasons, "free inodes")
	}
	return reasons
}

type LeaseRequest struct {
	LeaseKey     string        `json:"lease_key"`
	Requests     Resources     `json:"requests"`
	Limits       Resources     `json:"limits"`
	ObserverCost Resources     `json:"observer_cost"`
	SnapshotCost Resources     `json:"snapshot_cost"`
	Priority     int           `json:"priority"`
	Preemptible  bool          `json:"preemptible"`
	TTL          time.Duration `json:"ttl"`
}

type CapacitySnapshot struct {
	Allocatable    Resources `json:"allocatable"`
	Reserved       Resources `json:"reserved"`
	WarmPoolCost   Resources `json:"warm_pool_cost"`
	ControlReserve Resources `json:"control_reserve"`
	Pressure       Pressure  `json:"pressure"`
	ObservedAt     time.Time `json:"observed_at"`
}

type AdmissionDecision struct {
	Admitted        bool      `json:"admitted"`
	Reason          string    `json:"reason"`
	InputsDigest    string    `json:"inputs_digest"`
	EffectiveBudget Resources `json:"effective_budget"`
	AvailableBefore Resources `json:"available_before"`
	AvailableAfter  Resources `json:"available_after"`
	DecidedAt       time.Time `json:"decided_at"`
}

func Evaluate(request LeaseRequest, snapshot CapacitySnapshot, thresholds PressureThresholds, now time.Time) (AdmissionDecision, error) {
	decision := AdmissionDecision{DecidedAt: now.UTC()}
	digest, err := decisionDigest(request, snapshot, thresholds)
	if err != nil {
		return decision, err
	}
	decision.InputsDigest = digest
	if err := snapshot.Pressure.Validate(); err != nil {
		return decision, err
	}
	if err := thresholds.Validate(); err != nil {
		return decision, err
	}
	if request.LeaseKey == "" || request.TTL <= 0 || request.Requests.IsZero() {
		decision.Reason = "invalid lease resource contract"
		return decision, ErrInvalidResources
	}
	for _, resource := range []Resources{request.Requests, request.Limits, request.ObserverCost, request.SnapshotCost, snapshot.Allocatable, snapshot.Reserved, snapshot.WarmPoolCost, snapshot.ControlReserve} {
		if err := resource.Validate(); err != nil {
			return decision, err
		}
	}
	if !request.Requests.FitsWithin(request.Limits) {
		decision.Reason = ErrRequestOverLimit.Error()
		return decision, ErrRequestOverLimit
	}
	pressureReasons := snapshot.Pressure.unsafe(thresholds)
	if len(pressureReasons) > 0 {
		decision.Reason = "pressure: " + joinReasons(pressureReasons)
		return decision, ErrPressure
	}
	unavailable, err := snapshot.Reserved.Add(snapshot.WarmPoolCost)
	if err != nil {
		return decision, err
	}
	unavailable, err = unavailable.Add(snapshot.ControlReserve)
	if err != nil {
		return decision, err
	}
	available, err := snapshot.Allocatable.Subtract(unavailable)
	if err != nil {
		decision.Reason = "control reserve or existing work exceeds allocatable capacity"
		return decision, ErrInsufficient
	}
	decision.AvailableBefore = available
	required, err := request.Requests.Add(request.ObserverCost)
	if err != nil {
		return decision, err
	}
	required, err = required.Add(request.SnapshotCost)
	if err != nil {
		return decision, err
	}
	if !required.FitsWithin(available) {
		decision.Reason = ErrInsufficient.Error()
		return decision, ErrInsufficient
	}
	decision.AvailableAfter, _ = available.Subtract(required)
	decision.EffectiveBudget = request.Limits.Clone()
	decision.Admitted = true
	decision.Reason = "request, observer, snapshot, and control-plane headroom fit"
	return decision, nil
}

func decisionDigest(values ...any) (string, error) {
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("world-admission-decision-v1\x00"), encoded...))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func joinReasons(values []string) string {
	sort.Strings(values)
	result := ""
	for index, value := range values {
		if index > 0 {
			result += ", "
		}
		result += value
	}
	return result
}
