// Package androidcontract owns resource constraints shared by policy,
// provisioning, and Android runtime drivers.
package androidcontract

import "fmt"

const (
	Mebibyte                = int64(1 << 20)
	MinimumHostCPUMilli     = int64(1000)
	MaximumHostCPUMilli     = int64(64000)
	MinimumGuestMemoryMiB   = int64(1536)
	MaximumGuestMemoryMiB   = int64(8192)
	MinimumDataPartitionMiB = int64(64)
	MaximumDataPartitionMiB = int64(2047)
)

func ValidateHostCPUMilli(value int64) error {
	if value < MinimumHostCPUMilli || value > MaximumHostCPUMilli || value%1000 != 0 {
		return fmt.Errorf(
			"managed Android host CPU must be an exact whole-vCPU value from %d to %d milli-CPU",
			MinimumHostCPUMilli,
			MaximumHostCPUMilli,
		)
	}
	return nil
}

func ValidateGuestMemoryBytes(value int64) error {
	memoryMiB := value / Mebibyte
	if value%Mebibyte != 0 || memoryMiB < MinimumGuestMemoryMiB || memoryMiB > MaximumGuestMemoryMiB {
		return fmt.Errorf(
			"Android guest memory must be from %d to %d MiB inclusive and exactly MiB-aligned",
			MinimumGuestMemoryMiB,
			MaximumGuestMemoryMiB,
		)
	}
	return nil
}

func ValidateDataPartitionBytes(value int64) error {
	partitionMiB := value / Mebibyte
	if value%Mebibyte != 0 || partitionMiB < MinimumDataPartitionMiB || partitionMiB > MaximumDataPartitionMiB {
		return fmt.Errorf(
			"Android guest data partition must be from %d to %d MiB inclusive and exactly MiB-aligned",
			MinimumDataPartitionMiB,
			MaximumDataPartitionMiB,
		)
	}
	return nil
}
