package androidcontract

import "testing"

func TestHostCPUBoundariesRequireWholeVCPUs(t *testing.T) {
	for _, valid := range []int64{MinimumHostCPUMilli, MaximumHostCPUMilli, 4000} {
		if err := ValidateHostCPUMilli(valid); err != nil {
			t.Fatalf("valid host CPU %d: %v", valid, err)
		}
	}
	for _, invalid := range []int64{0, MinimumHostCPUMilli + 1, MaximumHostCPUMilli + 1000} {
		if err := ValidateHostCPUMilli(invalid); err == nil {
			t.Fatalf("invalid host CPU %d was accepted", invalid)
		}
	}
}

func TestGuestMemoryBoundariesMatchAndroidEmulatorCLI(t *testing.T) {
	for _, valid := range []int64{MinimumGuestMemoryMiB * Mebibyte, MaximumGuestMemoryMiB * Mebibyte} {
		if err := ValidateGuestMemoryBytes(valid); err != nil {
			t.Fatalf("valid guest memory %d: %v", valid, err)
		}
	}
	for _, invalid := range []int64{
		(MinimumGuestMemoryMiB - 1) * Mebibyte,
		(MaximumGuestMemoryMiB + 1) * Mebibyte,
		MinimumGuestMemoryMiB*Mebibyte + 1,
	} {
		if err := ValidateGuestMemoryBytes(invalid); err == nil {
			t.Fatalf("invalid guest memory %d was accepted", invalid)
		}
	}
}

func TestDataPartitionBoundariesAreExactAndAligned(t *testing.T) {
	for _, valid := range []int64{MinimumDataPartitionMiB * Mebibyte, MaximumDataPartitionMiB * Mebibyte} {
		if err := ValidateDataPartitionBytes(valid); err != nil {
			t.Fatalf("valid data partition %d: %v", valid, err)
		}
	}
	for _, invalid := range []int64{
		(MinimumDataPartitionMiB - 1) * Mebibyte,
		(MaximumDataPartitionMiB + 1) * Mebibyte,
		MinimumDataPartitionMiB*Mebibyte + 1,
	} {
		if err := ValidateDataPartitionBytes(invalid); err == nil {
			t.Fatalf("invalid data partition %d was accepted", invalid)
		}
	}
}
