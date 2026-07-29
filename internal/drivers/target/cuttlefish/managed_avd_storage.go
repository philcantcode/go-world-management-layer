package cuttlefish

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
)

const managedGuestMountTableOutputLimit int64 = 64 << 10

func configureManagedAVDDataPartition(avdPath, dataPath string, sizeBytes int64) error {
	configPath, content, err := readManagedAVDConfig(avdPath)
	if err != nil {
		return err
	}
	configured, err := rewriteManagedAVDConfig(content, managedAVDConfigUpdates(dataPath, sizeBytes))
	if err != nil {
		return err
	}
	if err := atomicfile.Write(configPath, configured, 0o600); err != nil {
		return fmt.Errorf("commit managed AVD data-partition configuration: %w", err)
	}
	return nil
}

func requireManagedAVDDataPartitionConfig(avdPath, dataPath string, sizeBytes int64) error {
	_, content, err := readManagedAVDConfig(avdPath)
	if err != nil {
		return err
	}
	expected, err := rewriteManagedAVDConfig(content, managedAVDConfigUpdates(dataPath, sizeBytes))
	if err != nil {
		return err
	}
	if !bytes.Equal(content, expected) {
		return fmt.Errorf("managed AVD configuration differs from the exact data-partition and writable-state plan")
	}
	return nil
}

func readManagedAVDConfig(avdPath string) (string, []byte, error) {
	configPath := filepath.Join(avdPath, "config.ini")
	content, err := os.ReadFile(configPath)
	if err != nil {
		return "", nil, fmt.Errorf("read managed AVD configuration: %w", err)
	}
	if len(content) > 1<<20 {
		return "", nil, fmt.Errorf("managed AVD configuration exceeds 1 MiB")
	}
	return configPath, content, nil
}

func managedAVDConfigUpdates(dataPath string, sizeBytes int64) map[string]string {
	return map[string]string{
		"disk.dataPartition.size":                strconv.FormatInt(sizeBytes, 10),
		"disk.dataPartition.path":                filepath.Clean(dataPath),
		"disk.cachePartition":                    "no",
		"hw.sdCard":                              "no",
		"fastboot.forceChosenSnapshotBoot":       "no",
		"fastboot.forceColdBoot":                 "yes",
		"fastboot.forceFastBoot":                 "no",
		"firstboot.bootFromDownloadableSnapshot": "no",
		"firstboot.bootFromLocalSnapshot":        "no",
		"firstboot.saveToLocalSnapshot":          "no",
	}
}

func rewriteManagedAVDConfig(content []byte, updates map[string]string) ([]byte, error) {
	seen := make(map[string]bool, len(updates))
	var output bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		key, _, found := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if found {
			if value, replace := updates[key]; replace {
				if seen[key] {
					return nil, fmt.Errorf("managed AVD configuration contains duplicate key %q", key)
				}
				seen[key] = true
				line = key + " = " + value
			}
		}
		output.WriteString(line)
		output.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan managed AVD configuration: %w", err)
	}
	for _, key := range []string{
		"disk.dataPartition.size", "disk.dataPartition.path", "disk.cachePartition", "hw.sdCard",
		"fastboot.forceChosenSnapshotBoot", "fastboot.forceColdBoot", "fastboot.forceFastBoot",
		"firstboot.bootFromDownloadableSnapshot", "firstboot.bootFromLocalSnapshot", "firstboot.saveToLocalSnapshot",
	} {
		if !seen[key] {
			output.WriteString(key + " = " + updates[key] + "\n")
		}
	}
	return output.Bytes(), nil
}

func (b *ManagedEmulatorBackend) requireExactGuestDataPartition(ctx context.Context, instance Instance) error {
	actual, err := b.observeExactGuestDataPartitionBytes(ctx, instance)
	if err != nil {
		return err
	}
	if actual != instance.Resources.StorageBytes {
		return fmt.Errorf("%w: Android /data block size is %d bytes, want exact configured %d bytes", errManagedGuestDataPartitionMismatch, actual, instance.Resources.StorageBytes)
	}
	return nil
}

func (b *ManagedEmulatorBackend) observeExactGuestDataPartitionBytes(ctx context.Context, instance Instance) (int64, error) {
	mounts, err := runExactSerialADBAt(
		ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial,
		managedGuestMountTableOutputLimit, "shell", "cat", "/proc/mounts",
	)
	if err != nil {
		return 0, fmt.Errorf("observe exact Android /data mount: %w", err)
	}
	device, err := mountedAndroidDataDevice(string(mounts.Stdout))
	if err != nil {
		return 0, err
	}
	size, err := runExactSerialADBAt(
		ctx, b.runner, b.adbBinary, b.adbServer, instance.Allocation.Serial,
		adbMetadataOutputLimit, "shell", "blockdev", "--getsize64", device,
	)
	if err != nil {
		return 0, fmt.Errorf("observe exact Android /data block size: %w", err)
	}
	actual, err := strconv.ParseInt(strings.TrimSpace(string(size.Stdout)), 10, 64)
	if err != nil || actual <= 0 {
		return 0, fmt.Errorf("Android /data block size %q is invalid", strings.TrimSpace(string(size.Stdout)))
	}
	return actual, nil
}

func mountedAndroidDataDevice(mounts string) (string, error) {
	var device string
	for _, line := range strings.Split(mounts, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[1] != "/data" {
			continue
		}
		if device != "" {
			return "", fmt.Errorf("Android mount table contains multiple /data devices")
		}
		if fields[2] != "ext4" {
			return "", fmt.Errorf("Android /data filesystem is %q, want exact ext4", fields[2])
		}
		device = fields[0]
	}
	if !strings.HasPrefix(device, "/dev/block/") || strings.ContainsAny(device, "\x00\r\n\t ") {
		return "", fmt.Errorf("Android /data mount does not identify one safe block device")
	}
	return device, nil
}
