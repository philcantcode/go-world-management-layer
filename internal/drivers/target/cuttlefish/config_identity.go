package cuttlefish

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

// ManagedEmulatorDeviceConfigIdentity is the immutable, non-secret physical
// configuration shared by daemon composition and capability construction.
type ManagedEmulatorDeviceConfigIdentity struct {
	EmulatorBinary         string
	ADBBinary              string
	SDKManagerBinary       string
	AVDManagerBinary       string
	SDKRoot                string
	ADBServerEndpoint      string
	ExpectedBackendVersion string
	ExpectedRuntimeVersion string
	BaseConsolePort        int
	LastConsolePort        int
	SystemImages           map[string]ManagedSystemImage
}

// ManagedEmulatorDeviceConfigDigest returns a deterministic digest. Mapping
// order and host path separator spelling cannot change the identity.
func ManagedEmulatorDeviceConfigDigest(config ManagedEmulatorDeviceConfigIdentity) (domain.Digest, error) {
	for name, value := range map[string]string{
		"emulator_binary": config.EmulatorBinary, "adb_binary": config.ADBBinary,
		"sdkmanager_binary": config.SDKManagerBinary, "avdmanager_binary": config.AVDManagerBinary,
		"sdk_root": config.SDKRoot, "adb_server_endpoint": config.ADBServerEndpoint,
		"expected_backend_version": config.ExpectedBackendVersion, "expected_runtime_version": config.ExpectedRuntimeVersion,
	} {
		if value == "" || value != strings.TrimSpace(value) || strings.IndexByte(value, 0) >= 0 {
			return domain.Digest{}, fmt.Errorf("managed emulator device configuration %s is blank, untrimmed, or contains NUL", name)
		}
	}
	if !filepath.IsAbs(config.SDKRoot) {
		return domain.Digest{}, fmt.Errorf("managed emulator SDK root must be absolute")
	}
	if !filepath.IsAbs(config.EmulatorBinary) {
		return domain.Digest{}, fmt.Errorf("managed emulator executable identity must be absolute")
	}
	if _, err := parseADBServerEndpoint(config.ADBServerEndpoint); err != nil {
		return domain.Digest{}, fmt.Errorf("managed emulator ADB server endpoint: %w", err)
	}
	if err := validateConsolePortRange(config.BaseConsolePort, config.LastConsolePort); err != nil {
		return domain.Digest{}, err
	}
	if len(config.SystemImages) == 0 {
		return domain.Digest{}, fmt.Errorf("managed emulator device configuration requires system-image mappings")
	}
	digests := make([]string, 0, len(config.SystemImages))
	for digestText, image := range config.SystemImages {
		if _, err := domain.ParseDigest(digestText); err != nil {
			return domain.Digest{}, fmt.Errorf("managed emulator system-image mapping key: %w", err)
		}
		if err := ValidateManagedSystemImagePackage(image.Package); err != nil {
			return domain.Digest{}, err
		}
		digests = append(digests, digestText)
	}
	sort.Strings(digests)
	mke2fsBinary, mke2fsConfig := managedMKE2FSPaths(config.SDKRoot)
	var canonical bytes.Buffer
	writeFingerprintString(&canonical, "world.managed-android-emulator-device-config.v2")
	for _, value := range []string{
		filepath.Clean(config.EmulatorBinary), filepath.Clean(config.ADBBinary), filepath.Clean(config.SDKManagerBinary),
		filepath.Clean(config.AVDManagerBinary), filepath.Clean(mke2fsBinary), filepath.Clean(mke2fsConfig), filepath.Clean(config.SDKRoot),
	} {
		writeFingerprintString(&canonical, filepath.ToSlash(value))
	}
	for _, value := range []string{config.ADBServerEndpoint, config.ExpectedBackendVersion, config.ExpectedRuntimeVersion} {
		writeFingerprintString(&canonical, value)
	}
	_ = binary.Write(&canonical, binary.BigEndian, int64(config.BaseConsolePort))
	_ = binary.Write(&canonical, binary.BigEndian, int64(config.LastConsolePort))
	for _, digestText := range digests {
		writeFingerprintString(&canonical, digestText)
		writeFingerprintString(&canonical, config.SystemImages[digestText].Package)
	}
	return domain.NewDigest(canonical.Bytes()), nil
}
