package cuttlefish

import (
	"path/filepath"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

func TestManagedEmulatorDeviceConfigDigestIsOrderStableAndExact(t *testing.T) {
	first := domain.NewDigest([]byte("first"))
	second := domain.NewDigest([]byte("second"))
	root := t.TempDir()
	base := ManagedEmulatorDeviceConfigIdentity{
		EmulatorBinary: filepath.Join(root, "sdk", "emulator", "emulator.exe"), ADBBinary: "adb", SDKManagerBinary: "sdkmanager.bat", AVDManagerBinary: "avdmanager.bat",
		SDKRoot: filepath.Join(root, "sdk"), ADBServerEndpoint: "127.0.0.1:5037",
		ExpectedBackendVersion: "Android emulator version 35.2.10", ExpectedRuntimeVersion: "aosp/emu:userdebug/test-keys",
		BaseConsolePort: 5554, LastConsolePort: 5584,
		SystemImages: map[string]ManagedSystemImage{
			first.String():  {Package: "system-images;android-35;google_apis;x86_64"},
			second.String(): {Package: "system-images;android-36;google_apis;x86_64"},
		},
	}
	digest, err := ManagedEmulatorDeviceConfigDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.SystemImages = map[string]ManagedSystemImage{
		second.String(): base.SystemImages[second.String()], first.String(): base.SystemImages[first.String()],
	}
	reorderedDigest, err := ManagedEmulatorDeviceConfigDigest(reordered)
	if err != nil || reorderedDigest != digest {
		t.Fatalf("mapping order changed identity: %s / %s / %v", digest, reorderedDigest, err)
	}
	changed := base
	changed.ExpectedRuntimeVersion = "aosp/emu:userdebug/other"
	changedDigest, err := ManagedEmulatorDeviceConfigDigest(changed)
	if err != nil || changedDigest == digest {
		t.Fatalf("runtime identity did not affect configuration digest: %s / %v", changedDigest, err)
	}
	relative := base
	relative.EmulatorBinary = "emulator"
	if _, err := ManagedEmulatorDeviceConfigDigest(relative); err == nil {
		t.Fatal("relative PATH-dependent emulator identity was accepted")
	}
}
