package cuttlefish

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
)

func TestRewriteManagedAVDConfigPinsOnlyGuestDataWritableSurface(t *testing.T) {
	configured, err := rewriteManagedAVDConfig([]byte(
		"disk.dataPartition.size = 6442450944\n"+
			"disk.cachePartition = yes\n"+
			"hw.sdCard = yes\n"+
			"unrelated = retained\n",
	), managedAVDConfigUpdates(`/world/state/world-userdata.img`, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	text := string(configured)
	for _, required := range []string{
		"disk.dataPartition.size = 1073741824",
		"disk.dataPartition.path = " + filepath.Clean(`/world/state/world-userdata.img`),
		"disk.cachePartition = no",
		"hw.sdCard = no",
		"fastboot.forceColdBoot = yes",
		"firstboot.saveToLocalSnapshot = no",
		"unrelated = retained",
	} {
		if !strings.Contains(text, required+"\n") {
			t.Fatalf("rewritten AVD configuration omits %q:\n%s", required, text)
		}
	}
}

func TestRewriteManagedAVDConfigRejectsDuplicateAuthorityKey(t *testing.T) {
	_, err := rewriteManagedAVDConfig(
		[]byte("disk.dataPartition.size=1\ndisk.dataPartition.size=2\n"),
		map[string]string{"disk.dataPartition.size": "3"},
	)
	if err == nil {
		t.Fatal("duplicate data-partition authority was accepted")
	}
}

func TestMountedAndroidDataDeviceRequiresOneSafeBlockDevice(t *testing.T) {
	device, err := mountedAndroidDataDevice("/dev/block/dm-7 /data ext4 rw 0 0\n")
	if err != nil || device != "/dev/block/dm-7" {
		t.Fatalf("exact /data device = %q, %v", device, err)
	}
	for _, invalid := range []string{
		"tmpfs /data tmpfs rw 0 0\n",
		"/dev/block/dm-7 /data f2fs rw 0 0\n",
		"/dev/block/dm-7 /data ext4 rw 0 0\n/dev/block/dm-8 /data ext4 rw 0 0\n",
		"/dev/block/dm-7 /system ext4 ro 0 0\n",
	} {
		if _, err := mountedAndroidDataDevice(invalid); err == nil {
			t.Fatalf("unsafe mount table %q was accepted", invalid)
		}
	}
}

func TestObserveExactGuestDataPartitionAllowsBoundedRealMountTable(t *testing.T) {
	const expectedBytes = int64(1 << 30)
	serial := "emulator-5556"
	mountTable := strings.Repeat("/dev/block/loop0 /system ext4 ro 0 0\n", 300) +
		"/dev/block/dm-46 /data ext4 rw 0 0\n"
	if int64(len(mountTable)) <= adbMetadataOutputLimit || int64(len(mountTable)) >= managedGuestMountTableOutputLimit {
		t.Fatalf("representative mount table has invalid test size %d", len(mountTable))
	}

	prefix := defaultADBServer.globalArgs("-s", serial)
	wantArgs := [][]string{
		append(append([]string(nil), prefix...), "shell", "cat", "/proc/mounts"),
		append(append([]string(nil), prefix...), "shell", "blockdev", "--getsize64", "/dev/block/dm-46"),
	}
	wantLimits := []int64{managedGuestMountTableOutputLimit, adbMetadataOutputLimit}
	outputs := [][]byte{[]byte(mountTable), []byte("1073741824\n")}
	call := 0
	runner := managedDataRunnerFunc(func(_ context.Context, invocation command.Invocation) (command.Result, error) {
		if call >= len(wantArgs) {
			t.Fatalf("unexpected extra command invocation: %#v", invocation)
		}
		if !reflect.DeepEqual(invocation.Args, wantArgs[call]) {
			t.Fatalf("command %d args = %#v, want %#v", call, invocation.Args, wantArgs[call])
		}
		if invocation.MaximumOutput != wantLimits[call] {
			t.Fatalf("command %d output limit = %d, want %d", call, invocation.MaximumOutput, wantLimits[call])
		}
		result := command.Result{Stdout: outputs[call]}
		call++
		return result, nil
	})
	backend := &ManagedEmulatorBackend{
		runner:    runner,
		adbBinary: "adb",
		adbServer: defaultADBServer,
	}
	actual, err := backend.observeExactGuestDataPartitionBytes(context.Background(), Instance{
		Allocation: Allocation{Serial: serial},
	})
	if err != nil {
		t.Fatal(err)
	}
	if actual != expectedBytes {
		t.Fatalf("observed /data size = %d, want %d", actual, expectedBytes)
	}
	if call != len(wantArgs) {
		t.Fatalf("command count = %d, want %d", call, len(wantArgs))
	}
}

type managedDataRunnerFunc func(context.Context, command.Invocation) (command.Result, error)

func (f managedDataRunnerFunc) Run(ctx context.Context, invocation command.Invocation) (command.Result, error) {
	return f(ctx, invocation)
}

func TestManagedDataStoragePathsAreGenerationScoped(t *testing.T) {
	root := t.TempDir()
	backend := &ManagedEmulatorBackend{}
	first := Instance{StateDirectory: filepath.Join(root, "generation-1")}
	second := Instance{StateDirectory: filepath.Join(root, "generation-2")}

	firstPath := backend.managedDataImagePath(first)
	secondPath := backend.managedDataImagePath(second)
	if firstPath == secondPath {
		t.Fatalf("distinct generation state directories share data backing %q", firstPath)
	}
	for instance, path := range map[string]string{
		first.StateDirectory:  firstPath,
		second.StateDirectory: secondPath,
	} {
		if path != filepath.Join(instance, managedEmulatorDataFilename) {
			t.Fatalf("generation-scoped data backing = %q, want exact child of %q", path, instance)
		}
	}
}

func TestRequireExactManagedQCOW2OverlayBindsBackingAndVirtualSize(t *testing.T) {
	const virtualBytes = int64(1 << 30)
	root := t.TempDir()
	backingPath := filepath.Join(root, managedEmulatorDataFilename)
	if err := os.WriteFile(backingPath, []byte("immutable backing identity"), 0o400); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name         string
		backingName  string
		virtualBytes int64
		mutate       func(*testing.T, string)
		wantError    bool
	}{
		{name: "exact", backingName: managedEmulatorDataFilename, virtualBytes: virtualBytes},
		{name: "foreign backing", backingName: "foreign-userdata.img", virtualBytes: virtualBytes, wantError: true},
		{name: "redirected backing", backingName: "../world-userdata.img", virtualBytes: virtualBytes, wantError: true},
		{name: "foreign virtual size", backingName: managedEmulatorDataFilename, virtualBytes: virtualBytes + 1, wantError: true},
		{name: "invalid magic", backingName: managedEmulatorDataFilename, virtualBytes: virtualBytes, wantError: true, mutate: func(t *testing.T, path string) {
			t.Helper()
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte("NOPE"), 0); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			overlayPath := filepath.Join(root, strings.ReplaceAll(test.name, " ", "-")+".qcow2")
			if err := writeManagedTestQCOW2(overlayPath, test.backingName, test.virtualBytes); err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(t, overlayPath)
			}
			err := requireExactManagedQCOW2Overlay(root, overlayPath, backingPath, virtualBytes)
			if (err != nil) != test.wantError {
				t.Fatalf("qcow2 authority validation error = %v, wantError=%t", err, test.wantError)
			}
		})
	}
}
