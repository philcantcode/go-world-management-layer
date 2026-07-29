package cuttlefish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

const (
	managedEmulatorDataFilename         = "world-userdata.img"
	managedEmulatorDataCreatingFilename = managedEmulatorDataFilename + ".creating"
	managedEmulatorDataOverlayFilename  = managedEmulatorDataFilename + ".qcow2"
	managedEmulatorDataIdentityFilename = "world-userdata.json"
	managedEmulatorDataIdentityVersion  = 1
	maximumManagedFormatterBinaryBytes  = int64(64 << 20)
	maximumManagedFormatterConfigBytes  = int64(1 << 20)
)

type managedDataFormatterIdentity struct {
	Binary       string `json:"binary"`
	BinaryDigest string `json:"binary_digest"`
	Config       string `json:"config"`
	ConfigDigest string `json:"config_digest"`
	Version      string `json:"version"`
}

type managedAVDStorageIdentity struct {
	Version            int                          `json:"version"`
	RuntimeID          string                       `json:"runtime_id"`
	StateDirectory     string                       `json:"state_directory"`
	DeviceConfigDigest string                       `json:"device_config_digest"`
	BackingFile        string                       `json:"backing_file"`
	BackingBytes       int64                        `json:"backing_bytes"`
	BackingDigest      string                       `json:"backing_digest"`
	BackingReadOnly    bool                         `json:"backing_read_only"`
	OverlayFile        string                       `json:"overlay_file"`
	Formatter          managedDataFormatterIdentity `json:"formatter"`
}

type managedDataStorageBinding struct {
	IdentityDigest string
	BackingPath    string
	BackingBytes   int64
	BackingDigest  string
	OverlayPath    string
	Formatter      managedDataFormatterIdentity
}

type managedDataStorageAuthority struct {
	DeviceConfigDigest    string `json:"device_config_digest"`
	IdentityDigest        string `json:"identity_digest"`
	BackingPath           string `json:"backing_path"`
	BackingBytes          int64  `json:"backing_bytes"`
	BackingDigest         string `json:"backing_digest"`
	OverlayPath           string `json:"overlay_path"`
	FormatterBinary       string `json:"formatter_binary"`
	FormatterBinaryDigest string `json:"formatter_binary_digest"`
	FormatterConfig       string `json:"formatter_config"`
	FormatterConfigDigest string `json:"formatter_config_digest"`
	FormatterVersion      string `json:"formatter_version"`
}

type managedDataOverlayRequirement uint8

const (
	managedDataOverlayAbsent managedDataOverlayRequirement = iota
	managedDataOverlayPresent
)

func managedMKE2FSPaths(sdkRoot string) (string, string) {
	name := "mke2fs"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	tools := filepath.Join(filepath.Clean(sdkRoot), "platform-tools")
	return filepath.Join(tools, name), filepath.Join(tools, "mke2fs.conf")
}

func (b *ManagedEmulatorBackend) observeManagedDataFormatter(ctx context.Context) (managedDataFormatterIdentity, error) {
	binary, config := managedMKE2FSPaths(b.sdkRoot)
	if err := requirePathWithin(b.sdkRoot, binary, false); err != nil {
		return managedDataFormatterIdentity{}, fmt.Errorf("canonicalize managed mke2fs executable: %w", err)
	}
	if err := requirePathWithin(b.sdkRoot, config, false); err != nil {
		return managedDataFormatterIdentity{}, fmt.Errorf("canonicalize managed mke2fs configuration: %w", err)
	}
	binaryBytes, binaryDigest, err := digestExactManagedRegularFile(ctx, b.sdkRoot, binary, -1, maximumManagedFormatterBinaryBytes, false)
	if err != nil || binaryBytes == 0 {
		return managedDataFormatterIdentity{}, fmt.Errorf("observe managed mke2fs executable identity: %w", nonNilManagedDataError(err, "executable is empty"))
	}
	configBytes, configDigest, err := digestExactManagedRegularFile(ctx, b.sdkRoot, config, -1, maximumManagedFormatterConfigBytes, false)
	if err != nil || configBytes == 0 {
		return managedDataFormatterIdentity{}, fmt.Errorf("observe managed mke2fs configuration identity: %w", nonNilManagedDataError(err, "configuration is empty"))
	}
	output, err := b.runTool(ctx, binary, []string{"-V"}, b.mke2fsEnvironment(config))
	if err != nil {
		return managedDataFormatterIdentity{}, fmt.Errorf("observe managed mke2fs version: %w", err)
	}
	version := firstNonBlankLine(output)
	if version == "" {
		return managedDataFormatterIdentity{}, fmt.Errorf("managed mke2fs returned no observed version")
	}
	return managedDataFormatterIdentity{
		Binary: binary, BinaryDigest: binaryDigest, Config: config, ConfigDigest: configDigest, Version: version,
	}, nil
}

func nonNilManagedDataError(err error, fallback string) error {
	if err != nil {
		return err
	}
	return errors.New(fallback)
}

func (b *ManagedEmulatorBackend) createExactManagedDataImage(ctx context.Context, instance Instance) (resultErr error) {
	if instance.Resources.StorageBytes <= 0 {
		return fmt.Errorf("managed data backing size must be positive")
	}
	storageRoot := filepath.Clean(instance.StateDirectory)
	if err := requirePathWithin(b.stateRoot, storageRoot, true); err != nil {
		return fmt.Errorf("canonicalize generation-scoped managed data directory: %w", err)
	}
	formatter, err := b.observeManagedDataFormatter(ctx)
	if err != nil {
		return err
	}
	creatingPath := filepath.Join(storageRoot, managedEmulatorDataCreatingFilename)
	backingPath := filepath.Join(storageRoot, managedEmulatorDataFilename)
	overlayPath := filepath.Join(storageRoot, managedEmulatorDataOverlayFilename)
	identityPath := filepath.Join(storageRoot, managedEmulatorDataIdentityFilename)
	for _, path := range []string{creatingPath, backingPath, overlayPath, identityPath} {
		if err := requirePathWithin(storageRoot, path, false); err != nil {
			return fmt.Errorf("canonicalize managed data artifact: %w", err)
		}
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("managed data artifact %q already exists before exact creation", filepath.Base(path))
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect managed data artifact %q: %w", filepath.Base(path), err)
		}
	}
	staged, err := os.OpenFile(creatingPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create exclusive managed data backing stage: %w", err)
	}
	defer func() {
		_ = staged.Close()
		if resultErr != nil {
			_ = makeManagedDataBackingWritable(creatingPath)
			_ = os.Remove(creatingPath)
			_ = makeManagedDataBackingWritable(backingPath)
			_ = os.Remove(backingPath)
			_ = os.Remove(identityPath)
		}
	}()
	if err := staged.Truncate(instance.Resources.StorageBytes); err != nil {
		return fmt.Errorf("size exact managed data backing stage: %w", err)
	}
	if err := staged.Sync(); err != nil {
		return fmt.Errorf("sync sized managed data backing stage: %w", err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("close sized managed data backing stage: %w", err)
	}
	formatArgs := []string{"-t", "ext4", "-F", "-m", "0", "-L", "data", creatingPath}
	if _, err := b.runTool(ctx, formatter.Binary, formatArgs, b.mke2fsEnvironment(formatter.Config)); err != nil {
		return fmt.Errorf("format exact managed data backing as ext4: %w", err)
	}
	if err := syncManagedRegularFile(creatingPath); err != nil {
		return fmt.Errorf("sync formatted managed data backing: %w", err)
	}
	if err := sealManagedDataBackingReadOnly(creatingPath); err != nil {
		return fmt.Errorf("seal managed data backing read-only: %w", err)
	}
	actualBytes, backingDigest, err := digestExactManagedRegularFile(ctx, storageRoot, creatingPath, instance.Resources.StorageBytes, instance.Resources.StorageBytes, true)
	if err != nil {
		return fmt.Errorf("verify formatted managed data backing stage: %w", err)
	}
	identity := managedAVDStorageIdentity{
		Version: managedEmulatorDataIdentityVersion, RuntimeID: instance.RuntimeID, StateDirectory: storageRoot,
		DeviceConfigDigest: instance.Fingerprint.DeviceConfigDigest.String(), BackingFile: managedEmulatorDataFilename,
		BackingBytes: actualBytes, BackingDigest: backingDigest, BackingReadOnly: true,
		OverlayFile: managedEmulatorDataOverlayFilename, Formatter: formatter,
	}
	if err := identity.validate(instance, formatter); err != nil {
		return err
	}
	if err := atomicfile.PublishExclusive(creatingPath, backingPath); err != nil {
		return fmt.Errorf("atomically publish exact managed data backing: %w", err)
	}
	if _, digest, err := digestExactManagedRegularFile(ctx, storageRoot, backingPath, instance.Resources.StorageBytes, instance.Resources.StorageBytes, true); err != nil || digest != backingDigest {
		return fmt.Errorf("verify published managed data backing identity: %w", nonNilManagedDataError(err, "published digest differs from completed stage"))
	}
	if err := writeExclusiveManagedManifest(identityPath, identity); err != nil {
		return fmt.Errorf("commit immutable managed data-storage identity: %w", err)
	}
	return nil
}

func syncManagedRegularFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func (b *ManagedEmulatorBackend) requireManagedDataStorage(ctx context.Context, instance Instance, overlayRequirement managedDataOverlayRequirement) (managedDataStorageBinding, error) {
	storageRoot := filepath.Clean(instance.StateDirectory)
	if err := requirePathWithin(b.stateRoot, storageRoot, true); err != nil {
		return managedDataStorageBinding{}, fmt.Errorf("canonicalize generation-scoped managed data directory: %w", err)
	}
	identity, err := readManagedAVDStorageIdentity(storageRoot)
	if err != nil {
		return managedDataStorageBinding{}, err
	}
	formatter, err := b.observeManagedDataFormatter(ctx)
	if err != nil {
		return managedDataStorageBinding{}, err
	}
	if err := identity.validate(instance, formatter); err != nil {
		return managedDataStorageBinding{}, err
	}
	backingPath := filepath.Join(storageRoot, identity.BackingFile)
	actualBytes, actualDigest, err := digestExactManagedRegularFile(ctx, storageRoot, backingPath, identity.BackingBytes, identity.BackingBytes, true)
	if err != nil {
		return managedDataStorageBinding{}, fmt.Errorf("re-prove exact managed data backing: %w", err)
	}
	if actualBytes != identity.BackingBytes || actualDigest != identity.BackingDigest {
		return managedDataStorageBinding{}, fmt.Errorf("managed data backing bytes or digest differ from immutable identity")
	}
	creatingPath := filepath.Join(storageRoot, managedEmulatorDataCreatingFilename)
	if _, err := os.Lstat(creatingPath); err == nil {
		return managedDataStorageBinding{}, fmt.Errorf("managed data backing retains an incomplete creation stage")
	} else if !os.IsNotExist(err) {
		return managedDataStorageBinding{}, fmt.Errorf("inspect managed data backing creation stage: %w", err)
	}
	overlayPath := filepath.Join(storageRoot, identity.OverlayFile)
	switch overlayRequirement {
	case managedDataOverlayAbsent:
		if _, err := os.Lstat(overlayPath); err == nil {
			return managedDataStorageBinding{}, fmt.Errorf("fresh managed data backing already has a writable overlay")
		} else if !os.IsNotExist(err) {
			return managedDataStorageBinding{}, fmt.Errorf("inspect fresh managed data overlay: %w", err)
		}
	case managedDataOverlayPresent:
		if err := requireExactManagedQCOW2Overlay(storageRoot, overlayPath, backingPath, identity.BackingBytes); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return managedDataStorageBinding{}, fmt.Errorf("%w: %v", errManagedDataOverlayNotReady, err)
			}
			return managedDataStorageBinding{}, err
		}
	default:
		return managedDataStorageBinding{}, fmt.Errorf("invalid managed data-overlay requirement")
	}
	identityDigest, err := identity.digest()
	if err != nil {
		return managedDataStorageBinding{}, err
	}
	binding := managedDataStorageBinding{
		IdentityDigest: identityDigest.String(), BackingPath: backingPath, BackingBytes: actualBytes,
		BackingDigest: actualDigest, OverlayPath: overlayPath, Formatter: formatter,
	}
	if err := binding.validate(instance); err != nil {
		return managedDataStorageBinding{}, err
	}
	return binding, nil
}

func requireExactManagedQCOW2Overlay(storageRoot, overlayPath, backingPath string, expectedVirtualBytes int64) error {
	file, info, err := openExactManagedRegularFile(storageRoot, overlayPath)
	if err != nil {
		return fmt.Errorf("verify exact managed qcow2 overlay: %w", err)
	}
	defer file.Close()
	const headerBytes = 72
	if info.Size() < headerBytes {
		return fmt.Errorf("managed qcow2 overlay is shorter than its version-2 header")
	}
	header := make([]byte, headerBytes)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("read managed qcow2 header: %w", err)
	}
	if !bytes.Equal(header[:4], []byte{'Q', 'F', 'I', 0xfb}) {
		return fmt.Errorf("managed data overlay does not have qcow2 magic")
	}
	version := binary.BigEndian.Uint32(header[4:8])
	if version != 2 && version != 3 {
		return fmt.Errorf("managed data overlay qcow2 version is %d, want 2 or 3", version)
	}
	backingOffset := binary.BigEndian.Uint64(header[8:16])
	backingSize := binary.BigEndian.Uint32(header[16:20])
	virtualBytes := binary.BigEndian.Uint64(header[24:32])
	if expectedVirtualBytes <= 0 || virtualBytes != uint64(expectedVirtualBytes) {
		return fmt.Errorf("managed qcow2 virtual size is %d bytes, want exact %d", virtualBytes, expectedVirtualBytes)
	}
	if backingOffset < headerBytes || backingSize == 0 || backingSize > 32<<10 || backingOffset > uint64(info.Size()) || uint64(backingSize) > uint64(info.Size())-backingOffset {
		return fmt.Errorf("managed qcow2 backing filename location is invalid")
	}
	backingName := make([]byte, backingSize)
	if _, err := file.ReadAt(backingName, int64(backingOffset)); err != nil {
		return fmt.Errorf("read managed qcow2 backing filename: %w", err)
	}
	expectedName := filepath.Base(backingPath)
	if string(backingName) != expectedName || strings.ContainsAny(string(backingName), `/\`+"\x00\r\n\t") {
		return fmt.Errorf("managed qcow2 backing filename %q differs from exact raw backing %q", string(backingName), expectedName)
	}
	return nil
}

func readManagedAVDStorageIdentity(storageRoot string) (managedAVDStorageIdentity, error) {
	path := filepath.Join(storageRoot, managedEmulatorDataIdentityFilename)
	file, info, err := openExactManagedRegularFile(storageRoot, path)
	if err != nil {
		return managedAVDStorageIdentity{}, fmt.Errorf("open immutable managed data-storage identity: %w", err)
	}
	defer file.Close()
	if info.Size() <= 0 || info.Size() > maximumManifestBytes {
		return managedAVDStorageIdentity{}, fmt.Errorf("managed data-storage identity is not a bounded non-empty file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, maximumManifestBytes+1))
	decoder.DisallowUnknownFields()
	var identity managedAVDStorageIdentity
	if err := decoder.Decode(&identity); err != nil {
		return managedAVDStorageIdentity{}, fmt.Errorf("decode managed data-storage identity: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return managedAVDStorageIdentity{}, fmt.Errorf("managed data-storage identity contains trailing JSON")
		}
		return managedAVDStorageIdentity{}, err
	}
	return identity, nil
}

func (identity managedAVDStorageIdentity) validate(instance Instance, formatter managedDataFormatterIdentity) error {
	storageRoot := filepath.Clean(instance.StateDirectory)
	if identity.Version != managedEmulatorDataIdentityVersion || identity.BackingFile != managedEmulatorDataFilename ||
		identity.OverlayFile != managedEmulatorDataOverlayFilename || identity.BackingBytes != instance.Resources.StorageBytes || !identity.BackingReadOnly ||
		identity.RuntimeID != instance.RuntimeID || identity.StateDirectory != storageRoot || identity.DeviceConfigDigest != instance.Fingerprint.DeviceConfigDigest.String() {
		return fmt.Errorf("managed data-storage identity differs from the exact backing/overlay plan")
	}
	if _, err := domain.ParseDigest(identity.BackingDigest); err != nil {
		return fmt.Errorf("managed data backing digest is invalid: %w", err)
	}
	if err := identity.Formatter.validate(formatter); err != nil {
		return err
	}
	for _, path := range []string{filepath.Join(storageRoot, identity.BackingFile), filepath.Join(storageRoot, identity.OverlayFile)} {
		if err := requirePathWithin(storageRoot, path, false); err != nil {
			return fmt.Errorf("managed data-storage identity redirects outside its AVD: %w", err)
		}
	}
	return nil
}

func (identity managedDataFormatterIdentity) validate(expected managedDataFormatterIdentity) error {
	for name, value := range map[string]string{
		"binary": identity.Binary, "binary_digest": identity.BinaryDigest, "config": identity.Config,
		"config_digest": identity.ConfigDigest, "version": identity.Version,
	} {
		if value == "" || value != strings.TrimSpace(value) || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("managed data formatter %s is blank, untrimmed, or contains NUL", name)
		}
	}
	if err := requireExactManagedProcessPathValue(identity.Binary, expected.Binary, runtime.GOOS == "windows"); err != nil {
		return fmt.Errorf("managed data formatter executable: %w", err)
	}
	if err := requireExactManagedProcessPathValue(identity.Config, expected.Config, runtime.GOOS == "windows"); err != nil {
		return fmt.Errorf("managed data formatter configuration: %w", err)
	}
	if identity.BinaryDigest != expected.BinaryDigest || identity.ConfigDigest != expected.ConfigDigest || identity.Version != expected.Version {
		return fmt.Errorf("managed data formatter observed identity changed")
	}
	for _, digest := range []string{identity.BinaryDigest, identity.ConfigDigest} {
		if _, err := domain.ParseDigest(digest); err != nil {
			return fmt.Errorf("managed data formatter digest is invalid: %w", err)
		}
	}
	return nil
}

func (identity managedAVDStorageIdentity) digest() (domain.Digest, error) {
	encoded, err := json.Marshal(identity)
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(append([]byte("world.managed-android-data-storage.v1\n"), encoded...)), nil
}

func (binding managedDataStorageBinding) authority(instance Instance) managedDataStorageAuthority {
	return managedDataStorageAuthority{
		DeviceConfigDigest: instance.Fingerprint.DeviceConfigDigest.String(), IdentityDigest: binding.IdentityDigest,
		BackingPath: binding.BackingPath, BackingBytes: binding.BackingBytes, BackingDigest: binding.BackingDigest,
		OverlayPath: binding.OverlayPath, FormatterBinary: binding.Formatter.Binary,
		FormatterBinaryDigest: binding.Formatter.BinaryDigest, FormatterConfig: binding.Formatter.Config,
		FormatterConfigDigest: binding.Formatter.ConfigDigest, FormatterVersion: binding.Formatter.Version,
	}
}

func (binding managedDataStorageBinding) validate(instance Instance) error {
	if binding.BackingBytes != instance.Resources.StorageBytes {
		return fmt.Errorf("managed data binding has %d backing bytes, want exact %d", binding.BackingBytes, instance.Resources.StorageBytes)
	}
	for _, digest := range []string{binding.IdentityDigest, binding.BackingDigest} {
		if _, err := domain.ParseDigest(digest); err != nil {
			return fmt.Errorf("managed data binding digest is invalid: %w", err)
		}
	}
	expectedBacking := filepath.Join(instance.StateDirectory, managedEmulatorDataFilename)
	expectedOverlay := filepath.Join(instance.StateDirectory, managedEmulatorDataOverlayFilename)
	if err := requireExactManagedProcessPathValue(binding.BackingPath, expectedBacking, runtime.GOOS == "windows"); err != nil {
		return fmt.Errorf("managed data backing path: %w", err)
	}
	if err := requireExactManagedProcessPathValue(binding.OverlayPath, expectedOverlay, runtime.GOOS == "windows"); err != nil {
		return fmt.Errorf("managed data overlay path: %w", err)
	}
	if err := binding.Formatter.validate(binding.Formatter); err != nil {
		return err
	}
	return nil
}

func (authority managedDataStorageAuthority) validate(instance Instance) error {
	if authority.DeviceConfigDigest != instance.Fingerprint.DeviceConfigDigest.String() || authority.BackingBytes != instance.Resources.StorageBytes ||
		authority.FormatterVersion == "" || authority.FormatterVersion != strings.TrimSpace(authority.FormatterVersion) {
		return fmt.Errorf("managed data runtime authority differs from its exact device configuration or storage plan")
	}
	for _, path := range []string{authority.BackingPath, authority.OverlayPath, authority.FormatterBinary, authority.FormatterConfig} {
		canonical, err := canonicalManagedProcessPathArgument(path)
		if err != nil || canonical != path {
			return fmt.Errorf("managed data runtime authority contains a non-canonical path")
		}
	}
	for _, digest := range []string{
		authority.DeviceConfigDigest, authority.IdentityDigest, authority.BackingDigest,
		authority.FormatterBinaryDigest, authority.FormatterConfigDigest,
	} {
		if _, err := domain.ParseDigest(digest); err != nil {
			return fmt.Errorf("managed data runtime authority digest is invalid: %w", err)
		}
	}
	expectedBacking := filepath.Join(instance.StateDirectory, managedEmulatorDataFilename)
	expectedOverlay := filepath.Join(instance.StateDirectory, managedEmulatorDataOverlayFilename)
	if err := requireExactManagedProcessPathValue(authority.BackingPath, expectedBacking, runtime.GOOS == "windows"); err != nil {
		return fmt.Errorf("managed data runtime backing path: %w", err)
	}
	if err := requireExactManagedProcessPathValue(authority.OverlayPath, expectedOverlay, runtime.GOOS == "windows"); err != nil {
		return fmt.Errorf("managed data runtime overlay path: %w", err)
	}
	return nil
}

func (authority managedDataStorageAuthority) validateStoredIdentity(instance Instance) error {
	if err := authority.validate(instance); err != nil {
		return err
	}
	identity, err := readManagedAVDStorageIdentity(instance.StateDirectory)
	if err != nil {
		return fmt.Errorf("read storage identity bound by runtime authority: %w", err)
	}
	identityDigest, err := identity.digest()
	if err != nil {
		return err
	}
	expected := managedDataStorageAuthority{
		DeviceConfigDigest: instance.Fingerprint.DeviceConfigDigest.String(), IdentityDigest: identityDigest.String(),
		BackingPath: filepath.Join(instance.StateDirectory, identity.BackingFile), BackingBytes: identity.BackingBytes,
		BackingDigest: identity.BackingDigest, OverlayPath: filepath.Join(instance.StateDirectory, identity.OverlayFile),
		FormatterBinary: identity.Formatter.Binary, FormatterBinaryDigest: identity.Formatter.BinaryDigest,
		FormatterConfig: identity.Formatter.Config, FormatterConfigDigest: identity.Formatter.ConfigDigest,
		FormatterVersion: identity.Formatter.Version,
	}
	if authority != expected || identity.Version != managedEmulatorDataIdentityVersion || identity.RuntimeID != instance.RuntimeID ||
		identity.StateDirectory != filepath.Clean(instance.StateDirectory) || identity.DeviceConfigDigest != instance.Fingerprint.DeviceConfigDigest.String() ||
		identity.BackingFile != managedEmulatorDataFilename || identity.OverlayFile != managedEmulatorDataOverlayFilename || !identity.BackingReadOnly {
		return fmt.Errorf("managed data runtime authority differs from immutable generation storage identity")
	}
	return nil
}

func (authority managedDataStorageAuthority) requireBinding(instance Instance, binding managedDataStorageBinding) error {
	if err := authority.validate(instance); err != nil {
		return err
	}
	if err := binding.validate(instance); err != nil {
		return err
	}
	expected := binding.authority(instance)
	if authority != expected {
		return fmt.Errorf("managed data runtime authority differs from the re-proven backing, overlay, or formatter identity")
	}
	return nil
}

func openExactManagedRegularFile(root, path string) (*os.File, os.FileInfo, error) {
	if err := requirePathWithin(root, path, false); err != nil {
		return nil, nil, err
	}
	lstat, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if lstat.Mode()&os.ModeSymlink != 0 || !lstat.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("path %q is not one exact regular file", path)
	}
	if err := requireManagedAVDPathNotReparse(path); err != nil {
		return nil, nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(lstat, opened) {
		_ = file.Close()
		return nil, nil, fmt.Errorf("path %q changed identity or resolves through indirection", path)
	}
	return file, opened, nil
}

func digestExactManagedRegularFile(ctx context.Context, root, path string, expectedBytes, maximumBytes int64, readOnly bool) (int64, string, error) {
	file, info, err := openExactManagedRegularFile(root, path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	if expectedBytes >= 0 && info.Size() != expectedBytes {
		return 0, "", fmt.Errorf("regular file size is %d bytes, want exact %d", info.Size(), expectedBytes)
	}
	if maximumBytes >= 0 && info.Size() > maximumBytes {
		return 0, "", fmt.Errorf("regular file size %d exceeds maximum %d", info.Size(), maximumBytes)
	}
	if readOnly {
		sealed, err := managedDataBackingReadOnly(path, info)
		if err != nil {
			return 0, "", err
		}
		if !sealed {
			return 0, "", fmt.Errorf("managed data backing is not read-only")
		}
	}
	hasher := sha256.New()
	written, err := io.Copy(hasher, &contextCheckingReader{ctx: ctx, reader: file})
	if err != nil {
		return 0, "", err
	}
	if written != info.Size() {
		return 0, "", fmt.Errorf("hashed %d bytes, want exact file size %d", written, info.Size())
	}
	after, err := file.Stat()
	if err != nil {
		return 0, "", fmt.Errorf("inspect regular file after hashing: %w", err)
	}
	if !os.SameFile(info, after) || after.Size() != info.Size() {
		return 0, "", fmt.Errorf("regular file identity or size changed while hashing")
	}
	return info.Size(), "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

type contextCheckingReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextCheckingReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func sealManagedDataBackingReadOnly(path string) error {
	if err := os.Chmod(path, 0o400); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	sealed, err := managedDataBackingReadOnly(path, info)
	if err != nil {
		return err
	}
	if !sealed {
		return fmt.Errorf("read-only backing seal was not applied")
	}
	return nil
}

func makeManagedDataBackingWritable(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("managed data artifact is not one exact regular file")
	}
	if err := requireManagedAVDPathNotReparse(path); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

func (b *ManagedEmulatorBackend) requireManagedDataOverlayAbsentForRecovery(instance Instance) error {
	storageRoot := filepath.Clean(instance.StateDirectory)
	if err := requirePathWithin(b.stateRoot, storageRoot, true); err != nil {
		return err
	}
	overlayPath := filepath.Join(storageRoot, managedEmulatorDataOverlayFilename)
	if _, err := os.Lstat(overlayPath); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect unstarted managed data overlay: %w", err)
	}
	return fmt.Errorf("managed data overlay exists without authoritative launch intent; prior guest execution cannot be excluded")
}

func (b *ManagedEmulatorBackend) removeExactManagedDataStorage(instance Instance) error {
	storageRoot := filepath.Clean(instance.StateDirectory)
	if err := requirePathWithin(b.stateRoot, storageRoot, true); err != nil {
		return err
	}
	for _, filename := range []string{
		managedEmulatorDataOverlayFilename, managedEmulatorDataFilename,
		managedEmulatorDataCreatingFilename, managedEmulatorDataIdentityFilename,
	} {
		path := filepath.Join(storageRoot, filename)
		file, _, err := openExactManagedRegularFile(storageRoot, path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("verify exact managed data artifact %q before removal: %w", filename, err)
		}
		if err := file.Close(); err != nil {
			return err
		}
		if filename == managedEmulatorDataFilename || filename == managedEmulatorDataCreatingFilename {
			if err := makeManagedDataBackingWritable(path); err != nil {
				return err
			}
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove exact managed data artifact %q: %w", filename, err)
		}
	}
	return nil
}

func (b *ManagedEmulatorBackend) mke2fsEnvironment(configPath string) []string {
	return replaceManagedEnvironment(b.sdkEnvironment(), "MKE2FS_CONFIG", configPath)
}

func replaceManagedEnvironment(environment []string, key, value string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		name, _, found := strings.Cut(entry, "=")
		if found && strings.EqualFold(name, key) {
			continue
		}
		result = append(result, entry)
	}
	return append(result, key+"="+value)
}
