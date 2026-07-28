package cuttlefish

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type ResetFingerprint struct {
	BackendVersion     string
	RuntimeVersion     string
	SystemImageDigest  domain.Digest
	DeviceConfigDigest domain.Digest
	Features           []string
}

func (f ResetFingerprint) Validate() error {
	if f.BackendVersion == "" || f.RuntimeVersion == "" || f.SystemImageDigest.IsZero() || f.DeviceConfigDigest.IsZero() {
		return fmt.Errorf("backend/runtime versions and image/device digests are required")
	}
	seen := make(map[string]struct{}, len(f.Features))
	for _, feature := range f.Features {
		if feature == "" {
			return fmt.Errorf("reset fingerprint contains a blank feature")
		}
		if _, duplicate := seen[feature]; duplicate {
			return fmt.Errorf("reset fingerprint contains duplicate feature %q", feature)
		}
		seen[feature] = struct{}{}
	}
	return nil
}

func (f ResetFingerprint) Digest() (domain.Digest, error) {
	if err := f.Validate(); err != nil {
		return domain.Digest{}, err
	}
	var canonical bytes.Buffer
	writeFingerprintString(&canonical, "world.android-reset-fingerprint.v1")
	writeFingerprintString(&canonical, f.BackendVersion)
	writeFingerprintString(&canonical, f.RuntimeVersion)
	writeFingerprintString(&canonical, f.SystemImageDigest.String())
	writeFingerprintString(&canonical, f.DeviceConfigDigest.String())
	features := append([]string(nil), f.Features...)
	sort.Strings(features)
	for _, feature := range features {
		writeFingerprintString(&canonical, feature)
	}
	return domain.NewDigest(canonical.Bytes()), nil
}

func (f ResetFingerprint) Compatible(snapshot ResetFingerprint) bool {
	left, leftErr := f.Digest()
	right, rightErr := snapshot.Digest()
	return leftErr == nil && rightErr == nil && left == right
}

func writeFingerprintString(buffer *bytes.Buffer, value string) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	_, _ = buffer.WriteString(value)
}

type Allocation struct {
	InstanceNumber int
	InstanceName   string
	Serial         string
	ADBAddress     string
}

func (a Allocation) Validate() error {
	if a.InstanceNumber <= 0 || a.InstanceName == "" || a.Serial == "" || a.ADBAddress == "" {
		return fmt.Errorf("positive instance number, name, serial, and ADB address are required")
	}
	return nil
}

type VirtualDevicePlan struct {
	Name                 string
	LeaseID              domain.LeaseID
	TargetID             domain.TargetID
	Generation           domain.TargetGeneration
	StateDirectory       string
	SystemImageDirectory string
	Allocation           Allocation
	Fingerprint          ResetFingerprint
	Resources            admission.Resources
	Labels               map[string]string
}

func (p VirtualDevicePlan) Validate(targetRoot, imageRoot string) error {
	if p.Name == "" || p.LeaseID.IsZero() || p.TargetID.IsZero() || !p.Generation.IsValid() {
		return fmt.Errorf("virtual-device identity and generation are required")
	}
	if err := requireBeneath(targetRoot, p.StateDirectory); err != nil {
		return fmt.Errorf("state directory: %w", err)
	}
	if err := requireBeneath(imageRoot, p.SystemImageDirectory); err != nil {
		return fmt.Errorf("system image directory: %w", err)
	}
	if err := p.Allocation.Validate(); err != nil {
		return err
	}
	if err := p.Fingerprint.Validate(); err != nil {
		return err
	}
	if err := p.Resources.Validate(); err != nil {
		return err
	}
	return nil
}

type BuildConfig struct {
	TargetRoot         string
	SystemImageRoot    string
	BackendVersion     string
	RuntimeVersion     string
	DeviceConfigDigest domain.Digest
	Features           []string
}

func BuildVirtualDevicePlan(input ports.TargetPlan, config BuildConfig, allocation Allocation) (VirtualDevicePlan, error) {
	if err := input.Validate(); err != nil {
		return VirtualDevicePlan{}, err
	}
	if input.Template.Kind != domain.TargetAndroidVirtualDevice {
		return VirtualDevicePlan{}, fmt.Errorf("Cuttlefish driver requires an android_virtual_device template")
	}
	if config.TargetRoot == "" || config.SystemImageRoot == "" {
		return VirtualDevicePlan{}, fmt.Errorf("target and system-image roots are required")
	}
	generation := input.Generation.Spec()
	imageName := strings.ReplaceAll(input.Template.ImageDigest.String(), ":", "-")
	plan := VirtualDevicePlan{
		Name:                 "world-android-" + generation.TargetID.UUID() + "-g" + strconv.FormatUint(uint64(generation.Generation), 10),
		LeaseID:              input.LeaseID,
		TargetID:             generation.TargetID,
		Generation:           generation.Generation,
		StateDirectory:       filepath.Join(config.TargetRoot, generation.TargetID.String(), "generations", strconv.FormatUint(uint64(generation.Generation), 10)),
		SystemImageDirectory: filepath.Join(config.SystemImageRoot, imageName),
		Allocation:           allocation,
		Fingerprint: ResetFingerprint{
			BackendVersion: config.BackendVersion, RuntimeVersion: config.RuntimeVersion, SystemImageDigest: input.Template.ImageDigest,
			DeviceConfigDigest: config.DeviceConfigDigest, Features: append([]string(nil), config.Features...),
		},
		Resources: input.Resources.Clone(),
		Labels: map[string]string{
			"world.role": "android-virtual-target", "world.lease": input.LeaseID.String(), "world.target": generation.TargetID.String(),
			"world.target-generation": strconv.FormatUint(uint64(generation.Generation), 10), "world.policy-digest": input.PolicyDigest.String(), "world.capability-digest": input.CapabilityFingerprintDigest.String(),
		},
	}
	if err := plan.Validate(config.TargetRoot, config.SystemImageRoot); err != nil {
		return VirtualDevicePlan{}, err
	}
	return plan, nil
}

func requireBeneath(root, candidate string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return fmt.Errorf("path is outside configured root")
	}
	return nil
}
