package daemon

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
	"github.com/philcantcode/go-world-management-layer/internal/application"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	observerprocess "github.com/philcantcode/go-world-management-layer/internal/drivers/observer/process"
	"github.com/philcantcode/go-world-management-layer/internal/localmaterial"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/policyregistry"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/internal/store"
	"github.com/philcantcode/go-world-management-layer/policy"
)

func TestLoadDeploymentBuildsExactImmutablePlans(t *testing.T) {
	fixture := newProfileFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deployment, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	effective := bindTestPolicies(t, ctx, &deployment)
	rawPolicyDigest := domain.NewDigest(deployment.policySources[fixture.profile.Policies[0].Reference])
	if rawPolicyDigest == effective.Digest() {
		t.Fatal("canonical effective-policy digest unexpectedly equals raw YAML bytes")
	}
	if deployment.agentRepository != "world-e2e:local" || deployment.targetRepository != "world-e2e:local" {
		t.Fatalf("repositories = %q, %q", deployment.agentRepository, deployment.targetRepository)
	}
	if deployment.profileDigest.IsZero() || len(deployment.targetTemplates) != 1 || deployment.runCount != 1 {
		t.Fatalf("incomplete deployment: %#v", deployment)
	}
	request := application.AcquireRequest{
		InputSelection:   fixture.profile.Acquisitions[0].Selection,
		PolicyDigest:     effective.Digest().String(),
		CapabilityDigest: effective.CapabilityFingerprintDigest().String(),
	}
	resolved, err := deployment.resolver.ResolveAcquisition(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.InputView.ID().IsZero() || len(resolved.Content) != 1 {
		t.Fatalf("resolved acquisition = %#v", resolved)
	}
	request.PolicyDigest = rawPolicyDigest.String()
	if _, err := deployment.resolver.ResolveAcquisition(ctx, request); err == nil {
		t.Fatal("raw YAML digest was accepted as an authorization identity")
	}
	run, err := deployment.resolver.ResolveTargetMaterial(ctx, application.StartTargetRunRequest{
		SpecimenOccurrenceRefs: []string{"specimen/native"},
		FixtureRefs:            []string{"fixture/config"},
	}, application.TargetRecord{Template: "linux-visible"})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Material) != 2 || run.MaximumDuration != 30*time.Second {
		t.Fatalf("resolved run = %#v", run)
	}
	if _, err := deployment.resolver.ResolveTargetMaterial(ctx, application.StartTargetRunRequest{
		SpecimenOccurrenceRefs: []string{"specimen/native"},
		FixtureRefs:            []string{"fixture/config"},
	}, application.TargetRecord{Template: "another-target"}); err == nil {
		t.Fatal("run material was accepted for an unauthorized target template")
	}
	source := resolved.Content["input/payload.txt"]
	opened, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	original, err := io.ReadAll(opened)
	if closeErr := opened.Close(); err == nil {
		err = closeErr
	}
	if err != nil || string(original) != "payload" {
		t.Fatalf("opened content = %q, %v", original, err)
	}
	if err := os.WriteFile(filepath.Join(fixture.sourceRoot, "payload.txt"), []byte("mutated"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := source.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	staged, readErr := io.ReadAll(reopened)
	if closeErr := reopened.Close(); readErr == nil {
		readErr = closeErr
	}
	if readErr != nil || string(staged) != "payload" {
		t.Fatalf("staged immutable content changed with its source: %q, %v", staged, readErr)
	}
	rawDigest := strings.TrimPrefix(source.Digest().String(), "sha256:")
	objectPath := filepath.Join(fixture.publicationRoot, "objects", "sha256", rawDigest[:2], rawDigest)
	if err := os.WriteFile(objectPath, make([]byte, source.Size()), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Open(ctx); err == nil {
		t.Fatal("staged material corruption was not detected")
	}
}

func TestBuildTargetPlanPropagatesCompleteAndroidRuntime(t *testing.T) {
	digest := domain.NewDigest([]byte("android-system-image"))
	configured := targetProfile{
		Reference: "android-instrumented",
		Template: targetTemplateProfile{
			Name: "android-instrumented", Kind: domain.TargetAndroidVirtualDevice,
			Driver: "android-emulator", SystemImageDigest: digest.String(), IsolationProfile: "instrumented-android",
			SystemImagePackage: "system-images;android-35;google_apis;x86_64",
			BaselineState:      "clean-boot", RequireHardwareAcceleration: true, Headless: true,
			Rooted: true, Debuggable: true, GuestMemoryBytes: 2 << 30, BootTimeout: "2m",
		},
		Resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 2 << 30, StorageBytes: 1 << 30},
	}
	reference, plan, image, err := buildTargetPlan(configured)
	if err != nil {
		t.Fatal(err)
	}
	if reference != configured.Reference || image.reference != "" || image.digest != digest || image.packageID != configured.Template.SystemImagePackage {
		t.Fatalf("Android image identity = %q %#v", reference, image)
	}
	template := plan.Template
	if template.ImageDigest != digest || template.BaselineState != "clean-boot" || template.GuestMemoryBytes != 2<<30 || template.BootTimeout != 2*time.Minute ||
		!template.RequireHardwareAcceleration || !template.Headless || !template.Rooted || !template.Debuggable {
		t.Fatalf("Android template fields were not propagated: %#v", template)
	}

	configured.Template.Image = "example.invalid/android@" + digest.String()
	if _, _, _, err := buildTargetPlan(configured); err == nil || !strings.Contains(err.Error(), "Linux-only") {
		t.Fatalf("Android template accepted a Docker image field: %v", err)
	}
}

func TestLoadDeploymentRequiresSingleAndroidSystemImageIdentity(t *testing.T) {
	firstDigest := domain.NewDigest([]byte("android-system-image-1"))
	secondDigest := domain.NewDigest([]byte("android-system-image-2"))
	const firstPackage = "system-images;android-35;google_apis;x86_64"
	const secondPackage = "system-images;android-35;google_apis_playstore;x86_64"
	tests := []struct {
		name          string
		secondDigest  domain.Digest
		secondPackage string
		wantError     bool
	}{
		{name: "shared identity", secondDigest: firstDigest, secondPackage: firstPackage},
		{name: "distinct digest", secondDigest: secondDigest, secondPackage: firstPackage, wantError: true},
		{name: "distinct package", secondDigest: firstDigest, secondPackage: secondPackage, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProfileFixture(t, false)
			policyReference := fixture.profile.Policies[0].Reference
			fixture.profile.Targets = []targetProfile{
				managedAndroidTargetProfile("android-primary", policyReference, firstDigest, firstPackage),
				managedAndroidTargetProfile("android-secondary", policyReference, test.secondDigest, test.secondPackage),
			}
			writeProfile(t, fixture.profilePath, fixture.profile)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			deployment, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20)
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "all Android targets must use one system-image digest/package identity") {
					t.Fatalf("loadDeployment() error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(deployment.androidTargets) != 2 || len(deployment.androidImages) != 1 || deployment.androidImages[firstDigest.String()] != firstPackage {
				t.Fatalf("shared Android identity was not retained exactly: targets=%d images=%#v", len(deployment.androidTargets), deployment.androidImages)
			}
		})
	}
}

func managedAndroidTargetProfile(reference, policyReference string, digest domain.Digest, packageID string) targetProfile {
	return targetProfile{
		Reference: reference, Policy: policyReference,
		Template: targetTemplateProfile{
			Name: reference, Kind: domain.TargetAndroidVirtualDevice, Driver: "android-emulator",
			SystemImageDigest: digest.String(), SystemImagePackage: packageID,
			IsolationProfile: "instrumented-android", BaselineState: "clean-boot",
			RequireHardwareAcceleration: true, Headless: true, Rooted: true, Debuggable: true,
			GuestMemoryBytes: 2 << 30, BootTimeout: "2m",
		},
		Resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 1 << 30},
	}
}

func TestBuildTargetPlanRejectsManagedAndroidResourcesTheBackendCannotEnforce(t *testing.T) {
	digest := domain.NewDigest([]byte("android-system-image"))
	valid := managedAndroidTargetProfile("android-instrumented", "policy", digest, "system-images;android-35;google_apis;x86_64")
	tests := []struct {
		name      string
		resources admission.Resources
		want      string
	}{
		{name: "fractional CPU", resources: admission.Resources{CPUMilli: 1500, MemoryBytes: 2 << 30, StorageBytes: 1 << 30}, want: "whole-vCPU"},
		{name: "memory must be positive", resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 0, StorageBytes: 1 << 30}, want: "memory limit must be positive"},
		{name: "storage below minimum", resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 63 << 20}, want: "from 64 to 2047 MiB"},
		{name: "unaligned storage", resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: (64 << 20) + 1}, want: "MiB-aligned"},
		{name: "storage above maximum", resources: admission.Resources{CPUMilli: 2000, MemoryBytes: 6 << 30, StorageBytes: 2 << 30}, want: "from 64 to 2047 MiB"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configured := valid
			configured.Resources = test.resources
			if _, _, _, err := buildTargetPlan(configured); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("buildTargetPlan() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDeploymentProfileRejectsAmbiguousOrUnsafeAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*deploymentProfile)
		want   string
	}{
		{
			name: "unknown acquisition policy",
			mutate: func(profile *deploymentProfile) {
				profile.Acquisitions[0].Policy = "unknown-policy@1"
			},
			want: "names unknown policy",
		},
		{
			name: "cross scope entry",
			mutate: func(profile *deploymentProfile) {
				profile.Material.Entries[0].SecurityScope = "another-scope"
			},
			want: "does not match",
		},
		{
			name: "unpinned image",
			mutate: func(profile *deploymentProfile) {
				profile.Acquisitions[0].AgentImage = "world-e2e:local"
			},
			want: "agent_image",
		},
		{
			name: "declared content mismatch",
			mutate: func(profile *deploymentProfile) {
				profile.Material.Entries[0].Digest = domain.NewDigest([]byte("other bytes")).String()
			},
			want: "declared digest",
		},
		{
			name: "declared size mismatch",
			mutate: func(profile *deploymentProfile) {
				profile.Material.Entries[0].Size++
			},
			want: "declared digest, size, mode, or logical path",
		},
		{
			name: "implicit material mode",
			mutate: func(profile *deploymentProfile) {
				profile.Material.Entries[0].Mode = 0
			},
			want: "permission bits",
		},
		{
			name: "unbounded resources",
			mutate: func(profile *deploymentProfile) {
				profile.Acquisitions[0].Resources.MemoryBytes = 0
			},
			want: "must all be positive",
		},
		{
			name: "excessive run duration",
			mutate: func(profile *deploymentProfile) {
				profile.Runs[0].MaximumDuration = "25h"
			},
			want: "maximum_duration",
		},
		{
			name: "hidden run material",
			mutate: func(profile *deploymentProfile) {
				profile.Runs[0].FixtureRefs = nil
			},
			want: "not selected",
		},
		{
			name: "unknown run target",
			mutate: func(profile *deploymentProfile) {
				profile.Runs[0].TargetReferences = []string{"unknown-target"}
			},
			want: "unknown target reference",
		},
		{
			name: "mixed selector forms",
			mutate: func(profile *deploymentProfile) {
				profile.Acquisitions[0].Selection.OccurrenceRefs = []string{"input/payload"}
			},
			want: "exactly one",
		},
		{
			name: "unregistered requested sidecar",
			mutate: func(profile *deploymentProfile) {
				profile.Acquisitions[0].Selection.AllowedSidecars = []string{"symbols"}
			},
			want: "is not registered",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProfileFixture(t, true)
			test.mutate(&fixture.profile)
			writeProfile(t, fixture.profilePath, fixture.profile)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("loadDeployment() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestBuildObserverPlansRejectsNonCanonicalCollectorReferencesEarly(t *testing.T) {
	for name, reference := range map[string]string{
		"separator": "process/trace",
		"percent":   "process%2Ftrace",
		"overlong":  strings.Repeat("x", ports.MaximumCollectorNameBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := buildObserverPlans([]observerProfile{{
				Reference: reference,
				Adapter:   "process",
				Version:   "1",
			}}, 1<<20)
			if !domain.IsCode(err, domain.CodeInvalidArgument) {
				t.Fatalf("buildObserverPlans() error = %v, want invalid argument", err)
			}
		})
	}
}

func TestBuildObserverPlansUsesSharedExactProcessConfiguration(t *testing.T) {
	program := filepath.Join(t.TempDir(), "adb.exe")
	configuration := observerprocess.AdapterConfiguration{
		Adapter: "logcat", Version: "1", SignalFamily: "android.logcat",
		Placement: domain.CollectorPlacementGuest, CoverageLevel: domain.CoverageLevelPartial,
		RuntimeBinding: observerprocess.RuntimeBindingAndroidExactADB,
		Program:        program,
		Args:           []string{"logcat"},
		VersionArgs:    []string{"version"}, ReadinessProgram: program,
		ReadinessArgs:     []string{"get-state"},
		ReadinessInterval: 250 * time.Millisecond,
	}
	digest, err := observerprocess.ConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	profile := observerProfile{
		Reference: "android-logcat", Adapter: configuration.Adapter, Version: configuration.Version,
		ConfigurationDigest: digest.String(), SignalFamily: configuration.SignalFamily,
		Placement: configuration.Placement, CoverageLevel: configuration.CoverageLevel, RuntimeBinding: configuration.RuntimeBinding, Required: true,
		Program: configuration.Program, Args: configuration.Args, VersionArgs: configuration.VersionArgs,
		Readiness: observerReadinessProfile{
			Program: configuration.ReadinessProgram, Args: configuration.ReadinessArgs,
			Interval: configuration.ReadinessInterval.String(),
		},
		MaximumBytes: 64 << 10,
	}
	plans, specs, err := buildObserverPlans([]observerProfile{profile}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || len(specs) != 1 || plans[0].Adapter.ConfigurationDigest != digest ||
		plans[0].Spec.ConfigurationDigest != digest || plans[0].Adapter.Name != configuration.Adapter {
		t.Fatalf("observer plans = %#v specs=%#v", plans, specs)
	}
	readiness, ok := plans[0].Adapter.Readiness.(observerprocess.CommandReadiness)
	if !ok || readiness.Program != configuration.ReadinessProgram || readiness.Interval != configuration.ReadinessInterval {
		t.Fatalf("observer readiness = %#v", plans[0].Adapter.Readiness)
	}
	profile.Environment = map[string]string{}
	if _, _, err := buildObserverPlans([]observerProfile{profile}, 1<<20); err == nil || !strings.Contains(err.Error(), "configuration_digest") {
		t.Fatalf("nil-to-empty configuration drift was accepted: %v", err)
	}
}

func TestDeploymentProfileAcceptsGenericBindingEmittedByWorldCapabilities(t *testing.T) {
	fixture := newProfileFixture(t, true)
	program := filepath.Join(t.TempDir(), "observer.exe")
	configuration := observerprocess.AdapterConfiguration{
		Adapter: "process", Version: "1", SignalFamily: "process.stdout",
		Placement: domain.CollectorPlacementHost, CoverageLevel: domain.CoverageLevelPartial,
		RuntimeBinding: observerprocess.RuntimeBindingNone,
		Program:        program, VersionArgs: []string{"--version"}, ReadinessProgram: program,
		ReadinessArgs: []string{"ready"}, ReadinessInterval: 250 * time.Millisecond,
	}
	digest, err := observerprocess.ConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	fixture.profile.Observers = []observerProfile{{
		Reference: "process-stdout", Adapter: configuration.Adapter, Version: configuration.Version,
		ConfigurationDigest: digest.String(), SignalFamily: configuration.SignalFamily,
		Placement: configuration.Placement, CoverageLevel: configuration.CoverageLevel, Required: true,
		Program: configuration.Program, VersionArgs: configuration.VersionArgs,
		Readiness: observerReadinessProfile{
			Program: configuration.ReadinessProgram, Args: configuration.ReadinessArgs,
			Interval: configuration.ReadinessInterval.String(),
		},
		MaximumBytes: 64 << 10,
	}}
	fixture.profile.Runs[0].CollectorReferences = []string{"process-stdout"}
	fixture.profile.Runs[0].RequiredCoverage = append(fixture.profile.Runs[0].RequiredCoverage, configuration.SignalFamily)

	encoded, err := json.Marshal(fixture.profile)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	var observers []map[string]any
	if err := json.Unmarshal(document["observers"], &observers); err != nil {
		t.Fatal(err)
	}
	observers[0]["runtime_binding"] = configuration.RuntimeBinding.String()
	document["observers"], err = json.Marshal(observers)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.profilePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deployment, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20)
	if err != nil {
		t.Fatalf("load profile containing world-capabilities runtime binding %q: %v", configuration.RuntimeBinding.String(), err)
	}
	if len(deployment.observerAdapters) != 1 || deployment.observerAdapters[0].Adapter.RuntimeBinding != observerprocess.RuntimeBindingNone {
		t.Fatalf("generic runtime binding was not normalized exactly: %#v", deployment.observerAdapters)
	}
}

func TestReadDeploymentProfileIsStrictAndBounded(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "profile.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"security_scope":"scope","unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDeploymentProfile(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":2,"security_scope":"scope","policies":[],"material":{"source_root":"x","max_object_bytes":1,"entries":[]},"acquisitions":[{"policy_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDeploymentProfile(path); err == nil || !strings.Contains(err.Error(), "policy_digest") {
		t.Fatalf("removed raw policy digest field error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{} {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDeploymentProfile(path); err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("trailing value error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"version":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDeploymentProfile(path); err == nil || !strings.Contains(err.Error(), "duplicate object key") {
		t.Fatalf("duplicate key error = %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat(" ", int(maxDeploymentProfileBytes+1))), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readDeploymentProfile(path); err == nil || !strings.Contains(err.Error(), "no larger") {
		t.Fatalf("oversized profile error = %v", err)
	}
}

func TestDeploymentProfileVersionThreeContract(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   int
		wantError string
	}{
		{name: "version 2 rejected", version: 2, wantError: "deployment profile version must be 3"},
		{name: "version 3 accepted", version: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProfileFixture(t, false)
			fixture.profile.Version = test.version
			writeProfile(t, fixture.profilePath, fixture.profile)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("loadDeployment(version=%d) error = %v, want %q", test.version, err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadDeployment(version=%d): %v", test.version, err)
			}
		})
	}
}

func TestPolicyPublicationRequiresDeclaredMetadataReference(t *testing.T) {
	fixture := newProfileFixture(t, true)
	fixture.profile.Policies[0].Reference = "different-policy@1"
	fixture.profile.Acquisitions[0].Policy = "different-policy@1"
	fixture.profile.Targets[0].Policy = "different-policy@1"
	writeProfile(t, fixture.profilePath, fixture.profile)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deployment, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := supportedPolicyFingerprint(t, deployment.policySources[fixture.profile.Policies[0].Reference])
	if err := deployment.publishAndBindPolicies(ctx, testPolicyAuthority(t), fingerprint); err == nil || !strings.Contains(err.Error(), "declares immutable reference") {
		t.Fatalf("metadata reference publication error = %v", err)
	}
}

func TestCompiledPolicyPreflightDoesNotAuthorizeBeforeCommit(t *testing.T) {
	fixture := newProfileFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deployment, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	source := deployment.policySources[fixture.profile.Policies[0].Reference]
	fingerprint := supportedPolicyFingerprint(t, source)
	compiled, err := deployment.compileAndBindPolicies(fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	effective := compiled[fixture.profile.Policies[0].Reference]
	authority := testPolicyAuthority(t)
	if _, err := authority.Resolve(ctx, effective.Digest().String(), effective.CapabilityFingerprintDigest().String()); err == nil {
		t.Fatal("preflight compilation durably authorized the policy pair")
	}
	if err := publishCompiledPolicies(ctx, authority, fingerprint, compiled); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Resolve(ctx, effective.Digest().String(), effective.CapabilityFingerprintDigest().String()); err != nil {
		t.Fatalf("committed policy pair did not resolve: %v", err)
	}
}

func TestDeploymentProfileBoundsAggregateMaterialBytes(t *testing.T) {
	fixture := newProfileFixture(t, false)
	fixture.profile.Material.MaxObjectBytes = 20
	writeProfile(t, fixture.profilePath, fixture.profile)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := loadDeployment(ctx, fixture.profilePath, fixture.publicationRoot, 20); err == nil ||
		!strings.Contains(err.Error(), "aggregate material bound") {
		t.Fatalf("aggregate bound error = %v", err)
	}
}

func TestParsePinnedImageBoundaries(t *testing.T) {
	digest := domain.NewDigest([]byte("image")).String()
	for _, valid := range []string{
		"world-e2e:local@" + digest,
		"localhost:5000/research/world-e2e:v1@" + digest,
		"ghcr.io/example/world-e2e@" + digest,
	} {
		if parsed, err := parsePinnedImage(valid); err != nil || parsed.reference != valid {
			t.Fatalf("parsePinnedImage(%q) = %#v, %v", valid, parsed, err)
		}
	}
	for _, invalid := range []string{
		"world-e2e:local",
		"world-e2e:local@sha256:deadbeef",
		"../world-e2e@" + digest,
		"world e2e@" + digest,
		"UPPER/repo@" + digest,
	} {
		if _, err := parsePinnedImage(invalid); err == nil {
			t.Fatalf("parsePinnedImage(%q) unexpectedly succeeded", invalid)
		}
	}
}

type profileFixture struct {
	profile         deploymentProfile
	profilePath     string
	sourceRoot      string
	publicationRoot string
}

func newProfileFixture(t *testing.T, withTarget bool) profileFixture {
	t.Helper()
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	if err := os.Mkdir(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"payload.txt":  "payload",
		"specimen":     "native specimen",
		"fixture.json": `{"fixture":true}`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(sourceRoot, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	scope := "local-e2e"
	policyReference := "e2e-directory-copy@1"
	image := "world-e2e:local@sha256:6105d6cc76af4009c44e4692f219054456e7111487afb0c71077d9f887668fef"
	policySource := filepath.Join(root, "e2e-directory-copy.yaml")
	policyBytes, err := os.ReadFile(filepath.Join("..", "..", "..", "policy", "deployment", "e2e-directory-copy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policySource, policyBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	profile := deploymentProfile{
		Version: deploymentProfileVersion, SecurityScope: scope,
		Policies: []policySourceProfile{{Reference: policyReference, SourcePath: policySource}},
		Material: materialProfile{
			SourceRoot: sourceRoot, MaxObjectBytes: 1 << 20,
			Entries: []materialEntryProfile{
				{Reference: "input/payload", SecurityScope: scope, SourcePath: "payload.txt", Digest: domain.NewDigest([]byte(files["payload.txt"])).String(), Size: int64(len(files["payload.txt"])), LogicalPath: "input/payload.txt", Mode: 0o444, Role: "input", Sensitivity: domain.SensitivityInternal},
				{Reference: "specimen/native", SecurityScope: scope, SourcePath: "specimen", Digest: domain.NewDigest([]byte(files["specimen"])).String(), Size: int64(len(files["specimen"])), LogicalPath: "specimens/native", Mode: 0o555, Role: "specimen", Sensitivity: domain.SensitivityRestricted},
				{Reference: "fixture/config", SecurityScope: scope, SourcePath: "fixture.json", Digest: domain.NewDigest([]byte(files["fixture.json"])).String(), Size: int64(len(files["fixture.json"])), LogicalPath: "fixtures/config.json", Mode: 0o444, Role: "fixture", Sensitivity: domain.SensitivityInternal},
			},
			Selections: []localmaterial.SelectionConfig{{Reference: "selection/default", SecurityScope: scope, Occurrences: []string{"input/payload"}}},
		},
		Acquisitions: []acquisitionProfile{{
			Selection:    application.InputSelectionRequest{FrozenSelectionRef: "selection/default", SecurityScope: scope},
			Construction: domain.InputViewAllowCopy, UpperByteLimit: 1 << 20, UpperInodeLimit: 128,
			Policy: policyReference, AgentImage: image,
			Resources: boundedTestResources(),
		}},
	}
	if withTarget {
		profile.Targets = []targetProfile{{
			Reference: "linux-visible", Policy: policyReference,
			Template: targetTemplateProfile{
				Name: "linux-visible", Kind: domain.TargetLinuxContainer, Driver: "docker",
				Runtime: "runc", Image: image, IsolationProfile: "observable-container",
			},
			Resources: boundedTargetTestResources(),
		}}
		profile.Runs = []runProfile{{
			TargetReferences:       []string{"linux-visible"},
			SpecimenOccurrenceRefs: []string{"specimen/native"}, FixtureRefs: []string{"fixture/config"},
			RequiredCoverage: []string{ports.TargetLifecycleSignal}, MaximumDuration: "30s",
			Material: []runMaterialProfile{
				{Reference: "specimen/native", LogicalPath: "bin/specimen", Mode: 0o555},
				{Reference: "fixture/config", LogicalPath: "config/fixture.json", Mode: 0o444},
			},
		}}
	}
	profilePath := filepath.Join(root, "deployment.json")
	writeProfile(t, profilePath, profile)
	return profileFixture{
		profile: profile, profilePath: profilePath, sourceRoot: sourceRoot,
		publicationRoot: filepath.Join(root, "publication"),
	}
}

func writeProfile(t *testing.T, path string, profile deploymentProfile) {
	t.Helper()
	content, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func boundedTestResources() admission.Resources {
	return admission.Resources{
		CPUMilli: 500, MemoryBytes: 64 << 20, StorageBytes: 4 << 20,
		CaptureBytes: 1 << 20, Inodes: 256, PIDs: 64,
	}
}

func boundedTargetTestResources() admission.Resources {
	return admission.Resources{CPUMilli: 500, MemoryBytes: 64 << 20, StorageBytes: 4 << 20, PIDs: 64}
}

func bindTestPolicies(t *testing.T, ctx context.Context, deployment *builtDeployment) *policy.EffectivePolicy {
	t.Helper()
	controlStore, err := store.Open(ctx, store.Options{Path: filepath.Join(t.TempDir(), "control.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	registry, err := policyregistry.New(controlStore)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := policyauthority.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	source := deployment.policySources["e2e-directory-copy@1"]
	fingerprint := supportedPolicyFingerprint(t, source)
	publications, err := deployment.compileAndBindPolicies(fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishCompiledPolicies(ctx, authority, fingerprint, publications); err != nil {
		t.Fatal(err)
	}
	return publications["e2e-directory-copy@1"]
}

func supportedPolicyFingerprint(t *testing.T, source []byte) policy.CapabilityFingerprint {
	t.Helper()
	requirements, err := policy.Requirements(source)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := make(map[string]policy.Capability, len(requirements))
	for _, requirement := range requirements {
		capabilities[requirement.Name], err = policy.NewCapability(policy.CapabilitySupported, requirement.Constraints, map[string]string{"test": "supported"})
		if err != nil {
			t.Fatal(err)
		}
	}
	fingerprint, err := policy.NewCapabilityFingerprint(capabilities, map[string]string{"test": "deployment"})
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

var _ ports.ContentSource = authorityContentSource{}
