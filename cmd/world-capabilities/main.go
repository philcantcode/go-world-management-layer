// Command world-capabilities probes the same Docker drivers used by worldd
// and prints profile-ready capability fingerprints. It never provisions a
// container and never weakens worldd's requirement for a trusted digest.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	agentdocker "github.com/philcantcode/go-world-management-layer/internal/drivers/agent/docker"
	"github.com/philcantcode/go-world-management-layer/internal/drivers/target/linuxcontainer"
	"github.com/philcantcode/go-world-management-layer/internal/orchestration/policyauthority"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
	"github.com/philcantcode/go-world-management-layer/policy"
)

const maximumPolicyBytes = int64(4 << 20)

type capabilityReport struct {
	Digest       string                      `json:"digest"`
	Evidence     map[string]string           `json:"evidence"`
	Capabilities map[string]capabilityDetail `json:"capabilities"`
}

type capabilityDetail struct {
	Status      string            `json:"status"`
	Constraints map[string]string `json:"constraints,omitempty"`
	Evidence    map[string]string `json:"evidence,omitempty"`
}

type report struct {
	Combined            capabilityReport                         `json:"combined"`
	Agent               capabilityReport                         `json:"agent"`
	AgentPhysical       ports.AgentWorkspacePhysicalPolicyReport `json:"agent_physical_policy"`
	LinuxTarget         *capabilityReport                        `json:"linux_target,omitempty"`
	LinuxTargetPhysical *ports.TargetPhysicalPolicyReport        `json:"linux_target_physical_policy,omitempty"`
	EffectivePolicy     *effectivePolicyReport                   `json:"effective_policy,omitempty"`
}

type effectivePolicyReport struct {
	Reference                   string `json:"reference"`
	Digest                      string `json:"digest"`
	CapabilityFingerprintDigest string `json:"capability_fingerprint_digest"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("world-capabilities", flag.ContinueOnError)
	dockerBinary := flags.String("docker-binary", "docker", "Docker CLI path")
	timeout := flags.Duration("timeout", 30*time.Second, "overall probe deadline")
	allowPtrace := flags.Bool("target-allow-ptrace", false, "match worldd target ptrace setting")
	agentContainerUser := flags.String("agent-container-user", "65532:65532", "match worldd unprivileged agent container user")
	linuxTargetDriver := flags.String("linux-target-driver", "docker", "Linux target driver: docker or none")
	workspaceDriver := flags.String("workspace-driver", "directory", "workspace driver (only directory is currently probeable)")
	observerDriver := flags.String("observer-driver", "none", "observer driver (only none is currently probeable)")
	captureDriver := flags.String("capture-driver", "none", "capture driver: none or ledger")
	policyPath := flags.String("policy", "", "strict policy YAML to compile against the probed complete capability fingerprint")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if *linuxTargetDriver != "docker" && *linuxTargetDriver != "none" {
		return fmt.Errorf("linux-target-driver must be docker or none")
	}
	if *workspaceDriver != "directory" {
		return fmt.Errorf("workspace-driver=%q cannot be truthfully probed by this binary", *workspaceDriver)
	}
	if *observerDriver != "none" {
		return fmt.Errorf("observer-driver=%q cannot be truthfully composed without the deployment's exact observer configurations", *observerDriver)
	}
	if *captureDriver != "none" && *captureDriver != "ledger" {
		return fmt.Errorf("capture-driver must be none or ledger")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	agent, err := agentdocker.New(agentdocker.Config{
		Build: agentdocker.BuildConfig{
			WorkspaceRoot: os.TempDir(), ImageRepository: "probe.invalid/world-agent", ContainerUser: *agentContainerUser,
		},
		Engine: agentdocker.NewCLIEngine(*dockerBinary, nil, nil),
	})
	if err != nil {
		return fmt.Errorf("configure agent probe: %w", err)
	}
	agentFingerprint, err := agent.Probe(ctx)
	if err != nil {
		return fmt.Errorf("probe Docker agent driver: %w", err)
	}

	agentReporter, ok := any(agent).(ports.AgentWorkspacePhysicalPolicyReporter)
	if !ok {
		return fmt.Errorf("Docker agent driver does not expose physical policy facts")
	}
	agentPhysical, err := agentReporter.AgentWorkspacePhysicalPolicy(ctx)
	if err != nil {
		return fmt.Errorf("probe Docker agent physical policy: %w", err)
	}
	agentPhysical = policyauthority.WithDirectoryWorkspaceEnforcement(agentPhysical)
	if *captureDriver == "ledger" {
		agentPhysical = policyauthority.WithBoundedLedgerCaptureEnforcement(agentPhysical)
	}
	agentPhysicalFingerprint, err := policyauthority.AgentPhysicalPolicyFingerprint(agentPhysical)
	if err != nil {
		return fmt.Errorf("fingerprint Docker agent physical policy: %w", err)
	}

	components := []policyauthority.CapabilityComponent{
		{Name: "agent", Fingerprint: agentFingerprint},
		{Name: "agent-physical", Fingerprint: agentPhysicalFingerprint},
	}
	coverage := make(map[string][]string)
	var targetReport *capabilityReport
	var targetPhysicalReport *ports.TargetPhysicalPolicyReport
	if *linuxTargetDriver == "docker" {
		target, err := linuxcontainer.New(linuxcontainer.Config{
			Build:   linuxcontainer.BuildConfig{TargetRoot: os.TempDir(), ImageRepository: "probe.invalid/world-target", AllowPtrace: *allowPtrace},
			Runtime: linuxcontainer.NewDockerRuntime(*dockerBinary, nil, nil),
			Collectors: linuxcontainer.CollectorReadinessFunc(func(context.Context, domain.TargetRunID, []ports.ObservationRequirement) error {
				return fmt.Errorf("probe-only readiness gate cannot start runs")
			}),
		})
		if err != nil {
			return fmt.Errorf("configure Linux target probe: %w", err)
		}
		template := ports.TargetTemplate{
			Name: "capability-probe", Kind: domain.TargetLinuxContainer, Driver: "docker", Runtime: "runc",
			ImageDigest: domain.NewDigest([]byte("probe-only-image")), IsolationProfile: "observable-container",
		}
		targetFingerprint, err := target.Probe(ctx, template)
		if err != nil {
			return fmt.Errorf("probe Docker Linux target driver: %w", err)
		}
		components = append(components, policyauthority.CapabilityComponent{Name: "linux-target", Fingerprint: targetFingerprint})
		targetReporter, ok := any(target).(ports.TargetPhysicalPolicyReporter)
		if !ok {
			return fmt.Errorf("Docker Linux target driver does not expose physical policy facts")
		}
		physical, err := targetReporter.TargetPhysicalPolicy(ctx, template)
		if err != nil {
			return fmt.Errorf("probe Docker Linux target physical policy: %w", err)
		}
		targetPhysicalFingerprint, err := policyauthority.TargetPhysicalPolicyFingerprint(physical)
		if err != nil {
			return fmt.Errorf("fingerprint Docker Linux target physical policy: %w", err)
		}
		components = append(components, policyauthority.CapabilityComponent{Name: "linux-target-physical", Fingerprint: targetPhysicalFingerprint})
		coverage["linux-container"] = []string{ports.TargetLifecycleSignal}
		mapped := mapCapabilityReport(targetFingerprint)
		targetReport = &mapped
		targetPhysicalReport = &physical
	}
	combined, err := policyauthority.BuildCapabilityFingerprint(policyauthority.CapabilityFacts{
		HostOS: runtime.GOOS, HostArchitecture: runtime.GOARCH, WorkspaceMode: "directory-copy-non-production",
		DirectoryCopy: true, Components: components, IntrinsicCoverage: coverage,
	})
	if err != nil {
		return fmt.Errorf("compose complete capability fingerprint: %w", err)
	}
	var effectivePolicy *effectivePolicyReport
	if *policyPath != "" {
		source, err := readPolicySource(*policyPath)
		if err != nil {
			return err
		}
		compiled, err := policy.Compile(source, policy.CompileOptions{Capabilities: combined})
		if err != nil {
			return fmt.Errorf("compile policy against probed capabilities: %w", err)
		}
		document := compiled.Policy()
		effectivePolicy = &effectivePolicyReport{
			Reference: fmt.Sprintf("%s@%d", document.Metadata.Name, document.Metadata.Revision),
			Digest:    compiled.Digest().String(), CapabilityFingerprintDigest: compiled.CapabilityFingerprintDigest().String(),
		}
	}

	encoded, err := json.MarshalIndent(report{
		Combined: mapCapabilityReport(combined), Agent: mapCapabilityReport(agentFingerprint), AgentPhysical: agentPhysical,
		LinuxTarget: targetReport, LinuxTargetPhysical: targetPhysicalReport, EffectivePolicy: effectivePolicy,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode capability report: %w", err)
	}
	_, err = fmt.Fprintln(os.Stdout, string(encoded))
	return err
}

func readPolicySource(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect policy: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("policy must be a regular file, not a symlink or special file")
	}
	opened, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open policy: %w", err)
	}
	defer opened.Close()
	openedInfo, err := opened.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened policy: %w", err)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("policy changed while it was opened")
	}
	source, err := io.ReadAll(io.LimitReader(opened, maximumPolicyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read policy: %w", err)
	}
	if len(source) == 0 || int64(len(source)) > maximumPolicyBytes {
		return nil, fmt.Errorf("policy must be non-empty and no larger than %d bytes", maximumPolicyBytes)
	}
	return source, nil
}

func mapCapabilityReport(fingerprint domain.CapabilityFingerprint) capabilityReport {
	capabilities := fingerprint.Capabilities()
	names := make([]string, 0, len(capabilities))
	for name := range capabilities {
		names = append(names, name)
	}
	sort.Strings(names)
	mapped := make(map[string]capabilityDetail, len(names))
	for _, name := range names {
		capability := capabilities[name]
		mapped[name] = capabilityDetail{
			Status: string(capability.Status()), Constraints: capability.Constraints(), Evidence: capability.Evidence(),
		}
	}
	return capabilityReport{Digest: fingerprint.Digest().String(), Evidence: fingerprint.Evidence(), Capabilities: mapped}
}
