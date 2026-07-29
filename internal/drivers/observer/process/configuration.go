package process

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const (
	androidLogcatSignalFamily           = "android.logcat"
	maximumConfigurationArguments       = 256
	maximumConfigurationArgumentBytes   = 32 << 10
	maximumConfigurationEnvironment     = 128
	maximumConfigurationReadinessPeriod = time.Minute
)

var (
	configurationEnvironmentNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// RuntimeBinding selects one closed, typed transformation from immutable
// adapter arguments and target-driver authority to an executable invocation.
// It is deliberately not a string-template mechanism.
type RuntimeBinding string

const (
	RuntimeBindingNone            RuntimeBinding = ""
	RuntimeBindingAndroidExactADB RuntimeBinding = "android-exact-adb"
)

func (b RuntimeBinding) String() string {
	if b == RuntimeBindingNone {
		return "none"
	}
	return string(b)
}

// ParseRuntimeBinding converts the canonical external representation into the
// internal binding value. RuntimeBindingNone remains the useful zero value,
// while external interfaces consistently use the non-blank name "none".
func ParseRuntimeBinding(value string) (RuntimeBinding, error) {
	switch value {
	case RuntimeBindingNone.String():
		return RuntimeBindingNone, nil
	case RuntimeBindingAndroidExactADB.String():
		return RuntimeBindingAndroidExactADB, nil
	default:
		return RuntimeBindingNone, fmt.Errorf("runtime binding must be %q or %q", RuntimeBindingNone.String(), RuntimeBindingAndroidExactADB.String())
	}
}

// UnmarshalText lets JSON deployment profiles consume the same canonical
// runtime-binding values emitted by capability reports.
func (b *RuntimeBinding) UnmarshalText(encoded []byte) error {
	parsed, err := ParseRuntimeBinding(string(encoded))
	if err != nil {
		return err
	}
	*b = parsed
	return nil
}

// AdapterConfiguration is the complete executable identity of one trusted
// process observer. Collector references, byte budgets, resources, and whether
// a particular run requires the collector are plan properties and therefore
// are intentionally not part of this adapter identity.
type AdapterConfiguration struct {
	Adapter           string
	Version           string
	SignalFamily      string
	Placement         domain.CollectorPlacement
	CoverageLevel     domain.CoverageLevel
	RuntimeBinding    RuntimeBinding
	Program           string
	Args              []string
	Environment       map[string]string
	VersionArgs       []string
	ReadinessProgram  string
	ReadinessArgs     []string
	ReadinessInterval time.Duration
}

// Validate rejects configurations that cannot be represented or executed
// exactly by the process observer. Absolute-path policy remains the caller's
// responsibility because the reusable driver also supports trusted PATH-based
// deployments.
func (c AdapterConfiguration) Validate() error {
	for _, field := range []struct{ name, value string }{
		{name: "adapter", value: c.Adapter}, {name: "version", value: c.Version},
		{name: "signal family", value: c.SignalFamily}, {name: "program", value: c.Program},
		{name: "readiness program", value: c.ReadinessProgram},
	} {
		if strings.TrimSpace(field.value) == "" || field.value != strings.TrimSpace(field.value) {
			return fmt.Errorf("%s must be non-blank and trimmed", field.name)
		}
	}
	if err := ports.ValidateCollectorName(c.Adapter); err != nil {
		return fmt.Errorf("adapter: %w", err)
	}
	requirement := ports.ObservationRequirement{
		SignalFamily: c.SignalFamily, Placement: c.Placement,
		MinimumLevel: c.CoverageLevel, Required: true,
	}
	if err := requirement.Validate(); err != nil {
		return err
	}
	if c.ReadinessInterval <= 0 || c.ReadinessInterval > maximumConfigurationReadinessPeriod {
		return fmt.Errorf("readiness interval must be positive and no greater than %s", maximumConfigurationReadinessPeriod)
	}
	for _, field := range []struct {
		name   string
		values []string
	}{
		{name: "args", values: c.Args}, {name: "version args", values: c.VersionArgs},
		{name: "readiness args", values: c.ReadinessArgs},
	} {
		if err := validateConfigurationArguments(field.name, field.values); err != nil {
			return err
		}
	}
	if err := validateConfiguredEnvironment(c.Environment); err != nil {
		return err
	}
	return validateAndroidLogcatConfiguration(c)
}

// validateAndroidLogcatConfiguration makes the typed binding strategy part of
// the trusted adapter contract. Static arguments start at the device-local adb
// action; the exact server and serial are supplied only by target authority.
func validateAndroidLogcatConfiguration(configuration AdapterConfiguration) error {
	if configuration.SignalFamily != androidLogcatSignalFamily {
		if configuration.RuntimeBinding != RuntimeBindingNone {
			return fmt.Errorf("runtime binding is supported only for the Android logcat signal")
		}
		return nil
	}
	if configuration.Adapter != "logcat" {
		return fmt.Errorf("android logcat signal requires the logcat adapter")
	}
	if configuration.RuntimeBinding != RuntimeBindingAndroidExactADB {
		return fmt.Errorf("android logcat requires the android-exact-adb runtime binding")
	}
	if configuration.Program != configuration.ReadinessProgram {
		return fmt.Errorf("android logcat collector and readiness must use the same exact ADB program")
	}
	if !slices.Equal(configuration.VersionArgs, []string{"version"}) {
		return fmt.Errorf("android logcat ADB version probe must be exactly version")
	}
	if len(configuration.Args) == 0 || configuration.Args[0] != "logcat" {
		return fmt.Errorf("android logcat collector arguments must begin with the exact device-local logcat action")
	}
	if !slices.Equal(configuration.ReadinessArgs, []string{"get-state"}) {
		return fmt.Errorf("android logcat readiness arguments must be exactly the device-local get-state action")
	}
	for name := range configuration.Environment {
		if adbSelectionEnvironmentName(name) {
			return fmt.Errorf("android logcat environment entry %q could select an ambient ADB server or device", name)
		}
	}
	return nil
}

func adbSelectionEnvironmentName(name string) bool {
	for _, reserved := range []string{"ANDROID_SERIAL", "ADB_SERVER_SOCKET", "ANDROID_ADB_SERVER_ADDRESS", "ANDROID_ADB_SERVER_PORT"} {
		if strings.EqualFold(name, reserved) {
			return true
		}
	}
	return false
}

func validateConfiguredEnvironment(environment map[string]string) error {
	if len(environment) > maximumConfigurationEnvironment {
		return fmt.Errorf("environment permits at most %d entries", maximumConfigurationEnvironment)
	}
	names := make([]string, 0, len(environment))
	for name := range environment {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := environment[name]
		if !configurationEnvironmentNamePattern.MatchString(name) || hasCaseInsensitivePrefix(name, "WORLD_") ||
			isPlatformRuntimeEnvironmentName(name) || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("environment entry %q is invalid or reserved", name)
		}
	}
	return nil
}

func hasCaseInsensitivePrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && strings.EqualFold(value[:len(prefix)], prefix)
}

func validateConfigurationArguments(name string, values []string) error {
	if len(values) > maximumConfigurationArguments {
		return fmt.Errorf("%s permits at most %d values", name, maximumConfigurationArguments)
	}
	for index, value := range values {
		if strings.ContainsRune(value, '\x00') || len(value) > maximumConfigurationArgumentBytes {
			return fmt.Errorf("%s[%d] contains NUL or exceeds %d KiB", name, index, maximumConfigurationArgumentBytes>>10)
		}
	}
	return nil
}

// ConfigurationDigest returns the stable identity used in deployment plans
// and process-observer capability fingerprints. Keep the explicit encoding
// schema: changing a field or its order is a compatibility-significant change.
func ConfigurationDigest(configuration AdapterConfiguration) (domain.Digest, error) {
	if err := configuration.Validate(); err != nil {
		return domain.Digest{}, err
	}
	encoded, err := json.Marshal(struct {
		Adapter, Version, SignalFamily, Placement, CoverageLevel, RuntimeBinding, Program, ReadinessProgram string
		Args, VersionArgs, ReadinessArgs                                                                    []string
		Environment                                                                                         map[string]string
		ReadinessInterval                                                                                   int64
	}{
		Adapter: configuration.Adapter, Version: configuration.Version, SignalFamily: configuration.SignalFamily,
		Placement: string(configuration.Placement), CoverageLevel: string(configuration.CoverageLevel), RuntimeBinding: string(configuration.RuntimeBinding), Program: configuration.Program,
		Args: configuration.Args, Environment: configuration.Environment, VersionArgs: configuration.VersionArgs,
		ReadinessProgram: configuration.ReadinessProgram, ReadinessArgs: configuration.ReadinessArgs,
		ReadinessInterval: int64(configuration.ReadinessInterval),
	})
	if err != nil {
		return domain.Digest{}, err
	}
	return domain.NewDigest(encoded), nil
}

// BuildAdapter creates the executable adapter from the exact configuration
// whose digest is placed in plans and capability constraints.
func BuildAdapter(configuration AdapterConfiguration) (Adapter, error) {
	digest, err := ConfigurationDigest(configuration)
	if err != nil {
		return Adapter{}, err
	}
	adapter := Adapter{
		Name: configuration.Adapter, Version: configuration.Version, ConfigurationDigest: digest,
		SignalFamily: configuration.SignalFamily, Placement: configuration.Placement, CoverageLevel: configuration.CoverageLevel,
		RuntimeBinding: configuration.RuntimeBinding, Program: configuration.Program, Args: append([]string(nil), configuration.Args...),
		Environment: cloneMap(configuration.Environment), VersionArgs: append([]string(nil), configuration.VersionArgs...),
		Readiness: CommandReadiness{
			Program: configuration.ReadinessProgram, Args: append([]string(nil), configuration.ReadinessArgs...),
			Interval: configuration.ReadinessInterval, RuntimeBinding: configuration.RuntimeBinding,
		},
	}
	if err := adapter.Validate(); err != nil {
		return Adapter{}, err
	}
	return adapter, nil
}
