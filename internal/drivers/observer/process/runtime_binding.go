package process

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

func validateAdapterRuntimeBinding(adapter Adapter) error {
	switch adapter.RuntimeBinding {
	case RuntimeBindingNone:
		if adapter.SignalFamily == androidLogcatSignalFamily {
			return fmt.Errorf("android logcat adapter requires the android-exact-adb runtime binding")
		}
		return nil
	case RuntimeBindingAndroidExactADB:
		if adapter.SignalFamily != androidLogcatSignalFamily || adapter.Name != "logcat" || len(adapter.Args) == 0 || adapter.Args[0] != "logcat" {
			return fmt.Errorf("android-exact-adb runtime binding requires the logcat adapter, Android logcat signal, and device-local logcat action")
		}
		if !slices.Equal(adapter.VersionArgs, []string{"version"}) {
			return fmt.Errorf("android-exact-adb version probe must be exactly version")
		}
		if err := validateConfigurationArguments("args", adapter.Args); err != nil {
			return err
		}
		for name := range adapter.Environment {
			if adbSelectionEnvironmentName(name) {
				return fmt.Errorf("android-exact-adb environment entry %q could select an ambient ADB server or device", name)
			}
		}
		readiness, ok := adapter.Readiness.(CommandReadiness)
		if !ok || readiness.RuntimeBinding != RuntimeBindingAndroidExactADB || readiness.Program != adapter.Program || !slices.Equal(readiness.Args, []string{"get-state"}) || readiness.Interval <= 0 || readiness.Interval > maximumConfigurationReadinessPeriod {
			return fmt.Errorf("android-exact-adb runtime binding requires matching typed command readiness")
		}
		if err := validateConfigurationArguments("readiness args", readiness.Args); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported observer runtime binding %q", adapter.RuntimeBinding)
	}
}

// bindRuntimeArguments is the single target-authority-to-argv bridge used by
// both collector startup and readiness. Static configuration supplies only a
// device-local action; no target value is interpreted as a template or option.
func bindRuntimeArguments(binding RuntimeBinding, static []string, plan ports.CollectorPlan) ([]string, error) {
	if binding == RuntimeBindingNone {
		return append([]string(nil), static...), nil
	}
	if binding != RuntimeBindingAndroidExactADB {
		return nil, fmt.Errorf("unsupported observer runtime binding %q", binding)
	}
	if plan.Requirement.SignalFamily != androidLogcatSignalFamily || plan.Attachment.TargetKind != domain.TargetAndroidVirtualDevice {
		return nil, fmt.Errorf("android-exact-adb binding requires an Android logcat collector plan")
	}
	if err := plan.Attachment.ADBDevice.Validate(); err != nil {
		return nil, fmt.Errorf("exact target ADB selection: %w", err)
	}
	if len(static) == 0 || static[0] != "logcat" && static[0] != "get-state" {
		return nil, fmt.Errorf("android-exact-adb binding accepts only a configured logcat or get-state action")
	}
	server := plan.Attachment.ADBDevice.Server
	arguments := []string{"-H", server.Host, "-P", strconv.Itoa(int(server.Port)), "-s", plan.Attachment.ADBDevice.Serial}
	return append(arguments, static...), nil
}
