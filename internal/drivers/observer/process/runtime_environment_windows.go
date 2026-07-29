//go:build windows

package process

import (
	"os"
	"strings"
)

const windowsSystemRootEnvironment = "SystemRoot"

// platformRuntimeEnvironment returns only the non-secret host state required
// by Windows process and Winsock initialization. The canonical key spelling is
// owned here rather than inherited from the ambient environment block.
func platformRuntimeEnvironment() map[string]string {
	value := os.Getenv(windowsSystemRootEnvironment)
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return map[string]string{windowsSystemRootEnvironment: value}
}

func isPlatformRuntimeEnvironmentName(name string) bool {
	return strings.EqualFold(name, windowsSystemRootEnvironment)
}
