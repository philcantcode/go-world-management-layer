//go:build !windows

package process

func platformRuntimeEnvironment() map[string]string { return nil }

func isPlatformRuntimeEnvironmentName(string) bool { return false }
