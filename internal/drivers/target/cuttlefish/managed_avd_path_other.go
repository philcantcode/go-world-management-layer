//go:build !windows

package cuttlefish

func requireManagedAVDPathNotReparse(string) error { return nil }
