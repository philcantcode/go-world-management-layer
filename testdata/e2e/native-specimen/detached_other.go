//go:build !linux

package main

import (
	"fmt"
	"time"
)

func startDetachedChild(_, _ string, _ time.Duration) (int, error) {
	return 0, fmt.Errorf("detached setsid fixture requires Linux")
}
