// native-specimen is a deterministic hostile-input fixture used only by the
// end-to-end Docker and Android qualification harnesses.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type probe struct {
	Path       string `json:"path"`
	Accessible bool   `json:"accessible"`
	Error      string `json:"error,omitempty"`
}

type result struct {
	InputPath   string    `json:"input_path"`
	InputDigest string    `json:"input_digest"`
	InputSize   int       `json:"input_size"`
	Probes      []probe   `json:"boundary_probes"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
}

var requiredBoundaryPaths = []string{
	"/workspace",
	"/var/run/docker.sock",
	"/run/containerd/containerd.sock",
	"/proc/1/root/workspace",
}

func main() {
	input := flag.String("input", "/target/input/payload.txt", "input file")
	output := flag.String("output", "/target/result.json", "result file")
	delay := flag.Duration("sleep", 0, "bounded cancellation fixture delay")
	exitCode := flag.Int("exit", 0, "requested terminal exit code")
	outputBytes := flag.Int("output-bytes", 0, "append deterministic padding bytes")
	verifyResult := flag.Bool("verify-result", false, "verify a prior specimen result and its isolation probes")
	expectedInputDigest := flag.String("expected-input-digest", "", "required input digest for -verify-result")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	started := time.Now().UTC()
	if *delay > 0 {
		timer := time.NewTimer(*delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			os.Exit(124)
		case <-timer.C:
		}
	}
	content, err := os.ReadFile(*input)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *verifyResult {
		if err := verifyRecordedResult(content, *expectedInputDigest); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(6)
		}
	}
	sum := sha256.Sum256(content)
	value := result{InputPath: *input, InputDigest: "sha256:" + hex.EncodeToString(sum[:]), InputSize: len(content), StartedAt: started}
	for _, path := range requiredBoundaryPaths {
		_, probeErr := os.Stat(path)
		item := probe{Path: path, Accessible: probeErr == nil}
		if probeErr != nil {
			item.Error = probeErr.Error()
		}
		value.Probes = append(value.Probes, item)
	}
	value.FinishedAt = time.Now().UTC()
	encoded, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if *outputBytes > 0 {
		encoded = append(encoded, []byte(strings.Repeat("x", *outputBytes))...)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	if err := os.WriteFile(*output, append(encoded, '\n'), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(5)
	}
	fmt.Printf("%s\n", value.InputDigest)
	os.Exit(*exitCode)
}

func verifyRecordedResult(content []byte, expectedInputDigest string) error {
	if expectedInputDigest == "" {
		return fmt.Errorf("expected input digest is required")
	}
	var recorded result
	if err := json.Unmarshal(content, &recorded); err != nil {
		return fmt.Errorf("decode recorded specimen result: %w", err)
	}
	if recorded.InputDigest != expectedInputDigest {
		return fmt.Errorf("recorded input digest %q does not match %q", recorded.InputDigest, expectedInputDigest)
	}
	required := make(map[string]bool, len(requiredBoundaryPaths))
	for _, path := range requiredBoundaryPaths {
		required[path] = false
	}
	for _, item := range recorded.Probes {
		seen, expected := required[item.Path]
		if !expected {
			continue
		}
		if seen {
			return fmt.Errorf("boundary probe %q is duplicated", item.Path)
		}
		if item.Accessible {
			return fmt.Errorf("boundary probe %q was accessible", item.Path)
		}
		required[item.Path] = true
	}
	for path, seen := range required {
		if !seen {
			return fmt.Errorf("boundary probe %q is missing", path)
		}
	}
	return nil
}
