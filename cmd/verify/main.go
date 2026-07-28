// Command verify runs the repository's deterministic local quality gates.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
)

const verificationSummaryVersion = 1

type verificationSummary struct {
	SchemaVersion int          `json:"schema_version"`
	StartedAt     time.Time    `json:"started_at"`
	FinishedAt    time.Time    `json:"finished_at"`
	Success       bool         `json:"success"`
	SelectedGate  string       `json:"selected_gate,omitempty"`
	Gates         []gateResult `json:"gates"`
}

type gateResult struct {
	Name                string    `json:"name"`
	Status              string    `json:"status"`
	StartedAt           time.Time `json:"started_at"`
	FinishedAt          time.Time `json:"finished_at"`
	DurationNanoseconds int64     `json:"duration_nanoseconds"`
	Error               string    `json:"error,omitempty"`
}

func main() {
	only := flag.String("only", "", "run one gate: module, format, schema, fuzz, contract, security, integration, test, race, vet, or linux")
	summaryPath := flag.String("summary", filepath.Join("verification", "summary.json"), "machine-readable verification summary path")
	flag.Parse()

	gates := map[string]func() error{
		"module":   func() error { return run("go", "mod", "tidy", "-diff") },
		"format":   checkFormat,
		"schema":   checkSchema,
		"fuzz":     func() error { return run("go", "test", "-run", "^Fuzz", "./...") },
		"contract": func() error { return run("go", "test", "./internal/ports", "./internal/testkit") },
		"security": func() error {
			return run("go", "test", "./internal/admission", "./internal/inputcache", "./internal/policyregistry", "./internal/rpc", "./internal/safepath")
		},
		"integration": func() error { return run("go", "test", "./internal/orchestration", "./internal/rpc") },
		"test":        func() error { return run("go", "test", "./...") },
		"race":        func() error { return run("go", "test", "-race", "./...") },
		"vet":         func() error { return run("go", "vet", "./...") },
		"linux":       compileLinux,
	}
	order := []string{"module", "format", "schema", "fuzz", "contract", "security", "integration", "test", "race", "vet", "linux"}
	selected := order
	if *only != "" {
		if _, ok := gates[*only]; !ok {
			fatalf("unknown gate %q", *only)
		}
		selected = []string{*only}
	}
	result := verificationSummary{SchemaVersion: verificationSummaryVersion, StartedAt: time.Now().UTC(), Success: true, SelectedGate: *only}
	var verificationErr error
	for _, name := range selected {
		gateResult, err := execute(name, gates[name])
		result.Gates = append(result.Gates, gateResult)
		if err != nil {
			result.Success = false
			verificationErr = err
			break
		}
	}
	result.FinishedAt = time.Now().UTC()
	if err := writeSummary(*summaryPath, result); err != nil {
		verificationErr = errors.Join(verificationErr, fmt.Errorf("write verification summary: %w", err))
	}
	if verificationErr != nil {
		fatalf("%v", verificationErr)
	}
}

func execute(name string, gate func() error) (gateResult, error) {
	fmt.Printf("verify: %s\n", name)
	result := gateResult{Name: name, Status: "passed", StartedAt: time.Now().UTC()}
	if err := gate(); err != nil {
		result.Status = "failed"
		result.Error = err.Error()
		result.FinishedAt = time.Now().UTC()
		result.DurationNanoseconds = result.FinishedAt.Sub(result.StartedAt).Nanoseconds()
		return result, fmt.Errorf("%s gate failed: %w", name, err)
	}
	result.FinishedAt = time.Now().UTC()
	result.DurationNanoseconds = result.FinishedAt.Sub(result.StartedAt).Nanoseconds()
	return result, nil
}

func writeSummary(path string, result verificationSummary) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("summary path must not be blank")
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return err
	}
	return atomicfile.Write(absolute, encoded, 0o600)
}

func checkFormat() error {
	files, err := goFiles(".")
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no Go files found")
	}
	const batchSize = 100
	unformatted := make([]string, 0)
	for start := 0; start < len(files); start += batchSize {
		end := start + batchSize
		if end > len(files) {
			end = len(files)
		}
		command := exec.Command("gofmt", append([]string{"-l"}, files[start:end]...)...)
		output, err := command.Output()
		if err != nil {
			return fmt.Errorf("gofmt: %w", err)
		}
		for _, value := range strings.Fields(string(output)) {
			unformatted = append(unformatted, filepath.ToSlash(value))
		}
	}
	if len(unformatted) > 0 {
		sort.Strings(unformatted)
		return fmt.Errorf("unformatted files:\n%s", strings.Join(unformatted, "\n"))
	}
	return nil
}

func checkSchema() error {
	if err := run("buf", "lint", "api/world/v1/world.proto"); err != nil {
		return err
	}
	if err := checkGeneratedBindings(); err != nil {
		return err
	}
	return run("go", "test", "./api/world/v1")
}

func checkGeneratedBindings() error {
	directory, err := os.MkdirTemp("", "world-protobuf-generation-")
	if err != nil {
		return fmt.Errorf("create protobuf generation directory: %w", err)
	}
	defer os.RemoveAll(directory)
	if err := run("buf", "generate", "--output", directory, "api/world/v1/world.proto"); err != nil {
		return err
	}
	for _, path := range []string{"api/world/v1/world.pb.go", "api/world/v1/world_grpc.pb.go"} {
		generated := filepath.Join(directory, filepath.FromSlash(path))
		if err := requireEqualFiles(path, generated); err != nil {
			return fmt.Errorf("generated protobuf bindings are stale; run `make generate`: %w", err)
		}
	}
	return nil
}

func requireEqualFiles(wantPath, gotPath string) error {
	want, err := os.ReadFile(wantPath)
	if err != nil {
		return fmt.Errorf("read checked-in file %s: %w", wantPath, err)
	}
	got, err := os.ReadFile(gotPath)
	if err != nil {
		return fmt.Errorf("read generated file %s: %w", gotPath, err)
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("%s differs from a clean generation", wantPath)
	}
	return nil
}

func goFiles(root string) ([]string, error) {
	files := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "bin" || entry.Name() == "dist" || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".go") {
			files = append(files, path)
		}
		return nil
	})
	sort.Strings(files)
	return files, err
}

func run(name string, args ...string) error {
	return runWithEnvironment(nil, name, args...)
}

func runWithEnvironment(environment []string, name string, args ...string) error {
	command := exec.Command(name, args...)
	if len(environment) > 0 {
		command.Env = append(os.Environ(), environment...)
	}
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Stdin = bytes.NewReader(nil)
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func compileLinux() error {
	directory, err := os.MkdirTemp("", "world-verify-linux-")
	if err != nil {
		return fmt.Errorf("create Linux build directory: %w", err)
	}
	defer os.RemoveAll(directory)
	outputDirectory := directory + string(filepath.Separator)
	return runWithEnvironment(
		[]string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"},
		"go", "test", "-c", "-o", outputDirectory, "./...",
	)
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, "verify: "+format+"\n", values...)
	os.Exit(1)
}
