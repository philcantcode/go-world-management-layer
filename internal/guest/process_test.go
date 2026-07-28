package guest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
	"time"
)

const osLauncherHelperEnvironment = "WORLD_GUEST_OS_LAUNCHER_HELPER"

func TestMain(m *testing.M) {
	switch os.Getenv(osLauncherHelperEnvironment) {
	case "argv":
		if err := json.NewEncoder(os.Stdout).Encode(os.Args[1:]); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	case "output":
		_, stdoutErr := fmt.Fprint(os.Stdout, "stdout-after-exit")
		_, stderrErr := fmt.Fprint(os.Stderr, "stderr-after-exit")
		if stdoutErr != nil || stderrErr != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestOSLauncherSuppliesExecutableAsArgvZero(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-input", "payload with spaces", "", "--literal=$()"}
	process, err := (OSLauncher{}).Launch(ProcessSpec{
		Executable: executable, Argv: want, WorkingDirectory: t.TempDir(),
		Environment: map[string]string{osLauncherHelperEnvironment: "argv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if err := process.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	stdout, stdoutErr := io.ReadAll(process.Stdout())
	stderr, stderrErr := io.ReadAll(process.Stderr())
	result := process.Wait()
	if stdoutErr != nil || stderrErr != nil || result.Err != nil || result.ExitCode != 0 {
		t.Fatalf("helper failed: stdout error=%v stderr error=%v result=%#v stderr=%q", stdoutErr, stderrErr, result, stderr)
	}
	cleanupContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	clean, err := process.ConfirmCleanup(cleanupContext)
	if err != nil || !clean {
		t.Fatalf("process cleanup = %t, %v", clean, err)
	}
	var got []string
	if err := json.Unmarshal(stdout, &got); err != nil {
		t.Fatalf("decode helper argv %q: %v", stdout, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("arguments after argv[0] = %#v, want %#v", got, want)
	}
}

func TestOSLauncherRetainsCompleteOutputAfterWait(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	process, err := (OSLauncher{}).Launch(ProcessSpec{
		Executable: executable, WorkingDirectory: t.TempDir(),
		Environment: map[string]string{osLauncherHelperEnvironment: "output"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if err := process.Stdin().Close(); err != nil {
		t.Fatal(err)
	}
	result := process.Wait()
	stdout, stdoutErr := io.ReadAll(process.Stdout())
	stderr, stderrErr := io.ReadAll(process.Stderr())
	if result.Err != nil || result.ExitCode != 0 || stdoutErr != nil || stderrErr != nil {
		t.Fatalf("wait/read failed: result=%#v stdout error=%v stderr error=%v", result, stdoutErr, stderrErr)
	}
	if string(stdout) != "stdout-after-exit" || string(stderr) != "stderr-after-exit" {
		t.Fatalf("output after Wait = stdout %q stderr %q", stdout, stderr)
	}
}
