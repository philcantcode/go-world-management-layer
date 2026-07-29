package cuttlefish

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const maximumManagedAVDINIBytes = int64(64 << 10)

type managedAVDPathState struct {
	name             string
	directory        string
	ini              string
	listed           bool
	directoryPresent bool
	iniPresent       bool
}

func (s managedAVDPathState) any() bool {
	return s.listed || s.directoryPresent || s.iniPresent
}

func (s managedAVDPathState) complete() bool {
	return s.listed && s.directoryPresent && s.iniPresent
}

func (b *ManagedEmulatorBackend) inspectExactManagedAVD(ctx context.Context, name string) (managedAVDPathState, error) {
	if !safeInstanceName(name) {
		return managedAVDPathState{}, fmt.Errorf("managed AVD name %q is unsafe", name)
	}
	home, err := filepath.Abs(b.avdHome)
	if err != nil {
		return managedAVDPathState{}, fmt.Errorf("canonicalize managed AVD home: %w", err)
	}
	state := managedAVDPathState{
		name: name, directory: filepath.Join(home, name+".avd"), ini: filepath.Join(home, name+".ini"),
	}
	for _, path := range []string{state.directory, state.ini} {
		if err := requirePathWithin(home, path, false); err != nil {
			return managedAVDPathState{}, fmt.Errorf("canonicalize exact managed AVD path: %w", err)
		}
	}
	present, err := b.listAVDs(ctx)
	if err != nil {
		return managedAVDPathState{}, err
	}
	_, state.listed = present[name]
	state.directoryPresent, err = inspectManagedAVDPath(state.directory, true)
	if err != nil {
		return managedAVDPathState{}, err
	}
	state.iniPresent, err = inspectManagedAVDPath(state.ini, false)
	if err != nil {
		return managedAVDPathState{}, err
	}
	return state, nil
}

func inspectManagedAVDPath(path string, directory bool) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect exact managed AVD path %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("exact managed AVD path %q is a symbolic link", path)
	}
	if err := requireManagedAVDPathNotReparse(path); err != nil {
		return false, err
	}
	if directory && !info.IsDir() {
		return false, fmt.Errorf("exact managed AVD path %q is not a regular directory", path)
	}
	if !directory && !info.Mode().IsRegular() {
		return false, fmt.Errorf("exact managed AVD path %q is not a regular file", path)
	}
	return true, nil
}

func (s managedAVDPathState) requireComplete() error {
	if !s.complete() {
		return fmt.Errorf(
			"managed AVD %q is incomplete (listed=%t directory=%t ini=%t)",
			s.name, s.listed, s.directoryPresent, s.iniPresent,
		)
	}
	return s.requireINIPathBinding()
}

func (s managedAVDPathState) requireINIPathBinding() error {
	if !s.iniPresent {
		return fmt.Errorf("managed AVD %q has no exact regular ini file", s.name)
	}
	lstat, err := os.Lstat(s.ini)
	if err != nil {
		return fmt.Errorf("inspect managed AVD ini before reading it: %w", err)
	}
	file, err := os.Open(s.ini)
	if err != nil {
		return fmt.Errorf("open managed AVD ini: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("inspect opened managed AVD ini: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(lstat, opened) {
		return fmt.Errorf("managed AVD ini identity changed or resolves through indirection")
	}
	content, err := io.ReadAll(io.LimitReader(file, maximumManagedAVDINIBytes+1))
	if err != nil {
		return fmt.Errorf("read managed AVD ini: %w", err)
	}
	if int64(len(content)) > maximumManagedAVDINIBytes {
		return fmt.Errorf("managed AVD ini exceeds %d bytes", maximumManagedAVDINIBytes)
	}
	var pathValue string
	pathCount := 0
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if found && strings.TrimSpace(key) == "path" {
			pathCount++
			pathValue = strings.TrimSpace(value)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan managed AVD ini: %w", err)
	}
	if pathCount != 1 || pathValue == "" || !filepath.IsAbs(pathValue) {
		return fmt.Errorf("managed AVD ini contains %d canonical path bindings; exactly one absolute path is required", pathCount)
	}
	actual, err := filepath.Abs(pathValue)
	if err != nil {
		return fmt.Errorf("canonicalize managed AVD ini path: %w", err)
	}
	expected, err := filepath.Abs(s.directory)
	if err != nil {
		return fmt.Errorf("canonicalize exact managed AVD directory: %w", err)
	}
	matches := filepath.Clean(actual) == filepath.Clean(expected)
	if runtime.GOOS == "windows" {
		matches = strings.EqualFold(filepath.Clean(actual), filepath.Clean(expected))
	}
	if !matches {
		return fmt.Errorf("managed AVD ini path %q redirects away from exact directory %q", actual, expected)
	}
	return nil
}

func removeExactManagedAVDPaths(state managedAVDPathState) error {
	var result error
	if state.directoryPresent {
		result = errors.Join(result, os.RemoveAll(state.directory))
	}
	if state.iniPresent {
		result = errors.Join(result, os.Remove(state.ini))
	}
	return result
}

func (b *ManagedEmulatorBackend) filesystemManagedAVDNames() (map[string]struct{}, error) {
	entries, err := os.ReadDir(b.avdHome)
	if err != nil {
		return nil, fmt.Errorf("inventory managed AVD filesystem: %w", err)
	}
	result := make(map[string]struct{})
	for _, entry := range entries {
		filename := entry.Name()
		var name string
		switch {
		case strings.HasSuffix(filename, ".avd"):
			name = strings.TrimSuffix(filename, ".avd")
		case strings.HasSuffix(filename, ".ini"):
			name = strings.TrimSuffix(filename, ".ini")
		default:
			continue
		}
		if strings.HasPrefix(name, "world-") && safeInstanceName(name) {
			result[name] = struct{}{}
		}
	}
	return result, nil
}
