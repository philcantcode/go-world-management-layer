// Package research implements the multi-dimensional vulnerability-research
// evidence backbone: stimulus classification, policy observation levels,
// per-action evidence bundles, and a conservative confidence floor.
//
// Capture authority lives here. Optional MCP facades only query sealed bundles.
package research

import (
	"path"
	"path/filepath"
	"strings"
)

// StimulusClass is derived only from executable basename and argv[0], never
// from model text or free-form descriptions.
type StimulusClass string

const (
	StimulusHTTPClient  StimulusClass = "http_client"
	StimulusPortScanner StimulusClass = "port_scanner"
	StimulusWebScanner  StimulusClass = "web_scanner"
	StimulusBrowser     StimulusClass = "browser"
	StimulusBinaryExec  StimulusClass = "binary_exec"
	StimulusGeneric     StimulusClass = "generic"
)

func (c StimulusClass) IsValid() bool {
	switch c {
	case StimulusHTTPClient, StimulusPortScanner, StimulusWebScanner, StimulusBrowser, StimulusBinaryExec, StimulusGeneric:
		return true
	}
	return false
}

// http client basenames (curl, httpie, wget, xh)
var httpClientNames = map[string]struct{}{
	"curl": {}, "curl.exe": {},
	"http": {}, "http.exe": {}, "https": {}, "https.exe": {}, // httpie entrypoints
	"httpie": {}, "httpie.exe": {},
	"wget": {}, "wget.exe": {},
	"xh": {}, "xh.exe": {},
}

// port scanners
var portScannerNames = map[string]struct{}{
	"nmap": {}, "nmap.exe": {},
	"masscan": {}, "masscan.exe": {},
	"naabu": {}, "naabu.exe": {},
	"rustscan": {}, "rustscan.exe": {},
}

// web scanners / content discovery
var webScannerNames = map[string]struct{}{
	"nuclei": {}, "nuclei.exe": {},
	"ffuf": {}, "ffuf.exe": {},
	"gobuster": {}, "gobuster.exe": {},
	"feroxbuster": {}, "feroxbuster.exe": {},
}

// browser automation / engines
var browserNames = map[string]struct{}{
	"chrome": {}, "chrome.exe": {},
	"chromium": {}, "chromium.exe": {},
	"google-chrome": {}, "google-chrome.exe": {},
	"msedge": {}, "msedge.exe": {},
	"firefox": {}, "firefox.exe": {},
	"playwright": {}, "playwright.exe": {},
	"chromium-browser": {},
}

// binary analysis / reverse-engineering tools — treated as binary_exec when
// the path looks like a native tool invocation rather than an interpreter.
var binaryToolNames = map[string]struct{}{
	"gdb": {}, "gdb.exe": {},
	"lldb": {}, "lldb.exe": {},
	"objdump": {}, "objdump.exe": {},
	"readelf": {}, "readelf.exe": {},
	"radare2": {}, "r2": {}, "r2.exe": {}, "radare2.exe": {},
	"ghidra": {}, "ghidra.exe": {}, "ghidraRun": {},
	"frida": {}, "frida.exe": {},
	"strace": {}, "strace.exe": {},
	"ltrace": {}, "ltrace.exe": {},
}

// ClassifyStimulus returns the stimulus class for an instrumented command.
// Classification uses only the executable path basename and argv[0] basename
// (when provided). Empty executable yields generic.
func ClassifyStimulus(executable string, argv0 string) StimulusClass {
	candidates := make([]string, 0, 2)
	if name := executableBasename(executable); name != "" {
		candidates = append(candidates, name)
	}
	if name := executableBasename(argv0); name != "" && (len(candidates) == 0 || name != candidates[0]) {
		candidates = append(candidates, name)
	}
	for _, name := range candidates {
		if _, ok := httpClientNames[name]; ok {
			return StimulusHTTPClient
		}
	}
	for _, name := range candidates {
		if _, ok := portScannerNames[name]; ok {
			return StimulusPortScanner
		}
	}
	for _, name := range candidates {
		if _, ok := webScannerNames[name]; ok {
			return StimulusWebScanner
		}
	}
	for _, name := range candidates {
		if _, ok := browserNames[name]; ok {
			return StimulusBrowser
		}
	}
	for _, name := range candidates {
		if _, ok := binaryToolNames[name]; ok {
			return StimulusBinaryExec
		}
	}
	// Heuristic: a path that ends in a known binary extension or has no
	// interpreter-like name is still generic unless it looks like a direct
	// native binary path under a bin directory with an unknown name.
	// Unknown tools stay generic; agents must not rely on silent reclassification.
	if len(candidates) == 0 {
		return StimulusGeneric
	}
	// Direct invocation of a non-interpreter path with an executable-looking name.
	for _, name := range candidates {
		if looksLikeNativeBinary(name) {
			return StimulusBinaryExec
		}
	}
	return StimulusGeneric
}

func executableBasename(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	// Prefer slash semantics for container/Linux paths; also strip Windows separators.
	value = strings.ReplaceAll(value, "\\", "/")
	base := path.Base(value)
	if base == "." || base == "/" || base == "" {
		base = filepath.Base(value)
	}
	return strings.ToLower(base)
}

func looksLikeNativeBinary(name string) bool {
	if name == "" {
		return false
	}
	// Interpreters and shells are not binary research stimuli.
	switch name {
	case "sh", "bash", "zsh", "fish", "dash", "cmd", "cmd.exe", "powershell", "powershell.exe",
		"pwsh", "pwsh.exe", "python", "python3", "python.exe", "python3.exe",
		"perl", "perl.exe", "ruby", "ruby.exe", "node", "node.exe", "deno", "deno.exe",
		"java", "java.exe", "dotnet", "dotnet.exe", "lua", "lua.exe", "php", "php.exe":
		return false
	}
	if strings.HasSuffix(name, ".exe") || strings.HasSuffix(name, ".bin") || strings.HasSuffix(name, ".elf") {
		return true
	}
	// Bare unknown name without extension → generic, not binary_exec.
	return false
}

// CompanionRole describes an evidence companion the policy intends to attach
// for a stimulus class. BuildCollectors instantiates real collectors (or
// gap-producing probes) for each intended role.
type CompanionRole string

const (
	CompanionNetworkCapture CompanionRole = "network_capture"
	CompanionNetworkDecode  CompanionRole = "network_decode"
	CompanionHostProcess    CompanionRole = "host_process"
	CompanionHostSyscall    CompanionRole = "host_syscall"
	CompanionStateDiff      CompanionRole = "state_diff"
	CompanionTargetOracle   CompanionRole = "target_oracle"
	CompanionStaticContext  CompanionRole = "static_context"
	CompanionReplay         CompanionRole = "replay"
)

// IntendedCompanions returns the companion plan for a class at the given
// observation level. Higher levels add invasive companions. The plan drives
// class-aware collector selection in BuildCollectors.
func IntendedCompanions(class StimulusClass, level ObservationLevel) []CompanionRole {
	base := []CompanionRole{CompanionHostProcess}
	switch class {
	case StimulusHTTPClient, StimulusPortScanner, StimulusWebScanner:
		base = append(base, CompanionNetworkCapture, CompanionNetworkDecode)
	case StimulusBrowser:
		base = append(base, CompanionNetworkCapture, CompanionNetworkDecode, CompanionTargetOracle)
	case StimulusBinaryExec:
		base = append(base, CompanionHostSyscall, CompanionStaticContext)
	default:
		// generic keeps host process only at baseline
	}
	if level.AtLeast(ObservationLevelDeep) {
		base = appendUniqueCompanion(base, CompanionStateDiff)
		base = appendUniqueCompanion(base, CompanionHostSyscall)
	}
	if level.AtLeast(ObservationLevelPayload) {
		base = appendUniqueCompanion(base, CompanionReplay)
		base = appendUniqueCompanion(base, CompanionTargetOracle)
	}
	return base
}

func appendUniqueCompanion(values []CompanionRole, role CompanionRole) []CompanionRole {
	for _, existing := range values {
		if existing == role {
			return values
		}
	}
	return append(values, role)
}
