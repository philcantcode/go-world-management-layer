package research

import "testing"

func TestClassifyStimulusTable(t *testing.T) {
	tests := []struct {
		name       string
		executable string
		argv0      string
		want       StimulusClass
	}{
		{"curl absolute", "/usr/bin/curl", "curl", StimulusHTTPClient},
		{"curl.exe", `C:\Tools\curl.exe`, "curl.exe", StimulusHTTPClient},
		{"wget", "wget", "", StimulusHTTPClient},
		{"httpie http", "/usr/local/bin/http", "http", StimulusHTTPClient},
		{"xh", "xh", "xh", StimulusHTTPClient},
		{"nmap", "/usr/bin/nmap", "nmap", StimulusPortScanner},
		{"masscan", "masscan", "", StimulusPortScanner},
		{"naabu", "naabu", "naabu", StimulusPortScanner},
		{"rustscan", "rustscan", "", StimulusPortScanner},
		{"nuclei", "/opt/nuclei", "nuclei", StimulusWebScanner},
		{"ffuf", "ffuf", "", StimulusWebScanner},
		{"gobuster", "gobuster", "", StimulusWebScanner},
		{"feroxbuster", "feroxbuster", "", StimulusWebScanner},
		{"chrome", "/usr/bin/google-chrome", "google-chrome", StimulusBrowser},
		{"playwright", "playwright", "", StimulusBrowser},
		{"gdb", "gdb", "", StimulusBinaryExec},
		{"objdump", "/usr/bin/objdump", "objdump", StimulusBinaryExec},
		{"frida", "frida", "", StimulusBinaryExec},
		{"native exe unknown", "specimen.exe", "", StimulusBinaryExec},
		{"python generic", "python3", "", StimulusGeneric},
		{"bash generic", "/bin/bash", "bash", StimulusGeneric},
		{"unknown tool", "weirdtool", "", StimulusGeneric},
		{"empty", "", "", StimulusGeneric},
		// argv0 can classify when executable is a path wrapper
		{"argv0 wins class", "/usr/bin/env", "nmap", StimulusPortScanner},
		// case insensitive
		{"curl upper path", "/USR/BIN/CURL", "CURL", StimulusHTTPClient},
		// model text must not be used — only basenames
		{"not from description", "echo", "echo", StimulusGeneric},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyStimulus(tc.executable, tc.argv0)
			if got != tc.want {
				t.Fatalf("ClassifyStimulus(%q, %q) = %q, want %q", tc.executable, tc.argv0, got, tc.want)
			}
		})
	}
}

func TestIntendedCompanionsScaleWithLevel(t *testing.T) {
	base := IntendedCompanions(StimulusHTTPClient, ObservationLevelBaseline)
	if !containsCompanion(base, CompanionNetworkCapture) || !containsCompanion(base, CompanionHostProcess) {
		t.Fatalf("http_client baseline companions = %v", base)
	}
	deep := IntendedCompanions(StimulusHTTPClient, ObservationLevelDeep)
	if !containsCompanion(deep, CompanionStateDiff) {
		t.Fatalf("deep should add state_diff: %v", deep)
	}
	payload := IntendedCompanions(StimulusHTTPClient, ObservationLevelPayload)
	if !containsCompanion(payload, CompanionReplay) {
		t.Fatalf("payload should add replay: %v", payload)
	}
}

func containsCompanion(values []CompanionRole, want CompanionRole) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
