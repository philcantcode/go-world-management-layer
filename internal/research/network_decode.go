package research

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// NetworkDecodeOptions configures semantic network decoding.
type NetworkDecodeOptions struct {
	LookPath func(file string) (string, error)
}

// DefaultNetworkDecodeCollector turns capture or flow tables into structured
// records using tshark when it is available and attributed connection metadata
// otherwise.
type DefaultNetworkDecodeCollector struct {
	opts NetworkDecodeOptions
}

// NewNetworkDecodeCollector builds a network_decode companion.
func NewNetworkDecodeCollector(opts NetworkDecodeOptions) *DefaultNetworkDecodeCollector {
	if opts.LookPath == nil {
		opts.LookPath = exec.LookPath
	}
	return &DefaultNetworkDecodeCollector{opts: opts}
}

// Decode produces structured HTTP/DNS/TLS-ish metadata from pcap (tshark) or
// from attributed connection tables when pcap tools are unavailable.
func (c *DefaultNetworkDecodeCollector) Decode(ctx context.Context, start ActionStart, network NetworkIndex, actionDir string) (NetworkDecodeResult, error) {
	if err := ctx.Err(); err != nil {
		return NetworkDecodeResult{}, err
	}
	// Prefer tshark on a real pcap artifact contained under the action directory.
	// Attribution follows the network index — unjoined window pcap must not
	// inflate semantic confidence.
	if network.ArtifactPath != "" && strings.Contains(network.CaptureMethod, "pcap") {
		if abs, ok := containPath(actionDir, network.ArtifactPath); ok {
			if result, ok := c.decodeWithTshark(ctx, abs, actionDir, network); ok {
				return result, nil
			}
		}
	}
	// Flow-table decode from attributed connections / ambient endpoints.
	records := decodeFromNetworkIndex(start, network)
	if len(records) == 0 {
		return NetworkDecodeResult{
			Available:  false,
			Attributed: false,
			Reason:     ReasonNetworkDecodeUnavailable,
			Method:     "none",
		}, nil
	}
	rel := filepath.ToSlash(filepath.Join("network", "decode.json"))
	payload := NetworkDecodePayload{ObservedAt: time.Now().UTC(), Records: records, Method: "flow_table"}
	if err := writeJSON(filepath.Join(actionDir, "network", "decode.json"), payload); err != nil {
		return NetworkDecodeResult{Available: false, Reason: ReasonNetworkDecodeFailed}, nil
	}
	return NetworkDecodeResult{
		Records:      payload,
		Scope:        network.Scope,
		Available:    true,
		Attributed:   network.Attributed,
		Method:       "flow_table",
		ArtifactPath: rel,
	}, nil
}

func (c *DefaultNetworkDecodeCollector) decodeWithTshark(ctx context.Context, pcapPath, actionDir string, network NetworkIndex) (NetworkDecodeResult, bool) {
	tool, err := c.opts.LookPath("tshark")
	if err != nil || tool == "" {
		return NetworkDecodeResult{}, false
	}
	if _, err := os.Stat(pcapPath); err != nil {
		return NetworkDecodeResult{}, false
	}
	// Lightweight fields only — no full packet dump.
	cmd := exec.CommandContext(ctx, tool, "-r", pcapPath, "-T", "fields",
		"-e", "frame.number",
		"-e", "ip.src",
		"-e", "ip.dst",
		"-e", "tcp.dstport",
		"-e", "udp.dstport",
		"-e", "http.host",
		"-e", "http.request.method",
		"-e", "tls.handshake.extensions_server_name",
		"-e", "dns.qry.name",
		"-c", "500",
	)
	out, err := cmd.Output()
	if err != nil {
		return NetworkDecodeResult{}, false
	}
	records := parseTsharkFields(string(out))
	if len(records) == 0 {
		return NetworkDecodeResult{}, false
	}
	rel := filepath.ToSlash(filepath.Join("network", "decode.json"))
	payload := NetworkDecodePayload{ObservedAt: time.Now().UTC(), Records: records, Method: "tshark"}
	if err := writeJSON(filepath.Join(actionDir, "network", "decode.json"), payload); err != nil {
		return NetworkDecodeResult{Available: false, Attributed: false, Reason: ReasonNetworkDecodeFailed}, true
	}
	scope := network.Scope
	if scope == "" {
		scope = "action_window"
	}
	reason := ""
	if !network.Attributed {
		reason = reasonOr(network.Reason, ReasonNetworkWindowUnjoined)
	}
	return NetworkDecodeResult{
		Records: payload, Scope: scope, Available: true, Attributed: network.Attributed,
		Method: "tshark", ArtifactPath: rel, Reason: reason,
	}, true
}

// containPath joins rel under root and rejects escapes.
func containPath(root, rel string) (string, bool) {
	rel = strings.TrimSpace(rel)
	if rel == "" || strings.Contains(rel, "..") {
		return "", false
	}
	joined := filepath.Join(root, filepath.FromSlash(rel))
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", false
	}
	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", false
	}
	relCheck, err := filepath.Rel(absRoot, absJoined)
	if err != nil || strings.HasPrefix(relCheck, "..") || filepath.IsAbs(relCheck) {
		return "", false
	}
	return absJoined, true
}

func parseTsharkFields(output string) []map[string]any {
	lines := strings.Split(output, "\n")
	records := make([]map[string]any, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		rec := map[string]any{}
		if len(fields) > 0 && fields[0] != "" {
			rec["frame"] = fields[0]
		}
		if len(fields) > 1 && fields[1] != "" {
			rec["src"] = boundText(fields[1], 128)
		}
		if len(fields) > 2 && fields[2] != "" {
			rec["dst"] = boundText(fields[2], 128)
		}
		if len(fields) > 3 && fields[3] != "" {
			rec["tcp_dport"] = fields[3]
		}
		if len(fields) > 4 && fields[4] != "" {
			rec["udp_dport"] = fields[4]
		}
		if len(fields) > 5 && fields[5] != "" {
			rec["http_host"] = boundText(fields[5], 256)
		}
		if len(fields) > 6 && fields[6] != "" {
			rec["http_method"] = boundText(fields[6], 16)
		}
		if len(fields) > 7 && fields[7] != "" {
			rec["tls_sni"] = boundText(fields[7], 256)
		}
		if len(fields) > 8 && fields[8] != "" {
			rec["dns_query"] = boundText(fields[8], 256)
		}
		if len(rec) > 0 {
			records = append(records, rec)
		}
		if len(records) >= 500 {
			break
		}
	}
	return records
}

func decodeFromNetworkIndex(start ActionStart, network NetworkIndex) []map[string]any {
	records := make([]map[string]any, 0)
	// Action endpoints from argv (sanitized) are semantic intent.
	for _, arg := range start.Argv {
		endpoints, _ := actionEndpoints([]string{arg}, 8)
		for _, ep := range endpoints {
			records = append(records, map[string]any{
				"kind":   "action_endpoint",
				"scheme": ep.Scheme,
				"host":   ep.Host,
				"port":   ep.Port,
			})
		}
	}
	// Attributed PID connections.
	switch flows := network.Flows.(type) {
	case NetworkCaptureObservation:
		for _, sock := range flows.PIDConnections {
			records = append(records, map[string]any{
				"kind":     "connection",
				"protocol": sock.Protocol,
				"local":    sock.LocalAddress,
				"remote":   sock.RemoteAddress,
				"state":    sock.State,
				"pid":      sock.PID,
			})
		}
		if ambient, ok := flows.Ambient.(LocalNetworkObservation); ok {
			for _, ep := range ambient.ActionEndpoints {
				records = append(records, map[string]any{
					"kind":   "action_endpoint",
					"scheme": ep.Scheme,
					"host":   ep.Host,
					"port":   ep.Port,
				})
			}
		}
	case LocalNetworkObservation:
		for _, ep := range flows.ActionEndpoints {
			records = append(records, map[string]any{
				"kind":   "action_endpoint",
				"scheme": ep.Scheme,
				"host":   ep.Host,
				"port":   ep.Port,
			})
		}
	default:
		// JSON-round-tripped maps from disk are not expected here at Seal time.
		if raw, err := json.Marshal(network.Flows); err == nil && len(raw) > 2 {
			_ = raw
		}
	}
	// Dedupe by JSON key.
	seen := make(map[string]struct{})
	unique := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		keyBytes, _ := json.Marshal(rec)
		key := string(keyBytes)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, rec)
		if len(unique) >= 256 {
			break
		}
	}
	return unique
}

// NetworkDecodePayload is written to network/decode.json.
type NetworkDecodePayload struct {
	ObservedAt time.Time        `json:"observed_at"`
	Method     string           `json:"method"`
	Records    []map[string]any `json:"records"`
}

var _ NetworkDecodeCollector = (*DefaultNetworkDecodeCollector)(nil)
