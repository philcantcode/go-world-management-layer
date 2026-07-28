package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// CPUQuantity stores CPU in millicores. It accepts decimal cores ("2",
// "0.5") and Kubernetes-style millicores ("500m").
type CPUQuantity int64

func (q CPUQuantity) MilliCPU() int64 { return int64(q) }

func (q CPUQuantity) String() string {
	if q%1000 == 0 {
		return strconv.FormatInt(int64(q)/1000, 10)
	}
	return strconv.FormatInt(int64(q), 10) + "m"
}

func (q CPUQuantity) MarshalJSON() ([]byte, error) { return json.Marshal(q.String()) }
func (q CPUQuantity) MarshalYAML() (any, error)    { return q.String(), nil }

func (q *CPUQuantity) UnmarshalJSON(data []byte) error {
	s, err := scalarJSONText(data)
	if err != nil {
		return fmt.Errorf("CPU quantity: %w", err)
	}
	value, err := parseCPU(s)
	if err != nil {
		return err
	}
	*q = value
	return nil
}

func (q *CPUQuantity) UnmarshalYAML(node *yaml.Node) error {
	value, err := parseCPU(node.Value)
	if err != nil {
		return err
	}
	*q = value
	return nil
}

func parseCPU(text string) (CPUQuantity, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, fmt.Errorf("CPU quantity must not be empty")
	}
	multiplier := int64(1000)
	if strings.HasSuffix(text, "m") {
		multiplier = 1
		text = strings.TrimSuffix(text, "m")
	}
	value, err := scaledNonNegativeInteger(text, multiplier)
	if err != nil {
		return 0, fmt.Errorf("invalid CPU quantity: %w", err)
	}
	return CPUQuantity(value), nil
}

// ByteQuantity stores an exact byte count. Binary IEC suffixes (Ki, Mi, Gi,
// Ti, Pi) and decimal SI suffixes (kB, MB, GB, TB, PB) are supported.
type ByteQuantity int64

func (q ByteQuantity) Bytes() int64 { return int64(q) }

func (q ByteQuantity) String() string {
	value := int64(q)
	if value == 0 {
		return "0B"
	}
	units := []struct {
		suffix string
		scale  int64
	}{
		{"Pi", 1 << 50},
		{"Ti", 1 << 40},
		{"Gi", 1 << 30},
		{"Mi", 1 << 20},
		{"Ki", 1 << 10},
	}
	for _, unit := range units {
		if value%unit.scale == 0 {
			return strconv.FormatInt(value/unit.scale, 10) + unit.suffix
		}
	}
	return strconv.FormatInt(value, 10) + "B"
}

func (q ByteQuantity) MarshalJSON() ([]byte, error) { return json.Marshal(q.String()) }
func (q ByteQuantity) MarshalYAML() (any, error)    { return q.String(), nil }

func (q *ByteQuantity) UnmarshalJSON(data []byte) error {
	s, err := scalarJSONText(data)
	if err != nil {
		return fmt.Errorf("byte quantity: %w", err)
	}
	value, err := parseBytes(s)
	if err != nil {
		return err
	}
	*q = value
	return nil
}

func (q *ByteQuantity) UnmarshalYAML(node *yaml.Node) error {
	value, err := parseBytes(node.Value)
	if err != nil {
		return err
	}
	*q = value
	return nil
}

var quantityPattern = regexp.MustCompile(`^(\d+(?:\.\d*)?|\.\d+)([A-Za-z]*)$`)

func parseBytes(text string) (ByteQuantity, error) {
	text = strings.TrimSpace(text)
	match := quantityPattern.FindStringSubmatch(text)
	if match == nil {
		return 0, fmt.Errorf("invalid byte quantity %q", text)
	}
	scales := map[string]int64{
		"": 1, "B": 1,
		"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30,
		"Ti": 1 << 40, "Pi": 1 << 50,
		"kB": 1_000, "KB": 1_000, "MB": 1_000_000, "GB": 1_000_000_000,
		"TB": 1_000_000_000_000, "PB": 1_000_000_000_000_000,
	}
	scale, ok := scales[match[2]]
	if !ok {
		return 0, fmt.Errorf("invalid byte quantity suffix %q", match[2])
	}
	value, err := scaledNonNegativeInteger(match[1], scale)
	if err != nil {
		return 0, fmt.Errorf("invalid byte quantity %q: %w", text, err)
	}
	return ByteQuantity(value), nil
}

func scaledNonNegativeInteger(text string, scale int64) (int64, error) {
	value, ok := new(big.Rat).SetString(text)
	if !ok || value.Sign() < 0 {
		return 0, fmt.Errorf("expected a non-negative decimal number")
	}
	value.Mul(value, new(big.Rat).SetInt64(scale))
	if !value.IsInt() {
		return 0, fmt.Errorf("value is not exactly representable in the target unit")
	}
	numerator := value.Num()
	if !numerator.IsInt64() {
		return 0, fmt.Errorf("value exceeds int64")
	}
	return numerator.Int64(), nil
}

// Duration is a non-negative time duration encoded using Go duration syntax.
// Whether zero is meaningful is decided by field-level semantic validation.
type Duration time.Duration

func (d Duration) Duration() time.Duration      { return time.Duration(d) }
func (d Duration) String() string               { return time.Duration(d).String() }
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }
func (d Duration) MarshalYAML() (any, error)    { return d.String(), nil }

func (d *Duration) UnmarshalJSON(data []byte) error {
	s, err := scalarJSONText(data)
	if err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	value, err := parseDuration(s)
	if err != nil {
		return err
	}
	*d = value
	return nil
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	value, err := parseDuration(node.Value)
	if err != nil {
		return err
	}
	*d = value
	return nil
}

func parseDuration(text string) (Duration, error) {
	parsed, err := time.ParseDuration(strings.TrimSpace(text))
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", text, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("duration must not be negative")
	}
	return Duration(parsed), nil
}

// Percent is a percentage in the inclusive range 0..100. YAML accepts either
// a number or a quoted value with an optional percent sign.
type Percent float64

func (p Percent) Float64() float64 { return float64(p) }

func (p *Percent) UnmarshalJSON(data []byte) error {
	s, err := scalarJSONText(data)
	if err != nil {
		return fmt.Errorf("percent: %w", err)
	}
	return p.parse(s)
}

func (p *Percent) UnmarshalYAML(node *yaml.Node) error { return p.parse(node.Value) }

func (p *Percent) parse(text string) error {
	text = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(text), "%"))
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("invalid percent %q", text)
	}
	if value < 0 || value > 100 {
		return fmt.Errorf("percent must be between 0 and 100")
	}
	*p = Percent(value)
	return nil
}

// Ratio is a finite ratio in the inclusive range 0..1.
type Ratio float64

func (r Ratio) Float64() float64 { return float64(r) }

func (r *Ratio) UnmarshalJSON(data []byte) error {
	s, err := scalarJSONText(data)
	if err != nil {
		return fmt.Errorf("ratio: %w", err)
	}
	return r.parse(s)
}

func (r *Ratio) UnmarshalYAML(node *yaml.Node) error { return r.parse(node.Value) }

func (r *Ratio) parse(text string) error {
	value, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("invalid ratio %q", text)
	}
	if value < 0 || value > 1 {
		return fmt.Errorf("ratio must be between 0 and 1")
	}
	*r = Ratio(value)
	return nil
}

func scalarJSONText(data []byte) (string, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return "", fmt.Errorf("empty JSON value")
	}
	if data[0] == '"' {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return "", err
		}
		return text, nil
	}
	return string(data), nil
}
