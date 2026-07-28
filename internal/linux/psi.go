package linux

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/internal/admission"
)

type PSILine struct {
	Average10   float64
	Average60   float64
	Average300  float64
	TotalMicros uint64
}

type PSISample struct {
	Some    PSILine
	Full    PSILine
	HasFull bool
}

func ParsePSI(text string) (PSISample, error) {
	var sample PSISample
	seenSome := false
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 5 || (fields[0] != "some" && fields[0] != "full") {
			return PSISample{}, fmt.Errorf("malformed PSI line")
		}
		line := PSILine{}
		for _, field := range fields[1:] {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) != 2 {
				return PSISample{}, fmt.Errorf("malformed PSI field")
			}
			var err error
			switch parts[0] {
			case "avg10":
				line.Average10, err = strconv.ParseFloat(parts[1], 64)
			case "avg60":
				line.Average60, err = strconv.ParseFloat(parts[1], 64)
			case "avg300":
				line.Average300, err = strconv.ParseFloat(parts[1], 64)
			case "total":
				line.TotalMicros, err = strconv.ParseUint(parts[1], 10, 64)
			default:
				return PSISample{}, fmt.Errorf("unknown PSI field %q", parts[0])
			}
			if err != nil {
				return PSISample{}, fmt.Errorf("parse PSI %s: %w", parts[0], err)
			}
		}
		if line.Average10 < 0 || line.Average10 > 100 || line.Average60 < 0 || line.Average60 > 100 || line.Average300 < 0 || line.Average300 > 100 {
			return PSISample{}, fmt.Errorf("PSI average is outside 0..100")
		}
		if fields[0] == "some" {
			if seenSome {
				return PSISample{}, fmt.Errorf("duplicate PSI some line")
			}
			sample.Some = line
			seenSome = true
		} else {
			if sample.HasFull {
				return PSISample{}, fmt.Errorf("duplicate PSI full line")
			}
			sample.Full = line
			sample.HasFull = true
		}
	}
	if err := scanner.Err(); err != nil {
		return PSISample{}, err
	}
	if !seenSome {
		return PSISample{}, fmt.Errorf("PSI some line is required")
	}
	return sample, nil
}

func (s PSISample) AdmissionFull(previous *PSISample) admission.PSI {
	current := 0.0
	if s.HasFull {
		current = s.Full.Average10 / 100
	}
	trend := 0.0
	if previous != nil && previous.HasFull {
		trend = current - previous.Full.Average10/100
	}
	return admission.PSI{Current: current, Trend: trend}
}
