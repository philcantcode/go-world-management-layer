package worldcli

import (
	"fmt"
	"io"
	"text/tabwriter"

	worldv1 "github.com/philcantcode/go-world-management-layer/api/world/v1"
)

// WriteTop renders one live snapshot as a stable, script-friendly table. It is
// intentionally one-shot; callers control polling and terminal behavior.
func WriteTop(output io.Writer, snapshot *worldv1.LiveSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("live snapshot is nil")
	}
	writer := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(writer, "CURSOR\t%d\n", snapshot.Cursor); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "SUBJECT\tSIGNAL\tMETRIC\tSTATE\tVALUE\tAGE\tDETAIL"); err != nil {
		return err
	}
	for index, metric := range snapshot.Metrics {
		if metric == nil {
			return fmt.Errorf("live snapshot metric %d is nil", index)
		}
		value := "-"
		if metric.Value != nil {
			value = fmt.Sprintf("%g", *metric.Value)
		}
		age, err := formatSampleAge(metric)
		if err != nil {
			return fmt.Errorf("live snapshot metric %d: %w", index, err)
		}
		if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", metric.SubjectId, metric.SignalFamily, metric.Name, metric.State, value, age, metric.Detail); err != nil {
			return err
		}
	}
	if len(snapshot.Coverage) > 0 {
		if _, err := fmt.Fprintln(writer, "\nCOLLECTOR\tSUBJECT\tSIGNAL\tLEVEL\tSTATUS\tDROPPED"); err != nil {
			return err
		}
		for index, coverage := range snapshot.Coverage {
			if coverage == nil {
				return fmt.Errorf("live snapshot coverage %d is nil", index)
			}
			if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%d\n", coverage.CollectorId, coverage.SubjectId, coverage.SignalFamily, coverage.Level, coverage.Status, coverage.DroppedRecords); err != nil {
				return err
			}
		}
	}
	if len(snapshot.Incidents) > 0 {
		if _, err := fmt.Fprintln(writer, "\nINCIDENT\tSUBJECT\tSIGNAL\tSTATE\tSUMMARY"); err != nil {
			return err
		}
		for index, incident := range snapshot.Incidents {
			if incident == nil {
				return fmt.Errorf("live snapshot incident %d is nil", index)
			}
			if _, err := fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\n", incident.IncidentId, incident.SubjectId, incident.SignalFamily, incident.State, incident.Summary); err != nil {
				return err
			}
		}
	}
	return writer.Flush()
}

func formatSampleAge(metric *worldv1.MetricSample) (string, error) {
	if metric.SampleAge == nil {
		return "-", nil
	}
	if err := metric.SampleAge.CheckValid(); err != nil {
		return "", fmt.Errorf("sample age: %w", err)
	}
	return metric.SampleAge.AsDuration().String(), nil
}
