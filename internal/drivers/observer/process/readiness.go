package process

import (
	"context"
	"fmt"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/drivers/command"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

type CommandReadiness struct {
	Runner         command.Runner
	Program        string
	Args           []string
	Interval       time.Duration
	RuntimeBinding RuntimeBinding
}

// Await polls a trusted adapter-specific command. The probe receives the same
// immutable run scope as the collector and must return success only for that
// exact collector identity.
func (r CommandReadiness) Await(ctx context.Context, plan ports.CollectorPlan) error {
	if r.Runner == nil {
		r.Runner = command.OS{}
	}
	if r.Program == "" {
		return fmt.Errorf("readiness program is required")
	}
	if r.Interval <= 0 {
		r.Interval = 100 * time.Millisecond
	}
	arguments, err := bindRuntimeArguments(r.RuntimeBinding, r.Args, plan)
	if err != nil {
		return err
	}
	invocation := command.Invocation{Program: r.Program, Args: arguments, Environment: observerEnvironment(nil, plan), MaximumOutput: 64 << 10}
	for {
		if _, err := r.Runner.Run(ctx, invocation); err == nil {
			return nil
		}
		timer := time.NewTimer(r.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

var _ Readiness = CommandReadiness{}
