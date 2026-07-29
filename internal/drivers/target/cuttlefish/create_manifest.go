package cuttlefish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/philcantcode/go-world-management-layer/internal/atomicfile"
	"github.com/philcantcode/go-world-management-layer/internal/domain"
	"github.com/philcantcode/go-world-management-layer/internal/ports"
)

const createIntentManifestFilename = "world-create-intent.json"

type createIntentManifest struct {
	Version          int                     `json:"version"`
	TargetPlanDigest string                  `json:"target_plan_digest"`
	RequestDigest    string                  `json:"request_digest"`
	TargetID         domain.TargetID         `json:"target_id"`
	Generation       domain.TargetGeneration `json:"generation"`
	RuntimeID        string                  `json:"runtime_id"`
	Allocation       Allocation              `json:"allocation"`
	PersistedAt      time.Time               `json:"persisted_at"`
}

func commitExpectedCreateIntent(input ports.TargetPlan, plan VirtualDevicePlan, at time.Time) (createIntentManifest, error) {
	expected, err := expectedCreateIntent(input, plan, at)
	if err != nil {
		return createIntentManifest{}, err
	}
	if existing, found, err := loadExpectedCreateIntent(input, plan); err != nil {
		return createIntentManifest{}, err
	} else if found {
		return existing, nil
	}
	if err := os.MkdirAll(plan.StateDirectory, 0o700); err != nil {
		return createIntentManifest{}, err
	}
	encoded, err := json.Marshal(expected)
	if err != nil {
		return createIntentManifest{}, err
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(plan.StateDirectory, createIntentManifestFilename)
	if err := atomicfile.WriteExclusive(path, encoded, 0o600); err != nil {
		if existing, found, loadErr := loadExpectedCreateIntent(input, plan); loadErr == nil && found {
			return existing, nil
		}
		return createIntentManifest{}, fmt.Errorf("commit immutable Android create intent: %w", err)
	}
	return expected, nil
}

func expectedCreateIntent(input ports.TargetPlan, plan VirtualDevicePlan, at time.Time) (createIntentManifest, error) {
	if err := input.Validate(); err != nil {
		return createIntentManifest{}, err
	}
	requestDigest, err := targetPlanSignature(input)
	if err != nil {
		return createIntentManifest{}, err
	}
	planDigest, err := virtualDevicePlanDigest(plan)
	if err != nil {
		return createIntentManifest{}, err
	}
	spec := input.Generation.Spec()
	instance := instanceFromPlan(plan)
	if spec.TargetID != plan.TargetID || spec.Generation != plan.Generation || input.LeaseID != plan.LeaseID || !instanceMatchesPlan(instance, plan) || at.IsZero() {
		return createIntentManifest{}, fmt.Errorf("Android create intent does not bind the exact target and runtime plan")
	}
	return createIntentManifest{
		Version: manifestVersion, TargetPlanDigest: planDigest.String(), RequestDigest: requestDigest.String(),
		TargetID: plan.TargetID, Generation: plan.Generation, RuntimeID: instance.RuntimeID,
		Allocation: plan.Allocation, PersistedAt: at.UTC(),
	}, nil
}

func loadExpectedCreateIntent(input ports.TargetPlan, plan VirtualDevicePlan) (createIntentManifest, bool, error) {
	path := filepath.Join(plan.StateDirectory, createIntentManifestFilename)
	var manifest createIntentManifest
	if err := readStrictManifest(path, &manifest); errors.Is(err, os.ErrNotExist) {
		return createIntentManifest{}, false, nil
	} else if err != nil {
		return createIntentManifest{}, false, err
	}
	requestDigest, requestErr := targetPlanSignature(input)
	planDigest, planErr := virtualDevicePlanDigest(plan)
	instance := instanceFromPlan(plan)
	if requestErr != nil || planErr != nil || manifest.Version != manifestVersion || manifest.PersistedAt.IsZero() ||
		manifest.RequestDigest != requestDigest.String() || manifest.TargetPlanDigest != planDigest.String() ||
		manifest.TargetID != plan.TargetID || manifest.Generation != plan.Generation || manifest.RuntimeID != instance.RuntimeID || manifest.Allocation != plan.Allocation {
		return createIntentManifest{}, false, fmt.Errorf("Android create intent differs from the exact expected request and runtime plan")
	}
	return manifest, true, nil
}

func (d *Driver) resumeExpectedCreateIntents(ctx context.Context, expected []expectedAndroidTarget, inventory map[string]struct{}) (bool, error) {
	mutated := false
	for _, item := range expected {
		targetPath := filepath.Join(item.directory, targetPlanManifestFilename)
		targetExists, err := strictManifestExists(targetPath)
		if err != nil {
			return mutated, err
		}
		runtimeExists, err := strictManifestExists(filepath.Join(item.directory, runtimePlanManifestFilename))
		if err != nil {
			return mutated, err
		}
		intentExists, err := strictManifestExists(filepath.Join(item.directory, createIntentManifestFilename))
		if err != nil {
			return mutated, err
		}
		if targetExists && runtimeExists {
			if intentExists {
				target, runtime, err := loadTargetRuntimeManifests(item.directory)
				if err != nil {
					return mutated, err
				}
				plan, err := BuildVirtualDevicePlan(item.input, d.build, target.Plan.Allocation)
				if err != nil || validateExpectedManifests(plan, target, runtime) != nil {
					return mutated, fmt.Errorf("manifested Android create intent differs from the expected target plan")
				}
				if _, _, err := loadExpectedCreateIntent(item.input, plan); err != nil {
					return mutated, err
				}
			}
			continue
		}
		if _, err := os.Lstat(filepath.Join(item.directory, resetTransitionManifestFilename)); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return mutated, err
		}
		allocation, found, err := d.lookupExpectedAllocation(ctx, item.ref)
		if err != nil {
			return mutated, err
		}
		if !found {
			continue
		}
		plan, err := BuildVirtualDevicePlan(item.input, d.build, allocation)
		if err != nil {
			return mutated, err
		}
		if !intentExists {
			if _, live := inventory[instanceFromPlan(plan).RuntimeID]; live || targetExists || runtimeExists {
				// An allocation alone is sufficient to resume a crash before
				// intent commit. A live or partially manifested runtime is not:
				// only the immutable create intent authorizes its adoption.
				continue
			}
		}
		if _, err := commitExpectedCreateIntent(item.input, plan, d.now()); err != nil {
			return mutated, err
		}
		instance, _, _, err := d.ensureManifestedAndroidInstance(ctx, plan, inventory)
		if err != nil {
			return mutated, err
		}
		inventory[instance.RuntimeID] = struct{}{}
		mutated = true
	}
	return mutated, nil
}
