package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/philcantcode/go-world-management-layer/cmd/internal/worldcli"
)

type mutationOptions = worldcli.MutationFlags

func addMutationFlags(flags *flag.FlagSet) *mutationOptions {
	return worldcli.AddMutationFlags(flags, os.Getenv("WORLD_POLICY_REFERENCE"))
}

func defaultEnv(name string) string { return strings.TrimSpace(os.Getenv(name)) }

type optionalInt32 struct {
	set   bool
	value int32
}

func (value *optionalInt32) String() string {
	if !value.set {
		return ""
	}
	return strconv.FormatInt(int64(value.value), 10)
}

func (value *optionalInt32) Set(text string) error {
	parsed, err := strconv.ParseInt(text, 10, 32)
	if err != nil {
		return fmt.Errorf("invalid int32 %q: %w", text, err)
	}
	value.value, value.set = int32(parsed), true
	return nil
}

func (value *optionalInt32) pointer() *int32 {
	if !value.set {
		return nil
	}
	result := value.value
	return &result
}
