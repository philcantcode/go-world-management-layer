package orchestration

import (
	"fmt"

	"github.com/philcantcode/go-world-management-layer/internal/safepath"
)

func openDurableNamespace(root, logicalDirectory string) (*safepath.Namespace, error) {
	namespace, err := safepath.OpenNamespace(root, logicalDirectory)
	if err != nil {
		return nil, fmt.Errorf("open durable namespace %q: %w", logicalDirectory, err)
	}
	return namespace, nil
}

func cleanupDurableNamespaceStages(namespace *safepath.Namespace) error {
	for _, prefix := range []string{".staging-", ".world-ns-"} {
		if err := namespace.CleanupPrefix(prefix); err != nil {
			return fmt.Errorf("clean abandoned durable stage with prefix %q: %w", prefix, err)
		}
	}
	return nil
}
