//go:build aix

package processlock

import (
	"errors"
	"os"
)

var errAIXStableNamespaceUnsupported = errors.New("stable process-lock namespace is unsupported on AIX")

func tryAcquire(_, _ string) (*os.File, *os.File, error) {
	return nil, nil, errAIXStableNamespaceUnsupported
}

func unlockFiles(file, namespaceFile *os.File) error {
	var fileErr, namespaceErr error
	if file != nil {
		fileErr = file.Close()
	}
	if namespaceFile != nil {
		namespaceErr = namespaceFile.Close()
	}
	return errors.Join(fileErr, namespaceErr)
}
