package forensicartifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	"github.com/philcantcode/go-world-management-layer/internal/domain"
)

type verifiedContentReader struct {
	mu           sync.Mutex
	ctx          context.Context
	reader       io.ReadCloser
	expected     domain.Digest
	expectedSize int64
	hasher       hash.Hash
	read         int64
	verified     bool
	terminalErr  error
	closed       bool
	closeErr     error
}

func newVerifiedContentReader(ctx context.Context, reader io.ReadCloser, expected domain.Digest, expectedSize int64) *verifiedContentReader {
	return &verifiedContentReader{ctx: ctx, reader: reader, expected: expected, expectedSize: expectedSize, hasher: sha256.New()}
}

func (r *verifiedContentReader) Digest() domain.Digest { return r.expected }
func (r *verifiedContentReader) Size() int64           { return r.expectedSize }

func (r *verifiedContentReader) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readLocked(buffer)
}

func (r *verifiedContentReader) readLocked(buffer []byte) (int, error) {
	if r.closed {
		return 0, domain.NewError(domain.CodeInvalidState, "forensic_artifacts.content.read", "reader", "is closed", nil)
	}
	if r.terminalErr != nil {
		return 0, r.terminalErr
	}
	if r.verified {
		return 0, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		r.terminalErr = contextError("forensic_artifacts.content.read", err)
		return 0, r.terminalErr
	}
	count, readErr := r.reader.Read(buffer)
	if count < 0 || count > len(buffer) {
		r.terminalErr = integrityError("reader returned an invalid byte count")
		return 0, r.terminalErr
	}
	if count > 0 {
		if int64(count) > r.expectedSize-r.read {
			r.terminalErr = integrityError("content exceeds its declared size")
			return 0, r.terminalErr
		}
		_, _ = r.hasher.Write(buffer[:count])
		r.read += int64(count)
	}
	if readErr == io.EOF {
		verificationErr := r.verifyLocked()
		if verificationErr != nil {
			return count, verificationErr
		}
		if count > 0 {
			return count, nil
		}
		return 0, io.EOF
	}
	if readErr != nil {
		r.terminalErr = domain.NewError(domain.CodeUnavailable, "forensic_artifacts.content.read", "stream", "repository stream failed", nil)
		return count, r.terminalErr
	}
	return count, nil
}

func (r *verifiedContentReader) verifyLocked() error {
	if r.read != r.expectedSize {
		r.terminalErr = integrityError("content does not match its declared size")
		return r.terminalErr
	}
	actual, err := domain.ParseDigest("sha256:" + hex.EncodeToString(r.hasher.Sum(nil)))
	if err != nil || actual != r.expected {
		r.terminalErr = integrityError("content does not match its declared digest")
		return r.terminalErr
	}
	r.verified = true
	return nil
}

func (r *verifiedContentReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	if r.terminalErr == nil && !r.verified {
		buffer := make([]byte, 32*1024)
		noProgress := 0
		for !r.verified && r.terminalErr == nil {
			count, err := r.readLocked(buffer)
			if count == 0 && err == nil {
				noProgress++
				if noProgress >= 100 {
					r.terminalErr = domain.NewError(domain.CodeUnavailable, "forensic_artifacts.content.close", "stream", "repository stream made no progress", nil)
				}
			} else {
				noProgress = 0
			}
			if err != nil && !errors.Is(err, io.EOF) {
				break
			}
		}
	}
	rawCloseErr := r.reader.Close()
	r.closed = true
	if r.terminalErr != nil {
		r.closeErr = r.terminalErr
	} else if rawCloseErr != nil {
		r.closeErr = domain.NewError(domain.CodeUnavailable, "forensic_artifacts.content.close", "stream", "repository stream could not be closed", nil)
	}
	return r.closeErr
}

func integrityError(message string) error {
	return domain.NewError(domain.CodeIntegrityViolation, "forensic_artifacts.content.verify", "content", message, nil)
}

func contextError(operation string, err error) error {
	code := domain.CodeUnavailable
	if errors.Is(err, context.DeadlineExceeded) {
		code = domain.CodeDeadlineExceeded
	}
	return domain.NewError(code, operation, "context", "content operation was interrupted", nil)
}
