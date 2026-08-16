package embed

import (
	"errors"
	"fmt"
)

// PermanentError marks a failure that retrying can never fix (e.g. the
// embedder returned the wrong number of dimensions, or a caller tried to
// double-prefix). Enrichment must dead-letter these (insert a failure event
// with permanent=true) instead of retrying (docs/02-storage.md E.7 item 7).
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanentf builds a PermanentError from a format string.
func Permanentf(format string, args ...any) *PermanentError {
	return &PermanentError{Err: fmt.Errorf(format, args...)}
}

// IsPermanent reports whether err (anywhere in its chain) is a
// PermanentError, i.e. must be dead-lettered rather than retried.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
