package embed

import (
	"context"
	"errors"
	"testing"
)

// TestWrongDimsIsPermanent: a 512-dim response (e.g. MOCK_BAD_DIMS_RATE in
// the mock embedder) must surface as a typed PermanentError so enrichment
// dead-letters it instead of retrying forever.
func TestWrongDimsIsPermanent(t *testing.T) {
	ctx := context.Background()
	mock := &recordingEmbedder{dims: 512}
	c := NewClient(mock, nil)

	_, err := c.EmbedDocument(ctx, "some text")
	if err == nil {
		t.Fatal("want error for 512-dim response")
	}
	if !IsPermanent(err) {
		t.Fatalf("512-dim error not permanent: %v", err)
	}
	var pe *PermanentError
	if !errors.As(err, &pe) {
		t.Fatalf("error not a *PermanentError: %T", err)
	}
}

// TestTransientErrorIsNotPermanent: ordinary transport failures must remain
// retryable (not dead-lettered).
func TestTransientErrorIsNotPermanent(t *testing.T) {
	if IsPermanent(errors.New("rpc error: code = Unavailable")) {
		t.Fatal("plain error misclassified as permanent")
	}
}
