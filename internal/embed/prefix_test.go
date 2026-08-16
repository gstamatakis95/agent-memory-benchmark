package embed

import (
	"context"
	"strings"
	"testing"
)

// recordingEmbedder is the recording fake from docs/06-testing.md Tier 0:
// it captures every text sent to the (unary) transport.
type recordingEmbedder struct {
	texts []string
	dims  int // 0 means Dims
}

func (r *recordingEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	r.texts = append(r.texts, text)
	d := r.dims
	if d == 0 {
		d = Dims
	}
	v := make([]float32, d)
	if d > 0 {
		v[0] = 2 // deliberately unnormalized
	}
	return v, nil
}

func (r *recordingEmbedder) last() string { return r.texts[len(r.texts)-1] }

// TestEmbedTextsAlwaysPrefixed is the nomic prefix guard (docs/06-testing.md
// Tier 0): the document path always sends "search_document: ", the query
// path "search_query: ", each exactly once.
func TestEmbedTextsAlwaysPrefixed(t *testing.T) {
	ctx := context.Background()
	mock := &recordingEmbedder{}
	c := NewClient(mock, nil)

	if _, err := c.EmbedDocument(ctx, "I visited Paris in May."); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(mock.last(), DocumentPrefix) {
		t.Fatalf("document embedded without %q prefix: %q", DocumentPrefix, mock.last())
	}
	if strings.Count(mock.last(), DocumentPrefix) != 1 {
		t.Fatalf("double document prefix: %q", mock.last())
	}
	if strings.Contains(mock.last(), QueryPrefix) {
		t.Fatalf("document text carries query prefix: %q", mock.last())
	}

	if _, err := c.EmbedQuery(ctx, "when did I go to Paris?"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(mock.last(), QueryPrefix) {
		t.Fatalf("query embedded without %q prefix: %q", QueryPrefix, mock.last())
	}
	if strings.Count(mock.last(), QueryPrefix) != 1 {
		t.Fatalf("double query prefix: %q", mock.last())
	}
	if strings.Contains(mock.last(), DocumentPrefix) {
		t.Fatalf("query text carries document prefix: %q", mock.last())
	}
}

// TestDoublePrefixGuard: handing the client pre-prefixed text is refused as
// a permanent error on both paths, for both prefixes.
func TestDoublePrefixGuard(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&recordingEmbedder{}, nil)
	for _, raw := range []string{
		DocumentPrefix + "already prefixed",
		QueryPrefix + "already prefixed",
	} {
		if _, err := c.EmbedDocument(ctx, raw); err == nil || !IsPermanent(err) {
			t.Fatalf("EmbedDocument(%q): want permanent error, got %v", raw, err)
		}
		if _, err := c.EmbedQuery(ctx, raw); err == nil || !IsPermanent(err) {
			t.Fatalf("EmbedQuery(%q): want permanent error, got %v", raw, err)
		}
	}
}

// TestClientNormalizesVectors: every vector handed back by the client is
// L2-normalized even when the transport is not.
func TestClientNormalizesVectors(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&recordingEmbedder{}, nil)
	vec, err := c.EmbedDocument(ctx, "some text")
	if err != nil {
		t.Fatal(err)
	}
	assertUnitNorm(t, vec)
}
