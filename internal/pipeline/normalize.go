// Package pipeline implements the no-LLM post-processing steps of
// docs/01-retrieval.md section 4.6: Unicode NFKC normalization, BM25
// tokenization (stopwords + Snowball stemming), rule-based date extraction,
// and round assembly. Everything here is deterministic and pure.
package pipeline

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Normalize applies Unicode NFKC normalization, lowercases, and collapses
// all runs of whitespace to a single ASCII space (docs/01-retrieval.md
// section 4.6 step 1). The result is what gets stored as normalized_text.
func Normalize(s string) string {
	s = norm.NFKC.String(s)
	s = strings.ToLower(s)
	return collapseWhitespace(s)
}

// collapseWhitespace trims leading/trailing whitespace and squeezes internal
// whitespace runs (including Unicode spaces already NFKC-folded) to one space.
func collapseWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			inSpace = true
			continue
		}
		if inSpace && b.Len() > 0 {
			b.WriteByte(' ')
		}
		inSpace = false
		b.WriteRune(r)
	}
	return b.String()
}

// FormatForEmbedding builds the text handed to the document embedder:
// a human-readable date + speaker prefix injected into the content
// (docs/01-retrieval.md section 4.6 step 3: "[2023-05-30] user: <text>").
// The nomic task prefix is NOT added here — that is owned exclusively by
// internal/embed.
func FormatForEmbedding(ts *time.Time, speaker, text string) string {
	var b strings.Builder
	if ts != nil {
		fmt.Fprintf(&b, "[%s] ", ts.UTC().Format("2006-01-02"))
	}
	if speaker != "" {
		b.WriteString(speaker)
		b.WriteString(": ")
	}
	b.WriteString(text)
	return b.String()
}
