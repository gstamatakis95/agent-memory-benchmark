package pipeline

import (
	"strings"
	"unicode"

	"github.com/kljensen/snowball/english"
)

// Tokenize produces the BM25 lexemes for a text (docs/01-retrieval.md
// section 4.6 step 2): NFKC-normalize + lowercase, split on any
// non-letter/non-digit rune (strips punctuation), drop English stopwords,
// then Snowball-stem what remains.
func Tokenize(s string) []string {
	s = Normalize(s)
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	out := make([]string, 0, len(fields))
	for _, w := range fields {
		if stopwords[w] {
			continue
		}
		stemmed := english.Stem(w, false)
		if stemmed == "" {
			continue
		}
		out = append(out, stemmed)
	}
	return out
}
