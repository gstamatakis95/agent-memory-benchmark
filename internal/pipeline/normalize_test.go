package pipeline

import (
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"nfkc ligature", "ﬁsh ﬂight", "fish flight"},
		{"nfkc fullwidth", "Ｈｅｌｌｏ　Ｗｏｒｌｄ", "hello world"},
		{"nfkc combining accent composed", "café", "café"},
		{"lowercase", "MiXeD CaSe", "mixed case"},
		{"whitespace collapse", "a  \t b\n\n c", "a b c"},
		{"trim", "  padded  ", "padded"},
		{"nbsp and unicode spaces", "a b c", "a b c"},
		{"empty", "", ""},
		{"only space", " \t\n", ""},
		{"nfkc superscript", "x²", "x2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Normalize(c.in); got != c.want {
				t.Fatalf("Normalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestFormatForEmbedding(t *testing.T) {
	ts := time.Date(2023, 5, 30, 14, 0, 0, 0, time.UTC)
	got := FormatForEmbedding(&ts, "user", "I went to Paris")
	want := "[2023-05-30] user: I went to Paris"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := FormatForEmbedding(nil, "speaker_a", "hi"); got != "speaker_a: hi" {
		t.Fatalf("nil ts: got %q", got)
	}
	if got := FormatForEmbedding(nil, "", "bare"); got != "bare" {
		t.Fatalf("bare: got %q", got)
	}
	// The date must be rendered in UTC regardless of the location on ts.
	off := time.Date(2023, 5, 31, 1, 0, 0, 0, time.FixedZone("plus3", 3*3600)) // 2023-05-30 22:00 UTC
	if got := FormatForEmbedding(&off, "u", "x"); got != "[2023-05-30] u: x" {
		t.Fatalf("utc render: got %q", got)
	}
}
