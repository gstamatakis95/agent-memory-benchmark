package prompts

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"
)

func baseExtract() ExtractInput {
	return ExtractInput{
		Summary: "Planning call summary", HeadingPath: "Planning > Risks",
		ChunkIndex: 2, ChunkCount: 5,
		ItemTimestamp: time.Date(2026, 3, 14, 9, 30, 0, 0, time.UTC), Context: "weekly sync",
		Metadata:    map[string]any{"source": "slack", "channel": "ops", "attempt": 2},
		EntityHints: []EntityHint{{"Emily", "person"}, {"Acme", "organization"}},
		Mission:     "Track commitments.", Content: "We ship on Friday.",
	}
}

func meta(source, channel string, attempt any) map[string]any {
	return map[string]any{"source": source, "channel": channel, "attempt": attempt}
}

func baseKey() (KeyInput, ExtractInput) {
	in := baseExtract()
	return KeyInput{ChunkHash: sha256.Sum256([]byte("chunk text")), PromptVersion: "extract/v1",
		Model: "fast-structured-1", SchemaVersion: 1, RenderHash: RenderHash(in)}, in
}

// TestExtraction_RenderHashKey is the T1 part of the N87 test of section 8.4: the extraction cache key is
// sha256(chunk_hash ‖ prompt_version ‖ model ‖ schema_version ‖ render_hash). It changes when any one of the five
// inputs changes, render_hash changes when any one of its components (the day of mentioned_at, context, metadata, the
// entity hints and their types, the mission, the heading path) changes, and neither changes with anything else: the
// time of day, the order of the hints or of the metadata keys, the chunk text (chunk_hash covers it) and the document
// summary. The last point is the register's reading (N87, N110(a): a summary refresh re-embeds the chunk without
// re-extracting); section 6.0 and the section 8.4 row say the summary is an input, which CONFLICTS.md #32 records.
func TestExtraction_RenderHashKey(t *testing.T) {
	k, in := baseKey()
	base := ExtractionKey(k)
	if ExtractionKey(k) != base {
		t.Fatal("the key is not deterministic")
	}

	// the five inputs of the key
	for name, mut := range map[string]func(*KeyInput){
		"chunk_hash":     func(k *KeyInput) { k.ChunkHash[0] ^= 1 },
		"prompt_version": func(k *KeyInput) { k.PromptVersion = "extract/v2" },
		"model":          func(k *KeyInput) { k.Model = "fast-structured-2" },
		"schema_version": func(k *KeyInput) { k.SchemaVersion = 2 },
		"render_hash":    func(k *KeyInput) { k.RenderHash[0] ^= 1 },
	} {
		m := k
		mut(&m)
		if ExtractionKey(m) == base {
			t.Errorf("changing %s does not change the key", name)
		}
	}

	// the components of render_hash: a changed mission, hints, timestamp day, context, metadata or heading path is a
	// new key
	rbase := RenderHash(in)
	for name, mut := range map[string]func(*ExtractInput){
		"day of mentioned_at": func(r *ExtractInput) { r.ItemTimestamp = r.ItemTimestamp.AddDate(0, 0, 1) },
		"context":             func(r *ExtractInput) { r.Context = "monthly sync" },
		"metadata value":      func(r *ExtractInput) { r.Metadata = meta("email", "ops", 2) },
		"metadata type": func(r *ExtractInput) {
			r.Metadata = meta("slack", "ops", "2")
		},
		"metadata key": func(r *ExtractInput) {
			r.Metadata = map[string]any{"origin": "slack", "channel": "ops", "attempt": 2}
		},
		"entity hint name": func(r *ExtractInput) { r.EntityHints = []EntityHint{{"Emily", "person"}} },
		"entity hint type": func(r *ExtractInput) {
			r.EntityHints = []EntityHint{{"Emily", "organization"}, {"Acme", "organization"}}
		},
		"mission":      func(r *ExtractInput) { r.Mission = "Track decisions." },
		"heading path": func(r *ExtractInput) { r.HeadingPath = "Planning > Costs" },
	} {
		m := in
		mut(&m)
		if RenderHash(m) == rbase {
			t.Errorf("changing %s does not change render_hash", name)
		}
		k2 := k
		k2.RenderHash = RenderHash(m)
		if ExtractionKey(k2) == base {
			t.Errorf("changing %s does not change the extraction key", name)
		}
	}

	// identical rendered variables hit: nothing else is an input
	same := in
	same.ItemTimestamp = time.Date(2026, 3, 14, 23, 59, 59, 0, time.UTC) // the same UTC day, another time
	same.EntityHints = []EntityHint{{"Acme", "organization"}, {"Emily", "person"}}
	same.Metadata = map[string]any{"attempt": 2, "channel": "ops", "source": "slack"}
	if RenderHash(same) != rbase {
		t.Error("the time of day, the order of the hints or of the metadata keys changed render_hash")
	}
	sameZone := in
	sameZone.ItemTimestamp = time.Date(2026, 3, 14, 11, 30, 0, 0, time.FixedZone("CET", 3600)) // 10:30 UTC, same day
	if RenderHash(sameZone) != rbase {
		t.Error("the day is taken in UTC")
	}

	// N87 / N110(a): a refreshed document summary changes the header the prompt shows but not the key, so the append
	// re-embeds the chunk without re-extracting it; a changed heading path is a new key
	summary := in
	summary.Summary = "A refreshed, longer summary of the planning call"
	if RenderHash(summary) != rbase {
		t.Error("a changed document summary changed render_hash (N87, N110(a): it must not)")
	}
	content := in
	content.Content = "We ship on Monday."
	if RenderHash(content) != rbase {
		t.Error("the chunk text is covered by chunk_hash, not by render_hash")
	}

	// fields are length-prefixed: moving a character between neighbours is a different key
	a, b := in, in
	a.Context, a.Mission = "ab", "c"
	b.Context, b.Mission = "a", "bc"
	if RenderHash(a) == RenderHash(b) {
		t.Error("render_hash fields are ambiguous")
	}
	c, d := k, k
	c.PromptVersion, c.Model = "extract/v1x", "m"
	d.PromptVersion, d.Model = "extract/v1", "xm"
	if ExtractionKey(c) == ExtractionKey(d) {
		t.Error("key fields are ambiguous")
	}
}

// The prompt id in the key is bound to the prompt content by the pin: the id of every released version appears in the
// key as the string of VERSION.
func TestExtraction_KeyUsesThePromptID(t *testing.T) {
	p := mustGet(t, "extract/v1")
	k, _ := baseKey()
	k.PromptVersion = p.Meta.ID
	if p.Meta.ID != "extract/v1" || p.Meta.SchemaVersion != k.SchemaVersion {
		t.Errorf("the fixture key does not match the released prompt: %+v", p.Meta)
	}
}

// The heading path the prompt shows is the one RenderHash covers: the header is built from Summary and HeadingPath, so
// there is no second field to drift. A refreshed summary changes the header and not the key; a new path changes both.
func TestExtraction_HeaderAndHashShareTheHeadingPath(t *testing.T) {
	p := mustGet(t, "extract/v1")
	render := func(in ExtractInput) string {
		vars, err := ExtractInputs(p, in)
		if err != nil {
			t.Fatal(err)
		}
		return vars["header"]
	}
	in := baseExtract()
	if got, want := render(in), "Planning call summary > Planning > Risks"; got != want {
		t.Errorf("header = %q, want %q", got, want)
	}
	fresh := in
	fresh.Summary = "A refreshed summary"
	if render(fresh) == render(in) || RenderHash(fresh) != RenderHash(in) {
		t.Error("a new summary must change the header and keep the key")
	}
	moved := in
	moved.HeadingPath = "Planning > Costs"
	if !strings.HasSuffix(render(moved), moved.HeadingPath) || RenderHash(moved) == RenderHash(in) {
		t.Error("a new heading path must be shown and must change the key")
	}
	if (ExtractInput{HeadingPath: "A"}).Header() != "A" || (ExtractInput{Summary: "S"}).Header() != "S" {
		t.Error("an empty part is left out of the header")
	}
}
