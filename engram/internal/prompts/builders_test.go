package prompts

import (
	"strings"
	"testing"
	"time"
)

func TestBuilders_OptionalSectionsAreOmittedWhenEmpty(t *testing.T) {
	p := mustGet(t, "extract/v1")
	in, err := ExtractInputs(p, ExtractInput{Summary: "h", ChunkIndex: 1, ChunkCount: 1, ItemTimestamp: t0,
		Content: "c"})
	if err != nil {
		t.Fatal(err)
	}
	optional := []string{"retain_mission_section", "retain_mission_preamble", "metadata_section",
		"entity_hints_section"}
	for _, k := range optional {
		if v, ok := in[k]; !ok || v != "" {
			t.Errorf("%s = %q, want present and empty", k, v)
		}
	}
	if in["item_timestamp"] != "2026-03-14T09:30:00Z" {
		t.Errorf("item_timestamp = %q", in["item_timestamp"])
	}
}

func TestBuilders_RouteDefaultsAndSaidAt(t *testing.T) {
	p := mustGet(t, "consolidate_route/v1")
	in, err := RouteInputs(p, RouteInput{Facts: []Fact{
		{ID: "a", Text: "earlier", MentionedAt: t0, SaidAt: t1},
		{ID: "b", Text: "same", MentionedAt: t0, SaidAt: t0},
		{ID: "c", Text: "later", MentionedAt: t1, SaidAt: t0}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(in["observations_mission"], "Track anything notable") || in["capacity_note"] != "" {
		t.Errorf("defaults not applied: %q / %q", in["observations_mission"], in["capacity_note"])
	}
	if in["candidates"] != "(none)" {
		t.Errorf("candidates = %q", in["candidates"])
	}
	lines := strings.Split(in["facts"], "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], "said_at=2026-03-02T18:05:00Z") ||
		strings.Contains(lines[1], "said_at") || strings.Contains(lines[2], "said_at") {
		t.Errorf("said_at is rendered only when earlier than mentioned_at:\n%s", in["facts"])
	}
}

func TestBuilders_WriteModeAndPrevious(t *testing.T) {
	p := mustGet(t, "consolidate_write/v1")
	if _, err := WriteInputs(p, WriteInput{Mode: "merge"}); err == nil {
		t.Error("an unknown mode was accepted")
	}
	in, err := WriteInputs(p, WriteInput{Mode: "rebuild", Previous: "must not be shown"})
	if err != nil {
		t.Fatal(err)
	}
	if in["previous"] != "" || in["attached"] != "(none)" {
		t.Errorf("rebuild shows no previous text: %+v", in)
	}
}

func TestBuilders_ReflectDispositionAndDirectives(t *testing.T) {
	p := mustGet(t, "reflect/v1")
	level3 := Disposition{Skepticism: 3, Literalism: 3, Empathy: 3}
	in, err := ReflectInputs(p, ReflectInput{Disposition: level3, Now: t0, MaxIterations: 10})
	if err != nil {
		t.Fatal(err)
	}
	if in["disposition_section"] != "" || in["directives_reminder"] != "" || in["directives"] != "(none)" {
		t.Errorf("level 3 and no directives render nothing: %+v", in)
	}
	in, err = ReflectInputs(p, ReflectInput{Directives: []Directive{{"tone", "Be brief."}},
		Disposition: Disposition{Skepticism: 5, Empathy: 2}, Now: t0, MaxIterations: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(in["disposition_section"], "- Skepticism: Question and doubt claims") ||
		!strings.Contains(in["disposition_section"], "- Empathy: Mention emotional context only when") ||
		strings.Contains(in["disposition_section"], "Literalism") {
		t.Errorf("disposition lines:\n%s", in["disposition_section"])
	}
	if !strings.Contains(in["directives_reminder"], "- [tone] Be brief.") ||
		!strings.Contains(in["directives_reminder"], "REJECTED") {
		t.Errorf("directives reminder:\n%s", in["directives_reminder"])
	}
	if _, err := ReflectInputs(p, ReflectInput{Disposition: Disposition{Skepticism: 6}}); err == nil {
		t.Error("a level outside 1 to 5 was accepted")
	}
}

func TestBuilders_PageEvidenceUsesUTC(t *testing.T) {
	p := mustGet(t, "page_full/v1")
	zone := time.FixedZone("CET", 3600)
	in, err := PageFullInputs(p, PageFullInput{Topic: "t", MaxTokens: 5,
		Evidence: []Evidence{{"e1", "fact", time.Date(2026, 3, 14, 10, 30, 0, 0, zone), "x"}}})
	if err != nil {
		t.Fatal(err)
	}
	if in["evidence"] != "[e1] (fact, 2026-03-14T09:30:00Z) x" {
		t.Errorf("evidence = %q", in["evidence"])
	}
}
