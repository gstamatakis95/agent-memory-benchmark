package prompts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The typed input builders turn the structured inputs of section 6 into the Inputs of Render, using the line formats
// and default texts of the released snippets.json (so a format change is a new prompt version). They hold no state and
// do not validate content: the output rules of section 6 are applied after decoding, by the activity that owns the
// call. Times are rendered as RFC 3339 in UTC.

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// fill replaces {key} in a snippet by the values of kv, one pass.
func fill(tpl string, kv map[string]string) string {
	return placeholder.ReplaceAllStringFunc(tpl, func(m string) string {
		if v, ok := kv[m[1:len(m)-1]]; ok {
			return v
		}
		return m
	})
}

// put stores the snippet key, filled with kv, under out[name].
func (p *Prompt) put(out Inputs, name, key string, kv map[string]string) error {
	v, err := p.line(key, kv)
	if err != nil {
		return err
	}
	out[name] = v
	return nil
}

func (p *Prompt) line(key string, kv map[string]string) (string, error) {
	s, err := p.Snippet(key)
	if err != nil {
		return "", err
	}
	return fill(s, kv), nil
}

// ---- summarize/v1 ----------------------------------------------------------------------------------------------

// SummarizeInput is the input of summarize/v1 (section 6.1).
type SummarizeInput struct {
	Title   string   // optional
	Outline []string // heading paths, at most 50
	Head    string   // first 6 000 characters
	Tail    string   // last 1 000 characters
}

// SummarizeInputs builds the Inputs of summarize/v1.
func SummarizeInputs(in SummarizeInput) Inputs {
	return Inputs{"title": in.Title, "outline": strings.Join(in.Outline, "\n"), "head": in.Head, "tail": in.Tail}
}

// ---- extract/v1 ------------------------------------------------------------------------------------------------

// EntityHint is a caller-supplied entity with its type.
type EntityHint struct{ Name, Type string }

// ExtractInput is the input of extract/v1 (section 6.2).
type ExtractInput struct {
	// Summary and HeadingPath make the chunk header the prompt shows ("summary > heading path", Header). RenderHash
	// covers the heading path only (a summary refresh is not a new key, N110(a)); because the header is built from
	// these two fields, the path that is hashed is the path that is shown.
	Summary       string
	HeadingPath   string
	ChunkIndex    int
	ChunkCount    int
	ItemTimestamp time.Time      // the chunk's mentioned_at (N86)
	Context       string         // item context, at most 500 characters
	Metadata      map[string]any // rendered as key: value; a value that is not a string is rendered as JSON
	EntityHints   []EntityHint
	Mission       string // retain.mission, optional
	Content       string
}

// Header is the chunk header of the prompt: the document summary and the heading path, joined by " > ".
func (in ExtractInput) Header() string {
	parts := make([]string, 0, 2)
	for _, p := range []string{in.Summary, in.HeadingPath} {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, " > ")
}

// ExtractInputs builds the Inputs of extract/v1 from the snippets of p, which must be an extract prompt.
func ExtractInputs(p *Prompt, in ExtractInput) (Inputs, error) {
	out := Inputs{
		"chunk_index": strconv.Itoa(in.ChunkIndex), "chunk_count": strconv.Itoa(in.ChunkCount),
		"item_timestamp": ts(in.ItemTimestamp), "context": in.Context, "header": in.Header(), "content": in.Content,
		"retain_mission_section": "", "retain_mission_preamble": "", "metadata_section": "", "entity_hints_section": "",
	}
	var err error
	if in.Mission != "" {
		kv := map[string]string{"mission": in.Mission}
		for _, k := range []string{"retain_mission_section", "retain_mission_preamble"} {
			if err = p.put(out, k, k, kv); err != nil {
				return nil, err
			}
		}
	}
	if len(in.Metadata) > 0 {
		keys := make([]string, 0, len(in.Metadata))
		for k := range in.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		lines := make([]string, len(keys))
		for i, k := range keys {
			kv := map[string]string{"key": k, "value": metaValue(in.Metadata[k])}
			if lines[i], err = p.line("metadata_line", kv); err != nil {
				return nil, err
			}
		}
		kv := map[string]string{"lines": strings.Join(lines, "\n")}
		if err = p.put(out, "metadata_section", "metadata_section", kv); err != nil {
			return nil, err
		}
	}
	if len(in.EntityHints) > 0 {
		lines := make([]string, len(in.EntityHints))
		for i, h := range in.EntityHints {
			kv := map[string]string{"name": h.Name, "type": h.Type}
			if lines[i], err = p.line("entity_hint_line", kv); err != nil {
				return nil, err
			}
		}
		kv := map[string]string{"lines": strings.Join(lines, "\n")}
		if err = p.put(out, "entity_hints_section", "entity_hints_section", kv); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// metaValue renders a metadata value: strings as they are, everything else as JSON.
func metaValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// ---- consolidate_route/v1 and consolidate_write/v1 -------------------------------------------------------------

// Fact is a fact as the consolidation prompts render it.
type Fact struct {
	ID          string
	Text        string
	MentionedAt time.Time
	SaidAt      time.Time // zero, or equal to MentionedAt, or later: not rendered; rendered only when earlier
}

// Source is a quoted source of an observation.
type Source struct{ FactID, Quote string }

// Candidate is a candidate observation of the routing prompt.
type Candidate struct {
	ID      string
	Text    string
	Sources []Source // at most five
}

// RouteInput is the input of consolidate_route/v1 (section 6.3.1).
type RouteInput struct {
	Mission    string // consolidate.mission; empty selects the default text of the prompt
	Facts      []Fact
	Candidates []Candidate
	Capacity   bool // the prompt_variant 'capacity' of N121: fills {capacity_note}
}

func (f Fact) saidAt(p *Prompt) (string, error) {
	if f.SaidAt.IsZero() || !f.SaidAt.Before(f.MentionedAt) {
		return "", nil
	}
	return p.line("said_at", map[string]string{"said_at": ts(f.SaidAt)})
}

// RouteInputs builds the Inputs of consolidate_route/v1.
func RouteInputs(p *Prompt, in RouteInput) (Inputs, error) {
	out := Inputs{"observations_mission": in.Mission, "capacity_note": "", "facts": "", "candidates": ""}
	var err error
	if in.Mission == "" {
		if out["observations_mission"], err = p.Snippet("default_mission"); err != nil {
			return nil, err
		}
	}
	if in.Capacity {
		if out["capacity_note"], err = p.Snippet("capacity_note"); err != nil {
			return nil, err
		}
	}
	lines := make([]string, len(in.Facts))
	for i, f := range in.Facts {
		said, err := f.saidAt(p)
		if err != nil {
			return nil, err
		}
		kv := map[string]string{"n": strconv.Itoa(i + 1), "id": f.ID, "mentioned_at": ts(f.MentionedAt),
			"said_at": said, "text": f.Text}
		if lines[i], err = p.line("fact_line", kv); err != nil {
			return nil, err
		}
	}
	if out["facts"], err = orNone(p, lines); err != nil {
		return nil, err
	}
	var cand []string
	for i, c := range in.Candidates {
		l, err := p.line("candidate_line", map[string]string{"n": strconv.Itoa(i + 1), "id": c.ID, "text": c.Text})
		if err != nil {
			return nil, err
		}
		cand = append(cand, l)
		for _, s := range c.Sources {
			sl, err := p.line("source_line", map[string]string{"fact_id": s.FactID, "quote": s.Quote})
			if err != nil {
				return nil, err
			}
			cand = append(cand, sl)
		}
	}
	if out["candidates"], err = orNone(p, cand); err != nil {
		return nil, err
	}
	return out, nil
}

// WriteInput is the input of consolidate_write/v1 (section 6.3.2).
type WriteInput struct {
	Mode     string // update | create | rebuild
	Previous string // shown for update only
	Sources  []Source
	Attached []Fact
}

// WriteInputs builds the Inputs of consolidate_write/v1.
func WriteInputs(p *Prompt, in WriteInput) (Inputs, error) {
	if in.Mode != "update" && in.Mode != "create" && in.Mode != "rebuild" {
		return nil, fmt.Errorf("prompts: consolidate_write mode %q is not update, create or rebuild", in.Mode)
	}
	out := Inputs{"mode": in.Mode, "previous": "", "sources": "", "attached": ""}
	if in.Mode == "update" {
		out["previous"] = in.Previous
	}
	src := make([]string, len(in.Sources))
	for i, s := range in.Sources {
		l, err := p.line("source_line", map[string]string{"fact_id": s.FactID, "quote": s.Quote})
		if err != nil {
			return nil, err
		}
		src[i] = l
	}
	att := make([]string, len(in.Attached))
	for i, f := range in.Attached {
		said, err := f.saidAt(p)
		if err != nil {
			return nil, err
		}
		kv := map[string]string{"id": f.ID, "mentioned_at": ts(f.MentionedAt), "said_at": said, "text": f.Text}
		l, err := p.line("attached_line", kv)
		if err != nil {
			return nil, err
		}
		att[i] = l
	}
	var err error
	out["sources"] = strings.Join(src, "\n")
	if out["attached"], err = orNone(p, att); err != nil {
		return nil, err
	}
	return out, nil
}

func orNone(p *Prompt, lines []string) (string, error) {
	if len(lines) == 0 {
		return p.Snippet("none")
	}
	return strings.Join(lines, "\n"), nil
}

// ---- dedup_adjudicate/v1 and reflect_structured/v1 ---------------------------------------------------------------

// DedupInputs builds the Inputs of dedup_adjudicate/v1 (section 6.4): the texts of the two observations.
func DedupInputs(a, b string) Inputs { return Inputs{"a": a, "b": b} }

// StructuredInputs builds the Inputs of reflect_structured/v1 (section 6.5.2): the caller's schema as JSON text.
func StructuredInputs(question string, schema json.RawMessage, answer string) Inputs {
	return Inputs{"question": question, "schema": string(schema), "answer": answer}
}

// ---- reflect/v1 --------------------------------------------------------------------------------------------------

// Directive is an active, tag-matched directive of the namespace.
type Directive struct{ Name, Content string }

// Disposition holds the three trait levels 1 to 5; 0 and 3 render nothing.
type Disposition struct{ Skepticism, Literalism, Empathy int }

// ReflectInput is the input of reflect/v1 (section 6.5.1).
type ReflectInput struct {
	Mission       string // reflect.mission; empty selects the default text of the prompt
	Directives    []Directive
	Disposition   Disposition
	Now           time.Time // query_timestamp
	MaxIterations int
}

// ReflectInputs builds the Inputs of reflect/v1.
func ReflectInputs(p *Prompt, in ReflectInput) (Inputs, error) {
	out := Inputs{"mission": in.Mission, "directives": "", "disposition_section": "", "directives_reminder": "",
		"now": ts(in.Now), "max_iterations": strconv.Itoa(in.MaxIterations)}
	var err error
	if in.Mission == "" {
		if out["mission"], err = p.Snippet("default_mission"); err != nil {
			return nil, err
		}
	}
	dl := make([]string, len(in.Directives))
	for i, d := range in.Directives {
		kv := map[string]string{"name": d.Name, "content": d.Content}
		if dl[i], err = p.line("directive_line", kv); err != nil {
			return nil, err
		}
	}
	if out["directives"], err = orNone(p, dl); err != nil {
		return nil, err
	}
	if len(in.Directives) > 0 {
		kv := map[string]string{"directives": out["directives"]}
		if err = p.put(out, "directives_reminder", "directives_reminder", kv); err != nil {
			return nil, err
		}
	}
	lines, err := p.dispositionLines(in.Disposition)
	if err != nil {
		return nil, err
	}
	if len(lines) > 0 {
		kv := map[string]string{"lines": strings.Join(lines, "\n")}
		if err = p.put(out, "disposition_section", "disposition_section", kv); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// dispositionLines renders one line per trait whose level is not 3; a level outside 1 to 5 is an error.
func (p *Prompt) dispositionLines(d Disposition) ([]string, error) {
	var table map[string]map[string]json.RawMessage
	raw, ok := p.snippets["disposition"]
	if !ok {
		return nil, fmt.Errorf("prompts: %s has no disposition table", p.Meta.ID)
	}
	if err := json.Unmarshal(raw, &table); err != nil {
		return nil, err
	}
	var out []string
	for _, t := range []struct {
		trait string
		level int
	}{{"skepticism", d.Skepticism}, {"literalism", d.Literalism}, {"empathy", d.Empathy}} {
		if t.level == 0 || t.level == 3 {
			continue
		}
		if t.level < 1 || t.level > 5 {
			return nil, fmt.Errorf("prompts: %s level %d is outside 1 to 5", t.trait, t.level)
		}
		text, err := joinText(table[t.trait][strconv.Itoa(t.level)], " ")
		if err != nil {
			return nil, err
		}
		title := strings.ToUpper(t.trait[:1]) + t.trait[1:]
		l, err := p.line("disposition_line", map[string]string{"trait": title, "text": text})
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// ---- page/v1 and page_full/v1 ------------------------------------------------------------------------------------

// Evidence is one rendered evidence item of a page prompt.
type Evidence struct {
	ID   string
	Kind string // fact | observation
	Date time.Time
	Text string
}

// Change is a changed evidence item.
type Change struct{ ID, Old, New string }

// Block is a block of a page section.
type Block struct{ ID, Text string }

// Section is a section of the current page document.
type Section struct {
	ID, Name string
	Blocks   []Block
}

// PageInput is the input of page/v1 (section 6.6.1).
type PageInput struct {
	Topic    string
	Sections []Section
	Added    []Evidence
	Changed  []Change
	Retired  []Evidence
}

func evidenceLines(p *Prompt, key string, ev []Evidence) ([]string, error) {
	out := make([]string, len(ev))
	for i, e := range ev {
		l, err := p.line(key, map[string]string{"id": e.ID, "kind": e.Kind, "date": ts(e.Date), "text": e.Text})
		if err != nil {
			return nil, err
		}
		out[i] = l
	}
	return out, nil
}

// PageInputs builds the Inputs of page/v1.
func PageInputs(p *Prompt, in PageInput) (Inputs, error) {
	var doc []string
	for _, s := range in.Sections {
		l, err := p.line("section_line", map[string]string{"name": s.Name, "id": s.ID})
		if err != nil {
			return nil, err
		}
		doc = append(doc, l)
		for _, b := range s.Blocks {
			bl, err := p.line("block_line", map[string]string{"id": b.ID, "text": b.Text})
			if err != nil {
				return nil, err
			}
			doc = append(doc, bl)
		}
	}
	added, err := evidenceLines(p, "added_line", in.Added)
	if err != nil {
		return nil, err
	}
	retired, err := evidenceLines(p, "retired_line", in.Retired)
	if err != nil {
		return nil, err
	}
	changed := make([]string, len(in.Changed))
	for i, c := range in.Changed {
		kv := map[string]string{"id": c.ID, "old": c.Old, "new": c.New}
		if changed[i], err = p.line("changed_line", kv); err != nil {
			return nil, err
		}
	}
	out := Inputs{"topic": in.Topic, "document": strings.Join(doc, "\n")}
	for k, v := range map[string][]string{"added": added, "changed": changed, "retired": retired} {
		if out[k], err = orNone(p, v); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// PageFullInput is the input of page_full/v1 (section 6.6.2).
type PageFullInput struct {
	Topic     string
	Evidence  []Evidence
	MaxTokens int
}

// PageFullInputs builds the Inputs of page_full/v1.
func PageFullInputs(p *Prompt, in PageFullInput) (Inputs, error) {
	lines, err := evidenceLines(p, "evidence_line", in.Evidence)
	if err != nil {
		return nil, err
	}
	ev, err := orNone(p, lines)
	if err != nil {
		return nil, err
	}
	return Inputs{"topic": in.Topic, "evidence": ev, "max_tokens": strconv.Itoa(in.MaxTokens)}, nil
}
