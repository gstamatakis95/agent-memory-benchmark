// Package derivation is the invariant code of formal/tla/Derivation.tla (PLAN.md section 7.2.1, N113, N115 to N121,
// N133, N135, N136, N144, N145, N162, N174): can a deleted, invalidated or superseded fact reach a reader through a
// fact, an observation version or a page version, at any as_of. The package holds a plain Go value model of the
// specification's variables (Params for its constants, State for its variables), the read-time predicates of N117
// (Served is the SQL rule) and one function per invariant. Ghost fields (gfinp, goinp, ginv) are part of the model:
// tests that drive the real implementation fill them from what the test knows to be the truth.
package derivation

import "github.com/gstamatakis95/engram/internal/formal/tla"

// Pair is a version of a node: <<node, v>>, v >= 1. Nodes are observations and pages.
type Pair struct {
	Node string
	V    int
}

// Params are the constants of a Derivation.tla configuration that the predicates read. Facts are 1..len(DocOf); the
// slices are indexed by fact id - 1 (DocOfDef, FVerDef, MentionedDef and TwinDef of the specification; Twin is 0 for no
// twin).
type Params struct {
	Docs      []string
	DocOf     []string
	FVer      []int
	Mentioned []int
	Twin      []int
	Obs       []string
	Pages     []string
	MaxT      int
	// Knobs that change the read-time predicates (the others change actions, which the model does not run).
	TombByVersion  bool // design TRUE: a tombstone hides versions 1..tomb[d] of d; FALSE hides the whole document id
	ReextractHides bool // FALSE in the design: derived versions are hidden by cause reextract
	MatSignalOnly  bool // FALSE in the design: Materialize is enabled only by a lossy post-ack signal
}

// Design returns the design instance of the specification: f1 = (d1, v1) with the re-extraction twin f4, f2 = (d1, v2),
// f3 = (d2, v1), one observation o1 and no page.
func Design() Params {
	return Params{
		Docs:           []string{"d1", "d2"},
		DocOf:          []string{"d1", "d1", "d2", "d1"},
		FVer:           []int{1, 2, 1, 1},
		Mentioned:      []int{1, 3, 2, 1},
		Twin:           []int{4, 0, 0, 0},
		Obs:            []string{"o1"},
		MaxT:           3,
		TombByVersion:  true,
		ReextractHides: false,
	}
}

// Rec is one observation or page version row (the spec's Rec, without the commit key).
type Rec struct {
	Root  int
	Finp  tla.Set[int]  // evidence rows: fact ids (no foreign key to facts, N135)
	Gfinp tla.Set[int]  // ghost: what the text was really derived from
	Oinp  tla.Set[Pair] // observation versions a page cites
	Goinp tla.Set[Pair]
	Eff   int    // effective_at
	Base  int    // the version an update extended, 0 for a root rebuild
	St    string // "live", "stub" or "absent"
}

// DhRow is a derived_hidden row <<node, root_version, from_version, cause>> with cause <<"doc", 0>> or <<"inv", fact>>.
type DhRow struct {
	Node       string
	Root, From int
	CauseKind  string
	CauseFact  int
}

// Clog is the subject's last curation_log action ("none", "inv", "res") and its operation id.
type Clog struct {
	Act string
	Tag int
}

// State mirrors the variables of Derivation.tla that the predicates and invariants read.
type State struct {
	Tomb     map[string]int    // document_tombstones: versions 1..Tomb[d] of d are deleted
	Ms       map[string]string // expunge_state: none, pending, materialized, purged, done
	Hidden   tla.Set[int]      // fact_hidden(cause = invalidate)
	HidRe    tla.Set[int]      // fact_hidden(cause = reextract)
	Ctomb    tla.Set[int]      // chunk_tombstones
	Born     tla.Set[int]
	Gone     tla.Set[int]
	Vers     map[string][]Rec // per node, versions 1..len
	Hw       map[string]int   // high-water mark of version numbers
	Dh       []DhRow
	Cv       map[string]int // current_version per node
	Ginv     tla.Set[int]   // ghost: the acknowledged invalidations
	Mat      tla.Set[int]   // fact_hidden.materialized_at set
	MatOwe   tla.Set[int]
	Sig      bool
	MatPhase string // "idle", "running" or "scanned"
	Itag     map[int]int
	Clog     map[int]Clog
}

// nodes lists observations then pages.
func (p Params) nodes() []string {
	out := make([]string, 0, len(p.Obs)+len(p.Pages))
	out = append(out, p.Obs...)
	return append(out, p.Pages...)
}

func (p Params) facts() []int {
	out := make([]int, len(p.DocOf))
	for i := range out {
		out[i] = i + 1
	}
	return out
}

func (p Params) docOf(f int) string            { return p.DocOf[f-1] }
func (p Params) fver(f int) int                { return p.FVer[f-1] }
func (p Params) mentioned(f int) int           { return p.Mentioned[f-1] }
func (p Params) twin(f int) int                { return p.Twin[f-1] }
func (p Params) bornLive(s State) tla.Set[int] { return s.Born.Minus(s.Gone) }

// Group is the subject of a fact: itself, its twin and the fact it is the twin of (same document, same content hash).
func (p Params) Group(f int) tla.Set[int] {
	out := tla.NewSet[int]()
	for _, g := range p.facts() {
		if g == f || p.twin(g) == f || p.twin(f) == g {
			out.Add(g)
		}
	}
	return out
}

// Subj is the representative of the subject (the smallest fact id of the group).
func (p Params) Subj(f int) int {
	m := f
	for g := range p.Group(f) {
		if g < m {
			m = g
		}
	}
	return m
}

// Victims is the facts of document d covered by its tombstone.
func (p Params) Victims(s State, d string) tla.Set[int] {
	out := tla.NewSet[int]()
	for _, f := range p.facts() {
		if p.docOf(f) == d && p.fver(f) <= s.Tomb[d] {
			out.Add(f)
		}
	}
	return out
}

func (s State) rec(x Pair) Rec { return s.Vers[x.Node][x.V-1] }

// Exists is the spec's Exists: the version row exists (a stub counts).
func (s State) Exists(x Pair) bool { return x.V >= 1 && x.V <= len(s.Vers[x.Node]) }

// Versions is the set of all existing <<node, v>> pairs.
func (p Params) Versions(s State) []Pair {
	var out []Pair
	for _, n := range p.nodes() {
		for v := 1; v <= len(s.Vers[n]); v++ {
			out = append(out, Pair{n, v})
		}
	}
	return out
}

// seg is root_version(v)..v of a version.
func (s State) seg(x Pair) []int {
	var out []int
	for w := s.rec(x).Root; w <= x.V; w++ {
		out = append(out, w)
	}
	return out
}

// DerivF is the real evidence rows of the segment (facts only, one level).
func (s State) DerivF(x Pair) tla.Set[int] {
	out := tla.NewSet[int]()
	for _, w := range s.seg(x) {
		out = out.Union(s.Vers[x.Node][w-1].Finp)
	}
	return out
}

// DerivO is the observation versions a page version's segment cites.
func (s State) DerivO(x Pair) tla.Set[Pair] {
	out := tla.NewSet[Pair]()
	for _, w := range s.seg(x) {
		out = out.Union(s.Vers[x.Node][w-1].Oinp)
	}
	return out
}

// GDerivF is the ghost fact set: never removed.
func (s State) GDerivF(x Pair) tla.Set[int] {
	out := tla.NewSet[int]()
	for _, w := range s.seg(x) {
		out = out.Union(s.Vers[x.Node][w-1].Gfinp)
	}
	return out
}

func (s State) gDerivO(x Pair) tla.Set[Pair] {
	out := tla.NewSet[Pair]()
	for _, w := range s.seg(x) {
		out = out.Union(s.Vers[x.Node][w-1].Goinp)
	}
	return out
}

// GDeriv is the ghost truth: everything a version's text was derived from, two levels (fact, observation, page).
func (s State) GDeriv(x Pair) tla.Set[int] {
	out := s.GDerivF(x)
	for y := range s.gDerivO(x) {
		if s.Exists(y) {
			out = out.Union(s.GDerivF(y))
		}
	}
	return out
}

// TombTrue is the truth: the fact is deleted.
func (p Params) TombTrue(s State, f int) bool { return p.fver(f) <= s.Tomb[p.docOf(f)] }

// TombImpl is what the predicate checks.
func (p Params) TombImpl(s State, f int) bool {
	if p.TombByVersion {
		return p.TombTrue(s, f)
	}
	return s.Tomb[p.docOf(f)] > 0
}

// Victim is the ghost: deleted or invalidated.
func (p Params) Victim(s State, f int) bool { return p.TombTrue(s, f) || s.Hidden.Has(f) }

// VictimD is the derived-version predicate: pending tombstones only (N117).
func (p Params) VictimD(s State, f int) bool {
	return (p.TombImpl(s, f) && s.Ms[p.docOf(f)] == "pending") || s.Hidden.Has(f) ||
		(p.ReextractHides && s.HidRe.Has(f))
}

// VictimF is the fact and chunk arms' predicate.
func (p Params) VictimF(s State, f int) bool {
	return p.TombImpl(s, f) || s.Hidden.Has(f) || s.HidRe.Has(f) || s.Ctomb.Has(f)
}

// RowCovered: a derived_hidden row of the node covers the version (same root, from_version <= v).
func (s State) RowCovered(x Pair) bool {
	root := s.rec(x).Root
	for _, r := range s.Dh {
		if r.Node == x.Node && r.Root == root && r.From <= x.V {
			return true
		}
	}
	return false
}

// Perm is the permanent coverage of a version or of an observation version it cites.
func (s State) Perm(x Pair) bool {
	if s.RowCovered(x) {
		return true
	}
	for y := range s.DerivO(x) {
		if s.Exists(y) && s.RowCovered(y) {
			return true
		}
	}
	return false
}

// visBase is N117: a version row exists and is live (fail closed), no victim in the segment's evidence, and no
// derived_hidden row covers it.
func (p Params) visBase(s State, x Pair) bool {
	if !s.Exists(x) || s.rec(x).St != "live" {
		return false
	}
	for f := range s.DerivF(x) {
		if p.VictimD(s, f) {
			return false
		}
	}
	return !s.RowCovered(x)
}

// VisN is visBase, and for a page version also every cited observation version.
func (p Params) VisN(s State, x Pair) bool {
	if !p.visBase(s, x) {
		return false
	}
	for y := range s.DerivO(x) {
		if !s.Exists(y) || !p.visBase(s, y) {
			return false
		}
	}
	return true
}

// VisFacts is the facts the fact and chunk arms serve.
func (p Params) VisFacts(s State) tla.Set[int] {
	out := tla.NewSet[int]()
	for f := range p.bornLive(s) {
		if !p.VictimF(s, f) {
			out.Add(f)
		}
	}
	return out
}

// Served is the SQL rule of N117 at time T (C-14): per node, the version current at T (the latest with effective_at <=
// T), served only if visible, else nothing for that node.
func (p Params) Served(s State, t int) []Pair {
	var out []Pair
	for _, x := range p.Versions(s) {
		if s.rec(x).Eff > t {
			continue
		}
		latest := true
		for u := x.V + 1; u <= len(s.Vers[x.Node]); u++ {
			if s.Vers[x.Node][u-1].Eff <= t {
				latest = false
				break
			}
		}
		if latest && p.VisN(s, x) {
			out = append(out, x)
		}
	}
	return out
}

// ServedFacts is the visible facts mentioned at or before T.
func (p Params) ServedFacts(s State, t int) tla.Set[int] {
	out := tla.NewSet[int]()
	for f := range p.VisFacts(s) {
		if p.mentioned(f) <= t {
			out.Add(f)
		}
	}
	return out
}

// HitF is the victims V that a version cites (directly, or through a cited observation version).
func (s State) HitF(x Pair, v tla.Set[int]) tla.Set[int] {
	rec := s.rec(x)
	cited := rec.Finp.Clone()
	for y := range rec.Oinp {
		if s.Exists(y) {
			cited = cited.Union(s.DerivF(y))
		}
	}
	return v.Inter(cited)
}

// Owed: some version that cites f is not yet covered by a derived_hidden row.
func (p Params) Owed(s State, f int) bool {
	one := tla.NewSet(f)
	for _, x := range p.Versions(s) {
		if s.HitF(x, one).Has(f) && !s.RowCovered(x) {
			return true
		}
	}
	return false
}

// MatWork: Materialize is enabled by an open tombstone or, from the markers, by an unstamped invalidation with rows
// owed (MatSignalOnly: only by the signal).
func (p Params) MatWork(s State) bool {
	for _, d := range p.Docs {
		if s.Ms[d] == "pending" {
			return true
		}
	}
	if p.MatSignalOnly {
		return s.Sig
	}
	for f := range s.Hidden.Minus(s.Mat) {
		if p.Owed(s, f) {
			return true
		}
	}
	return false
}
