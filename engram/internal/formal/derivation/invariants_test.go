package derivation

import (
	"reflect"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

func set(xs ...int) tla.Set[int] { return tla.NewSet(xs...) }

func initial() State {
	return State{
		Tomb:   map[string]int{"d1": 0, "d2": 0},
		Ms:     map[string]string{"d1": "none", "d2": "none"},
		Hidden: set(), HidRe: set(), Ctomb: set(), Born: set(), Gone: set(),
		Vers: map[string][]Rec{"o1": nil},
		Hw:   map[string]int{"o1": 0},
		Cv:   map[string]int{"o1": 0},
		Ginv: set(), Mat: set(), MatOwe: set(),
		MatPhase: "idle",
		Itag:     map[int]int{},
		Clog:     map[int]Clog{},
	}
}

// live returns a live observation version at root cited from facts (real and ghost evidence alike).
func live(root, eff int, facts ...int) Rec {
	return Rec{Root: root, Finp: set(facts...), Gfinp: set(facts...), Oinp: tla.NewSet[Pair](),
		Goinp: tla.NewSet[Pair](), Eff: eff, St: "live"}
}

// oneVersion is a state with fact 1 born and observation o1 holding one live version that cites it.
func oneVersion() State {
	s := initial()
	s.Born = set(1)
	s.Vers["o1"] = []Rec{live(1, 1, 1)}
	s.Hw["o1"] = 1
	s.Cv["o1"] = 1
	return s
}

func check(t *testing.T, p Params, s State, want ...string) {
	t.Helper()
	got := Check(p, s)
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("violations = %v, want %v", got, want)
	}
}

func has(t *testing.T, p Params, s State, name string) {
	t.Helper()
	for _, g := range Check(p, s) {
		if g == name {
			return
		}
	}
	t.Fatalf("%s holds in a state that must violate it (violations: %v)", name, Check(p, s))
}

func TestInvariants_HoldOnInitialAndServedStates(t *testing.T) {
	p := Design()
	check(t, p, initial())
	check(t, p, oneVersion())
	if got := p.Served(oneVersion(), 3); len(got) != 1 || got[0] != (Pair{"o1", 1}) {
		t.Fatalf("Served = %v", got)
	}
	if got := p.Served(oneVersion(), 0); len(got) != 0 {
		t.Fatalf("a version effective at 1 is not served at 0: %v", got)
	}
}

// Served is the SQL rule: a deleted victim hides the version while the tombstone is pending, and the permanent
// derived_hidden row takes over once it is materialized (N117).
func TestServed_DeleteHidesThroughPendingAndMaterialized(t *testing.T) {
	p := Design()
	s := oneVersion()
	s.Tomb["d1"], s.Ms["d1"] = 1, "pending"
	if len(p.Served(s, 3)) != 0 {
		t.Fatal("pending tombstone must hide the derived version")
	}
	check(t, p, s)
	s.Ms["d1"] = "materialized" // the predicate stops looking at the tombstone; the row must be there
	if len(p.Served(s, 3)) != 1 {
		t.Fatal("materialized without a derived_hidden row serves the version")
	}
	has(t, p, s, NoDeletedDerivationServedName)
	has(t, p, s, MaterializeCompleteName)
	s.Dh = []DhRow{{Node: "o1", Root: 1, From: 1, CauseKind: "doc"}}
	check(t, p, s)
}

// Derivation_TombByDocId.cfg (4 states): the tombstone of d1 hides the whole document id, so f2 = (d1, v2), ingested
// right after the delete, is hidden by the old tombstone.
func TestInvariants_TombByDocIdCounterexample(t *testing.T) {
	p := Design()
	p.TombByVersion = false
	s := initial()
	s.Born = set(1, 2)
	s.Tomb["d1"], s.Ms["d1"] = 1, "pending"
	has(t, p, s, NoOverHidingName)
	p.TombByVersion = true
	check(t, p, s)
}

// Derivation_ReextractHides.cfg (7 states): the derived version cites f1, f1 is re-extracted (hidRe), and the predicate
// hides derived versions by cause reextract.
func TestInvariants_ReextractHidesCounterexample(t *testing.T) {
	p := Design()
	p.ReextractHides = true
	s := oneVersion()
	s.HidRe = set(1)
	has(t, p, s, ReextractKeepsDerivedVisibleName)
	p.ReextractHides = false
	check(t, p, s)
}

// Derivation_CascadeEvidence.cfg (10 states): the evidence row died with the fact, the delete finds no victim in the
// derivation, and the version stays served. The ghost keeps what the text was derived from.
func TestInvariants_CascadeEvidenceCounterexample(t *testing.T) {
	p := Design()
	s := oneVersion()
	s.Vers["o1"][0].Finp = set()
	s.Tomb["d1"], s.Ms["d1"] = 1, "pending"
	has(t, p, s, NoDeletedDerivationServedName)
	has(t, p, s, EvidenceOutlivesFactsName)
}

// Derivation_EffCited.cfg (7 states): effective_at counts only cited facts, so a version derived from f2 (mentioned at
// 3) is effective at 1 and served at T = 1.
func TestInvariants_EffCitedCounterexample(t *testing.T) {
	p := Design()
	s := initial()
	s.Born = set(2)
	s.Tomb["d1"] = 0
	s.Vers["o1"] = []Rec{live(1, 1, 2)}
	s.Hw["o1"], s.Cv["o1"] = 1, 1
	has(t, p, s, AsOfNoLeakName)
	s.Vers["o1"][0].Eff = 3
	check(t, p, s)
}

// Derivation_PurgeDropsStub.cfg (13 states): DerivedPurge deleted the version row, so the version numbers are reused.
func TestInvariants_PurgeDropsStubCounterexample(t *testing.T) {
	p := Design()
	s := initial()
	s.Hw["o1"] = 1 // version 1 existed once; the row is gone
	check(t, p, s, FailClosedName)
	s.Vers["o1"] = []Rec{{Root: 1, Finp: set(), Gfinp: set(1), Oinp: tla.NewSet[Pair](), Goinp: tla.NewSet[Pair](),
		Eff: 1, St: "stub"}}
	s.Born = set(1)
	s.Tomb["d1"], s.Ms["d1"] = 1, "done"
	s.Dh = []DhRow{{Node: "o1", Root: 1, From: 1, CauseKind: "doc"}}
	check(t, p, s) // the stub keeps its number and is never served
}

// Derivation_CasBeforeIdem.cfg (7 states): a retried commit advanced current_version to a version that does not exist.
func TestInvariants_CasBeforeIdemCounterexample(t *testing.T) {
	p := Design()
	s := oneVersion()
	s.Cv["o1"] = 2
	check(t, p, s, NoPhantomVersionName)
}

// Derivation_CascadeHidden.cfg (9 states): purging the retired fact dropped its fact_hidden(invalidate) row; the ghost
// of the acknowledged invalidation still names it.
func TestInvariants_CascadeHiddenCounterexample(t *testing.T) {
	p := Design()
	s := oneVersion()
	s.Ginv = set(1)
	check(t, p, s, NoGhostInvalidatedServedName)
	s.Hidden = set(1)
	s.Itag[1], s.Clog[1] = 1, Clog{Act: "inv", Tag: 1}
	s.Dh = []DhRow{{Node: "o1", Root: 1, From: 1, CauseKind: "inv", CauseFact: 1}}
	s.Mat = set(1)
	check(t, p, s)
}

// Derivation_MatSignalOnly.cfg (8 states): an invalidation is unstamped with rows owed, Materialize waits for a lost
// signal, so nothing is enabled and the owed rows are stranded.
func TestInvariants_MatSignalOnlyCounterexample(t *testing.T) {
	p := Design()
	p.MatSignalOnly = true
	s := oneVersion()
	s.Hidden = set(1)
	s.Itag[1], s.Clog[1] = 1, Clog{Act: "inv", Tag: 1}
	s.Ginv = set(1)
	has(t, p, s, MaterializeCompleteName)
	s.Sig = true // the signal arrived: Materialize is enabled
	check(t, p, s)
	p.MatSignalOnly = false // the design finds the work from the markers
	s.Sig = false
	check(t, p, s)
}

// Derivation_RestoreNoLock.cfg and _MatOnce.cfg (11 states): a Materialize batch scanned, Restore ran, the batch wrote:
// a permanent derived_hidden row covers a version none of whose facts is hidden any more.
func TestInvariants_RestoreNoLockCounterexample(t *testing.T) {
	p := Design()
	s := oneVersion()
	s.Dh = []DhRow{{Node: "o1", Root: 1, From: 1, CauseKind: "inv", CauseFact: 1}}
	has(t, p, s, RestoreExactName)
}

// Derivation_RestoreOnlySelf.cfg and _RestoreByVisibleTwin.cfg (6 and 5 states): Restore(f1) un-hid only f1 (or only
// the twins visible for another cause); the twin f4 hidden by the same Invalidate stays hidden.
func TestInvariants_RestoreOnlySelfCounterexample(t *testing.T) {
	p := Design()
	s := initial()
	s.Born = set(1, 4)
	s.Hidden = set(4)
	s.Itag[4] = 1
	s.Clog[1] = Clog{Act: "res"}
	has(t, p, s, RestoreExactName)
	s.Hidden = set(1, 4)
	s.Itag[1], s.Itag[4] = 1, 1
	s.Clog[1] = Clog{Act: "inv", Tag: 1}
	check(t, p, s)
}

// Derivation_LazyTwinNoLock.cfg (7 states): the twin was born hidden under a tag that Restore already removed.
func TestInvariants_LazyTwinNoLockCounterexample(t *testing.T) {
	p := Design()
	s := initial()
	s.Born = set(1, 4)
	s.Hidden = set(4)
	s.Itag[4] = 1 // the tag Restore removed from f1
	s.Clog[1] = Clog{Act: "res"}
	has(t, p, s, RestoreExactName)
	has(t, p, s, TagFromLogName)
}

// Derivation_LazyTagFromLastLog.cfg (6 states): a repeated Invalidate logged a fresh id although its rows keep the old
// one; the lazy twin carries the log's tag, which Restore of the subject does not resolve.
func TestInvariants_LazyTagFromLastLogCounterexample(t *testing.T) {
	p := Design()
	s := initial()
	s.Born = set(1, 4)
	s.Hidden = set(1, 4)
	s.Itag[1], s.Itag[4] = 1, 2
	s.Clog[1] = Clog{Act: "inv", Tag: 2}
	has(t, p, s, TagFromLogName)
	has(t, p, s, RestoreExactName)
	s.Itag[4] = 1
	s.Clog[1] = Clog{Act: "inv", Tag: 1}
	check(t, p, s)
}

// Derivation_NoLock.cfg, _PageNoVerify.cfg and _StaleProposal.cfg: a derived version built from a deleted fact is
// visible once the tombstone is materialized and no derived_hidden row covers it.
func TestInvariants_StaleWriterCounterexample(t *testing.T) {
	p := Design()
	s := oneVersion()
	s.Tomb["d1"], s.Ms["d1"] = 1, "materialized"
	has(t, p, s, NoDeletedDerivationServedName)
}

func TestInvariants_UpdateChain(t *testing.T) {
	p := Design()
	s := initial()
	s.Born = set(1)
	r1 := live(1, 1, 1)
	r2 := live(1, 1, 1)
	r2.Base = 1
	r3 := live(3, 1, 1)
	r3.Base = 1 // an update from version 1 although version 2 exists
	s.Vers["o1"] = []Rec{r1, r2, r3}
	s.Hw["o1"], s.Cv["o1"] = 3, 3
	has(t, p, s, BaseCurrentAtCommitName)
	// a rebuild at version 2 lies between version 3 and its base 1
	r2.Root = 2
	r3.Base = 1
	s.Vers["o1"] = []Rec{r1, r2, r3}
	has(t, p, s, NoLostRebuildName)
}

func TestGroupAndSubject(t *testing.T) {
	p := Design()
	if g := p.Group(1); !g.Equal(set(1, 4)) {
		t.Fatalf("Group(1) = %v", g)
	}
	if p.Subj(4) != 1 || p.Subj(2) != 2 {
		t.Fatal("Subj")
	}
	s := initial()
	s.Tomb["d1"] = 1
	if v := p.Victims(s, "d1"); !v.Equal(set(1, 4)) {
		t.Fatalf("Victims = %v", v)
	}
}

// supersededState: version 1 of o1 cites visible fact 3 (effective at 1); version 2 is a root rebuild citing fact 1
// (effective at 3), and fact 1 is invalidated, so version 2 is hidden.
func supersededState() State {
	s := initial()
	s.Born = set(1, 3)
	s.Hidden = set(1)
	s.Itag[1] = 1
	s.Clog[1] = Clog{Act: "inv", Tag: 1}
	s.Vers["o1"] = []Rec{live(1, 1, 3), live(2, 3, 1)}
	s.Hw["o1"], s.Cv["o1"] = 2, 2
	return s
}

// N117: Served is the version CURRENT at T, served only if visible, "else nothing for that node, no fallback". The
// older visible version must not be served when the current one is hidden.
func TestServed_CurrentVersionHiddenServesNothing(t *testing.T) {
	p := Design()
	s := supersededState()
	if got := p.Served(s, 3); len(got) != 0 {
		t.Fatalf("Served(3) = %v: version 2 is current at 3 and hidden, version 1 must not be served instead", got)
	}
	if got := p.Served(s, 1); len(got) != 1 || got[0] != (Pair{"o1", 1}) {
		t.Fatalf("Served(1) = %v: at T = 1 version 1 is the current one and visible", got)
	}
}

// ... and a version whose effective_at is after T is not current at T: the older version is served.
func TestServed_FutureVersionIsNotCurrent(t *testing.T) {
	p := Design()
	s := supersededState()
	s.Hidden = set()
	s.Vers["o1"][1].Eff = 3
	if got := p.Served(s, 2); len(got) != 1 || got[0] != (Pair{"o1", 1}) {
		t.Fatalf("Served(2) = %v: version 2 is effective at 3 > T, version 1 is current", got)
	}
	if got := p.Served(s, 3); len(got) != 1 || got[0] != (Pair{"o1", 2}) {
		t.Fatalf("Served(3) = %v: version 2 is current at 3", got)
	}
}

// VisN: a page version is hidden when an observation version it cites is hidden (or missing), even if the page's own
// evidence is clean.
func TestVisN_PageNeedsItsCitedObservationVersions(t *testing.T) {
	p := Design()
	p.Pages = []string{"p1"}
	s := oneVersion()
	s.Vers["p1"] = []Rec{{Root: 1, Finp: set(), Gfinp: set(), Oinp: tla.NewSet(Pair{"o1", 1}),
		Goinp: tla.NewSet(Pair{"o1", 1}), Eff: 1, St: "live"}}
	s.Hw["p1"], s.Cv["p1"] = 1, 1
	if !p.VisN(s, Pair{"p1", 1}) {
		t.Fatal("a page over a visible observation version is visible")
	}
	s.Hidden = set(1) // the fact behind the cited observation version
	s.Itag[1], s.Clog[1] = 1, Clog{Act: "inv", Tag: 1}
	if p.VisN(s, Pair{"p1", 1}) {
		t.Fatal("a page citing a hidden observation version must be hidden")
	}
	s.Hidden = set()
	s.Vers["o1"] = nil // the cited version does not exist: fail closed
	if p.VisN(s, Pair{"p1", 1}) {
		t.Fatal("a page citing a missing observation version must be hidden")
	}
}

// derived_hidden(root_version, from_version) covers versions from from_version on, not the ones before it.
func TestRowCovered_FromVersion(t *testing.T) {
	s := initial()
	s.Vers["o1"] = []Rec{live(1, 1, 1), live(1, 2, 1)}
	s.Dh = []DhRow{{Node: "o1", Root: 1, From: 2, CauseKind: "doc"}}
	if s.RowCovered(Pair{"o1", 1}) {
		t.Fatal("a row from version 2 does not cover version 1")
	}
	if !s.RowCovered(Pair{"o1", 2}) {
		t.Fatal("a row from version 2 covers version 2")
	}
	s.Dh[0].Root = 2
	if s.RowCovered(Pair{"o1", 2}) {
		t.Fatal("a row of another root does not cover the segment")
	}
}

// TagFromLog: the subject's last curation_log action must be an invalidation, not only carry the tag.
func TestTagFromLog_LastActionMustBeInvalidate(t *testing.T) {
	p := Design()
	s := initial()
	s.Born = set(1, 4)
	s.Hidden = set(1, 4)
	s.Itag[1], s.Itag[4] = 1, 1
	s.Clog[1] = Clog{Act: "res", Tag: 1}
	if TagFromLog(p, s) {
		t.Fatal("a restore as the last logged action with hidden facts violates TagFromLog")
	}
	s.Clog[1] = Clog{Act: "inv", Tag: 1}
	if !TagFromLog(p, s) {
		t.Fatal("holds with act = inv and the tag of the hidden facts")
	}
}
