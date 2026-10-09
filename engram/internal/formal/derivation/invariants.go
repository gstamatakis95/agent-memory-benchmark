package derivation

import "github.com/gstamatakis95/engram/internal/formal/tla"

// Names of the invariants, as they appear in formal/tla/Derivation.tla and formal/tla/EXPECT.
const (
	NoDeletedDerivationServedName     = "NoDeletedDerivationServed"
	NoInvalidatedDerivationServedName = "NoInvalidatedDerivationServed"
	RestoreExactName                  = "RestoreExact"
	TagFromLogName                    = "TagFromLog"
	NoOverHidingName                  = "NoOverHiding"
	AsOfNoLeakName                    = "AsOfNoLeak"
	MaterializeCompleteName           = "MaterializeComplete"
	NoGhostInvalidatedServedName      = "NoGhostInvalidatedServed"
	NoPhantomVersionName              = "NoPhantomVersion"
	EvidenceOutlivesFactsName         = "EvidenceOutlivesFacts"
	BaseCurrentAtCommitName           = "BaseCurrentAtCommit"
	NoLostRebuildName                 = "NoLostRebuild"
	NoVictimTextAfterPurgeName        = "NoVictimTextAfterPurge"
	FailClosedName                    = "FailClosed"
	ReextractKeepsDerivedVisibleName  = "ReextractKeepsDerivedVisible"
)

func (p Params) live(s State, x Pair) bool { return s.rec(x).St == "live" }

// NoDeletedDerivationServed: after DeleteDocument is acknowledged, no served fact or derivation has a deleted fact.
func NoDeletedDerivationServed(p Params, s State) bool {
	for t := 1; t <= p.MaxT; t++ {
		for f := range p.ServedFacts(s, t) {
			if p.TombTrue(s, f) {
				return false
			}
		}
		for _, x := range p.Served(s, t) {
			for f := range s.GDeriv(x) {
				if p.TombTrue(s, f) {
					return false
				}
			}
		}
	}
	return true
}

// NoInvalidatedDerivationServed: no served version has an invalidated fact in its derivation.
func NoInvalidatedDerivationServed(p Params, s State) bool {
	for t := 1; t <= p.MaxT; t++ {
		for _, x := range p.Served(s, t) {
			if s.GDeriv(x).Intersects(s.Hidden) {
				return false
			}
		}
	}
	return true
}

// RestoreExact: Invalidate;Restore leaves no trace (a version with no currently hidden fact in its derivation is
// visible exactly as if the invalidation had never existed), and an invalidation is atomic over its subject (N162): a
// live fact of the subject is hidden by it or by none of it, so no twin survives a Restore.
func RestoreExact(p Params, s State) bool {
	for _, x := range p.Versions(s) {
		if !p.live(s, x) {
			continue
		}
		g := s.GDeriv(x)
		if g.Intersects(s.Hidden) {
			continue
		}
		noneDeleted := true
		for f := range g {
			if p.TombTrue(s, f) {
				noneDeleted = false
			}
		}
		if p.VisN(s, x) != noneDeleted {
			return false
		}
	}
	liveFacts := p.bornLive(s)
	for h := range s.Hidden.Inter(liveFacts) {
		for t := range p.Group(h).Inter(liveFacts) {
			if !s.Hidden.Has(t) || s.Itag[t] != s.Itag[h] {
				return false
			}
		}
	}
	return true
}

// TagFromLog (N174(2), C8-12): the curation_log's last action of a subject carries the tag of every hidden fact of the
// subject, so the lazy twin that reads it resolves with the subject's Restore.
func TagFromLog(p Params, s State) bool {
	for f := range s.Hidden.Inter(p.bornLive(s)) {
		c := s.Clog[p.Subj(f)]
		if c.Act != "inv" || c.Tag != s.Itag[f] {
			return false
		}
	}
	return true
}

// NoOverHiding: a fact or version with no victim in its derivation is visible (the blast-radius guard: the re-used
// document id is not hidden by the old tombstone; REPLACE and re-extraction hide only the fact arms).
func NoOverHiding(p Params, s State) bool {
	vis := p.VisFacts(s)
	for f := range p.bornLive(s) {
		if !p.Victim(s, f) && !s.Ctomb.Has(f) && !s.HidRe.Has(f) && !vis.Has(f) {
			return false
		}
	}
	for _, x := range p.Versions(s) {
		if !p.live(s, x) {
			continue
		}
		none := true
		for f := range s.GDeriv(x) {
			if p.Victim(s, f) {
				none = false
			}
		}
		if none && !p.VisN(s, x) {
			return false
		}
	}
	return true
}

// AsOfNoLeak: served at T implies nothing in the derivation was mentioned after T.
func AsOfNoLeak(p Params, s State) bool {
	for t := 1; t <= p.MaxT; t++ {
		for _, x := range p.Served(s, t) {
			for f := range s.GDeriv(x) {
				if p.mentioned(f) > t {
					return false
				}
			}
		}
	}
	return true
}

// MaterializeComplete: once a document is materialized, derived_hidden covers every committed version with a victim of
// its tombstone in the derivation; a stamped invalidation is covered on every node; and an owed one is never stranded.
func MaterializeComplete(p Params, s State) bool {
	for _, d := range p.Docs {
		switch s.Ms[d] {
		case "materialized", "purged", "done":
			victims := p.Victims(s, d)
			for _, x := range p.Versions(s) {
				if s.GDeriv(x).Intersects(victims) && !s.Perm(x) {
					return false
				}
			}
		}
	}
	for f := range s.Mat {
		one := tla.NewSet(f)
		for _, x := range p.Versions(s) {
			if s.HitF(x, one).Has(f) && !s.RowCovered(x) {
				return false
			}
		}
	}
	owed := false
	for f := range s.Hidden.Minus(s.Mat) {
		if p.Owed(s, f) {
			owed = true
		}
	}
	return !owed || s.MatPhase != "idle" || p.MatWork(s)
}

// NoGhostInvalidatedServed (N145): an acknowledged invalidation stays in force until Restore, whatever happened to the
// fact row (Ginv is the ghost of the acknowledged set).
func NoGhostInvalidatedServed(p Params, s State) bool {
	for t := 1; t <= p.MaxT; t++ {
		for _, x := range p.Served(s, t) {
			if s.GDeriv(x).Intersects(s.Ginv) {
				return false
			}
		}
	}
	return true
}

// NoPhantomVersion (N144): current_version always names an existing row of the node, never ahead of the rows.
func NoPhantomVersion(p Params, s State) bool {
	for _, n := range p.nodes() {
		cv := s.Cv[n]
		if cv == 0 {
			continue
		}
		if cv > len(s.Vers[n]) {
			return false
		}
		r := s.Vers[n][cv-1]
		if r.St == "absent" || r.Root > cv {
			return false
		}
	}
	return true
}

// EvidenceOutlivesFacts (N135(1)): a live version's evidence is everything its text was derived from.
func EvidenceOutlivesFacts(p Params, s State) bool {
	for _, x := range p.Versions(s) {
		allLive := true
		for _, w := range s.seg(x) {
			if s.Vers[x.Node][w-1].St != "live" {
				allLive = false
			}
		}
		if allLive && !s.GDerivF(x).SubsetOf(s.DerivF(x)) {
			return false
		}
	}
	return true
}

// BaseCurrentAtCommit (N120(3)): an update extends the version it follows.
func BaseCurrentAtCommit(p Params, s State) bool {
	for _, x := range p.Versions(s) {
		if b := s.rec(x).Base; b != 0 && b != x.V-1 {
			return false
		}
	}
	return true
}

// NoLostRebuild: no root rebuild lies between an update and its base, so stale text never overwrites a rebuild.
func NoLostRebuild(p Params, s State) bool {
	for _, x := range p.Versions(s) {
		b := s.rec(x).Base
		if b <= 0 {
			continue
		}
		for u := b + 1; u <= x.V-1; u++ {
			if s.Vers[x.Node][u-1].Root == u {
				return false
			}
		}
	}
	return true
}

// NoVictimTextAfterPurge (N136): after DerivedPurge no live version holds text derived from the deleted document.
func NoVictimTextAfterPurge(p Params, s State) bool {
	for _, d := range p.Docs {
		if s.Ms[d] != "done" {
			continue
		}
		victims := p.Victims(s, d)
		for _, x := range p.Versions(s) {
			if p.live(s, x) && s.GDeriv(x).Intersects(victims) {
				return false
			}
		}
	}
	return true
}

// FailClosed (N135(4), N136): version rows are never deleted (version numbers are not reused), and a stub or missing
// row is never served.
func FailClosed(p Params, s State) bool {
	for _, n := range p.nodes() {
		if len(s.Vers[n]) != s.Hw[n] {
			return false
		}
	}
	for _, x := range p.Versions(s) {
		if !p.live(s, x) && p.VisN(s, x) {
			return false
		}
	}
	return true
}

// ReextractKeepsDerivedVisible (N135(2)-(3)): a derived version whose derivation holds only re-extracted
// (reextract-hidden) facts as hidden ones stays visible.
func ReextractKeepsDerivedVisible(p Params, s State) bool {
	for _, x := range p.Versions(s) {
		if !p.live(s, x) {
			continue
		}
		g := s.GDeriv(x)
		if !g.Intersects(s.HidRe) || s.RowCovered(x) {
			continue
		}
		clean := true
		for f := range g {
			if p.Victim(s, f) {
				clean = false
			}
		}
		if clean && !p.VisN(s, x) {
			return false
		}
	}
	return true
}

// Invariants maps each safety invariant name to its predicate.
var Invariants = map[string]func(Params, State) bool{
	NoDeletedDerivationServedName:     NoDeletedDerivationServed,
	NoInvalidatedDerivationServedName: NoInvalidatedDerivationServed,
	RestoreExactName:                  RestoreExact,
	TagFromLogName:                    TagFromLog,
	NoOverHidingName:                  NoOverHiding,
	AsOfNoLeakName:                    AsOfNoLeak,
	MaterializeCompleteName:           MaterializeComplete,
	NoGhostInvalidatedServedName:      NoGhostInvalidatedServed,
	NoPhantomVersionName:              NoPhantomVersion,
	EvidenceOutlivesFactsName:         EvidenceOutlivesFacts,
	BaseCurrentAtCommitName:           BaseCurrentAtCommit,
	NoLostRebuildName:                 NoLostRebuild,
	NoVictimTextAfterPurgeName:        NoVictimTextAfterPurge,
	FailClosedName:                    FailClosed,
	ReextractKeepsDerivedVisibleName:  ReextractKeepsDerivedVisible,
}

// Check returns the sorted names of the invariants that do not hold in s.
func Check(p Params, s State) []string {
	bound := make(map[string]func(State) bool, len(Invariants))
	for name, f := range Invariants {
		bound[name] = func(st State) bool { return f(p, st) }
	}
	return tla.Failed(s, bound)
}
