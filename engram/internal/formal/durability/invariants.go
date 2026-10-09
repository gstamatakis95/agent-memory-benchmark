// Package durability is the invariant code of formal/tla/Durability.tla (PLAN.md section 7.2.3, N122, N134, N146, N150,
// N182): acknowledged deletes and invalidations survive restore and failover. The functions read a plain Go value model
// of the specification's variables; the intent replay tests build a State from the shard's deletion_log, the intent
// objects and the operations' acknowledgements.
package durability

import "github.com/gstamatakis95/engram/internal/formal/tla"

// ShapeOp is one entry of the fixed Shape of operations: the subject ("y" document, "x" fact) and the kind ("del",
// "ret", "inv", "res").
type ShapeOp struct {
	S, K string
}

// Params are the constants the invariants read: the Shape, whose operation i (1-based) is Shape[i-1].
type Params struct {
	Shape []ShapeOp
}

// ShapeDoc and ShapeFact are the two shapes of the shipped configurations.
var (
	ShapeDoc  = []ShapeOp{{"y", "del"}, {"y", "del"}, {"y", "ret"}}
	ShapeFact = []ShapeOp{{"x", "inv"}, {"x", "res"}, {"x", "inv"}}
)

// Op is one operation of the Shape (the spec's ops[i] record). Ph is "none", "new", "committed", "acked" or "failed".
type Op struct {
	Ph   string
	Ep   int          // namespace epoch of its marker
	It   int          // issue time
	At   int          // recorded deleted_at (issue time minus clock skew)
	Ct   int          // commit time
	Cn0  int          // position in the commit order (0: never committed)
	Eff  tla.Set[int] // the versions a delete covers
	Prev int          // prev_operation_id
	Obs  int          // the marker this attempt relies on (its own, or the one a duplicate observed)
	Rd   int          // the previous entry read by the transaction (-1: not begun)
	Be   int          // epoch at TxnBegin
}

// Entry is a marker row the shard has applied: [op, eff, ep].
type Entry struct {
	Op  int
	Eff tla.Set[int]
	Ep  int
}

// State mirrors the variables of Durability.tla that the invariants read. Ops is keyed by operation id 1..N.
type State struct {
	Ops     map[int]Op
	Intents tla.Set[int] // operation ids whose intent object exists
	Dbq     []Entry      // marker rows the shard applied, in application order
	Sst     string       // "active" or "restoring"
	Ta      bool         // the tenant delete was acknowledged
	Tdp     int          // tenant delete phase: 0..3, 4 = reverted by a catalog restore
}

// Names of the invariants, as they appear in formal/tla/Durability.tla and formal/tla/EXPECT.
const (
	AckImpliesIntentName          = "AckImpliesIntent"
	AckedDeleteSurvivesName       = "AckedDeleteSurvives"
	IntentOrderLastWinsName       = "IntentOrderLastWins"
	NoUnackedEffectOnLaterAckName = "NoUnackedEffectOnLaterAck"
)

const (
	tenantRevertedPhase = 4
	retKind             = "ret"
	delKind             = "del"
	invKind             = "inv"
	ackedPhase          = "acked"
	factSubject         = "x"
)

func (p Params) subject(i int) string { return p.Shape[i-1].S }
func (p Params) kind(i int) string    { return p.Shape[i-1].K }

// DB is the set of operations whose marker the shard has applied.
func (s State) DB() tla.Set[int] {
	out := tla.NewSet[int]()
	for _, e := range s.Dbq {
		out.Add(e.Op)
	}
	return out
}

// LastOn is the operation of the last non-retain entry of subject sub in application order, 0 if none.
func (p Params) LastOn(s State, sub string) int {
	last := 0
	for _, e := range s.Dbq {
		if p.subject(e.Op) == sub && p.kind(e.Op) != retKind {
			last = e.Op
		}
	}
	return last
}

// HiddenX: the fact subject's last applied marker is an invalidation.
func (p Params) HiddenX(s State) bool {
	l := p.LastOn(s, factSubject)
	return l != 0 && p.kind(l) == invKind
}

// LastCommitted is the operation of subject sub that committed last (by commit order), 0 if none committed.
func (p Params) LastCommitted(s State, sub string) int {
	best := 0
	for i := 1; i <= len(p.Shape); i++ {
		if p.subject(i) == sub && s.Ops[i].Cn0 > 0 && (best == 0 || s.Ops[i].Cn0 > s.Ops[best].Cn0) {
			best = i
		}
	}
	return best
}

// AckImpliesIntent: every acknowledged delete or invalidation has a durable intent.
func AckImpliesIntent(p Params, s State) bool {
	for i := 1; i <= len(p.Shape); i++ {
		o := s.Ops[i]
		if o.Ph == ackedPhase && p.kind(i) != retKind && !s.Intents.Has(o.Obs) {
			return false
		}
	}
	return true
}

// AckedDeleteSurvives: the marker an acknowledged delete relies on is in force whenever the shard serves, and an
// acknowledged tenant delete is never reverted (N182).
func AckedDeleteSurvives(p Params, s State) bool {
	if s.Sst == "active" {
		db := s.DB()
		for i := 1; i <= len(p.Shape); i++ {
			o := s.Ops[i]
			if o.Ph == ackedPhase && p.kind(i) == delKind && !db.Has(o.Obs) {
				return false
			}
		}
	}
	return !s.Ta || s.Tdp != tenantRevertedPhase
}

// IntentOrderLastWins: after a restore the state is that of the operation that committed last.
func IntentOrderLastWins(p Params, s State) bool {
	if s.Sst != "active" {
		return true
	}
	l := p.LastCommitted(s, factSubject)
	if l == 0 || s.Ops[l].Ph != ackedPhase {
		return true
	}
	return p.HiddenX(s) == (p.kind(l) == invKind)
}

// NoUnackedEffectOnLaterAck: every applied marker was written by a committed operation and covers only versions
// committed before it.
func NoUnackedEffectOnLaterAck(p Params, s State) bool {
	for _, e := range s.Dbq {
		if p.kind(e.Op) == retKind {
			continue
		}
		if s.Ops[e.Op].Cn0 <= 0 {
			return false
		}
		for r := range e.Eff {
			if r != 0 && s.Ops[r].Cn0 >= s.Ops[e.Op].Cn0 {
				return false
			}
		}
	}
	return true
}

// Invariants maps each invariant name to its predicate.
var Invariants = map[string]func(Params, State) bool{
	AckImpliesIntentName:          AckImpliesIntent,
	AckedDeleteSurvivesName:       AckedDeleteSurvives,
	IntentOrderLastWinsName:       IntentOrderLastWins,
	NoUnackedEffectOnLaterAckName: NoUnackedEffectOnLaterAck,
}

// Check returns the sorted names of the invariants that do not hold in s.
func Check(p Params, s State) []string {
	bound := make(map[string]func(State) bool, len(Invariants))
	for name, f := range Invariants {
		bound[name] = func(st State) bool { return f(p, st) }
	}
	return tla.Failed(s, bound)
}
