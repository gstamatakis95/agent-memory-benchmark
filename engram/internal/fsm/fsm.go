// Package fsm holds the transition tables of the three state machines of PLAN.md section 2.1 (register N132, N133a):
// the namespace ownership row, operations and moves. Legal transitions are data, so code, SQL and the TLA+
// specifications share one table. It is a leaf: the standard library only, no I/O.
package fsm

import (
	"errors"
	"fmt"
	"slices"
)

// ErrIllegalTransition is returned (wrapped in a *TransitionError) for a (from, edge, role) triple that no row allows.
// Callers render it as errs.PreconditionFailed.
var ErrIllegalTransition = errors.New("fsm: illegal transition")

// Table is a transition table keyed by (from state, edge, role). The ownership table is GENERATED from
// ownership_transitions (section 3.3.1), the move and operation tables from section 5; the TLA+ specs use the same
// rows.
type Table[S, E ~string] interface {
	// Next returns the state reached, or an error wrapping ErrIllegalTransition for an illegal pair.
	Next(from S, edge E, role string) (S, error)
	// Edges lists the edges leaving from (any role), sorted, without duplicates.
	Edges(from S) []E
	// States lists every state the table mentions, in order of first appearance, without duplicates.
	States() []S
}

// Row is one allowed transition. An empty Role means every role may take the edge.
type Row[S, E ~string] struct {
	From S
	Edge E
	Role string
	To   S
}

// TransitionError describes a refused transition. RoleOnly is true when the (from, edge) pair exists for another role,
// which the database reports as a privilege error rather than a check violation (N101).
type TransitionError struct {
	Table    string
	From     string
	Edge     string
	Role     string
	RoleOnly bool
}

// Error implements error.
func (e *TransitionError) Error() string {
	why := "no such edge"
	if e.RoleOnly {
		why = "edge exists only for other roles"
	}
	return fmt.Sprintf("%s: %s table: %q --%s--> as role %q (%s)", ErrIllegalTransition.Error(), e.Table, e.From,
		e.Edge, e.Role, why)
}

// Is makes errors.Is(err, ErrIllegalTransition) true.
func (e *TransitionError) Is(target error) bool { return target == ErrIllegalTransition }

type key[S, E ~string] struct {
	from S
	edge E
	role string
}

type table[S, E ~string] struct {
	name   string
	next   map[key[S, E]]S
	anyOf  map[key[S, E]]bool // (from, edge) pairs that exist for at least one role
	edges  map[S][]E
	states []S
}

// New builds a Table from rows. A duplicate (from, edge, role) is a programming error and panics at construction.
func New[S, E ~string](name string, rows []Row[S, E]) Table[S, E] {
	t := &table[S, E]{name: name, next: map[key[S, E]]S{}, anyOf: map[key[S, E]]bool{}, edges: map[S][]E{}}
	seen := map[S]bool{}
	addState := func(s S) {
		if s != "" && !seen[s] {
			seen[s] = true
			t.states = append(t.states, s)
		}
	}
	for _, r := range rows {
		k := key[S, E]{r.From, r.Edge, r.Role}
		if _, dup := t.next[k]; dup {
			panic(fmt.Sprintf("fsm %s: duplicate row %q --%s--> role %q", name, r.From, r.Edge, r.Role))
		}
		t.next[k] = r.To
		t.anyOf[key[S, E]{from: r.From, edge: r.Edge}] = true
		if !slices.Contains(t.edges[r.From], r.Edge) {
			t.edges[r.From] = append(t.edges[r.From], r.Edge)
		}
		addState(r.From)
		addState(r.To)
	}
	for _, es := range t.edges {
		slices.Sort(es)
	}
	return t
}

func (t *table[S, E]) Next(from S, edge E, role string) (S, error) {
	if to, ok := t.next[key[S, E]{from, edge, role}]; ok {
		return to, nil
	}
	if to, ok := t.next[key[S, E]{from, edge, ""}]; ok {
		return to, nil
	}
	return "", &TransitionError{Table: t.name, From: string(from), Edge: string(edge), Role: role,
		RoleOnly: t.anyOf[key[S, E]{from: from, edge: edge}]}
}

func (t *table[S, E]) Edges(from S) []E { return slices.Clone(t.edges[from]) }

func (t *table[S, E]) States() []S { return slices.Clone(t.states) }
