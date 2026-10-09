// Package tla holds the few value helpers the invariant packages under internal/formal share: a generic finite set with
// the operators the TLA+ specifications use (union, intersection, difference, subset). It imports the standard library
// only; the invariant code is meant to run against a plain Go value model of a specification's variables (PLAN.md
// section 7.4(a)).
package tla

import "sort"

// Set is a finite set of comparable values (TLA+ `SUBSET S` element). The zero value is the empty set and is usable for
// reads; writers must construct sets with NewSet or make.
type Set[T comparable] map[T]struct{}

// NewSet returns the set holding xs.
func NewSet[T comparable](xs ...T) Set[T] {
	s := make(Set[T], len(xs))
	for _, x := range xs {
		s[x] = struct{}{}
	}
	return s
}

// Has reports x \in s.
func (s Set[T]) Has(x T) bool {
	_, ok := s[x]
	return ok
}

// Len is Cardinality(s).
func (s Set[T]) Len() int { return len(s) }

// Empty reports s = {}.
func (s Set[T]) Empty() bool { return len(s) == 0 }

// Clone returns a copy of s.
func (s Set[T]) Clone() Set[T] {
	c := make(Set[T], len(s))
	for x := range s {
		c[x] = struct{}{}
	}
	return c
}

// Add inserts xs into s in place and returns s.
func (s Set[T]) Add(xs ...T) Set[T] {
	for _, x := range xs {
		s[x] = struct{}{}
	}
	return s
}

// Union is s \cup t.
func (s Set[T]) Union(t Set[T]) Set[T] {
	u := s.Clone()
	for x := range t {
		u[x] = struct{}{}
	}
	return u
}

// Inter is s \cap t.
func (s Set[T]) Inter(t Set[T]) Set[T] {
	i := make(Set[T])
	for x := range s {
		if t.Has(x) {
			i[x] = struct{}{}
		}
	}
	return i
}

// Minus is s \ t.
func (s Set[T]) Minus(t Set[T]) Set[T] {
	d := make(Set[T])
	for x := range s {
		if !t.Has(x) {
			d[x] = struct{}{}
		}
	}
	return d
}

// SubsetOf is s \subseteq t.
func (s Set[T]) SubsetOf(t Set[T]) bool {
	for x := range s {
		if !t.Has(x) {
			return false
		}
	}
	return true
}

// Equal is s = t.
func (s Set[T]) Equal(t Set[T]) bool { return len(s) == len(t) && s.SubsetOf(t) }

// Intersects reports s \cap t # {}.
func (s Set[T]) Intersects(t Set[T]) bool {
	for x := range s {
		if t.Has(x) {
			return true
		}
	}
	return false
}

// Sorted returns the elements of an ordered set in ascending order.
func Sorted[T interface{ ~int | ~string }](s Set[T]) []T {
	out := make([]T, 0, len(s))
	for x := range s {
		out = append(out, x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// MaxInt is MaxSet for sets of ints: the largest element, or 0 for the empty set (the specs' MaxSet).
func MaxInt(s Set[int]) int {
	m := 0
	for x := range s {
		if x > m {
			m = x
		}
	}
	return m
}

// Max2 is the specs' Max2.
func Max2(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// Failed returns the names of the invariants in checks that do not hold, in a stable (sorted) order. checks maps an
// invariant name to its predicate over the state.
func Failed[S any](state S, checks map[string]func(S) bool) []string {
	var out []string
	for name, f := range checks {
		if !f(state) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
