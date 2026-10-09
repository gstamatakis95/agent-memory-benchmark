package tla

import (
	"reflect"
	"testing"
)

func TestSet_Operators(t *testing.T) {
	a, b := NewSet(1, 2, 3), NewSet(3, 4)
	if got := Sorted(a.Union(b)); !reflect.DeepEqual(got, []int{1, 2, 3, 4}) {
		t.Fatalf("Union = %v", got)
	}
	if got := Sorted(a.Inter(b)); !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("Inter = %v", got)
	}
	if got := Sorted(a.Minus(b)); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("Minus = %v", got)
	}
	if !NewSet(1).SubsetOf(a) || a.SubsetOf(b) || !a.Equal(NewSet(3, 2, 1)) || a.Equal(b) {
		t.Fatal("SubsetOf/Equal")
	}
	if !a.Intersects(b) || a.Intersects(NewSet(9)) || !NewSet[int]().Empty() || a.Len() != 3 {
		t.Fatal("Intersects/Empty/Len")
	}
	c := a.Clone().Add(7)
	if a.Has(7) || !c.Has(7) {
		t.Fatal("Clone must not alias")
	}
	var zero Set[int]
	if zero.Has(1) || zero.Len() != 0 {
		t.Fatal("the zero Set reads as empty")
	}
}

func TestMaxHelpers(t *testing.T) {
	if MaxInt(NewSet[int]()) != 0 || MaxInt(NewSet(2, 9, 4)) != 9 || Max2(3, 5) != 5 || Max2(5, 3) != 5 {
		t.Fatal("MaxInt/Max2")
	}
}

func TestFailed_SortedNames(t *testing.T) {
	checks := map[string]func(int) bool{
		"B": func(x int) bool { return x > 5 },
		"A": func(x int) bool { return x > 1 },
		"C": func(x int) bool { return true },
	}
	if got := Failed(0, checks); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("Failed = %v", got)
	}
	if got := Failed(9, checks); got != nil {
		t.Fatalf("Failed = %v", got)
	}
}
