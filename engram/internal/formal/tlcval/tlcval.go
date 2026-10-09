// Package tlcval decodes the values of a TLC error-trace state (the JSON encoding of trace.ParseTLA) into plain Go
// values. It exists for the cross-check tests of the invariant packages, which take the counterexample states TLC
// printed for the must-fail configurations, rebuild the Go value model from them and require the Go invariant code to
// agree with the specification's oracle. The functions panic on a value of the wrong shape: the input is a TLC log
// committed with the specifications, and a wrong shape is a bug in a decoder, to be seen at once in a test.
package tlcval

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gstamatakis95/engram/internal/formal/tla"
)

func bad(what string, v any) string { return fmt.Sprintf("tlcval: not %s: %v", what, v) }

// Int decodes a TLA+ integer.
func Int(v any) int {
	n, ok := v.(json.Number)
	if !ok {
		panic(bad("an integer", v))
	}
	i, err := strconv.Atoi(n.String())
	if err != nil {
		panic(bad("an integer", v))
	}
	return i
}

// Bool decodes TRUE or FALSE.
func Bool(v any) bool {
	b, ok := v.(bool)
	if !ok {
		panic(bad("a boolean", v))
	}
	return b
}

// Str decodes a TLA+ string or a model value (the name without its "@").
func Str(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimPrefix(x, "@")
	case map[string]any:
		if s, ok := x["$str"].(string); ok {
			return s
		}
	}
	panic(bad("a string or model value", v))
}

// Items decodes a set or a tuple into its elements (a set in sorted order of its rendering is not promised).
func Items(v any) []any {
	switch x := v.(type) {
	case []any:
		return x
	case map[string]any:
		if s, ok := x["$set"].([]any); ok {
			return s
		}
	}
	panic(bad("a set or a tuple", v))
}

// Ints decodes a set or tuple of integers.
func Ints(v any) []int {
	items := Items(v)
	out := make([]int, len(items))
	for i, e := range items {
		out[i] = Int(e)
	}
	return out
}

// IntSet decodes a set of integers.
func IntSet(v any) tla.Set[int] { return tla.NewSet(Ints(v)...) }

// Rec decodes a record.
func Rec(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		panic(bad("a record", v))
	}
	return m
}

// Entry is one key and value of a function.
type Entry struct{ K, V any }

// Fn decodes a function: (k :> v @@ ...), a tuple (keys 1..n) or a record whose fields are the keys (the keys of a
// record are strings). Entries are in the order TLC printed them.
func Fn(v any) []Entry {
	switch x := v.(type) {
	case []any:
		out := make([]Entry, len(x))
		for i, e := range x {
			out[i] = Entry{json.Number(strconv.Itoa(i + 1)), e}
		}
		return out
	case map[string]any:
		if pairs, ok := x["$fn"].([]any); ok {
			out := make([]Entry, len(pairs))
			for i, p := range pairs {
				kv := p.([]any)
				out[i] = Entry{kv[0], kv[1]}
			}
			return out
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]Entry, len(keys))
		for i, k := range keys {
			out[i] = Entry{k, x[k]}
		}
		return out
	}
	panic(bad("a function", v))
}

// FnStr decodes a function with string or model-value keys into a map.
func FnStr[T any](v any, val func(any) T) map[string]T {
	out := map[string]T{}
	for _, e := range Fn(v) {
		out[Str(e.K)] = val(e.V)
	}
	return out
}

// FnInt decodes a function with integer keys into a map.
func FnInt[T any](v any, val func(any) T) map[int]T {
	out := map[int]T{}
	for _, e := range Fn(v) {
		out[Int(e.K)] = val(e.V)
	}
	return out
}

// Render gives a canonical text for a decoded value, usable as a map key for compound TLA+ values (tuples, sets).
func Render(v any) string {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = Render(e)
		}
		return "<<" + strings.Join(parts, ",") + ">>"
	case map[string]any:
		if s, ok := x["$set"].([]any); ok {
			parts := make([]string, len(s))
			for i, e := range s {
				parts[i] = Render(e)
			}
			sort.Strings(parts)
			return "{" + strings.Join(parts, ",") + "}"
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = k + "=" + Render(x[k])
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	return fmt.Sprint(v)
}
