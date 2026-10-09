package main

import (
	"fmt"
	"sort"
	"strings"
)

// classOrder is the order of the classes in the document: the three move classes of N137 and the shard-local tables.
var classOrder = []string{"insert-only", "mutable", "expiring", "shard-local"}

var classNote = map[string]string{
	"insert-only": "Write-once rows, no UPDATE path; only the expunge deletes (N113); each has ins_seq (N137).",
	"mutable":     "Narrow mutable state in fillfactor 50 to 70 tables that stay HOT (N113, N137).",
	"expiring":    "Expiring or derived state, maintained by sweepers rather than written on the commit path (N137).",
	"shard-local": "State machine or log of the shard itself: the shard-level tables of section 3.3.1 (N124).",
}

// genClasses renders the table classes. The DDL carries the one list (COMMENT ON TABLE 'class: <tag>'); PLAN.md N113,
// N124 and section 3 render from it, and no hand-written class list exists. The run fails when a table has no class or
// two, or a class names a table that no migration creates.
func genClasses(shard *DDL) (string, []string, error) {
	if len(shard.Classes) == 0 {
		return "", nil, fmt.Errorf("classes: no table class list found in the shard migrations")
	}
	var problems []string
	owner := map[string]string{}
	for _, c := range sortedKeys(shard.Classes) {
		for _, t := range shard.Classes[c] {
			if prev, dup := owner[t]; dup {
				addf(&problems, "classes: table %s is in two classes (%s and %s)", t, prev, c)
			}
			owner[t] = c
		}
	}
	created := map[string]bool{}
	for _, t := range shard.Tables {
		created[t] = true
		if _, ok := owner[t]; !ok {
			addf(&problems, "classes: table %s has no class tag (COMMENT ON TABLE 'class: <tag>', N113)", t)
		}
	}
	for _, t := range sortedKeys(owner) {
		if !created[t] {
			addf(&problems, "classes: the %s list names %s, which no migration creates", owner[t], t)
		}
	}
	for c := range shard.Classes {
		if !contains(classOrder, c) {
			addf(&problems, "classes: unknown class %q", c)
		}
	}
	d := NewDoc("Table classes", "`migrations/shard` (COMMENT ON TABLE 'class: <tag>')", "register N113, N124, N137")
	d.Para("Every table of the shard schema carries one class tag. The DDL holds the one list and the move, the "+
		"expunge and `engramlint sql` read it; this document renders it. %d tables.", len(created))
	for _, c := range classOrder {
		ts := append([]string(nil), shard.Classes[c]...)
		sort.Strings(ts)
		d.H2(fmt.Sprintf("%s (%d)", c, len(ts)))
		d.Para("%s", classNote[c])
		d.Bullet(strings.Join(quoteEach(ts), ", "))
		d.EndList()
	}
	return d.String(), problems, nil
}

func quoteEach(ts []string) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = "`" + t + "`"
	}
	return out
}
