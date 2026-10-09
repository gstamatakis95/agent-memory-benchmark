package fsm_test

import (
	"errors"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/fsm"
)

func TestTable_IllegalTransition(t *testing.T) {
	tests := []struct {
		name     string
		from     fsm.OwnershipState
		edge     fsm.OwnershipEdge
		role     string
		roleOnly bool
	}{
		{"no such edge from the state", fsm.OwnershipFrozenDelete, fsm.EdgeThawMove, fsm.RoleMove, false},
		{"frozen/delete has no way back", fsm.OwnershipFrozenDelete, fsm.EdgeRestoreDone, fsm.RoleAdmin, false},
		{"unknown edge", fsm.OwnershipActive, "teleport", fsm.RoleAdmin, false},
		{"engram_move never sets reason delete (N101)", fsm.OwnershipActive, fsm.EdgeFreezeDelete, fsm.RoleMove, true},
		{"engram_app cannot freeze for a move", fsm.OwnershipActive, fsm.EdgeFreezeMove, fsm.RoleApp, true},
		{"empty role matches no role-specific row", fsm.OwnershipActive, fsm.EdgeFreezeMove, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fsm.Ownership.Next(tc.from, tc.edge, tc.role)
			if !errors.Is(err, fsm.ErrIllegalTransition) {
				t.Fatalf("Next = %q, %v; want ErrIllegalTransition", got, err)
			}
			var te *fsm.TransitionError
			if !errors.As(err, &te) || te.RoleOnly != tc.roleOnly {
				t.Fatalf("error %v: RoleOnly = %v, want %v", err, te != nil && te.RoleOnly, tc.roleOnly)
			}
			if got != "" {
				t.Errorf("a refused transition must return the zero state, got %q", got)
			}
		})
	}
}

func TestTable_LegalTransition(t *testing.T) {
	tests := []struct {
		from fsm.OwnershipState
		edge fsm.OwnershipEdge
		role string
		want fsm.OwnershipState
	}{
		{fsm.OwnershipNone, fsm.EdgeCreate, fsm.RoleApp, fsm.OwnershipActive},
		{fsm.OwnershipActive, fsm.EdgeFreezeMove, fsm.RoleMove, fsm.OwnershipFrozenMove},
		{fsm.OwnershipFrozenMove, fsm.EdgeCutoverC, fsm.RoleMove, fsm.OwnershipMovedOut},
		{fsm.OwnershipIncoming, fsm.EdgeReadyTarget, fsm.RoleMove, fsm.OwnershipReady},
		{fsm.OwnershipReady, fsm.EdgeActivateTarget, fsm.RoleMove, fsm.OwnershipActive},
		{fsm.OwnershipFrozenRestore, fsm.EdgeRestoreDelete, fsm.RoleAdmin, fsm.OwnershipFrozenDelete},
		{fsm.OwnershipFrozenDelete, fsm.EdgePurgeDeleted, fsm.RoleAdmin, fsm.OwnershipNone},
	}
	for _, tc := range tests {
		got, err := fsm.Ownership.Next(tc.from, tc.edge, tc.role)
		if err != nil || got != tc.want {
			t.Errorf("Next(%q, %s, %s) = %q, %v; want %q", tc.from, tc.edge, tc.role, got, err, tc.want)
		}
	}
}

func TestTable_AnyRoleRowAndEdges(t *testing.T) {
	type S string
	type E string
	tab := fsm.New("t", []fsm.Row[S, E]{
		{From: "a", Edge: "z", To: "b"},
		{From: "a", Edge: "y", Role: "r1", To: "c"},
		{From: "a", Edge: "y", Role: "r2", To: "d"},
		{From: "b", Edge: "z", To: "a"},
	})
	if to, err := tab.Next("a", "z", "anyone"); err != nil || to != "b" {
		t.Errorf("an empty-role row admits every role: %q, %v", to, err)
	}
	if to, _ := tab.Next("a", "y", "r2"); to != "d" {
		t.Errorf("role-specific row r2 = %q, want d", to)
	}
	if got := tab.Edges("a"); !slices.Equal(got, []E{"y", "z"}) {
		t.Errorf("Edges(a) = %v, want sorted [y z] without duplicates", got)
	}
	got := tab.Edges("a")
	got[0] = "mutated"
	if tab.Edges("a")[0] != "y" {
		t.Error("Edges must return a copy")
	}
	if got := tab.States(); !slices.Equal(got, []S{"a", "b", "c", "d"}) {
		t.Errorf("States() = %v, want [a b c d] in order of first appearance", got)
	}
	if got := tab.Edges("nowhere"); len(got) != 0 {
		t.Errorf("Edges of an unknown state = %v", got)
	}
}

func TestTable_DuplicateRowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a duplicate (from, edge, role) row must panic at construction")
		}
	}()
	type S string
	type E string
	fsm.New("dup", []fsm.Row[S, E]{{From: "a", Edge: "e", To: "b"}, {From: "a", Edge: "e", To: "c"}})
}

func TestFSM_OperationMonotoneAndFinal(t *testing.T) {
	for _, s := range []fsm.OperationState{fsm.OpSucceeded, fsm.OpFailed, fsm.OpCancelled} {
		if !s.IsTerminal() || len(fsm.Operation.Edges(s)) != 0 {
			t.Errorf("%s must be terminal with no outgoing edge", s)
		}
	}
	for _, s := range []fsm.OperationState{fsm.OpPending, fsm.OpRunning, fsm.OpDeferred} {
		if s.IsTerminal() {
			t.Errorf("%s must not be terminal", s)
		}
	}
	// No edge enters PENDING: a retry of a FAILED operation is a new operation (N139).
	for _, from := range fsm.Operation.States() {
		for _, e := range fsm.Operation.Edges(from) {
			if to, _ := fsm.Operation.Next(from, e, ""); to == fsm.OpPending {
				t.Errorf("edge %s from %s re-enters PENDING", e, from)
			}
		}
	}
	steps := []struct {
		e    fsm.OperationEdge
		want fsm.OperationState
	}{{fsm.OpEdgeStart, fsm.OpRunning}, {fsm.OpEdgeDefer, fsm.OpDeferred}, {fsm.OpEdgeResume, fsm.OpRunning},
		{fsm.OpEdgeSucceed, fsm.OpSucceeded}}
	cur := fsm.OpPending
	for _, st := range steps {
		next, err := fsm.Operation.Next(cur, st.e, "")
		if err != nil || next != st.want {
			t.Fatalf("%s --%s--> %q, %v; want %q", cur, st.e, next, err, st.want)
		}
		cur = next
	}
	if _, err := fsm.Operation.Next(fsm.OpDeferred, fsm.OpEdgeSucceed, ""); !errors.Is(err, fsm.ErrIllegalTransition) {
		t.Error("a deferred operation must resume before it can succeed")
	}
	if _, err := fsm.Operation.Next(fsm.OpFailed, fsm.OpEdgeStart, ""); !errors.Is(err, fsm.ErrIllegalTransition) {
		t.Error("FAILED -> RUNNING must be illegal")
	}
}

func TestFSM_MoveNoRollbackAfterCommit(t *testing.T) {
	after := []fsm.MoveState{fsm.MoveCommitted, fsm.MoveCleaning, fsm.MoveDone, fsm.MoveLost, fsm.MoveRolledBack}
	for _, from := range after {
		if _, err := fsm.Move.Next(from, fsm.MoveEdgeRollback, ""); !errors.Is(err, fsm.ErrIllegalTransition) {
			t.Errorf("rollback from %s must be illegal (the catalog CAS is the point of no return)", from)
		}
	}
	for _, from := range []fsm.MoveState{fsm.MovePlanned, fsm.MoveFrozen, fsm.MoveCopied, fsm.MoveCutover} {
		if to, err := fsm.Move.Next(from, fsm.MoveEdgeRollback, ""); err != nil || to != fsm.MoveRolledBack {
			t.Errorf("rollback from %s = %q, %v; want rolled_back", from, to, err)
		}
	}
}

// pair is a (from, to) state pair as the SQL files spell it.
type pair struct{ from, to string }

func readPlanSQL(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/plan/sql/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestFSM_MoveMatchesSQL pins fsm.Move to the pairs of catalog_check_move_transition until M0.8 generates the table.
func TestFSM_MoveMatchesSQL(t *testing.T) {
	sql := readPlanSQL(t, "catalog_schema.sql")
	start := strings.Index(sql, "IF (OLD.state, NEW.state) IN (")
	end := start + strings.Index(sql[start:], "THEN")
	var want []pair
	for _, m := range regexp.MustCompile(`\('(\w+)', '(\w+)'\)`).FindAllStringSubmatch(sql[start:end], -1) {
		want = append(want, pair{m[1], m[2]})
	}
	var got []pair
	for _, from := range fsm.Move.States() {
		for _, e := range fsm.Move.Edges(from) {
			to, _ := fsm.Move.Next(from, e, "")
			got = append(got, pair{string(from), string(to)})
		}
	}
	less := func(a, b pair) int { return strings.Compare(a.from+">"+a.to, b.from+">"+b.to) }
	slices.SortFunc(want, less)
	slices.SortFunc(got, less)
	if len(want) != 12 || !slices.Equal(got, want) {
		t.Fatalf("move table differs from catalog_check_move_transition:\n got  %v\n want %v", got, want)
	}
}

// TestFSM_OwnershipMatchesSQL pins fsm.OwnershipRows to the INSERT of ownership_transitions in shard_schema.sql.
func TestFSM_OwnershipMatchesSQL(t *testing.T) {
	sql := readPlanSQL(t, "shard_schema.sql")
	start := strings.Index(sql, "INSERT INTO ownership_transitions")
	end := start + strings.Index(sql[start:], "'DELETE after NamespacePurged'")
	val := func(v, reason string) string {
		if v == "NULL" {
			return ""
		}
		v = strings.Trim(v, "'")
		if v == "frozen" {
			return "frozen/" + strings.Trim(reason, "'")
		}
		return v
	}
	re := regexp.MustCompile(`\(\s*'(\w+)',\s*'(\w+)',\s*(NULL|'\w+'),\s*(NULL|'\w+'),\s*(NULL|'\w+'),\s*(NULL|'\w+'),`)
	var want []string
	for _, m := range re.FindAllStringSubmatch(sql[start:end], -1) {
		want = append(want, strings.Join([]string{m[1], m[2], val(m[3], m[4]), val(m[5], m[6])}, "|"))
	}
	var got []string
	for _, r := range fsm.OwnershipRows {
		got = append(got, strings.Join([]string{string(r.Edge), r.Role, string(r.From), string(r.To)}, "|"))
	}
	if len(want) != 30 || !slices.Equal(got, want) {
		t.Fatalf("ownership rows differ from ownership_transitions (%d parsed):\n got  %v\n want %v", len(want), got,
			want)
	}
}
