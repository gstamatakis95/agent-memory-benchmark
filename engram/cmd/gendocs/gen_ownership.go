package main

import (
	"fmt"
	"strings"
)

// transition is a row of the immutable table ownership_transitions (PLAN.md 3.3.1; N101, N125, N177).
type transition struct {
	Edge, Role            string
	From, FromReason      sqlVal
	To, ToReason          sqlVal
	EpochRule             string
	MoveEffect, TgtEffect string
	Note                  string
}

func readTransitions(ddl *DDL) ([]transition, error) {
	rows := ddl.Inserts["ownership_transitions"]
	if len(rows) == 0 {
		return nil, fmt.Errorf("ownership: no INSERT INTO ownership_transitions in the shard migrations")
	}
	out := make([]transition, len(rows))
	for i, r := range rows {
		out[i] = transition{Edge: r["edge"].S, Role: r["role_name"].S, From: r["from_state"],
			FromReason: r["from_reason"],
			To:         r["to_state"], ToReason: r["to_reason"], EpochRule: r["epoch_rule"].S,
			MoveEffect: r["move_effect"].S,
			TgtEffect:  r["target_effect"].S, Note: r["note"].S}
	}
	return out, nil
}

func stateLabel(st, reason sqlVal) string {
	switch {
	case st.Null:
		return "(none)"
	case !reason.Null:
		return st.S + "/" + reason.S
	}
	return st.S
}

// Per-edge column rules the trigger engram_check_ownership enforces beyond the columns of ownership_transitions
// (migrations/shard/0001_init.sql, the checks after the edge match): N179(1) ready_target stamps floor_lsn and, with
// the table's CHECKs for state 'ready', w_final; return_move clears floor_lsn; N177 freeze_delete needs
// move_id IS NULL.
// The integration test executes every generated statement against the DDL, so a rule missing here fails there.
var (
	edgeSet = map[string][]string{
		"ready_target": {"floor_lsn = $copy_end_lsn", "w_final = $w_final"},
		"return_move":  {"floor_lsn = NULL"},
	}
	edgeWhere = map[string]string{"freeze_delete": "move_id IS NULL"}
)

// transitionSQL renders the statement a role issues for an edge. It is the same table the trigger
// engram_check_ownership enforces: the statement names the guard (state, freeze reason, epoch) the trigger matches, the
// columns the move and target effects require and the per-edge column rules above, so a statement that does not look
// like this one is refused.
func transitionSQL(t transition) []string {
	where := func() string {
		w := "namespace_id = $1 AND state = '" + t.From.S + "'"
		if !t.FromReason.Null {
			w += " AND freeze_reason = '" + t.FromReason.S + "'"
		}
		if extra := edgeWhere[t.Edge]; extra != "" {
			w += " AND " + extra
		}
		return w + " AND epoch = $epoch"
	}
	switch {
	case t.From.Null:
		cols := []string{"namespace_id", "tenant_id", "shard_id", "epoch", "state"}
		vals := []string{"$1", "$tenant_id", "$shard_id", epochValue(t.EpochRule), "'" + t.To.S + "'"}
		if !t.ToReason.Null {
			cols, vals = append(cols, "freeze_reason"), append(vals, "'"+t.ToReason.S+"'")
		}
		if t.To.S == "incoming" {
			cols, vals = append(cols, "move_id", "move_epoch"), append(vals, "$move_id", "$move_epoch")
		}
		return []string{"INSERT INTO namespace_ownership (" + strings.Join(cols, ", ") + ")",
			"VALUES (" + strings.Join(vals, ", ") + ");"}
	case t.To.Null:
		return []string{"DELETE FROM namespace_ownership", " WHERE " + where() + ";"}
	}
	set := []string{"state = '" + t.To.S + "'"}
	switch {
	case !t.ToReason.Null:
		set = append(set, "freeze_reason = '"+t.ToReason.S+"'")
	case !t.FromReason.Null:
		set = append(set, "freeze_reason = NULL")
	}
	if t.EpochRule != "same" {
		set = append(set, "epoch = "+epochValue(t.EpochRule))
	}
	switch t.MoveEffect {
	case "open":
		set = append(set, "move_id = $move_id", "move_epoch = epoch + 1")
	case "close":
		set = append(set, "move_id = NULL", "move_epoch = NULL")
	case "retarget":
		set = append(set, "move_id = $new_move_id", "move_epoch = NULL")
	}
	switch t.TgtEffect {
	case "set":
		set = append(set, "target_shard_id = $target_shard_id", "target_epoch = epoch + 1")
	case "clear":
		set = append(set, "target_shard_id = NULL", "target_epoch = NULL")
	case "restore":
		set = append(set, "target_shard_id = $hint_shard_id", "target_epoch = $hint_epoch")
	}
	set = append(set, edgeSet[t.Edge]...)
	out := []string{"UPDATE namespace_ownership"}
	for i, s := range set {
		pre := "   SET "
		if i > 0 {
			pre = "       "
		}
		suf := ","
		if i == len(set)-1 {
			suf = ""
		}
		out = append(out, pre+s+suf)
	}
	return append(out, " WHERE "+where()+";")
}

func epochValue(rule string) string {
	switch rule {
	case "one":
		return "1"
	case "plus1":
		return "epoch + 1"
	case "same":
		return "epoch"
	}
	return "$new_epoch"
}

// genOwnership renders ownership_transitions as a table and as the statement of every edge and role.
func genOwnership(ddl *DDL) (string, error) {
	ts, err := readTransitions(ddl)
	if err != nil {
		return "", err
	}
	d := NewDoc("Ownership transitions", "`migrations/shard` (the rows of `ownership_transitions`)",
		"PLAN.md 3.3.1, 5.5; register N101, N125, N177")
	d.Para("`ownership_transitions` is the immutable edge table of the ownership state machine. The trigger " +
		"`engram_check_ownership` accepts an `INSERT`, `UPDATE` or `DELETE` of a `namespace_ownership` row only " +
		"when it matches an edge of `current_user`, then checks the epoch rule and the move and target column " +
		"effects. A state " +
		"pair that exists for no role is `23514`; a pair that exists only for other roles is `42501`. Epoch " +
		"rules: `one` = epoch 1, `any`, `same`, `plus1` = exactly +1, `greater` = strictly greater. The " +
		"statements below are " +
		"the rendering of each row (`$epoch` is the epoch the caller read, `$new_epoch` the epoch the rule allows).")
	d.Para("Beyond the columns of the table the trigger enforces three per-edge rules, which the statements carry: " +
		"`ready_target` stamps `floor_lsn` and `w_final` (N179(1), N147), `return_move` clears `floor_lsn`, and " +
		"`freeze_delete` needs `move_id IS NULL` (N177). `$1` is the namespace id; the other `$name` placeholders " +
		"are the values the caller supplies. The T3 test `TestIso_Ownership_Transitions` executes every statement " +
		"below, as the role of its row, against the real DDL.")
	d.Para("%d edges, %d rows (an edge appears once per role and source state).", countEdges(ts), len(ts))
	d.H2("Edge table")
	var rows [][]string
	for _, t := range ts {
		rows = append(rows, []string{"`" + t.Edge + "`", "`" + t.Role + "`", stateLabel(t.From, t.FromReason),
			stateLabel(t.To, t.ToReason), t.EpochRule, t.MoveEffect, t.TgtEffect})
	}
	d.Table([]string{"Edge", "Role", "From", "To", "Epoch", "Move", "Target"}, rows)
	d.H2("Statements")
	for _, t := range ts {
		from, to := stateLabel(t.From, t.FromReason), stateLabel(t.To, t.ToReason)
		d.H3(fmt.Sprintf("%s (%s): %s -> %s", t.Edge, t.Role, from, to))
		d.Code("sql", transitionSQL(t))
		if t.Note != "" {
			d.Para("%s", t.Note)
		}
	}
	return d.String(), nil
}

func countEdges(ts []transition) int {
	seen := map[string]bool{}
	for _, t := range ts {
		seen[t.Edge] = true
	}
	return len(seen)
}
