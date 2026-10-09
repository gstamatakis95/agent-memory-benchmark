//go:build integration

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

var wantPlaceholderRe = regexp.MustCompile(`\$[a-z_0-9]+`)

// seedRow is the ownership row an edge starts from. The seeds are written by the superuser with the ownership trigger
// switched off, because the trigger refuses to create an arbitrary state: the point of the test is the edge, not the
// way to the state.
type seedRow struct {
	state, reason            string
	moveID, moveEpoch        any
	targetShard, targetEpoch any
	wFinal, floorLSN         any
}

const (
	testEpoch    = 5
	testNewEpoch = 9
	moveUUID     = "0190c000-0000-7000-8000-0000000000a1"
	newMoveUUID  = "0190c000-0000-7000-8000-0000000000a2"
)

func seedFor(t transition) seedRow {
	r := seedRow{state: t.From.S, reason: t.FromReason.S}
	source := t.From.S == "active" || t.From.S == "frozen"
	switch {
	case t.From.S == "incoming" || t.From.S == "ready":
		r.moveID = moveUUID
	case t.MoveEffect == "close" || (t.From.S == "frozen" && t.FromReason.S == "move"):
		r.moveID = moveUUID
		if source {
			r.moveEpoch = testEpoch + 1
		}
	}
	if t.From.S == "ready" {
		r.wFinal, r.floorLSN = 0, "0/16"
	}
	if t.From.S == "moved_out" {
		// A moved-out source carries the floor_lsn that ready_target stamped (N179).
		r.targetShard, r.targetEpoch, r.floorLSN = 2, testEpoch+1, "0/16"
	}
	return r
}

// bind turns the named placeholders of a generated statement into SQL literals.
func bind(sql, ns string) string {
	vals := map[string]string{
		"$1": "'" + ns + "'::uuid", "$epoch": fmt.Sprint(testEpoch), "$new_epoch": fmt.Sprint(testNewEpoch),
		"$tenant_id": "'acme'", "$shard_id": fmt.Sprint(pgtest.ShardID), "$move_id": "'" + moveUUID + "'::uuid",
		"$new_move_id": "'" + newMoveUUID + "'::uuid", "$move_epoch": fmt.Sprint(testEpoch + 1),
		"$target_shard_id": "2", "$hint_shard_id": "2", "$hint_epoch": fmt.Sprint(testEpoch + 1),
		"$copy_end_lsn": "'0/16'::pg_lsn", "$w_final": "0",
	}
	return wantPlaceholderRe.ReplaceAllStringFunc(sql, func(p string) string {
		if v, ok := vals[p]; ok {
			return v
		}
		return p
	})
}

// TestIso_Ownership_Transitions (PLAN.md section 3.3.1, section 8; the name is section 8's) is here the positive half:
// every edge statement that cmd/gendocs renders from ownership_transitions is executed, as the role its row names,
// against a real shard schema, from a row in the edge's source state. It must change exactly one row to the edge's
// target state (or insert it, or delete it). A column rule of the trigger that the rendering misses (floor_lsn,
// w_final, move_id) fails here with the trigger's own message.
//
// The negative half (one refusal per forbidden role x state pair, CHECK or grant) is E1's M1.8 clause. The statements
// come from cmd/gendocs, a main package, so this half lives beside the generator; internal/store declares no test of
// this name today. E1 either adds the negative half here, as sibling subtests of this test, or declares the section 8
// name in internal/store and moves the statement renderer to an importable package (CONFLICTS.md #30 is the place to
// record which).
func TestIso_Ownership_Transitions(t *testing.T) {
	ctx := context.Background()
	ddl, err := ParseDDL(filepath.Join(repoRoot(t), "migrations", "shard"))
	if err != nil {
		t.Fatal(err)
	}
	ts, err := readTransitions(ddl)
	if err != nil {
		t.Fatal(err)
	}
	super, err := pgtest.Connect(ctx, pgtest.Super)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = super.Close(ctx) }()

	for i, tr := range ts {
		ns := fmt.Sprintf("0190c000-0000-7000-8000-%012x", 0x2000+i)
		name := fmt.Sprintf("%02d_%s_%s", i, tr.Edge, tr.Role)
		t.Run("positive/"+name, func(t *testing.T) {
			if !tr.From.Null {
				seed(ctx, t, super, ns, seedFor(tr))
			}
			conn, err := pgtest.Connect(ctx, pgtest.Role(tr.Role))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close(ctx) }()
			if _, err := conn.Exec(ctx, `SELECT set_config('engram.namespace_id', $1, false),
				set_config('engram.tenant_id', 'acme', false), set_config('engram.epoch', $2, false)`,
				ns, fmt.Sprint(testEpoch)); err != nil {
				t.Fatal(err)
			}
			stmt := bind(strings.Join(transitionSQL(tr), "\n"), ns)
			if left := wantPlaceholderRe.FindString(stmt); left != "" {
				t.Fatalf("placeholder %s has no value in the test:\n%s", left, stmt)
			}
			tag, err := conn.Exec(ctx, stmt)
			if err != nil {
				t.Fatalf("%s:\n%s\n%v", name, stmt, err)
			}
			if tag.RowsAffected() != 1 {
				t.Fatalf("%s affected %d rows:\n%s", tag, tag.RowsAffected(), stmt)
			}
			var state string
			var reason *string
			err = super.QueryRow(ctx, `SELECT state::text, freeze_reason FROM namespace_ownership
				WHERE namespace_id = $1`, ns).Scan(&state, &reason)
			switch {
			case tr.To.Null:
				if err != pgx.ErrNoRows {
					t.Errorf("the row is still there after the deleting edge (err %v, state %s)", err, state)
				}
			case err != nil:
				t.Fatal(err)
			default:
				want := stateLabel(tr.To, tr.ToReason)
				got := state
				if reason != nil {
					got += "/" + *reason
				}
				if got != want {
					t.Errorf("after %s the row is %s, want %s", name, got, want)
				}
			}
		})
	}
}

// seed writes a row in the given state with the ownership trigger disabled, then enables it again (ENABLE ALWAYS, as
// the migration leaves it).
func seed(ctx context.Context, t *testing.T, super *pgx.Conn, ns string, r seedRow) {
	t.Helper()
	tx, err := super.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, q := range []string{`ALTER TABLE namespace_ownership DISABLE TRIGGER namespace_ownership_check`} {
		if _, err := tx.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	var reason any
	if r.reason != "" {
		reason = r.reason
	}
	if _, err := tx.Exec(ctx, `INSERT INTO namespace_ownership (namespace_id, tenant_id, shard_id, epoch, state,
		freeze_reason, move_id, move_epoch, target_shard_id, target_epoch, w_final, floor_lsn)
		VALUES ($1, 'acme', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::pg_lsn)`,
		ns, pgtest.ShardID, testEpoch, r.state, reason, r.moveID, r.moveEpoch, r.targetShard, r.targetEpoch, r.wFinal,
		r.floorLSN); err != nil {
		t.Fatalf("seed %+v: %v", r, err)
	}
	enable := `ALTER TABLE namespace_ownership ENABLE ALWAYS TRIGGER namespace_ownership_check`
	if _, err := tx.Exec(ctx, enable); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
