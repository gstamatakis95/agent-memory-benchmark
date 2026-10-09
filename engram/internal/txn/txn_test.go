package txn_test

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/txn"
)

func key(s string) txn.UsageKey { return txn.UsageKey(sha256.Sum256([]byte(s))) }

func TestMemTx_RecordIsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 10, 9, 13, 0, 0, 0, time.UTC)
	tx := txn.NewMemTx(id.Scope{Tenant: "acme", Namespace: id.NewNamespaceID(), Shard: 7, Epoch: 1})
	u := tx.Usage()
	ev := txn.UsageEvent{Op: "extract", Model: "m", Day: day, PromptTokens: 100, CompletionTokens: 20}

	for i, want := range []bool{true, false, false} {
		got, err := u.Record(ctx, key("op/extract/chunk-1"), ev)
		if err != nil || got != want {
			t.Fatalf("Record #%d = %v, %v; want %v (usage_key makes a replay a no-op)", i, got, err, want)
		}
	}
	if _, err := u.Record(ctx, key("op/extract/chunk-2"), ev); err != nil {
		t.Fatal(err)
	}
	if tx.Events() != 2 {
		t.Fatalf("Events() = %d, want 2", tx.Events())
	}
	tokens, facts, err := u.Day(ctx, day.Add(5*time.Hour))
	if err != nil || tokens != 240 || facts != 0 {
		t.Fatalf("Day = %d tokens, %d facts, %v; want 240, 0 (two distinct keys of 120)", tokens, facts, err)
	}
	tx.SetFacts(day, 9)
	if _, facts, _ = u.Day(ctx, day); facts != 9 {
		t.Fatalf("facts = %d, want 9", facts)
	}
}

func TestMemTx_DayIsolation(t *testing.T) {
	ctx := context.Background()
	d1 := time.Date(2026, 10, 9, 23, 59, 0, 0, time.UTC)
	d2 := d1.Add(2 * time.Minute) // next UTC day
	tx := txn.NewMemTx(id.Scope{})
	_, _ = tx.Usage().Record(ctx, key("a"), txn.UsageEvent{Day: d1, PromptTokens: 5})
	_, _ = tx.Usage().Record(ctx, key("b"), txn.UsageEvent{Day: d2, PromptTokens: 7})
	if got, _, _ := tx.Usage().Day(ctx, d1); got != 5 {
		t.Errorf("day 1 = %d, want 5", got)
	}
	if got, _, _ := tx.Usage().Day(ctx, d2); got != 7 {
		t.Errorf("day 2 = %d, want 7", got)
	}
}

func TestMemTx_HonoursCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tx := txn.NewMemTx(id.Scope{})
	if _, err := tx.Usage().Record(ctx, key("a"), txn.UsageEvent{}); err == nil {
		t.Error("Record on a cancelled context must fail")
	}
	if _, _, err := tx.Usage().Day(ctx, time.Now()); err == nil {
		t.Error("Day on a cancelled context must fail")
	}
}
