/-
  Engram — token-budget packing (register D10, "Packing" row).

  Status: NOT type-checked (Lean 4 is not installed in the planning environment). Lean 4 core only.

  Semantics being pinned:
    * items are visited in rank order;
    * an item is taken iff it fits in the *remaining* budget;
    * an item that does not fit is skipped (never truncated) and counted;
    * packing never stops early: later, smaller items may still be taken.

  `keep` and `skipped` are two structural recursions over the same decision so that every theorem is a plain
  induction; `pack` pairs them (the Go code computes both in one pass, which is observationally the same).

  Go counterpart: internal/recall/pack.go `Pack(items []Item, budget int) (kept []Item, skipped int)`;
  internal/recall/pack_test.go (pgregory.net/rapid) checks the same theorems on random item lists with token counts
  in [0, 2·budget].
-/
namespace Engram.Packer

/-- An item is just its token count here; the payload is irrelevant to the proofs. -/
structure Item where
  tokens : Nat
  deriving Repr, DecidableEq

/-- Items kept by the greedy scan, in input order. -/
def keep : List Item → Nat → List Item
  | [], _ => []
  | x :: xs, rem => if x.tokens ≤ rem then x :: keep xs (rem - x.tokens) else keep xs rem

/-- Number of items skipped by the greedy scan. -/
def skipped : List Item → Nat → Nat
  | [], _ => 0
  | x :: xs, rem => if x.tokens ≤ rem then skipped xs (rem - x.tokens) else skipped xs rem + 1

def pack (items : List Item) (budget : Nat) : List Item × Nat :=
  (keep items budget, skipped items budget)

def total (l : List Item) : Nat := (l.map Item.tokens).sum

theorem total_nil : total [] = 0 := rfl
theorem total_cons (x : Item) (l : List Item) : total (x :: l) = x.tokens + total l := by
  simp [total, List.map_cons, List.sum_cons]

/-! ### 1. The packed set never exceeds the budget -/

theorem keep_total_le : ∀ (items : List Item) (rem : Nat), total (keep items rem) ≤ rem := by
  intro items
  induction items with
  | nil => intro rem; simp [keep, total]
  | cons x xs ih =>
    intro rem
    by_cases h : x.tokens ≤ rem
    · simp only [keep, h, if_true, total_cons]
      have := ih (rem - x.tokens)
      omega
    · simp only [keep, h, if_false]
      exact ih rem

theorem pack_total_le (items : List Item) (budget : Nat) : total (pack items budget).1 ≤ budget :=
  keep_total_le items budget

/-! ### 2. Output order is input order (the kept list is a sublist) -/

theorem keep_sublist : ∀ (items : List Item) (rem : Nat), (keep items rem).Sublist items := by
  intro items
  induction items with
  | nil => intro rem; simp [keep]
  | cons x xs ih =>
    intro rem
    by_cases h : x.tokens ≤ rem
    · simp only [keep, h, if_true]
      exact List.Sublist.cons₂ x (ih (rem - x.tokens))
    · simp only [keep, h, if_false]
      exact List.Sublist.cons x (ih rem)

theorem pack_sublist (items : List Item) (budget : Nat) : (pack items budget).1.Sublist items :=
  keep_sublist items budget

/-! ### 3. Skip semantics -/

/-- kept + skipped = length: every item is either kept or counted as skipped. -/
theorem keep_count : ∀ (items : List Item) (rem : Nat),
    (keep items rem).length + skipped items rem = items.length := by
  intro items
  induction items with
  | nil => intro rem; simp [keep, skipped]
  | cons x xs ih =>
    intro rem
    by_cases h : x.tokens ≤ rem
    · simp only [keep, skipped, h, if_true, List.length_cons]
      have := ih (rem - x.tokens)
      omega
    · simp only [keep, skipped, h, if_false, List.length_cons]
      have := ih rem
      omega

/-- An oversize item is skipped and the scan continues unchanged. -/
theorem keep_skip_oversize (big : Item) (rest : List Item) (rem : Nat) (h : rem < big.tokens) :
    keep (big :: rest) rem = keep rest rem ∧ skipped (big :: rest) rem = skipped rest rem + 1 := by
  have h' : ¬ big.tokens ≤ rem := Nat.not_le.mpr h
  simp [keep, skipped, h']

/-- Prefix-closure: packing `l₁ ++ l₂` starts with exactly the result of packing `l₁` with the same budget, i.e.
    later items never change earlier decisions (the scan is a left fold). -/
theorem keep_append : ∀ (l₁ l₂ : List Item) (rem : Nat),
    keep (l₁ ++ l₂) rem = keep l₁ rem ++ keep l₂ (rem - total (keep l₁ rem)) := by
  intro l₁
  induction l₁ with
  | nil => intro l₂ rem; simp [keep, total]
  | cons x xs ih =>
    intro l₂ rem
    by_cases h : x.tokens ≤ rem
    · simp only [List.cons_append, keep, h, if_true, ih, total_cons, Nat.sub_sub]
    · simp only [List.cons_append, keep, h, if_false, ih]

/-- A kept item fits the budget that remained when it was reached: for the item at position `i`, `tokens ≤ rem -
    total (keep (take i) rem)` whenever it is kept. Stated for the head after a prefix; proof deferred. -/
theorem kept_fits (l₁ : List Item) (x : Item) (l₂ : List Item) (rem : Nat)
    (hx : x ∈ keep (l₁ ++ x :: l₂) rem) (hnotin : x ∉ keep l₁ rem)
    (hnot₂ : x ∉ keep l₂ (rem - total (keep l₁ rem) - x.tokens)) :
    x.tokens ≤ rem - total (keep l₁ rem) := by
  sorry -- TODO(formal): rewrite with keep_append, then case on the head decision;
        -- the two exclusion hypotheses rule out an equal item elsewhere.

/-! ### Evaluation checks (same cases as the Go table test) -/

example : pack [⟨3⟩, ⟨5⟩, ⟨2⟩] 6 = ([⟨3⟩, ⟨2⟩], 1) := by decide
-- oversize first item skipped, scan continues
example : pack [⟨7⟩, ⟨1⟩] 6 = ([⟨1⟩], 1) := by decide
example : pack [] 6 = ([], 0) := by decide
example : pack [⟨0⟩, ⟨0⟩] 0 = ([⟨0⟩, ⟨0⟩], 0) := by decide    -- zero-token items always fit
-- never stops early, never truncates
example : pack [⟨4⟩, ⟨4⟩, ⟨4⟩] 8 = ([⟨4⟩, ⟨4⟩], 1) := by decide

end Engram.Packer
