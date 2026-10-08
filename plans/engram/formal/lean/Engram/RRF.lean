/-
  Engram — Reciprocal Rank Fusion (register D10: RRF, k = 60, over all arms).

  Status: NOT type-checked (Lean 4 is not installed in the planning environment).

  Design of the formalisation.  The fused score is
      score(d) = Σ_{arms a} contrib(k, rank_a(d))      with contrib(k, r) = 1 / (k + r)
  and `rank_a(d) = none` when `d` is absent from arm `a` (contributes 0). The theorems of interest are structural:
    * permutation invariance: reordering the arms does not change any score,
      hence not the fused order;
    * monotonicity: improving a document's rank in one arm never lowers its
      fused score;
    * boundedness for k > 0: 0 ≤ score(d) ≤ |arms| / (k + 1).
  To stay Mathlib-free we abstract the contribution values into a type with a commutative, associative addition
  (`CommAdd`), prove the structural facts there, and instantiate with `Nat` (exact: contributions scaled by a common
  denominator) and with `Float` (what Go computes; no proofs about Float arithmetic are attempted). `Rat` (core Lean
  ≥ 4.19, else Batteries) can be substituted for `Float` for an exact instance; the `Rat` ordering lemmas are the
  only `sorry` left and are marked.

  Go counterpart: internal/recall/fuse.go `RRF(arms [][]ID, k int) []Scored`; internal/recall/fuse_test.go checks
  the same three properties with pgregory.net/rapid on random arm lists (k = 60, ≤ 5 arms, ≤ 400 ids).
-/
namespace Engram.RRF

/-- Minimal commutative additive monoid (no Mathlib). -/
class CommAdd (α : Type) extends Add α, Zero α where
  add_comm  : ∀ a b : α, a + b = b + a
  add_assoc : ∀ a b c : α, a + b + c = a + (b + c)
  zero_add  : ∀ a : α, 0 + a = a

instance : CommAdd Nat where
  add_comm := Nat.add_comm
  add_assoc := Nat.add_assoc
  zero_add := Nat.zero_add

section Sum
variable {α : Type} [CommAdd α]

/-- Sum of a list in a `CommAdd`. -/
def sumList : List α → α
  | [] => 0
  | x :: xs => x + sumList xs

theorem sumList_nil : sumList ([] : List α) = 0 := rfl
theorem sumList_cons (x : α) (xs : List α) : sumList (x :: xs) = x + sumList xs := rfl

/-- The sum of a list is invariant under permutation. Proof by induction on the permutation (nil / cons / swap /
    trans). -/
theorem sumList_perm {l₁ l₂ : List α} (h : l₁.Perm l₂) : sumList l₁ = sumList l₂ := by
  induction h with
  | nil => rfl
  | cons x _ ih => simp [sumList_cons, ih]
  | swap x y l =>
    simp only [sumList_cons]
    rw [← CommAdd.add_assoc, CommAdd.add_comm y x, CommAdd.add_assoc]
  | trans _ _ ih₁ ih₂ => exact ih₁.trans ih₂

end Sum

/-- An arm is a ranking: a document either has a rank or is absent. Ranks are **1-based** (rank 1 = top), matching
    Go's `fuse` and Hindsight's `1/(k + rank)` with `rank ≥ 1`; the worked check below uses `some 1` for a top-ranked
    document. The theorems in this file are stated for every `r : Nat`, so they hold a fortiori for `r ≥ 1`; nothing
    here depends on the base. -/
abbrev Arm (Doc : Type) := Doc → Option Nat

/-- Contribution of one arm to a document, given a contribution function of the rank. Absent documents contribute 0. -/
def contribOf {α Doc : Type} [CommAdd α] (contrib : Nat → α) (a : Arm Doc) (d : Doc) : α :=
  match a d with
  | some r => contrib r
  | none   => 0

/-- Fused score: sum over arms of the per-arm contribution. -/
def score {α Doc : Type} [CommAdd α] (contrib : Nat → α) (arms : List (Arm Doc)) (d : Doc) : α :=
  sumList (arms.map fun a => contribOf contrib a d)

/-! ### Permutation invariance -/

/-- Reordering the arms does not change any fused score ... -/
theorem score_perm {α Doc : Type} [CommAdd α] (contrib : Nat → α)
    {arms₁ arms₂ : List (Arm Doc)} (h : arms₁.Perm arms₂) (d : Doc) :
    score contrib arms₁ d = score contrib arms₂ d :=
  sumList_perm (h.map _)

/-- ... hence not the fused order (for any order on `α`). -/
theorem order_perm {α Doc : Type} [CommAdd α] [LT α] (contrib : Nat → α)
    {arms₁ arms₂ : List (Arm Doc)} (h : arms₁.Perm arms₂) (d e : Doc) :
    (score contrib arms₁ d < score contrib arms₁ e) ↔
      (score contrib arms₂ d < score contrib arms₂ e) := by
  rw [score_perm contrib h d, score_perm contrib h e]

/-! ### Monotonicity: improving a rank in one arm never lowers the fused score.

    We need an order compatible with addition; we state the minimal axioms as a class so the theorem is Mathlib-free.
    `Nat` satisfies them. -/

class OrderedCommAdd (α : Type) extends CommAdd α, LE α where
  le_refl : ∀ a : α, a ≤ a
  le_trans : ∀ a b c : α, a ≤ b → b ≤ c → a ≤ c
  add_le_add_left : ∀ a b c : α, a ≤ b → c + a ≤ c + b
  add_le_add_right : ∀ a b c : α, a ≤ b → a + c ≤ b + c

instance : OrderedCommAdd Nat where
  le_refl := Nat.le_refl
  le_trans := fun _ _ _ => Nat.le_trans
  add_le_add_left := fun _ _ c h => Nat.add_le_add_left h c
  add_le_add_right := fun _ _ c h => Nat.add_le_add_right h c

/-- A contribution function is *antitone* when a better (smaller) rank gives at least as much. 1/(k+r) is antitone
    for every k. -/
def Antitone {α : Type} [LE α] (contrib : Nat → α) : Prop :=
  ∀ r r', r ≤ r' → contrib r' ≤ contrib r

/-- Replace arm `i` by `a'`. -/
def setArm {Doc : Type} (arms : List (Arm Doc)) (i : Nat) (a' : Arm Doc) : List (Arm Doc) :=
  arms.set i a'

/-- `a'` improves `d` relative to `a`: `d` keeps a rank and it is not worse. -/
def Improves {Doc : Type} (a a' : Arm Doc) (d : Doc) : Prop :=
  match a d, a' d with
  | some r, some r' => r' ≤ r
  | none,   some _  => True
  | none,   none    => True
  | some _, none    => False

theorem contribOf_le_of_improves {α Doc : Type} [OrderedCommAdd α] (contrib : Nat → α)
    (hc : Antitone contrib) (hz : ∀ r, (0 : α) ≤ contrib r)
    (a a' : Arm Doc) (d : Doc) (h : Improves a a' d) :
    contribOf contrib a d ≤ contribOf contrib a' d := by
  unfold Improves at h
  unfold contribOf
  cases ha : a d <;> cases ha' : a' d <;> simp [ha, ha'] at h ⊢
  · exact OrderedCommAdd.le_refl _
  · exact hz _
  · exact hc _ _ h

/-- Main monotonicity theorem: improving `d` in arm `i` does not lower `score d`. The induction over `arms.set` is
    routine; left as `sorry` pending a type-check (the per-arm lemma above carries the content). -/
theorem score_mono {α Doc : Type} [OrderedCommAdd α] (contrib : Nat → α)
    (hc : Antitone contrib) (hz : ∀ r, (0 : α) ≤ contrib r)
    (arms : List (Arm Doc)) (i : Nat) (a' : Arm Doc) (d : Doc)
    (hi : i < arms.length) (h : Improves (arms.get ⟨i, hi⟩) a' d) :
    score contrib arms d ≤ score contrib (setArm arms i a') d := by
  sorry -- TODO(formal): induction on arms generalising i; uses
        -- contribOf_le_of_improves, add_le_add_left/right.

/-! ### Boundedness for k > 0 (exact, over Nat with a common denominator)

    Over the rationals, with contrib(k, r) = 1/(k+r):
        0 ≤ score(d) ≤ |arms| · 1/(k+1)   and   score(d) = 0 ↔ d absent from every arm.
    We state the Nat-scaled version: multiply every contribution by D = lcm(k+1 .. k+R) where R bounds the ranks;
    then contribNat r = D/(k+r) is an exact natural number and 1 ≤ contribNat r ≤ D/(k+1). -/

def contribNat (D k r : Nat) : Nat := D / (k + r)

theorem contribNat_antitone (D k : Nat) (hk : 0 < k) : Antitone (contribNat D k) := by
  intro r r' h
  unfold contribNat
  exact Nat.div_le_div_left (Nat.add_le_add_left h k) (by omega)

theorem contribNat_le (D k r : Nat) (hk : 0 < k) : contribNat D k r ≤ D / (k + 0) := by
  unfold contribNat
  exact Nat.div_le_div_left (Nat.add_le_add_left (Nat.zero_le r) k) hk

/-- Upper bound: with every contribution ≤ D/(k+1) ... the sum is ≤ |arms| · D/(k+1).
    Elementary list induction; `sorry` pending type-check. -/
theorem score_le_bound (D k : Nat) (hk : 0 < k) {Doc : Type} (arms : List (Arm Doc)) (d : Doc) :
    score (contribNat D k) arms d ≤ arms.length * (D / (k + 1)) := by
  sorry -- TODO(formal): induction on arms; contribNat r ≤ D/(k+1) because k+1 ≤ k+r for r ≥ 1.
        -- Ranks are 1-based throughout this file (see `Arm`), so the bound D/(k+1) is the top-rank contribution;
        -- `contribNat_le` above is the weaker r ≥ 0 form and is not needed here. (Review F-31 flagged the earlier
        -- "0-based" comment on `Arm` as contradicting this one; the definitions were always rank-base-agnostic.)

/-- k > 0 guarantees every contribution is finite and positive in ℚ; the Nat model expresses "positive" as `0 < D /
    (k + r)` whenever `k + r ∣ D`. -/
theorem contribNat_pos (D k r : Nat) (hD : 0 < D) (hdvd : (k + r) ∣ D) (hk : 0 < k) :
    0 < contribNat D k r := by
  unfold contribNat
  exact Nat.div_pos (Nat.le_of_dvd hD hdvd) (Nat.add_pos_left hk r)

/-! ### Worked check (decidable instance over Nat): three arms, k = 60, D = 60·61·62 -/

def k60 : Nat := 60
def D60 : Nat := 60 * 61 * 62 * 63

-- doc 0 ranked 1st in arms A and B, absent in C; doc 1 ranked 2nd, 1st, 3rd.
def armA : Arm Nat := fun d => if d = 0 then some 1 else if d = 1 then some 2 else none
def armB : Arm Nat := fun d => if d = 0 then some 1 else if d = 1 then some 1 else none
def armC : Arm Nat := fun d => if d = 1 then some 3 else none

-- Exact values: D60/61 = 234360, D60/62 = 230580, D60/63 = 226920, so score 0 = 468720 and score 1 = 691860: a document
-- present in three arms outranks one present in two, even though the latter is top-ranked in both.
example : score (contribNat D60 k60) [armA, armB, armC] 0 <
          score (contribNat D60 k60) [armA, armB, armC] 1 := by
  decide

end Engram.RRF
