/-
  Engram — temporal-window arithmetic (register D10, temporal arm: "facts whose occurrence window overlaps the query
  window, ordered by distance to query_timestamp").

  Status: NOT type-checked (Lean 4 is not installed in the planning environment). Lean 4 core only; all arithmetic
  is over `Int` (Unix seconds) and the proofs are `omega`-shaped.

  Go counterpart: internal/recall/temporal.go (`Window`, `Overlaps`, `Contains`, `DistanceTo`);
  internal/recall/temporal_test.go (pgregory.net/rapid) checks the same theorems on random windows.
-/
namespace Engram.TemporalWindow

/-- A closed window [lo, hi] with lo ≤ hi. Open-ended windows are encoded by the caller with Int.min/Int.max
    sentinels, outside this module. -/
structure Window where
  lo : Int
  hi : Int
  ok : lo ≤ hi

/-- Two closed windows overlap iff each starts no later than the other ends. -/
def overlaps (a b : Window) : Prop := a.lo ≤ b.hi ∧ b.lo ≤ a.hi

/-- `contains a b` ↔ b ⊆ a. -/
def contains (a b : Window) : Prop := a.lo ≤ b.lo ∧ b.hi ≤ a.hi

/-- Distance from an anchor instant to a window: 0 inside, else the gap. -/
def distanceTo (w : Window) (t : Int) : Int :=
  if t < w.lo then w.lo - t
  else if w.hi < t then t - w.hi
  else 0

instance (a b : Window) : Decidable (overlaps a b) := inferInstanceAs (Decidable (_ ∧ _))
instance (a b : Window) : Decidable (contains a b) := inferInstanceAs (Decidable (_ ∧ _))

/-! ### Overlap -/

theorem overlaps_symm (a b : Window) (h : overlaps a b) : overlaps b a :=
  ⟨h.2, h.1⟩

theorem overlaps_refl (a : Window) : overlaps a a :=
  ⟨a.ok, a.ok⟩

/-- Overlap is *not* transitive (a=[0,1], b=[1,2], c=[2,3]); stated as a counterexample so nobody "fixes" the Go code
    to assume it. -/
example : overlaps ⟨0, 1, by decide⟩ ⟨1, 2, by decide⟩ ∧
          overlaps ⟨1, 2, by decide⟩ ⟨2, 3, by decide⟩ ∧
          ¬ overlaps ⟨0, 1, by decide⟩ ⟨2, 3, by decide⟩ := by decide

/-- A window overlaps b iff some instant lies in both. -/
theorem overlaps_iff_exists (a b : Window) :
    overlaps a b ↔ ∃ t : Int, a.lo ≤ t ∧ t ≤ a.hi ∧ b.lo ≤ t ∧ t ≤ b.hi := by
  constructor
  · intro h
    refine ⟨max a.lo b.lo, ?_, ?_, ?_, ?_⟩ <;>
      first
        | exact Int.le_max_left _ _
        | exact Int.le_max_right _ _
        | exact Int.max_le.mpr ⟨a.ok, h.2⟩
        | exact Int.max_le.mpr ⟨h.1, b.ok⟩
  · rintro ⟨t, h1, h2, h3, h4⟩
    exact ⟨Int.le_trans h1 h4, Int.le_trans h3 h2⟩

/-! ### Containment -/

theorem contains_refl (a : Window) : contains a a := ⟨Int.le_refl _, Int.le_refl _⟩

theorem contains_trans (a b c : Window) (hab : contains a b) (hbc : contains b c) : contains a c :=
  ⟨Int.le_trans hab.1 hbc.1, Int.le_trans hbc.2 hab.2⟩

theorem contains_antisymm (a b : Window) (hab : contains a b) (hba : contains b a) :
    a.lo = b.lo ∧ a.hi = b.hi :=
  ⟨Int.le_antisymm hab.1 hba.1, Int.le_antisymm hba.2 hab.2⟩

/-- Containment implies overlap (both windows are non-empty). -/
theorem contains_imp_overlaps (a b : Window) (h : contains a b) : overlaps a b :=
  ⟨Int.le_trans h.1 b.ok, Int.le_trans b.ok h.2⟩

/-- If a contains b and b overlaps c then a overlaps c. -/
theorem overlaps_of_contains (a b c : Window) (hab : contains a b) (hbc : overlaps b c) :
    overlaps a c :=
  ⟨Int.le_trans hab.1 hbc.1, Int.le_trans hbc.2 hab.2⟩

/-! ### Distance to anchor -/

theorem distanceTo_nonneg (w : Window) (t : Int) : 0 ≤ distanceTo w t := by
  unfold distanceTo
  split <;> (try split) <;> omega

theorem distanceTo_eq_zero_iff (w : Window) (t : Int) :
    distanceTo w t = 0 ↔ (w.lo ≤ t ∧ t ≤ w.hi) := by
  unfold distanceTo
  constructor
  · intro h
    split at h
    · omega
    · split at h <;> omega
  · intro h
    have h1 : ¬ t < w.lo := by omega
    have h2 : ¬ w.hi < t := by omega
    simp [h1, h2]

/-- Widening the window never increases the distance (monotone under containment). -/
theorem distanceTo_mono (a b : Window) (h : contains a b) (t : Int) :
    distanceTo a t ≤ distanceTo b t := by
  unfold distanceTo
  have := h.1
  have := h.2
  have := a.ok
  have := b.ok
  split <;> split <;> (try split) <;> (try split) <;> omega

/-- The distance is 1-Lipschitz in the anchor: moving the query timestamp by
    δ changes the distance by at most |δ|. -/
theorem distanceTo_lipschitz (w : Window) (t t' : Int) :
    distanceTo w t - distanceTo w t' ≤ (if t ≤ t' then t' - t else t - t') := by
  unfold distanceTo
  have := w.ok
  split <;> split <;> (try split) <;> (try split) <;> (try split) <;> omega

/-- Ordering by distance is a total preorder: the Go sort is well-defined and ties are broken by fact id
    (deterministic streaming order). -/
theorem distance_total (w₁ w₂ : Window) (t : Int) :
    distanceTo w₁ t ≤ distanceTo w₂ t ∨ distanceTo w₂ t ≤ distanceTo w₁ t :=
  Int.le_total _ _

/-! ### Evaluation checks (same cases as the Go table test) -/

example : distanceTo ⟨10, 20, by decide⟩ 15 = 0 := by decide
example : distanceTo ⟨10, 20, by decide⟩ 4 = 6 := by decide
example : distanceTo ⟨10, 20, by decide⟩ 25 = 5 := by decide
example : overlaps ⟨10, 20, by decide⟩ ⟨20, 30, by decide⟩ := by decide   -- closed windows touch
example : ¬ overlaps ⟨10, 20, by decide⟩ ⟨21, 30, by decide⟩ := by decide

end Engram.TemporalWindow
