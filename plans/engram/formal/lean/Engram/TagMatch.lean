/-
  Engram — tag-match semantics (register D10, Hindsight-compatible).

  Status: NOT type-checked (Lean 4 is not installed in the planning
  environment).  Written against Lean 4 core only (no Mathlib, no Batteries):
  tags are `List String`, "sets" are lists compared by membership, which is
  exactly what the Go implementation does after sorting + dedup.

  Q = query tags (the filter), I = item tags.

    ANY        : I = ∅ ∨ I ∩ Q ≠ ∅          (untagged items pass)
    ANY_STRICT : I ∩ Q ≠ ∅
    ALL        : I = ∅ ∨ Q ⊆ I
    ALL_STRICT : Q ⊆ I ∧ I ≠ ∅
    EXACT      : I = Q                       (as sets: Q ⊆ I ∧ I ⊆ Q)
    unset      : everything matches          (modelled as `Mode.unfiltered`)

  The Go function `tags.Match(mode, q, i []string) bool` in
  internal/recall/tags.go must agree with `TagMatch.matches` on every input;
  `internal/recall/tags_test.go` exhaustively compares the two on all
  tag lists drawn from a 4-symbol alphabet of length ≤ 3 (the same universe
  used by `decide` below).
-/
namespace Engram.TagMatch

abbrev Tag := String

inductive Mode where
  | unfiltered
  | any
  | anyStrict
  | all
  | allStrict
  | exact
  deriving DecidableEq, Repr

/-- `subset Q I` ↔ every tag of `Q` occurs in `I` (list-as-set inclusion). -/
def subset (Q I : List Tag) : Prop := ∀ t, t ∈ Q → t ∈ I

/-- `inter Q I` ↔ some tag occurs in both. -/
def inter (Q I : List Tag) : Prop := ∃ t, t ∈ Q ∧ t ∈ I

instance (Q I : List Tag) : Decidable (subset Q I) :=
  inferInstanceAs (Decidable (∀ t, t ∈ Q → t ∈ I))

instance (Q I : List Tag) : Decidable (inter Q I) :=
  inferInstanceAs (Decidable (∃ t, t ∈ Q ∧ t ∈ I))

/-- The five modes plus "unset", as propositions. -/
def Matches (m : Mode) (Q I : List Tag) : Prop :=
  match m with
  | .unfiltered => True
  | .any        => I = [] ∨ inter Q I
  | .anyStrict  => inter Q I
  | .all        => I = [] ∨ subset Q I
  | .allStrict  => subset Q I ∧ I ≠ []
  | .exact      => subset Q I ∧ subset I Q

/-- Every mode is decidable, hence `matches` is a decision procedure. -/
instance (m : Mode) (Q I : List Tag) : Decidable (Matches m Q I) := by
  cases m <;> simp only [Matches] <;> infer_instance

/-- The executable decision procedure (what Go implements). -/
def matches (m : Mode) (Q I : List Tag) : Bool := decide (Matches m Q I)

theorem matches_iff (m : Mode) (Q I : List Tag) : matches m Q I = true ↔ Matches m Q I := by
  simp [matches]

/-! ### Implications between modes (the lemmas requested by D10) -/

theorem anyStrict_imp_any (Q I : List Tag) (h : Matches .anyStrict Q I) : Matches .any Q I :=
  Or.inr h

theorem allStrict_imp_all (Q I : List Tag) (h : Matches .allStrict Q I) : Matches .all Q I :=
  Or.inr h.1

/-- EXACT → ALL_STRICT needs Q ≠ ∅: with Q = [] = I, EXACT holds but ALL_STRICT does not. -/
theorem exact_imp_allStrict (Q I : List Tag) (hQ : Q ≠ []) (h : Matches .exact Q I) :
    Matches .allStrict Q I := by
  refine ⟨h.1, ?_⟩
  intro hI
  apply hQ
  cases Q with
  | nil => rfl
  | cons t ts =>
    have : t ∈ I := h.1 t (List.mem_cons_self t ts)
    rw [hI] at this
    exact absurd this (List.not_mem_nil t)

/-- ALL_STRICT → ANY_STRICT needs Q ≠ ∅ (a vacuous subset gives no witness). -/
theorem allStrict_imp_anyStrict (Q I : List Tag) (hQ : Q ≠ []) (h : Matches .allStrict Q I) :
    Matches .anyStrict Q I := by
  cases Q with
  | nil => exact absurd rfl hQ
  | cons t ts => exact ⟨t, List.mem_cons_self t ts, h.1 t (List.mem_cons_self t ts)⟩

/-- EXACT is symmetric in Q and I. -/
theorem exact_symm (Q I : List Tag) (h : Matches .exact Q I) : Matches .exact I Q :=
  ⟨h.2, h.1⟩

/-! ### Monotonicity in the item's tags -/

/-- Adding tags to an item never breaks ANY_STRICT. -/
theorem anyStrict_mono (Q I I' : List Tag) (hI : subset I I') (h : Matches .anyStrict Q I) :
    Matches .anyStrict Q I' := by
  obtain ⟨t, htQ, htI⟩ := h
  exact ⟨t, htQ, hI t htI⟩

/-- Adding tags to an item never breaks ALL_STRICT. -/
theorem allStrict_mono (Q I I' : List Tag) (hI : subset I I') (h : Matches .allStrict Q I) :
    Matches .allStrict Q I' := by
  refine ⟨fun t ht => hI t (h.1 t ht), ?_⟩
  intro hI'
  apply h.2
  cases I with
  | nil => rfl
  | cons t ts =>
    have : t ∈ I' := hI t (List.mem_cons_self t ts)
    rw [hI'] at this
    exact absurd this (List.not_mem_nil t)

/-- ANY and ALL are *not* monotone in I: an untagged item passes, a tagged
    one may not.  Witness: Q = ["a"], I = [], I' = ["b"]. -/
example : Matches .any ["a"] [] ∧ ¬ Matches .any ["a"] ["b"] := by decide

example : Matches .all ["a"] [] ∧ ¬ Matches .all ["a"] ["b"] := by decide

/-- Narrowing the query never breaks ALL / ALL_STRICT (antitone in Q). -/
theorem allStrict_anti (Q Q' I : List Tag) (hQ : subset Q' Q) (h : Matches .allStrict Q I) :
    Matches .allStrict Q' I :=
  ⟨fun t ht => h.1 t (hQ t ht), h.2⟩

/-! ### Sanity checks by evaluation (the Go table test uses the same cases) -/

example : matches .any ["a", "b"] [] = true := by decide
example : matches .anyStrict ["a", "b"] [] = false := by decide
example : matches .anyStrict ["a", "b"] ["b", "c"] = true := by decide
example : matches .all ["a", "b"] ["b"] = false := by decide
example : matches .all ["a", "b"] ["b", "a", "c"] = true := by decide
example : matches .allStrict ["a"] [] = false := by decide
example : matches .exact ["a", "b"] ["b", "a"] = true := by decide
example : matches .exact ["a", "b"] ["b", "a", "a"] = true := by decide   -- duplicates ignored
example : matches .exact ["a"] ["a", "b"] = false := by decide
example : matches .unfiltered ["zzz"] [] = true := by decide

end Engram.TagMatch
