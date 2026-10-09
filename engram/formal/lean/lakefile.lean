-- NOT TYPE-CHECKED: Lean 4 is not available in the planning environment (N142).
import Lake
open Lake DSL

package engram where
  leanOptions := #[⟨`autoImplicit, false⟩]

@[default_target]
lean_lib Engram where
  roots := #[`Engram.RRF, `Engram.Packer, `Engram.TagMatch, `Engram.TemporalWindow]
