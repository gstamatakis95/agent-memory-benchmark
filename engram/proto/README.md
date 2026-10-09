# Engram protobuf contracts

Source of truth for every Engram API surface. Generated Go stubs (`gen/go`),
the MCP adapter and the Connect/JSON adapter are all derived from these
files; nothing is hand-written against them (task statement, "Style").

## Layout

- `memory/v1/common.proto` (package `memory.v1`; served: public). RequestMeta, NamespaceRef, paging, Budget, FactType,
  MemoryKind, UpdateMode, TagMatchMode/TagFilter, Provenance, Scores, TemporalWindow, Timestamps, EntityRef/EntityHint,
  MetadataFilter
- `memory/v1/errors.proto` (package `memory.v1`; served: public). Typed `google.rpc.Status` details: ValidationError,
  QuotaExceeded, NotFound, WrongShardOrEpoch (no shard fields), NamespaceFrozen (+ `frozen_until_estimate` for a move
  freeze, N160), NamespaceNotReady, OperationConflict, PreconditionFailed (+ NamespaceState, ResourceKind,
  FreezeReason). The write-path details moved to `engram/internal/errors/v1` (N128)
- `memory/v1/operation.proto` (package `memory.v1`; served: public). OperationService (Get, List, Cancel, Wait), the
  Operation resource, `superseded_by` (a version number), `CancelReason`, `DELETE_TENANT` kind, `ExpungeSla`; per-kind
  workflow mapping, non-cancellable `DELETE_*` and singleton-backed `REFRESH_PAGE` (N127, N136, N157)
- `memory/v1/memory.proto` (package `memory.v1`; served: public). MemoryService (Retain, Recall stream, Reflect stream,
  GetMemory, ListMemories, BatchGetMemories, Invalidate, Restore) and the Memory resource
- `memory/v1/document.proto` (package `memory.v1`; served: public). DocumentService (Get, List, Delete → tombstone ack +
  expunge Operation, GetDocumentVersion, GetDocumentBody, ListTags, UpdateDocumentTags with `tag_generation`); a
  `DELETING` document is returned as a content-free tombstone view and covered versions are filtered from every path
  (N136, N139, N157)
- `memory/v1/namespace.proto` (package `memory.v1`; served: public). NamespaceService (Create, Get, List, Update with
  field mask, GetEffectiveConfig, Delete → ack + Operation), Disposition, typed Directive (N129)
- `memory/v1/export.proto` (package `memory.v1`; served: public (phase 3)). ExportService (CreateSnapshot → Operation,
  GetSnapshotManifest with the live `hidden_overlay`, ListSnapshots, StreamSnapshot 1 MiB parts; always-emitted delta
  with a `documents` part, `deleted_ids`, `SnapshotManifest.expires_at`; the overlay is computed from the live
  predicate, N126, N145, N157)
- `memory/v1/page.proto` (package `memory.v1`; served: public (phase 3)). PageService (Create, Get with as_of, List,
  Update, Delete, Refresh → Operation), staleness fields
- `memory/admin/v1/admin.proto` (package `memory.admin.v1`; served: admin route (the tenant's own lifecycle methods also
  on the tenant-facing listener, N157)). TenantService (+ tenant-scoped GetTenantOperation; CreateTenant and
  UpdateTenantLimits are `engram.operator`-only, UpdateTenant is `tenant.admin` for `display_name`/`config`, N167),
  ShardService, MoveService (freeze-then-copy: window estimate and operator window, freeze, copy from the static source,
  VerifyFrozen, BuildIndexes, `ready` cutover, catalog CAS `COMMITTED` as the point of no return, cleanup 24 h after
  activation, seal before the cut; N160, N170, N179); `engram.worker` is valid on `ReleaseNamespace` and `CleanupMove`
  only — the only API where shard ids and epochs appear
- `engram/internal/workflow/v1/workflow.proto` (package `engram.internal.workflow.v1`; served: never). Temporal
  payloads: RetainDocumentInput, LoadItemResult, ChunkWork, Extract/Embed/Resolve/Link/Commit results, two-stage
  Consolidate* (RouteBatch, WriteObservation, StoreProposal, ApplyBatch), RefreshPageInput, ExpungeInput and phase
  messages, TenantDeleteInput, RetainBackfillInput, ReembedNamespaceInput, SweeperInput, ExportInput, MoveInput,
  CopyProgress, VerifyFrozenReport, SealCopyResult, BuildIndexesReport, MoveCheckpoint
- `engram/internal/errors/v1/errors.proto` (package `engram.internal.errors.v1`; served: never). Write-path and routing
  error details that must not cross the API boundary: MovedOutHint, FenceBusy, DocumentBusy, InputBlobMissing (N128)
- `engram/internal/events/v1/events.proto` (package `engram.internal.events.v1`; served: never). Outbox/Kafka `Event`
  envelope with the payload oneof; ids are 16-byte `bytes` (`*_bytes` fields), ≤ 256 per event, paged
  (`page`/`page_count`) and elided above 4,096 (N80); `DocumentDeleted` is an O(1) marker event; `DocumentTagsUpdated`
  (N157); no replay events
- `buf.yaml` (package —; served: —). Workspace: two BSR modules (`buf.build/engram/memory`, `buf.build/engram/internal`)
  at the same root, split by `includes`; lint STANDARD; breaking FILE (from v1.0.0 on)
- `buf.gen.yaml` (package —; served: —). protoc-gen-go, protoc-gen-go-grpc, protoc-gen-connect-go → `../gen/go`
  (`<repo>/gen/go` per D14)

Only well-known types are imported (`google/protobuf/{timestamp,duration,
field_mask,struct,any}.proto`), which buf ships built in, so every command
below works with no network access and no BSR dependency. `google/rpc/*` and
`google/api/*` are deliberately not imported: the typed error details live in
`memory/v1/errors.proto` and are attached to `google.rpc.Status` at runtime.

## Generated Go paths

`buf generate` writes to `<repo>/gen/go` (run from this directory; the plugin options are
`module=github.com/gstamatakis95/engram` and `out: ..`). The public packages keep their directory (`gen/go/memory/v1`,
`gen/go/memory/admin/v1`). The three `engram.internal.*` packages are generated into
`gen/go/engram/private/{errors,events,workflow}/v1`, not into `gen/go/engram/internal/...`: Go refuses to import a
package whose path contains an `internal` element from outside the parent of that element, so `internal/errs`,
`internal/store` and `internal/workflows` could not import them (the `go_package` option of those three files names the
`private` path).

## CI commands

Run from this directory:

```bash
buf lint                                   # STANDARD rules, zero tolerance
buf build                                  # compiles all modules, catches import cycles / unused imports
buf format -d --exit-code                  # canonical formatting
buf generate                               # → <repo>/gen/go (Go, gRPC, Connect; out: .. with module=)
```

`buf breaking` is not run before `v1.0.0`: the protos are pre-1.0, breaking changes are allowed and removed
fields are not reserved (plan section 4.5). CI gates on `buf lint`, `buf build` and `buf format -d --exit-code`.
From `v1.0.0` on the baseline is the latest release tag (`.git#tag=proto/v1.0.0,subdir=…`), a failure blocks the
merge and the only override is a `v2` package.

Release tagging (on `main`, after the checks above):

```bash
buf push --label "v$(cat VERSION)"        # publishes both modules to the BSR
```

Every top-level internal payload carries `schema_version = 2`.
