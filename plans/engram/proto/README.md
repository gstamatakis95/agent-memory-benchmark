# Engram protobuf contracts

Source of truth for every Engram API surface. Generated Go stubs (`gen/go`),
the MCP adapter and the Connect/JSON adapter are all derived from these
files; nothing is hand-written against them (task statement, "Style").

## Layout

| Path | Package | Served? | Contents |
|---|---|---|---|
| `memory/v1/common.proto` | `memory.v1` | public | RequestMeta, NamespaceRef, paging, Budget, FactType, MemoryKind, UpdateMode, TagMatchMode/TagFilter, Provenance, Scores, TemporalWindow, Timestamps, EntityRef/EntityHint, MetadataFilter |
| `memory/v1/errors.proto` | `memory.v1` | public | Typed `google.rpc.Status` details: ValidationError, QuotaExceeded, NotFound, WrongShardOrEpoch (no shard fields), NamespaceFrozen, NamespaceNotReady, OperationConflict, PreconditionFailed (+ NamespaceState, ResourceKind, FreezeReason). The write-path details moved to `engram/internal/errors/v1` (N128) |
| `memory/v1/operation.proto` | `memory.v1` | public | OperationService (Get, List, Cancel, Wait), the Operation resource, `superseded_by` (a version number), `CancelReason`, `DELETE_TENANT` kind, `ExpungeSla`; per-kind workflow mapping, non-cancellable `DELETE_*` and singleton-backed `REFRESH_PAGE` (N127, N136, N157) |
| `memory/v1/memory.proto` | `memory.v1` | public | MemoryService (Retain, Recall stream, Reflect stream, GetMemory, ListMemories, BatchGetMemories, Invalidate, Restore) and the Memory resource |
| `memory/v1/document.proto` | `memory.v1` | public | DocumentService (Get, List, Delete → tombstone ack + expunge Operation, GetDocumentVersion, GetDocumentBody, ListTags, UpdateDocumentTags with `tag_generation`); a `DELETING` document is returned as a content-free tombstone view and covered versions are filtered from every path (N136, N139, N157) |
| `memory/v1/namespace.proto` | `memory.v1` | public | NamespaceService (Create, Get, List, Update with field mask, GetEffectiveConfig, Delete → ack + Operation), Disposition, typed Directive (N129) |
| `memory/v1/export.proto` | `memory.v1` | public (phase 3) | ExportService (CreateSnapshot → Operation, GetSnapshotManifest with the live `hidden_overlay`, ListSnapshots, StreamSnapshot 1 MiB parts; always-emitted delta with a `documents` part, `deleted_ids`, `SnapshotManifest.expires_at`; the overlay is computed from the live predicate, N126, N145, N157) |
| `memory/v1/page.proto` | `memory.v1` | public (phase 3) | PageService (Create, Get with as_of, List, Update, Delete, Refresh → Operation), staleness fields |
| `memory/admin/v1/admin.proto` | `memory.admin.v1` | admin route (the tenant's own lifecycle methods also on the tenant-facing listener, N157) | TenantService (+ tenant-scoped GetTenantOperation; CreateTenant and the quota and isolation paths of UpdateTenant are `engram.operator`-only), ShardService, MoveService (dirty copy, pre-freeze verification, freeze, reconcile by insertion sequence, `ready` cutover, catalog CAS `COMMITTED` as the point of no return) — the only API where shard ids and epochs appear |
| `engram/internal/workflow/v1/workflow.proto` | `engram.internal.workflow.v1` | never | Temporal payloads: RetainDocumentInput, LoadItemResult, ChunkWork, Extract/Embed/Resolve/Link/Commit results, two-stage Consolidate* (RouteBatch, WriteObservation, StoreProposal, ApplyBatch), RefreshPageInput, ExpungeInput and phase messages, TenantDeleteInput, RetainBackfillInput, ReembedNamespaceInput, SweeperInput, ExportInput, MoveInput, CopyProgress, PreVerifyReport, AwaitIndexesReport, ReconcileReport, ReconcileInReport, MoveCheckpoint |
| `engram/internal/errors/v1/errors.proto` | `engram.internal.errors.v1` | never | Write-path and routing error details that must not cross the API boundary: MovedOutHint, FenceBusy, DocumentBusy, InputBlobMissing (N128) |
| `engram/internal/events/v1/events.proto` | `engram.internal.events.v1` | never | Outbox/Kafka `Event` envelope with the payload oneof; ids are 16-byte `bytes` on new field numbers, ≤ 256 per event, paged (`page`/`page_count`) and elided above 4,096 (N80); `DocumentDeleted` is an O(1) marker event; `DocumentTagsUpdated` (N157); no replay events (the N81 payloads are removed) |
| `buf.yaml` | — | — | Workspace: two BSR modules (`buf.build/engram/memory`, `buf.build/engram/internal`) at the same root, split by `includes`; lint STANDARD; breaking FILE |
| `buf.gen.yaml` | — | — | protoc-gen-go, protoc-gen-go-grpc, protoc-gen-connect-go → `../gen/go` (`<repo>/gen/go` per D14) |

Only well-known types are imported (`google/protobuf/{timestamp,duration,
field_mask,struct,any}.proto`), which buf ships built in, so every command
below works with no network access and no BSR dependency. `google/rpc/*` and
`google/api/*` are deliberately not imported: the typed error details live in
`memory/v1/errors.proto` and are attached to `google.rpc.Status` at runtime.

## CI commands

Run from this directory:

```bash
buf lint                                   # STANDARD rules, zero tolerance
buf build                                  # compiles all modules, catches import cycles / unused imports
buf format -d --exit-code                  # canonical formatting
buf generate                               # → ../gen/go (Go, gRPC, Connect)
```

Run from the repository root (the `.git#` input needs the repository, so this one
does not run from this directory; in the real repository the module path is `proto`):

```bash
# pull requests: the merge target, so a break against main's latest commit is reported (N157, review A-4)
buf breaking plans/engram/proto --against '.git#branch=main,subdir=plans/engram/proto'
# the push-to-main job only: the previous commit, until v1.0.0 (N139)
buf breaking plans/engram/proto --against '.git#branch=main,ref=HEAD~1,subdir=plans/engram/proto'
```

Release tagging (on `main`, after the checks above):

```bash
buf push --label "v$(cat VERSION)"        # publishes both modules to the BSR
```

`buf breaking` runs on every pull request. No release tag exists yet, so until
`v1.0.0` ships a pull request is compared with the merge target (`main`) and only
the push-to-main job uses `main@HEAD~1` (decisions N139, N157); each intended
break needs a `buf-breaking-exception` label on the pull request and a
changelog entry, and the breaks of rounds 3 and 4 are listed in plan section
4.5. From `v1.0.0` on the baseline is the latest release tag
(`.git#tag=proto/v1.0.0,subdir=…`), a failure blocks the merge and the only
override is a `v2` package. Every top-level internal payload carries
`schema_version = 2`.
