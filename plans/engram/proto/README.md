# Engram protobuf contracts

Source of truth for every Engram API surface. Generated Go stubs (`gen/go`),
the MCP adapter and the Connect/JSON adapter are all derived from these
files; nothing is hand-written against them (task statement, "Style").

## Layout

| Path | Package | Served? | Contents |
|---|---|---|---|
| `memory/v1/common.proto` | `memory.v1` | public | RequestMeta, NamespaceRef, paging, Budget, FactType, MemoryKind, UpdateMode, TagMatchMode/TagFilter, Provenance, Scores, TemporalWindow, Timestamps, EntityRef/EntityHint, MetadataFilter |
| `memory/v1/errors.proto` | `memory.v1` | public | Typed `google.rpc.Status` details: ValidationError, QuotaExceeded, NotFound, WrongShardOrEpoch (+ API-internal target shard hint, N98), NamespaceFrozen, OperationConflict, PreconditionFailed, and the write-path-internal DocumentBusy (N83) and InputBlobMissing (N100) (+ NamespaceState, ResourceKind, FreezeReason) |
| `memory/v1/operation.proto` | `memory.v1` | public | OperationService (Get, List, Cancel, Wait) and the Operation resource |
| `memory/v1/memory.proto` | `memory.v1` | public | MemoryService (Retain, Recall stream, Reflect stream, GetMemory, ListMemories, BatchGetMemories, Invalidate, Restore) and the Memory resource |
| `memory/v1/document.proto` | `memory.v1` | public | DocumentService (Get, List, Delete → Operation, GetDocumentVersion) |
| `memory/v1/namespace.proto` | `memory.v1` | public | NamespaceService (Create, Get, List, Update with field mask, Delete → Operation), Disposition |
| `memory/v1/export.proto` | `memory.v1` | public (phase 3) | ExportService (CreateSnapshot → Operation, GetSnapshotManifest, ListSnapshots, StreamSnapshot 1 MiB parts) |
| `memory/v1/page.proto` | `memory.v1` | public (phase 3) | PageService (Create, Get with as_of, List, Update, Delete, Refresh → Operation), staleness fields |
| `memory/admin/v1/admin.proto` | `memory.admin.v1` | admin only | TenantService, ShardService, MoveService — the only API where shard ids and epochs appear |
| `engram/internal/workflow/v1/workflow.proto` | `engram.internal.workflow.v1` | never | Temporal payloads: RetainDocumentInput, LoadItemResult, ChunkWork, Extract/Embed/Resolve/Link/Commit results, Consolidate*, StoreProposal*/ApplyBatch*, RefreshPageInput, PurgeInput, ExportInput, MoveInput, CopyProgress |
| `engram/internal/events/v1/events.proto` | `engram.internal.events.v1` | never | Outbox/Kafka `Event` envelope with the payload oneof; ids are 16-byte `bytes`, ≤ 256 per event, paged (`page`/`page_count`) and elided above 4,096 (N80); the N81 events for event-less writes (ProposalStored, ProposalDiscarded, BatchApplied, IdempotencyKeyStored, OperationTransitioned, BlobTombstoned, VersionsFlagged, SnapshotsExpired) |
| `buf.yaml` | — | — | Workspace: two BSR modules (`buf.build/engram/memory`, `buf.build/engram/internal`) at the same root, split by `includes`; lint STANDARD; breaking FILE |
| `buf.gen.yaml` | — | — | protoc-gen-go, protoc-gen-go-grpc, protoc-gen-connect-go → `../gen/go` (`<repo>/gen/go` per D14) |

Only well-known types are imported (`google/protobuf/{timestamp,duration,
field_mask,struct,any}.proto`), which buf ships built in, so every command
below works with no network access and no BSR dependency. `google/rpc/*` and
`google/api/*` are deliberately not imported: the typed error details live in
`memory/v1/errors.proto` and are attached to `google.rpc.Status` at runtime.

## CI commands (run from this directory)

```bash
buf lint                                   # STANDARD rules, zero tolerance
buf build                                  # compiles all modules, catches import cycles / unused imports
buf format -d --exit-code                  # canonical formatting
buf breaking --against '.git#branch=main,subdir=plans/engram/proto'   # FILE category vs main
buf generate                               # → ../gen/go (Go, gRPC, Connect)
```

Release tagging (on `main`, after the checks above):

```bash
buf push --label "v$(cat VERSION)"        # publishes both modules to the BSR
```

`buf breaking` runs on every pull request; a failure blocks the merge and the
only override is a `v2` package (see plan section 4.5).
