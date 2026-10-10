# Hindsight (vectorize-io/hindsight) — reference notes for a Go+Postgres reimplementation

Compiled 2026-09-30 from the public GitHub repo (`main`), the docs site (hindsight.vectorize.io),
and the paper arXiv:2512.12818. These notes REPORT what Hindsight does; they do not design anything.

**License.** MIT. `LICENSE` at repo root begins:
`MIT License` / `Copyright (c) 2025 Vectorize AI, Inc.` — all prompt text quoted in §10 is copied
from MIT-licensed source files under that copyright and must carry the notice if reused.

**How to read "verified" vs "UNVERIFIED".** Anything cited to a file path was read from the raw
file on `main` via a summarising fetch; verbatim quotes are marked as such. Numbers cited only to a
docs page are from the docs. Items I could not confirm are tagged **UNVERIFIED**.

**Important repo-layout fact.** The Python source lives under
`hindsight-api-slim/hindsight_api/` (not `hindsight-api/`, which is a thin wrapper package that
bundles the slim package with extra deps). Alembic migrations live at
`hindsight-api-slim/hindsight_api/alembic/versions/` (109 files as of `main`).

Top-level repo directories (from README/tree): `hindsight-api/`, `hindsight-api-slim/`,
`hindsight-all/`, `hindsight-all-slim/`, `hindsight-all-npm/`, `hindsight-cli/`,
`hindsight-clients/` (Python/Node/Go generated SDKs), `hindsight-control-plane/` (web UI),
`hindsight-docs/`, `hindsight-embed/`, `hindsight-extensions/`, `hindsight-integrations/`,
`hindsight-integration-tests/`, `hindsight-system-evals/`, `hindsight-system-tests/`,
`hindsight-tools/hindsight-agent-sdk/`, `helm/hindsight/`, `docker/`, `cookbook/`, `monitoring/`.

Key engine modules (all under `hindsight-api-slim/hindsight_api/`):

| Area | Path |
|---|---|
| Config (env vars, defaults) | `config.py`, `config_resolver.py` (per-bank "hierarchical" overrides) |
| Retain | `engine/retain/orchestrator.py`, `engine/retain/fact_extraction.py`, `engine/retain/chunk_storage.py`, `engine/retain/fact_storage.py`, `engine/retain/embedding_utils.py`, `engine/retain/memory_budget.py` |
| Links / entities | `engine/memories/pg/links.py` |
| Recall | `engine/search/retrieval.py` (orchestration), `engine/search/fusion.py` (RRF), `engine/search/reranking.py`, `engine/search/recall_boost.py`, `engine/search/graph_retrieval.py`, `engine/search/trace.py`; SQL arms in `engine/memories/pg/recall.py` and `engine/memories/pg/link_expansion.py` (**not read**) |
| Consolidation | `engine/consolidation/consolidator.py`, `engine/consolidation/prompts.py`, `engine/memories/pg/consolidation.py` |
| Reflect | `engine/reflect/agent.py`, `engine/reflect/prompts.py`, `engine/reflect/tools.py`, `engine/reflect/tools_schema.py`, `engine/reflect/delta_ops.py`, `engine/reflect/structured_doc.py`, `engine/reflect/retractions.py`, `engine/reflect/presentation.py` |
| Mental models | `engine/mental_model_refresh.py` |
| Directives | `engine/directives/models.py` |
| Cross-encoder | `engine/cross_encoder.py`, `engine/jina_mlx_reranker.py` |
| Embeddings | `engine/embeddings.py` |
| LLM | `engine/llm_interface.py`, `engine/llm_wrapper.py`, `engine/multi_llm.py`, `engine/providers/*.py` |
| MCP | `mcp_tools.py`, `api/mcp.py` |
| Extensions | `extensions/tenant.py`, `extensions/operation_validator.py`, `extensions/builtin/tenant.py` |
| Maintenance / worker | `engine/maintenance.py`, `engine/db/ops.py`, `engine/db/ops_postgresql.py` |
| Prompt preview | `engine/prompt_preview.py`, `engine/prompt_utils.py` |

---

## 1. Concepts

### Banks
- "A bank is an isolated memory store — one 'brain' for one user, agent, or project. Isolation is
  strict: no cross-bank leakage." (README)
- Banks are auto-created on first use: "You don't need to pre-create a bank. Hindsight will
  automatically create it with default settings when you first use it." (docs `/developer/api/memory-banks`)
- Profile fields: `bank_id`, `name`, `mission`, `background`, `metadata`. Config fields are a
  separate object (`PATCH /config`) — see §12 for the per-bank config keys.
- **Aliases**: "An alias is an extra id a bank answers to... nothing is copied, moved or
  duplicated." One alias may be `is_primary` (migrations `c8d1e4f7a20b_add_bank_aliases`,
  `d4f8b1c6e903_add_bank_alias_is_primary`).
- Banks table originally had a `personality JSONB` column; migrations
  `rename_personality_to_disposition` and `e0a1b2c3d4e5_disposition_to_3_traits` moved it to
  three traits; `x9s0t1u2v3w4_add_bank_config_column` added a per-bank `config` column.

### Memory / fact types
- Current public types: `world`, `experience`, `observation`.
  - world = "objective claim" (e.g. "Alice works at Google").
  - experience = "something the bank itself did" (e.g. "I recommended Python to Bob").
  - observation = "consolidated from many facts, with its evidence and history".
- History: the initial schema CHECK was `fact_type IN ('world','bank','opinion','observation')`
  (`alembic/versions/5a366d414dce_initial_schema.py`). Later migrations:
  `d9f6a3b4c5e2_rename_bank_to_interactions`, `i4d5e6f7g8h9_delete_opinions`,
  `g2h3i4j5k6l7_remove_opinion_fact_type`. The paper (2025-12) still describes four networks
  World/Experience(Bank)/Opinion/Observation with opinion = (text, confidence∈[0,1], timestamp);
  **opinions no longer exist in `main`**.
- The extraction LLM labels facts `"world"` or `"assistant"` (`fact_extraction.py`,
  `ExtractedFact.fact_type: Literal["world","assistant"]`); `"assistant"` is stored as
  `experience` — **UNVERIFIED** (mapping code not read, but the API only exposes world/experience).
- Each fact carries `occurred_start`, `occurred_end`, `mentioned_at` (paper: (τs, τe, τm)),
  plus `event_date` (DB column; made nullable in `aa2b3c4d5e6f_nullable_event_date`).

### Observations
- "consolidated, evidence-backed beliefs formed from many memories" (README). Each has
  `proof_count`, `source_memory_ids`, `history` JSONB, inherits tags and temporal bounds from
  sources; "references the specific memories (with quotes) that support it" (docs).
- **Observation scopes** (per retain item, `observation_scopes`): `"combined"` (default — one
  observation set per distinct full tag-set of the memory), `"shared"` (one global untagged
  belief), `"per_tag"`, `"all_combinations"`, or a custom array of tag lists. "Memories targeting
  different observation scopes must never share an LLM call." (`consolidator.py`)

### Mental models
- "a standing answer to a question about a bank" — user-curated; `name`, `source_query`, `tags`,
  `max_tokens`, `trigger` (refresh config), `content` (markdown), `reflect_response` provenance,
  `is_stale`, `last_refreshed_at`, `last_memory_seen_at`, version history. Details §6.

### Knowledge pages
- "living documents a bank writes about itself, organized in folders like a wiki, searchable, and
  projectable onto disk as ordinary markdown" (README). Built from observations, delta-refreshed
  after consolidation; nested folders; `hindsight fs mount --bank my-bank`. Details §6.

### Mission / directives / disposition
- `mission`: "bank's identity in plain language"; there are also per-operation missions in bank
  config: `retain_mission` (extraction guidance, "max ~500 chars recommended"),
  `observations_mission` (consolidation guidance), `reflect_mission` ("first-person narrative
  framing for reflect").
- `directives`: "hard rules it must never break"; fields `name`, `content`, `priority`,
  `is_active`, `tags`; "Untagged directives are global; tagged directives apply only when the
  reflect request carries matching tags." Migration `k6f7g8h9i0j1_add_directive_subtype`.
- `disposition`: `disposition_skepticism`, `disposition_literalism`, `disposition_empathy`,
  each 1–5, default 3 ("soft influence" vs directives = hard rules). Paper's Θ=(S,L,E,β) also had a
  bias strength β∈[0,1] (β=0.2 in experiments); β is **not** in the current API (**UNVERIFIED**
  whether any remnant exists in code).

### Tags
- Visibility scoping labels on memories/documents/directives/mental models (e.g. `user:alice`,
  `session:123`). Memories are "visible only if tags intersect with recall filter" subject to
  `tags_match`. Modes (recall/reflect): `any` (≥1 tag; includes untagged), `any_strict` (≥1; excludes
  untagged), `all` (all tags; includes untagged), `all_strict` (all; excludes untagged), `exact`
  (set equality; excludes untagged). `tag_groups` = compound boolean (`and`/`or`/`not`)
  expressions, mutually exclusive with `tags`. Migrations `g2a3b4c5d6e7_add_tags_column`,
  `z1u2v3w4x5y6_add_observation_tags_to_memory_units`.
- `GET /v1/{tenant}/banks/{bank_id}/tags?source=mental_models` lists tags by source; MCP
  `list_tags` supports glob `q`.

### document_id / upsert semantics
- `document_id` is caller-supplied (else random UUID per request → re-ingestion duplicates).
- `update_mode="replace"` (default): "Deletes old document and all associated memories, processes
  new content from scratch" — but with **delta retain**: chunks whose `content_hash` is unchanged are
  skipped and their facts preserved; changed chunks re-extracted; removed chunks tombstoned and
  their observations swept (`orchestrator.py`, `attempts_delta_retain()`).
- `update_mode="append"`: "Concatenates new content to existing document, reprocesses combined
  text (delta skips unchanged chunks)"; monotonicity guard `assert_append_extends_stored_body()`;
  `append_base_hash` + `ConcurrentAppendConflict` on concurrent modification.
- `operation_id` may be supplied by the caller for idempotent retries.
- "Reprocessing a document resets any curation applied to extracted facts."
- Documents store `retain_params` for replay (`c8e5f2a3b4d1_add_retain_params_to_documents`);
  `_RETAIN_PARAMS_NOT_REPLAYED` excludes content/document_id/update_mode/tags/force_reextract.

### Memory defense
- Opt-in per bank via `memory_defense` config: `{enabled, default_action, protected_namespaces,
  immutable_namespaces, rules:[{on:"sensitive_data", action:"redact"|"block"}], detector_overrides}`.
- "scrubs secrets and PII from retain content using a 45-pattern regex set"; matches replaced with
  `[REDACTED:type]` (e.g. `[REDACTED:github_token]`) "before content reaches memory units or the
  document body". Categories: AI/LLM keys (Anthropic, OpenAI, Google, Groq, HF, Replicate,
  Perplexity, Databricks), cloud (AWS, DigitalOcean), source control (GitHub PAT, GitLab, npm,
  PyPI), payments (Stripe, Square, Braintree), comms (Slack, Twilio, SendGrid, Mailgun, Discord,
  Telegram), Shopify, DB connection strings (Postgres/MySQL/Mongo), PEM keys, JWTs, US credit
  cards, SSNs.
- `block`: item dropped; if every item blocked → HTTP 422 (`MemoryDefenseAllBlockedError`);
  partial → 200 with violations logged. Not retroactive. Fires `memory_defense.triggered` webhook
  and an audit-log entry. Implemented as `MemoryDefenseExtension.screen()` in "Phase 0" of retain.

---

## 2. Retain pipeline

Source: `engine/retain/orchestrator.py`, `engine/retain/fact_extraction.py`,
`engine/memories/pg/links.py`, docs `/developer/retain`, `/developer/api/retain`.

### Endpoint & inputs
`POST /v1/{tenant}/banks/{bank_id}/memories/retain` with `items[]` of
`{content, timestamp|"unset", context, metadata{}, document_id, tags[], entities[{text,type?}],
resolve_entities=true, observation_scopes="combined", update_mode="replace"}`, plus top-level
`async=false`, `operation_id`. `retain-files` variant (PDF/DOCX/PPT/XLSX/images with OCR/audio
with transcription/HTML/text; ≤10 files, ≤100 MB; always async; per-file context/document_id/tags).

### Stages (progress stage names from `set_stage()` in `orchestrator.py`)
0. **Memory defense** (`screen()` per item → ALLOW/REDACT/BLOCK).
1. **Chunking** — `fact_extraction.iter_chunks()`; streamed ("peak here is one chunk rather than
   the intermediate splits"). Params from resolved bank config: `retain_chunk_size` (default
   **3000 characters**), `retain_structured_chunk_size` (separate size so structured records such
   as JSON arrays are not split), `retain_max_attachments_per_chunk`. Chunks are batched into
   `retain_chunk_batch_size` (default **100** chunks) per streaming batch. Chunk overlap:
   **UNVERIFIED** (not seen). Paper describes the goal as "2–5 comprehensive facts per
   conversation", narrative not sentence-level.
2. **`retain.extract_and_embed`** — `extract_facts_from_contents()` → one LLM call per chunk
   (structured output, `ExtractedFact` list) then `generate_embeddings_batch()`.
   `_process_extracted_facts()` filters degenerate facts and remaps causal relations
   (`_remap_causal_relations()`).
3. **`retain.phase1.resolve`** — entity resolution on a *separate* connection (no row locks):
   trigram GIN scan + co-occurrence fetch (`entity_processing.resolve_entities()`), placeholder
   unit ids; also precomputes semantic ANN candidates.
4. **`retain.phase2.insert_facts`** — one write transaction: `insert_facts_batch()`,
   `reassert_entities_batch()` (locks parent entities so `prune_orphan_entities` cannot race),
   `link_units_to_entities_batch()` (writes `unit_entities` with `fact_date`), temporal links,
   semantic links (ANN from phase 1 + within-batch), causal links; `handle_document_tracking()`
   writes the `documents` row on the first batch and tombstones the previous document on replace.
5. **Phase 3 (best-effort, post-commit)** — entity links are *not* materialised; graph retrieval
   uses a `unit_entities` self-join (migration `e9b2c7d1f3a4_drop_entity_memory_links`).
   `_run_final_semantic_ann()` with `_ANN_CHUNK_SIZE = 1000` seeds/query, `_ANN_PARALLELISM = 4`.
6. Consolidation is enqueued as a separate `consolidation` operation (bank-deduped) if
   `enable_auto_consolidation` (default true).

Multi-document retain runs up to `_GROUP_CONCURRENCY = 8` document groups concurrently.
Per-retain memory ceiling `HINDSIGHT_API_RETAIN_MEMORY_BUDGET_MB = 128`.

### Extraction LLM call
- Modes (`retain_extraction_mode`): `concise` (default), `verbose`, `custom`
  (`retain_custom_instructions`), `verbatim` ("The original text will be stored as-is in code. Your
  ONLY job is to extract metadata... Produce EXACTLY ONE entry per input chunk"), `chunks`
  (**UNVERIFIED** semantics; presumably no LLM extraction, chunks stored directly).
- Temperature `0.1`; `HINDSIGHT_API_RETAIN_MAX_COMPLETION_TOKENS = 64000`;
  `HINDSIGHT_API_RETAIN_EXTRACT_CAUSAL_LINKS = true`;
  `HINDSIGHT_API_RETAIN_OPTIONAL_FACT_DIMENSIONS = false` (when true, when/where/who/why may be
  empty); optional Batch API (`HINDSIGHT_API_RETAIN_BATCH_ENABLED`, "50% LLM cost savings",
  async only). Concurrency `HINDSIGHT_API_RETAIN_MAX_CONCURRENT = 32` and global
  `HINDSIGHT_API_LLM_MAX_CONCURRENT = 32`.
- **Schema** (`fact_extraction.py`, verbatim field list):

```python
class FactCausalRelation(BaseModel):
    target_index: int      # "PREVIOUS fact only (0-based, < this fact's position)"
    relation_type: Literal["caused_by"]

class ExtractedFact(BaseModel):
    what: str              # "Core fact - concise but complete (1-2 sentences)"
    when: str              # "When it happened. 'N/A' if unknown."
    where: str             # "Location if relevant. 'N/A' if none."
    who: str               # "People involved with relationships. 'N/A' if general."
    why: str               # "Context/significance if important. 'N/A' if obvious."
    fact_kind: str = "conversation"        # "'event' or 'conversation'"
    occurred_start: str | None = None      # "ISO timestamp for events"
    occurred_end: str | None = None        # "ISO timestamp for event end"
    fact_type: Literal["world", "assistant"]
    entities: list[str] = []               # plain strings
    causal_relations: list[FactCausalRelation] | None = None
    from_attachments: list[int] | None = None
```
- How the stored `text` is assembled from what/when/where/who/why: **UNVERIFIED** (assembly code
  not read; docs examples show narrative sentences like "Alice Chen worked 80-hour weeks and
  burned out", suggesting `what` plus appended dimensions).
- Per-chunk user message (verbatim template):
  ```
  {mission_preamble}Extract facts from the following chunk.

  Chunk: {chunk_index + 1}/{total_chunks}
  Event Date: {event_date_str}
  Context: {sanitized_context}{metadata_section}{narrator_section}{attachment_section}

  Content:
  {sanitized_chunk}
  ```
- Language: "Write every fact in the same language and script as the input text. Never translate."
  (`HINDSIGHT_API_LLM_OUTPUT_LANGUAGE` can force a language.)
- Vision: image attachments go to a VLM slot (`HINDSIGHT_API_VLM_*`, default = retain LLM);
  `RETAIN_ATTACHMENT_MAX_SIZE_MB=20`, `RETAIN_ATTACHMENT_MAX_COUNT=50`.

### Embeddings
- Default provider `local` with `BAAI/bge-small-en-v1.5` (384-d; matches `vector(384)` in initial
  schema). Alternatives: `onnx` (`intfloat/multilingual-e5-small`, 512 max tokens, mean pooling,
  L2-normalize=true, batch 32), `openai` (`text-embedding-3-small`, batch 100), `tei`, `gemini`
  (`gemini-embedding-001`, 768-d), `cohere` (`embed-english-v3.0`), `litellm`.
- `EMBEDDINGS_MAX_INPUT_TOKENS=8192` truncation; `EMBEDDINGS_QUERY_PREFIX` /
  `EMBEDDINGS_PASSAGE_PREFIX` instruction prefixes (unset by default); concurrency 8; retries 4,
  backoff 0.5→4.0 s, retry budget 15 s.
- Embedding dimension is fixed per deployment ("Embeddings provider settings are
  deployment-wide, not per-bank").

### Entity resolution
- "fuzzy name matching, reinforced by co-occurrence and temporal proximity" (docs). Paper: weighted
  similarity of string similarity + co-occurrence + temporal proximity.
- Implementation: pg_trgm GIN index on `entities.canonical_name` (bank-scoped, case-insensitive:
  migrations `c1a2b3d4e5f6_enable_pg_trgm_and_entities_trgm_index`,
  `7c2e5a9d1f40_entities_bank_scoped_trgm_index`, `2eee35aa3cfc_case_insensitive_entities_trgm_index`,
  `b3e8d1c6f4a9_entity_kind_partial_trgm_index`).
- Thresholds: `ENTITY_TRGM_SIMILARITY_THRESHOLD=0.15`, `ENTITY_INTRABATCH_MERGE_SIMILARITY=0.5`,
  `ENTITY_MERGE_MIN_SIMILARITY=0.3`; `RETAIN_ENTITY_LOOKUP="trigram"` (or `full`);
  `RETAIN_ENTITY_RESOLUTION_BATCH_SIZE=100` names/query; `..._MAX_CANDIDATES=200` per mention.
- Names normalised (`_normalize_entity_name()` collapses whitespace); names > `_MAX_ENTITY_NAME_CHARS = 512`
  dropped as "extraction artifact[s]". Unique index `(bank_id, LOWER(canonical_name))`.
- `entity_cooccurrences(entity_id_1 < entity_id_2, cooccurrence_count, last_cooccurred)` maintained.
- Caller may pass guaranteed `entities` and `entity_labels` (controlled vocabulary with types
  `value|multi-values|text|multi-text|map`; `entities_allow_free_form` toggles NER alongside).

### Link building (`engine/memories/pg/links.py`)
| Link type | How | Weight | Caps / constants |
|---|---|---|---|
| temporal | `create_temporal_links_batch_per_fact()`; facts within `time_window_hours=24`, grouped by fact_type, sorted by `event_date`, walk next `max_per_unit` | `max(0.3, 1.0 - time_diff_hours/24)` | `MAX_TEMPORAL_LINKS_PER_UNIT = 20`; `TEMPORAL_LATERAL_BATCH = 500` |
| semantic | `compute_semantic_links_ann()` (pgvector `1 - (embedding <=> seed)` LATERAL, `top_k=50`, per fact_type) + `compute_semantic_links_within_batch()` (numpy, `_SEMANTIC_WITHIN_BATCH_BLOCK_ROWS=256`) | cosine sim | threshold `SEMANTIC_LINK_MIN_SIMILARITY = 0.7` |
| causal | `create_causal_links_batch()` from `ExtractedFact.causal_relations` (target must be an earlier fact in same batch) | `1.0` | canonical type `caused_by`; legacy `causes/caused_by/enables/prevents` accepted on import (`restore_legacy_causal_links_batch()`); archive table `c7d1e9a4b3f2_add_archive_causal_links` |
| entity | not stored as links since `e9b2c7d1f3a4`; derived from `unit_entities` self-join at query time (paper: w=1.0) | — | link expansion per-entity limit `200` (`LINK_EXPANSION_PER_ENTITY_LIMIT`) |

All link inserts go through `_bulk_insert_links()` sorted by `_lock_order_key()` =
`(LEAST(from,to), GREATEST(from,to), link_type, entity)` to avoid deadlocks; `memory_links` FK made
deferrable (`9f8e7d6c5b4a`); a `graph_maintenance` operation drains a "link top-up" queue and
"entity prune" queue after deletes (`b5a4c3e2f1d8_add_graph_maintenance_queue`,
`c4f7a91b2d38_add_entity_maintenance_queue`).

### Async operations
- `async=true` → `202`-style response `{operation_id, async:true}`; sync response
  `{success, bank_id, items_count, async:false, usage{input_tokens,output_tokens,total_tokens},
  memory_ids[]}`.
- Operation types: `retain`, `retain_batch` (parent of children), `file_convert_retain`,
  `consolidation`, `refresh_mental_model`, `graph_maintenance`, `webhook_delivery`.
  Statuses: `pending|processing|completed|failed|cancelled`. Worker pool
  `HINDSIGHT_API_WORKER_MAX_SLOTS=10`, per-type reservations (`WORKER_SLOT_TYPE_DEFAULTS`:
  consolidation 2, others 0), `WORKER_MAX_RETRIES=3`, `OPERATION_RETENTION_DAYS`, slots rotate
  across banks. `_persist_operation_document_id()` appends document ids to `result_metadata`.

---

## 3. Recall

Sources: docs `/developer/retrieval`, `/developer/api/recall`; `config.py`;
`engine/search/retrieval.py` (orchestration only — "hands every recall arm ... to the memories
store's `recall_unified`"; per-arm SQL in `engine/memories/pg/recall.py` and
`link_expansion.py`, **not read**).

### Request
`POST /v1/{tenant}/banks/{bank_id}/memories/recall`
`{query (≤500 tokens, RECALL_MAX_QUERY_TOKENS), types[world|experience|observation],
prefer_observations=false, budget=low|mid|high (default mid), max_tokens=4096,
query_timestamp (ISO; default server now), temporal_window{start,end}, tags[], tags_match=any,
tag_groups[], include{chunks: bool|{max_tokens:8192}, source_facts: bool|{max_tokens:4096},
entities: bool|{max_tokens:500} (on by default)}, trace=false,
min_scores{semantic,keyword,reranker,final}}`.

### Budget
| budget | fixed (default `recall_budget_function=fixed`) | adaptive (`ratio × max_tokens`, clamp [20, 2000]) |
|---|---|---|
| low | 100 | 0.025 × max_tokens |
| mid | 300 (default) | 0.075 × max_tokens |
| high | 1000 | 0.25 × max_tokens |

Budget = "how many candidates each strategy considers". Per-arm split of the budget:
**UNVERIFIED** (lives in `pg/recall.py`). `RECALL_MAX_CANDIDATES_PER_SOURCE` (0 = off) can cap
per-arm candidates. Arms can be toggled: `ENABLE_TEXT_SEARCH`, `ENABLE_TEMPORAL_RETRIEVAL`,
`ENABLE_GRAPH_RETRIEVAL`, `ENABLE_RERANKING` (all per-bank overridable).
`RECALL_STRATEGY_BOOSTS="strategy:level,..."` with levels low/medium/high (per-arm RRF weighting).

### The four arms
1. **Semantic** — pgvector cosine (`<=>`), HNSW (`vector_cosine_ops`), `SEMANTIC_MIN_SIMILARITY=0.3`;
   `ANN_ITERATIVE_SCAN=true`, `ANN_MAX_SCAN_TUPLES=4000`; alternative index backends
   `vchord|pgvectorscale|scann`; optional per-bank partial vector indexes
   (`VECTOR_INDEX_MIN_ROWS`, `a4b5c6d7e8f9_fix_per_bank_vector_index_type`).
2. **Keyword / BM25** — backend `native` (Postgres `tsvector` generated column
   `search_vector = to_tsvector('english', text || ' ' || context)` + GIN; language configurable
   via `TEXT_SEARCH_EXTENSION_NATIVE_LANGUAGE`, migration `p4q5r6s7t8u9_configurable_bm25_language`),
   or `vchord`, `pg_textsearch`, `pgroonga`, `pg_search` (ParadeDB). "Only `pg_search` and `native`
   support Citus". Query capped at `BM25_MAX_QUERY_TERMS=16`, keeping "most selective terms"
   (`BM25_SELECTIVE_TERMS=true`, test `test_bm25_term_selection.py`); `BM25_MIN_SCORE=0.0`. The
   initial schema also had a `memory_units_bm25` materialized view with a `doc_length_factor`
   (`log(1 + len/avg_len)`), later dropped (`f3a5b7c9d1e2_drop_memory_units_bm25_matview`); so
   "BM25" in native mode is Postgres `ts_rank`-style scoring — exact function **UNVERIFIED**.
   A `text_signals` column exists (`a2b3c4d5e6f8_add_text_signals_column`) — purpose **UNVERIFIED**.
3. **Graph** — "spreading activation from semantic entry points" (paper); seeds need
   `GRAPH_SEED_MIN_SIMILARITY=0.3`; expands over `memory_links` (semantic/causal/temporal) and
   `unit_entities` self-join (entity), `LINK_EXPANSION_PER_ENTITY_LIMIT=200`; "explores up to
   recall budget nodes". Scoring = three additive signals: entity overlap
   `tanh(shared_entity_count × 0.5)` (1→≈0.46, 2→≈0.76, 3+→≈0.91+), semantic-link weight ∈[0.7,1.0],
   causal-link weight ∈[0,1.0]; sum ∈ [0,3]. Hop count: **UNVERIFIED** (docs mention "multi-hop").
4. **Temporal** — rule-based date parsing (paper mentions a T5-small fallback — **UNVERIFIED** in
   current code; `query_analyzer.py` exists); the window (explicit `temporal_window` or parsed) is
   divided into time buckets; "selection is relevance-first"; "The strongest match from each
   populated bucket is taken first"; `TEMPORAL_SEMANTIC_MIN_SIMILARITY=0.1`. Temporal date indexes
   (`b3c4d5e6f7g8_add_temporal_date_indexes`).

### Fusion, reranking, boosts, packing
- **RRF**: `score(d) = Σ 1/(k + rank_i(d))`, `k = 60`, "All four strategies are weighted equally"
  (`engine/search/fusion.py`; docs).
- **Reranker**: `RERANKER_PROVIDER=local`, model `cross-encoder/ms-marco-MiniLM-L-6-v2`
  (`engine/cross_encoder.py`); also `tei`, `cohere`, `openrouter`, Jina-MLX. Pre-filter cap
  `RERANKER_MAX_CANDIDATES=300` (per-budget overrides `_LOW/_MID/_HIGH`, 0 = global); batch 32
  local / 128 TEI; `RERANKER_LOCAL_TIMEOUT=300s`, `RERANKER_LOCAL_MAX_CONCURRENT=4`. Scores in
  [0,1] pass through; raw logits → sigmoid; if no cross-encoder, "RRF-derived scores [0.1, 1.0]".
- **Boosts** (multiplicative, `engine/search/recall_boost.py`):
  `final = CE_normalized × recency × temporal × proof`.
  - recency α=0.2 (±10%): `clamp(1 - days_ago/365, 0.1, 1.0)` relative to `query_timestamp`;
    no-date memories neutral (0.5). Decay function options `linear|exponential|none`.
  - temporal proximity α=0.2 (±10%), only when the query has a time reference:
    `1 - min(days_from_center / (window_days/2), 1)`.
  - proof count α=0.1 (±5%) for observations: `clamp(0.5 + ln(proof_count)/10, 0, 1)`
    (1→neutral, 3→+1.1%, 150+→+5%).
  - combined best ≈ +27%, worst ≈ −23%.
- **Token packing**: results taken "top-down until the max_tokens budget is exhausted"; "A single
  long fact does not cost you the shorter ones ranked behind it" (skip-and-continue); the
  top-ranked result is always returned whole; only memory text counts, metadata is free.
- **Raw chunks**: not a retrieval arm — chunks are returned as context for matched facts when
  `include.chunks` is set (chunk `max_tokens` default 8192); each result carries `chunk_id`.
  (In `verbatim`/`chunks` extraction modes the memory text *is* the chunk text — see §2.)
- `prefer_observations=true`: "Suppress raw facts when observations subsume them".
- Concurrency: `RECALL_MAX_CONCURRENT=32`, `RECALL_CONNECTION_BUDGET=4` DB connections per
  recall; admission control `ADMISSION_RECALL_MAX_WAIT_MS=30000`. Read replica supported
  (`READ_DATABASE_URL`).

### Response
`results[]: {id, text, type, context, metadata, tags[], entities[] (canonical names),
occurred_start, occurred_end, mentioned_at, document_id, chunk_id, source_fact_ids[] (observations),
scores{final, reranker (0–1), semantic (0–1), keyword (≥0)}}`; plus `source_facts{id→RecallResult}`,
`source_facts_truncated`, `chunks{id→{id,text,chunk_index,truncated}}`,
`entities{name→{entity_id, canonical_name, observations}}`, `trace` (timings, per-arm results,
RRF data, `rerank_prefilter` kept/dropped, active boosts). Docs note scores are "relative signals
... not absolute cross-query confidence".

---

## 4. Consolidation into observations

Sources: `engine/consolidation/consolidator.py`, `engine/consolidation/prompts.py`,
docs `/developer/observations`, `/developer/api/memory-banks`.

- **Trigger**: `run_consolidation_job(bank_id, operation_id?, observation_scopes?, pending_refresh_tags?)`
  runs "as a background job after retain operations complete" (also after delete/update when
  `enable_auto_consolidation`); manual via consolidate endpoint / MCP; disable with
  `HINDSIGHT_API_ENABLE_AUTO_CONSOLIDATION=false`; `enable_observations=false` disables entirely.
  The `consolidation` operation is bank-deduped ("repeat submits return existing operation_id").
- **Selection**: `_fetch_unconsolidated_rows()` — facts with NULL `consolidated_at`
  (`s4n5o6p7q8r9_add_consolidated_at_to_memory_units`; failures stamped in
  `consolidation_failed_at`, `a3b4c5d6e7f8`). Scoped queries OR each "tags ⊇ scope". Fair fetch when
  `consolidation_llm_parallelism > 1`: read `_FAIR_FETCH_OVERFETCH = 5` rounds, take at most
  `ceil(limit / parallelism)` per group. `_COUNT_LIMIT = 100_000`.
- **Grouping/batching**: group key `_consolidation_batch_key()` = resolved observation scopes
  (security boundary; "must never share an LLM call"); each group split into
  `consolidation_llm_batch_size` facts per LLM call (docs default **8**); batches within a group
  serial; groups parallel up to `consolidation_llm_parallelism`;
  `consolidation_max_memories_per_round` caps a round (0 = unlimited) and re-queues with
  `pending_refresh_tags`. Scope locks taken in `_scope_sort_key()` order.
- **Candidate observations per batch**: `recall_unified(fact_types=["observation"], dense + BM25,
  limit=_DEDUP_TOP_K=5 ..., tags_match="all_strict" if scoped else "any", no graph, no temporal)`;
  shown to the LLM as existing observations (with an `observation_capacity_note` when
  `max_observations_per_scope` applies: −1 unlimited, 0 none, >0 hard cap).
- **LLM output** (temperature 0.0):
  ```python
  class _ConsolidationBatchResponse(BaseModel):
      creates: list[_CreateAction]   # text, source_fact_ids[], reason
      updates: list[_UpdateAction]   # observation_id, text, source_fact_ids[], reason
      deletes: list[_DeleteAction]   # observation_id (alias "id"), reason
  ```
  Text sanitised via `sanitize_llm_output()`. Exact-text dedup `_duplicate_create_target()` drops a
  CREATE whose whitespace-collapsed text equals a shown observation or an UPDATE in the same
  response.
- **Observation row**: `text`, `embedding`, `proof_count` ("Number of supporting memories"),
  `source_memory_ids[]` (GIN index, `a2b3c4d5e6f8`; normalised into `observation_sources` table
  `k6l7m8n9o0p1`), `history` JSONB (split into own tables `a7b8c9d0e1f2_split_history_into_own_tables`;
  `ENABLE_OBSERVATION_HISTORY`, `OBSERVATION_HISTORY_MAX_ENTRIES`), tags inherited, temporal bounds
  merged via `_TemporalBounds.merged_with()` (`event_date`/`occurred_start` = min,
  `occurred_end`/`mentioned_at` = max). Per-observation evidence token caps:
  `consolidation_source_facts_max_tokens` (unlimited), `..._per_observation` (256).
- **Bisect-on-failure**: on LLM failure (schema/rate-limit/timeout) with `len(sub_batch) > 1`,
  split at `mid = len//2` and re-queue both halves at the front; a single-fact batch that still
  fails is stamped `consolidation_failed_at` and not retried.
- **Commit discipline**: `store.mark_consolidated(when, failed=...)` is written in the same
  transaction as the creates/updates/deletes ("a stamp and the writes it accounts for must never
  commit apart").
- **Near-duplicate reconciliation** (Postgres only, when `consolidation_dedup_threshold < 1.0`;
  default `0.97` cosine): `_dedup_adjudicate()` probes the new text embedding against top-5
  observations; candidates ≥ threshold go to an LLM `_DEDUP_PROMPT` ("You reconcile long-term
  memory observations... If they assert the SAME fact (wording aside), set 'action' to 'merge' and
  provide 'text': a single observation that preserves EVERY detail from both") →
  `_DedupDecision{action: merge|keep, text}`. Merge: CREATE folds sources into the twin
  (`fold_sources_into_observation()`); UPDATE folds and deletes the updated row
  (`_apply_dedup_update_fold()` + `_execute_delete_action()`).
- **Deletes**: when source memories are deleted, dependent observations are deleted and the
  remaining source memories' `consolidated_at` reset (`delete_stale_observations_for_memories()`;
  orphan sweeps `g7h8i9j0k1l2` / `c4x5y6z7a8b9`). Retagging a document with a different tag set
  triggers re-consolidation.
- **Freshness**: observations record last update; reflect treats "stale observations" (newer
  unconsolidated memories in scope) as needing verification against raw facts.
- After a *complete* run (not a round-limited re-queue) `_trigger_mental_model_refreshes()` fires
  for models/pages whose scope tags were touched.
- Observations are searchable by BM25 too (`c3f7a1b9d2e4_backfill_observation_search_vector`).

---

## 5. Reflect

Sources: `engine/reflect/agent.py`, `engine/reflect/prompts.py`, docs `/developer/reflect`,
`/developer/api/reflect`.

- Endpoint `POST /v1/{tenant}/banks/{bank_id}/reflect` (path inferred from SDK; docs page omits
  it — **UNVERIFIED path**). Request: `query` (only required field), `budget=low` (default for
  reflect; `mid`/`high`), `max_tokens=4096` (visible answer length), `response_schema` (JSON Schema
  object with ≥1 property), `tags`, `tags_match`, `tag_groups`,
  `reflect_search_observations_max_tokens=5000`, `reflect_search_observations_include_entities=true`,
  `include{facts: bool, tool_calls: bool|{output:false}}`, `context`.
- Response: `text` (markdown), `structured_output` / `structured_output_error`,
  `based_on{memories[{id,text,type,context,occurred_start,occurred_end}], mental_models[{id,text,context}],
  directives[{id,name,content}]}`, `usage{input_tokens,output_tokens,total_tokens}`,
  `trace{tool_calls[{tool,input,output,duration_ms,iteration}], llm_calls[{scope,duration_ms}]}`.
- **Agent loop** (`agent.py`): `DEFAULT_MAX_ITERATIONS = 10`; `max_context_tokens = 100_000`
  (forces final synthesis when `_count_messages_tokens(messages) >= max_context_tokens` and
  evidence exists); no wall-clock timeout in `agent.py` — `HINDSIGHT_API_REFLECT_WALL_TIMEOUT`
  bounds the operation at a higher layer (value default **UNVERIFIED**);
  `HINDSIGHT_API_REFLECT_LLM_TIMEOUT=30s` per LLM call; temperature 0.9. Tool-result size shared
  across parallel tool calls via `_resolve_tool_arg_ceiling()` between `_TOOL_ARG_MIN_TOKENS=1000`
  and `_TOOL_ARG_MAX_TOKENS=16000`.
- **Tools** (`reflect/tools.py`, `tools_schema.py`), in forced hierarchical order then `auto`:
  1. `search_mental_models(query, max_results)` → `{mental_models:[{id, snippet, is_stale}]}`;
     best match in full, snippets of others; if `_all_mental_models_are_usable_and_fresh()` and
     budget is low/mid, forcing stops (`stop_forcing_from_iteration`).
  2. `read_mental_models(mental_model_ids, max_tokens)` → full content; `DEFAULT_MENTAL_MODELS_READ_MAX_TOKENS = 6000`.
  3. `search_observations(query, max_tokens)` → `DEFAULT_OBSERVATIONS_TOOL_MAX_TOKENS = 5000`.
  4. `recall(query, max_tokens, max_chunk_tokens)` → raw facts (+ chunks).
  5. `expand(memory_ids, depth)` → surrounding source text (omitted when `store_document_text=False`).
  6. `done(answer, document, memory_ids, mental_model_ids, observation_ids)`.
  Knowledge-page tools (`agent_knowledge_*`) exist for the agent SDK (docs) — whether they are in
  the reflect tool set: **UNVERIFIED**. Directive rules can gate tool eligibility
  (`get_reflect_tools(..., directive_rules=...)`).
- **Guardrails**: `done` without any evidence id (and not last iteration) is rejected; `done`
  with no `answer`/`document` → `ReflectNoAnswerError`; empty final synthesis →
  `ReflectNoAnswerError` (issue #2959). Empty *retrieval* is not an error.
- **Citations**: ids tracked per tool (`available_memory_ids`, `available_mental_model_ids`,
  `available_observation_ids`); in `_process_done_tool()` only ids in those sets pass
  ("only IDs that were actually retrieved can be cited"); hallucinated ids silently dropped.
- **Mission/directives/disposition**: `bank_profile["mission"]` (plus `reflect_mission`) feeds
  `build_system_prompt_for_tools()` and `build_final_system_prompt()`; directives injected "at
  START and END" of the system prompt; disposition traits verbalised only when set (see §10).
- **Structured output**: after the answer, a second LLM call (scope `reflect_structured`,
  temperature 0.0, "You are a precise data extraction assistant...", `response_format=DynamicModel`
  built from the schema) → `structured_output` or `error`.
- **Split synthesis** when context overflows: `split_context_history()` → parallel map calls
  (`build_chunk_claims_prompt()`, scope `final_map_N`, temp 0.0) producing claim bullets with
  `(mentioned_at: …; occurred: …; memory_ids: …)` provenance → single reduce (scope `final`).
  `synthesis_max_completion_tokens = reflect_max_completion_tokens` (uncapped by default).
- **Prompt caching**: `incremental_caching` — `_schedule_cache(upto)` builds a provider cache
  session in the background after each tool round; next `auto` turn passes
  `cached_prefix=rolling_cache_name`; cleanup detached (`_spawn_cache_cleanup()`).
  `HINDSIGHT_API_REFLECT_PROMPT_CACHE_ENABLED=true`, `LLM_CACHE_AFFINITY=auto`.
- Failure modes: 500 on tool failure, empty model output, provider error.

---

## 6. Mental models / knowledge pages

Sources: docs `/developer/mental-models`, `/developer/api/mental-models`, `/developer/knowledge-pages`,
`/developer/mcp-server`; `engine/mental_model_refresh.py` (not read), `reflect/delta_ops.py`,
`reflect/structured_doc.py`, `reflect/retractions.py`.

### Mental models
- Definition: `{id (lowercase alnum + hyphens; text id since m8h9i0j1k2l3/u6p7q8r9s0t1), name,
  source_query, tags[], max_tokens, trigger{...}}`; creation runs a reflect with `source_query`
  and saves `content` + `reflect_response` ("saved reflect responses").
- `trigger` fields and defaults: `mode="full"|"delta"` (default full; knowledge pages default
  delta), `refresh_after_consolidation=false`, `refresh_cron=null` (5-field UTC; mutually
  exclusive with consolidation trigger per MCP docs), `min_refresh_interval_seconds=null`,
  `tags_match="all_strict"` for tagged models, `tag_groups`, `fact_types`,
  `exclude_mental_models=false`, `exclude_mental_model_ids`, `include_chunks`,
  `recall_max_tokens`, `recall_chunks_max_tokens`, `reflect_search_observations_max_tokens=5000`,
  `reflect_search_observations_include_entities=true`, `budget="mid"`, `response_schema`,
  `keep_trace=false`.
- **Staleness**: `last_refreshed_at` (wall clock) vs `last_memory_seen_at` (newest in-scope memory
  read); `is_stale` = bank `last_memory_write_at` newer than `last_memory_seen_at` within scope.
  "Untagged writes don't mark tagged models stale. Deletions are invisible (no write artifact). No
  LLM call if nothing is in scope to read." Stale models "are still shown, but [no longer]
  short-circuit retrieval" in reflect.
- **Refresh**: `POST .../mental-models/{id}/refresh` (coalesces: ≥1 queued per model; serial per
  model, parallel across models); `dry-run-refresh` returns `requested_mode/effective_mode`,
  `mode_fallback_reason`, `scope`, `window{created_after,created_before}`, `facts.retrieved/used`,
  `delta_operations{emitted,applied,skipped}`, unified `diff`, `outcome/would_persist`, `warnings`;
  `clear` wipes content to force a full rebuild.
- **Delta mode** ops (`reflect/delta_ops.py`): `add_section`, `remove_section`, `rename_section`,
  `replace_section_blocks`, `append_block`, `insert_block`, `replace_block`, `remove_block`;
  "Preserves unchanged content byte-identical". Falls back to full when: no baseline,
  `source_query` changed, structured doc unreadable, all ops rejected. Retractions
  (`retractions.py`) remove content resting on retracted facts only.
- **History**: `GET .../history` → entries `{previous_content, changed_at, kind:"refresh_failed"?,
  outcome, failure_reason (retrieval_failed|no_answer|empty_candidate|delta_ops_failed|
  structured_output_failed|…), error_message}`; `ENABLE_MENTAL_MODEL_HISTORY=true`,
  `MENTAL_MODEL_HISTORY_MAX_ENTRIES=50`; global floor
  `MENTAL_MODEL_MIN_REFRESH_INTERVAL_SECONDS=0`; refresh LLM defaults to the reflect LLM.
- Detail levels on list: `metadata` (default: id, bank_id, name, tags, is_stale,
  last_refreshed_at, last_memory_seen_at, created_at), `content` (+source_query, content,
  max_tokens, trigger), `full` (+reflect_response).
- Schema lineage: `h3c4d5e6f7g8_mental_models_v4`, `p1k2l3m4n5o6_new_knowledge_architecture`,
  `t5o6p7q8r9s0_rename_mental_models_to_observations` (the *old* "mental models" became today's
  observations), `j5e6f7g8h9i0_mental_model_versions`, `f2a7c9d4b168_add_mental_models_cron_index`,
  `f4d1c2b3a5e6_add_scheduled_mental_model_refresh_routine`, `b8d3f1a6c2e4_..._last_refresh_failed_at`.

### Knowledge pages
- "Each page answers one question"; organised in folders ("Nesting is arbitrary, page names are
  unique within their folder, and deleting a folder deletes its subtree"); "built from the bank's
  observations" (observation-only fact type by default), refresh in delta mode after consolidation.
- Search: "at the document level: a query returns whole pages, ranked, with snippets... combines
  full-text and semantic matching, fused server-side, with no reranking step" (MCP
  `search_knowledge_base(query, limit 1–50 default 10)`).
- Status: "up to date", "out of date", "just updated". Disk projection: `hindsight fs mount
  --bank my-bank` → directories + markdown with YAML frontmatter, background refresh loop.
- API/MCP: `get_knowledge_base_tree`, `get_knowledge_page`, `create_knowledge_folder(name,parent_id)`,
  `create_knowledge_page(name, source_query, parent_id, tags, max_tokens, trigger,
  refresh_after_consolidation)`, `update_knowledge_node`, `delete_knowledge_node`.
  Migration `a9b8c7d6e5f4_add_knowledge_pages`.

---

## 7. Delete semantics

- **Document delete** `DELETE /banks/{bank_id}/documents/{document_id}`: "permanently removes all
  memories extracted from it. This action cannot be undone." Cascade: memories (FK
  `memory_units(document_id, bank_id) → documents ON DELETE CASCADE`), chunks
  (`f6g7h8i9j0k1_chunk_fk_cascade_delete`), derived observations invalidated/deleted and
  survivors' `consolidated_at` reset, links via FK cascade, entities pruned later by the
  `graph_maintenance` operation ("Reconciles derived state after deletes. Drains link top-up and
  entity prune queues in committed batches").
- **Document replace** (retain with same `document_id`): old facts tombstoned except those from
  unchanged chunks (delta); observations tied to removed facts swept *before* the replace.
- **Memory-level soft invalidation** (`PATCH /memories/{id}` with `state="invalidated"`, `reason`):
  "Memory disappears from recall and consolidation, links are pruned, derived observations
  recomputed without it, but row remains in bank for audit. Fully reversible" (`state="valid"`
  restores and re-consolidates). Column added in `c9a1b2d3e4f5_add_invalidated_memory_units`.
  Only `world`/`experience` can be curated; PATCH on an observation → 400.
- **Memory edit** (`text`, `context`, `occurred_*` ("" clears), `fact_type`, `entities` ([] detaches),
  `resolve_entities`, `reason`): "Re-embeds the fact, drops the observations and links derived
  from the old version, and re-consolidates"; sets `edited_at`.
- `DELETE /memories/{id}/observations` clears a memory's derived observations.
- **Bank delete**: cascades to memories, documents, entities, relationships, directives,
  observations, mental models, knowledge pages, operations (`e5f6g7h8i9j0_cascade_delete_ops_on_bank_delete`).
  **Clear memories** `POST /clear-memories` removes facts, keeps documents and config (MCP variant
  can filter by type). Alias delete "only closes that door".
- Deleting a mental model / knowledge node is permanent (folder delete removes subtree).
- Deletions do not produce a write artifact, so mental-model staleness ignores them.

---

## 8. Data model (Postgres)

Source: `hindsight-api-slim/hindsight_api/alembic/versions/5a366d414dce_initial_schema.py`
(baseline, read in full) + names of the 108 later migrations (listed by code search; only
selected ones read). Alembic is the migration tool; `RUN_MIGRATIONS_ON_STARTUP=true`;
`MIGRATION_DATABASE_URL` bypasses poolers; `MIGRATION_ISOLATION` (subprocess); extensions can add
their own alembic version locations (`alembic_version_locations()`); Oracle has a separate
baseline (`o1a2b3c4d5e6_oracle_baseline.py`). Tables live in a per-tenant schema (default
`public`, `HINDSIGHT_API_DATABASE_SCHEMA`).

### Baseline DDL (verbatim from initial migration; pgvector `vector(384)`)
```sql
CREATE TABLE banks (
    bank_id TEXT NOT NULL, name TEXT,
    personality JSONB NOT NULL DEFAULT '{}'::jsonb, background TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bank_id));

CREATE TABLE documents (
    id TEXT NOT NULL, bank_id TEXT NOT NULL, original_text TEXT, content_hash TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, bank_id));

CREATE TABLE async_operations (
    operation_id UUID NOT NULL DEFAULT gen_random_uuid(), bank_id TEXT NOT NULL,
    operation_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ, error_message TEXT, result_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (operation_id),
    CHECK (status IN ('pending','processing','completed','failed')));

CREATE TABLE entities (
    id UUID NOT NULL DEFAULT gen_random_uuid(), canonical_name TEXT NOT NULL, bank_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
    mention_count INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (id));

CREATE TABLE memory_units (
    id UUID NOT NULL DEFAULT gen_random_uuid(), bank_id TEXT NOT NULL, document_id TEXT,
    text TEXT NOT NULL, embedding vector(384), context TEXT,
    event_date TIMESTAMPTZ NOT NULL, occurred_start TIMESTAMPTZ, occurred_end TIMESTAMPTZ,
    mentioned_at TIMESTAMPTZ, fact_type TEXT NOT NULL DEFAULT 'world', confidence_score FLOAT,
    access_count INTEGER NOT NULL DEFAULT 0, metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    search_vector tsvector GENERATED ALWAYS AS
        (to_tsvector('english', COALESCE(text,'') || ' ' || COALESCE(context,''))) STORED,
    PRIMARY KEY (id),
    FOREIGN KEY (document_id, bank_id) REFERENCES documents(id, bank_id) ON DELETE CASCADE,
    CHECK (fact_type IN ('world','bank','opinion','observation')),
    CHECK (confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0)));

CREATE TABLE entity_cooccurrences (
    entity_id_1 UUID NOT NULL, entity_id_2 UUID NOT NULL,
    cooccurrence_count INTEGER NOT NULL DEFAULT 1, last_cooccurred TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id_1, entity_id_2), CHECK (entity_id_1 < entity_id_2),
    FOREIGN KEY (entity_id_1) REFERENCES entities(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id_2) REFERENCES entities(id) ON DELETE CASCADE);

CREATE TABLE memory_links (
    from_unit_id UUID NOT NULL, to_unit_id UUID NOT NULL, link_type TEXT NOT NULL,
    entity_id UUID, weight FLOAT NOT NULL DEFAULT 1.0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (from_unit_id) REFERENCES memory_units(id) ON DELETE CASCADE,
    FOREIGN KEY (to_unit_id) REFERENCES memory_units(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE,
    CHECK (link_type IN ('temporal','semantic','entity','causes','caused_by','enables','prevents')),
    CHECK (weight >= 0.0 AND weight <= 1.0));

CREATE TABLE unit_entities (
    unit_id UUID NOT NULL, entity_id UUID NOT NULL, PRIMARY KEY (unit_id, entity_id),
    FOREIGN KEY (unit_id) REFERENCES memory_units(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE);
```
Indexes in the baseline: `idx_memory_units_embedding USING hnsw (embedding vector_cosine_ops)`;
`idx_memory_units_text_search USING gin(search_vector)`; b-trees on bank_id, document_id,
event_date DESC, (bank_id, fact_type, event_date DESC), partial indexes for opinion/observation;
`UNIQUE idx_entities_bank_lower_name (bank_id, LOWER(canonical_name))`;
`UNIQUE idx_memory_links_unique (from_unit_id, to_unit_id, link_type, COALESCE(entity_id, '0000…'))`;
materialized view `memory_units_bm25` (later dropped).

### Schema evolution (by migration name; columns/tables inferred from names unless noted)
| Migration | Effect |
|---|---|
| `b7c4d8e9f1a2_add_chunks_table` | `memory_chunks`-style table storing chunk text per document (`chunk_index`, later `content_hash` via `b3c4d5e6f7a8`); FK cascade `f6g7h8i9j0k1` |
| `g2a3b4c5d6e7_add_tags_column` | `tags` array on memory_units (and documents) |
| `z1u2v3w4x5y6_add_observation_tags_to_memory_units`, `b4c5d6e7f8a9_backfill_observation_scopes` | observation scope tags |
| `s4n5o6p7q8r9_add_consolidated_at_to_memory_units`, `a3b4c5d6e7f8_add_consolidation_failed_at_to_memory_units` | consolidation bookkeeping columns |
| `c9a1b2d3e4f5_add_invalidated_memory_units` | soft-invalidation `state` |
| `a2b3c4d5e6f8_add_gin_index_source_memory_ids`, `k6l7m8n9o0p1_create_observation_sources_table` | observation provenance |
| `a7b8c9d0e1f2_split_history_into_own_tables`, `a1c9e7f3b2d8_observation_history_drop_memory_units_fk` | observation/mental-model history tables |
| `h3c4d5e6f7g8_mental_models_v4`, `t5o6p7q8r9s0_rename_mental_models_to_observations`, `p1k2l3m4n5o6_new_knowledge_architecture`, `a9b8c7d6e5f4_add_knowledge_pages`, `j5e6f7g8h9i0_mental_model_versions`, `e7c3a91f4b62_add_mental_model_last_memory_seen_at`, `v7q8r9s0t1u2_add_max_tokens_to_mental_models`, `b3w4x5y6z7a8_add_structured_content_to_mental_models` | mental models / knowledge pages tables |
| `k6f7g8h9i0j1_add_directive_subtype` | directives table (created earlier; exact migration **UNVERIFIED**) |
| `e4f5a6b7c8d9_add_webhooks_tables`, `f7g8h9i0j1k2_add_webhook_http_config` | webhooks |
| `c2d3e4f5g6h7_add_audit_log_table`, `d3e4f5a6b7c8_add_llm_requests_table`, `a6c4e8f1b203_add_reasoning_tokens_to_llm_requests` | audit log, LLM request log |
| `l7g8h9i0j1k2_add_worker_columns`, `d9c1a7b4e2f6_async_operations_serialization_key`, `i4j5k6l7m8n9_add_cancelled_status_to_async_operations`, `a8c1e4f7b0d3_add_operation_retention_indexes`, `y0t1u2v3w4x5_add_result_metadata_gin_index` | DB-backed worker queue |
| `b5a4c3e2f1d8_add_graph_maintenance_queue`, `c4f7a91b2d38_add_entity_maintenance_queue`, `e5f6a7b8c9d0_add_maintenance_routines` | maintenance queues/routines |
| `e9b2c7d1f3a4_drop_entity_memory_links`, `c5d6e7f8a9b0_add_bank_id_to_memory_links`, `9f8e7d6c5b4a_memory_links_deferrable_fk`, `d2e3f4a5b6c7_add_memory_links_expansion_indexes` | link table changes |
| `c1a2b3d4e5f6_enable_pg_trgm_and_entities_trgm_index` + trgm index migrations | pg_trgm for entity resolution |
| `a1b2c3d4e5f6_add_file_storage_table`, `e2f4a6c8b0d1_add_attachments` | file/attachment storage |
| `c8d1e4f7a20b_add_bank_aliases`, `b57a7c9e0d13_add_bank_stats_cache`, `x9s0t1u2v3w4_add_bank_config_column` | bank aliases/stats/config |
| `p4q5r6s7t8u9_configurable_bm25_language`, `a2b3c4d5e6f8_add_text_signals_column`, `f3a5b7c9d1e2_drop_memory_units_bm25_matview`, `b8c9d0e1f2a3_vchord_cosine_opclass` | text/vector search backend changes |
| `e4a7c1b9d2f6_drop_memory_units_access_count`, `d6e7f8a9b0c1_drop_documents_metadata_column`, `aa2b3c4d5e6f_nullable_event_date` | column removals/relaxations |

Required Postgres ≥ 15, pgvector ≥ 0.5.0; docs: "Hindsight does not abstract storage behind a
generic interface" (though an Oracle backend and store-owned document stores exist). Graph queries
use recursive CTEs (docs). Citus horizontal scaling supported with `native`/`pg_search` text
backends.

---

## 9. API surface

Base path `/v1/{tenant}/banks/{bank_id}/...` (tenant is `default` unless a TenantExtension maps
keys to schemas). Port 8888 (API + MCP at `/mcp`), 9999 (control plane UI). Prometheus metrics
(`metrics.py`), OpenTelemetry optional.

| Method & path | Purpose |
|---|---|
| `POST /banks/{id}` / `GET` / `PATCH` / `DELETE`, `GET /banks` | bank CRUD (`name, mission, background, metadata`) |
| `GET /banks/{id}/stats` | memory/document/entity counts, last activity |
| `POST /banks/{id}/clear-memories` | drop facts, keep docs/config |
| `GET/PATCH /banks/{id}/config`, `POST /config/reset` | per-bank config (resolved + raw overrides) |
| `POST/GET/PATCH/DELETE /banks/{id}/aliases[/{alias}]` | aliases |
| `POST/GET/PATCH/DELETE /banks/{id}/directives[/{directive_id}]` | directives |
| `POST /banks/{id}/prompts/preview` | render retain/consolidation/reflect messages without LLM |
| `POST /banks/{id}/memories/retain`, `/retain-files` | ingest |
| `POST /banks/{id}/memories/recall` | search |
| `POST /banks/{id}/reflect` (**path UNVERIFIED**) | reasoning |
| `GET /banks/{id}/memories/list`, `GET /memories/{mid}`, `GET /memories/{mid}/history`, `PATCH /memories/{mid}`, `DELETE /memories/{mid}/observations` | memory browse/curate |
| `GET /banks/{id}/documents`, `GET/PATCH/DELETE /documents/{doc}` | documents (PATCH = tags only, replace semantics) |
| `GET /banks/{id}/operations`, `GET/DELETE /operations/{op}`, `POST /operations/{op}/retry` | async ops |
| `POST/GET/PATCH/DELETE /banks/{id}/mental-models[/{mm}]`, `POST .../refresh`, `.../dry-run-refresh`, `.../clear`, `GET .../history` | mental models |
| knowledge-base tree/page endpoints (`/developer/api/knowledge-pages`) | knowledge pages |
| `GET /banks/{id}/tags?source=` | tags |
| `POST /banks/{id}/transfer/export|import`, `POST /banks/{id}/clone?target_bank_id=` | export/import (ZIP with `manifest.json` + `documents/{id}.json`; import modes `restore|merge` with `document_conflict` skip/replace/new-id; external facts carry text, fact_type, chunk_index, context, entities, metadata, tags, observation_scopes, causal_relations, dates) |
| webhooks endpoints (`/developer/api/webhooks`) | events incl. `memory_defense.triggered`, consolidation complete |
| `/ext/...` | HttpExtension endpoints |

Memory list filters: `state=valid|invalidated`, `type`, `q`, `document_id`, `entity_id`,
`start_date`/`end_date` (half-open), `time_field=created_at|updated_at|mentioned_at|occurred_start|occurred_end`.
Document list: `q`, `tags`, `tags_match=any_strict` (default), `limit=100`, `offset`,
`start_date`, `end_date`, `time_field=updated_at`. Document object: `{id, bank_id, original_text,
content_hash, memory_unit_count, nodes_by_fact_type{world,experience,observation}, created_at, updated_at}`.

**MCP server** (`mcp_tools.py`, `api/mcp.py`; enabled by default at `/mcp/` (multi-bank) and
`/mcp/{bank_id}/` (single-bank); env `HINDSIGHT_API_MCP_ENABLED`, `HINDSIGHT_MCP_BANK_ID`,
`HINDSIGHT_API_MCP_INSTRUCTIONS`; per-bank allowlist `mcp_enabled_tools`). Tools: `retain`,
`sync_retain`, `recall` (default budget high), `reflect` (default budget low),
`create/list/get/update/delete/refresh/clear_mental_model`, `list/create/delete_directive`,
`list_memories`, `get_memory`, `clear_memories`, `list/get/delete_document`,
`list/get/cancel_operation`, `list_tags`, `get_bank`, `update_bank`, `delete_bank`, multi-bank
only `list_banks`, `create_bank`, `get_bank_stats`, plus knowledge base
`get_knowledge_base_tree`, `search_knowledge_base`, `get_knowledge_page`,
`create_knowledge_folder`, `create_knowledge_page`, `update_knowledge_node`,
`delete_knowledge_node`. `MCPExtension` can register more.

**SDK method names**: Python `client.retain/retain_batch/retain_files/recall/reflect/
create_bank/get_bank/update_bank_config/get_bank_config/reset_bank_config/list_memories/get_memory/
get_observation_history/update_memory/get_document/update_document/delete_document/list_documents/
create_directive/list_directives/create_mental_model/list_mental_models/get_mental_model/
refresh_mental_model/dry_run_refresh_mental_model/clear_mental_model/update_mental_model/
delete_mental_model/get_mental_model_history/operations.list_operations/get_operation_status/
cancel_operation/retry_operation/aexport_bank/aimport_bank/aclone_bank`; Node camelCase
equivalents; Go generated client `client.MemoryAPI.RetainMemories(ctx, bank).RetainRequest(...).Execute()`,
`RecallMemories`, `Reflect`, `BanksAPI.CreateOrUpdateBank`, `MentalModelsAPI.*`, `OperationsAPI.*`,
`FilesAPI.FileRetain`; CLI `hindsight memory retain|recall|reflect`, `hindsight document ...`,
`hindsight mental-model ...`, `hindsight operation ...`, `hindsight fs mount`.

---

## 10. Prompts (MIT — Copyright (c) 2025 Vectorize AI, Inc.)

All text below is copied from files in `hindsight-api-slim/hindsight_api/engine/` on `main`,
licensed MIT (`LICENSE`: "MIT License / Copyright (c) 2025 Vectorize AI, Inc."). Reuse requires
keeping that notice.

### 10.1 Fact extraction — `engine/retain/fact_extraction.py`
Concise mode system prompt = `_BASE_FACT_EXTRACTION_PROMPT.format(_CONCISE_GUIDELINES, _CONCISE_EXAMPLES)`.
Verbatim (as assembled; `{language_section}` and `{retain_mission_section}` are runtime inserts):

```
Extract SIGNIFICANT facts from text. Be SELECTIVE - only extract facts worth remembering long-term.

{language_section}{retain_mission_section}══════════════════════════════════════════════════════════════════════════
SELECTIVITY - CRITICAL (Reduces 90% of unnecessary output)
══════════════════════════════════════════════════════════════════════════

ONLY extract facts that are:
✅ Personal info: names, relationships, roles, background
✅ Preferences: likes, dislikes, habits, interests (e.g., "Alice likes coffee")
✅ Significant events: milestones, decisions, achievements, changes
✅ Plans/goals: future intentions, deadlines, commitments
✅ Expertise: skills, knowledge, certifications, experience
✅ Important context: projects, problems, constraints
✅ Sensory/emotional details: feelings, sensations, perceptions that provide context
✅ Observations: descriptions of people, places, things with specific details

DO NOT extract:
❌ Generic greetings: "how are you", "hello", pleasantries without substance
❌ Pure filler: "thanks", "sounds good", "ok", "got it", "sure"
❌ Process chatter: "let me check", "one moment", "I'll look into it"
❌ Repeated info: if already stated, don't extract again

CONSOLIDATE related statements into ONE fact when possible.

══════════════════════════════════════════════════════════════════════════
FACT FORMAT - BE CONCISE
══════════════════════════════════════════════════════════════════════════

1. "what": Core fact - concise but complete (1-2 sentences max)
2. "when": Temporal info if mentioned. "N/A" if none. Use day name when known.
3. "where": Location if relevant. "N/A" if none.
4. "who": People involved with relationships. "N/A" if just general info.
5. "why": Context/significance ONLY if important. "N/A" if obvious.

CONCISENESS: Capture the essence, not every word. One good sentence beats three mediocre ones.

══════════════════════════════════════════════════════════════════════════
COREFERENCE RESOLUTION
══════════════════════════════════════════════════════════════════════════

Link generic references to names when both appear:
- "my roommate" + "Emily" → use "Emily (user's roommate)"
- "the manager" + "Sarah" → use "Sarah (the manager)"

══════════════════════════════════════════════════════════════════════════
CLASSIFICATION
══════════════════════════════════════════════════════════════════════════

fact_kind:
- "event": Specific datable occurrence (set occurred_start/end)
- "conversation": Ongoing state, preference, trait (no dates)

fact_type:
- "world": Objective/external facts, including the user's preferences, rules, corrections, constraints, plans, traits, or context. These stay "world" even when the user states them during an assistant interaction (e.g., "User prefers browser_navigate over web_search", "User corrected the project deadline").
- "assistant": Actions, experiences, or observations the assistant/agent actually performed (e.g., "I changed X", "I discovered Y", "I debugged Z"). Use this for the assistant/agent doing, trying, learning, deciding, recommending, or responding — not merely for user facts mentioned in conversation.

══════════════════════════════════════════════════════════════════════════
TEMPORAL HANDLING
══════════════════════════════════════════════════════════════════════════

Use "Event Date" from input as reference for relative dates.
- CRITICAL: Convert ALL relative temporal expressions to absolute dates in the fact text itself.
  "yesterday" → write the resolved date (e.g. "on November 12, 2024"), NOT the word "yesterday"
  "last night", "this morning", "today", "tonight" → convert to the resolved absolute date
- For events: set occurred_start AND occurred_end (same for point events)
- Coarse dates (only a year, or only a month, is stated): span the WHOLE period —
  "in 2015" → 2015-01-01 to 2015-12-31, "in March 2026" → 2026-03-01 to 2026-03-31.
  Never collapse to the period's first day or to the Event Date, current year included.
- For conversation facts: NO occurred dates

══════════════════════════════════════════════════════════════════════════
ENTITIES
══════════════════════════════════════════════════════════════════════════

ALWAYS return "entities" as an array of plain strings — never objects, never null.
Correct: entities=["Alice", "Kubernetes", "CKA"]
Wrong:   entities as an array of objects with a "text" key ← never use this form
Use an empty array [] only when the fact truly names nothing.

Include: people names, organizations, places, key objects, abstract concepts (career, friendship, etc.)
Always include "user" when fact is about the user.

══════════════════════════════════════════════════════════════════════════
EXAMPLES (shown in English for illustration; for non-English input, ALL output values MUST be in the input language)
══════════════════════════════════════════════════════════════════════════

The examples below demonstrate output format and selectivity only. Never emit
their facts, entities, or dates unless those details also appear in the actual
input text being processed.

Example 1 - Selective extraction (Event Date: June 10, 2024):
Input: "Hey! How's it going? Good morning! So I'm planning my wedding - want a small outdoor ceremony. Just got back from Emily's wedding, she married Sarah at a rooftop garden. It was nice weather. I grabbed a coffee on the way."

Output: ONLY 2 facts (skip greetings, weather, coffee):
1. what="User planning wedding, wants small outdoor ceremony", who="user", why="N/A", entities=["user", "wedding"]
2. what="Emily married Sarah at rooftop garden", who="Emily (user's friend), Sarah", occurred_start="2024-06-09", entities=["Emily", "Sarah", "wedding"]

Example 2 - Professional context:
Input: "Alice has 5 years of Kubernetes experience and holds CKA certification. She's been leading the infrastructure team since March. By the way, she prefers dark roast coffee."

Output: ONLY 2 facts (skip coffee preference - too trivial):
1. what="Alice has 5 years Kubernetes experience, CKA certified", who="Alice", entities=["Alice", "Kubernetes", "CKA"]
2. what="Alice leads infrastructure team since March", who="Alice", entities=["Alice", "infrastructure"]

══════════════════════════════════════════════════════════════════════════
QUALITY OVER QUANTITY
══════════════════════════════════════════════════════════════════════════

Ask: "Would this be useful to recall in 6 months?" If no, skip it.

IMPORTANT: Sensory/emotional details and observations that provide meaningful context
about experiences ARE important to remember, even if they seem small (e.g., how food
tasted, how someone looked, how loud music was). Extract these if they characterize
an experience or person.
```
Other modes (same file; only key lines captured): **verbose** — "CAPTURE ALL DETAILS - NEVER
SUMMARIZE OR OMIT: objects, actions, quantities, details"; requires day names in `when`
("Saturday, June 9, 2024"); coreference mandatory. **verbatim** — "The original text will be stored
as-is in code. Your ONLY job is to extract metadata." / "Produce EXACTLY ONE entry per input chunk.
DO NOT include a 'what' field—it is not part of the output schema." Language rule (all modes):
"Write every fact in the same language and script as the input text. Never translate. Names,
identifiers, code, and quoted text stay verbatim." Causal relation instruction text:
**UNVERIFIED** (not captured; schema allows only `caused_by` to an earlier fact index).

### 10.2 Consolidation — `engine/consolidation/prompts.py`
Functions: `build_consolidation_system_prompt(llm_output_language)`,
`build_mission_section(observations_mission)`,
`build_consolidation_input(facts_text, observations_text, observations_mission, observation_capacity_note)`.
Structure: language rule → MISSION section → PROCESSING RULES (9 numbered) → INPUT FORMAT NOTE →
DECISION GUIDE → OUTPUT FORMAT (with escaped-JSON examples). The fetch tool declined to reproduce
the full text; verified excerpts:

- Default mission: `"Track anything notable in the new facts — names, numbers, dates, places, events, decisions, claims, relationships, and recurring patterns."`
- `"If anything in this MISSION conflicts with the PROCESSING RULES, DECISION GUIDE, or OUTPUT FORMAT below, the MISSION takes priority."`
- Language: `"Write every observation in the language of its own source facts — never translate them."`
- Rule 1: `"PREFER UPDATE OVER CREATE (when there is something to merge with): if new facts describe the same canonical event..."`
- Rule 4: `"STATE CHANGES — UPDATE CONCISELY: when a fact changes the state of something...UPDATE the matching observation to reflect the current state."`
- Rule 8: `"NO COMPUTATION: you do not have the full picture — never calculate, derive, or adjust numeric values."`
- Decision guide: `"Same canonical event, decision, claim, or facet as an existing observation → UPDATE"`
- Output: `{"creates": [{text, source_fact_ids, reason}], "updates": [{observation_id, text, source_fact_ids, reason}], "deletes": [{observation_id, reason}]}` — `reason` required on each.
- Dedup prompt (`consolidator.py` `_DEDUP_PROMPT`): `"You reconcile long-term memory observations... If they assert the SAME fact (wording aside), set 'action' to 'merge' and provide 'text': a single observation that preserves EVERY detail from both."` → `{action: "merge"|"keep", text}`.
- **Full rule text (rules 2,3,5,6,7,9, INPUT FORMAT NOTE, examples): UNVERIFIED — read the file directly.**

### 10.3 Reflect — `engine/reflect/prompts.py` (verified excerpts)
Tool-calling agent system prompt (`build_system_prompt_for_tools()`):
- Opening: `"CRITICAL: You MUST ONLY use information from retrieved tool results. NEVER make up names, people, events, or entities."`
- Default mission: `"You are a reflection agent that answers questions by reasoning over retrieved memories."`
- `"- ONLY use information from tool results - no external knowledge or guessing\n- You SHOULD synthesize, infer, and reason from the retrieved memories"`
- Grounding boundary (paraphrase of a verified section): infer freely about what retrieved data
  covers; never produce values (numbers, dates, names, status) for periods/entities the data does
  not cover; extrapolating trends or borrowing from similar entities is invention.
- Hierarchy: `search_mental_models` (user-curated, highest quality, try first) →
  `search_observations` (auto-consolidated, freshness checks) → `recall` (ground-truth fallback).
- Temporal rule: when facts about the same facet conflict, the fact with the LATEST `mentioned_at`
  is authoritative; later statements SUPERSEDE earlier ones; apply later events on top of
  authoritative states.
- Disposition: skepticism/literalism/empathy (1–5, neutral 3) verbalised per level (exact wording
  not captured — **UNVERIFIED**; docs give: skepticism 1 "Trusting, accepts information at face
  value" … 5 "Questions and doubts claims"; literalism 1 "Flexible interpretation, reads between
  the lines" … 5 "Takes things at face value"; empathy 1 "Detached, focuses on facts" … 5
  "Considers emotional context").
- Output rules: `"- NEVER include memory IDs, UUIDs, or 'Memory references' in the answer text\n- Put IDs ONLY in memory_ids arrays, not in the answer"`;
  `"- CRITICAL: This is a NON-CONVERSATIONAL system. NEVER ask follow-up questions, offer further assistance, or suggest next steps."`
- Language: `"By default, detect the language of the user's question and respond in that SAME language... DIRECTIVES section above has HIGHER PRIORITY."`
- Directives block: `"## DIRECTIVES (MANDATORY)\nThese are hard rules you MUST follow in ALL responses:\n[rules]\nNEVER violate these directives, even if other context suggests otherwise.\nIMPORTANT: Do NOT explain or justify how you handled directives in your answer. Just follow them silently."`
  and end reminder: `"Your response will be REJECTED if it violates any directive above.\nDo NOT include any commentary about how you handled directives - just follow them."`

Final synthesis system prompt (`build_final_system_prompt()`):
- `"You are a thoughtful assistant that synthesizes answers from retrieved memories."`
- `"Use proper markdown formatting in your answer: Headers (##, ###) for sections, Lists for enumerations, Bold/italic for emphasis, Tables with proper syntax (ensure blank line before and after), Code blocks where appropriate, CRITICAL: Always add blank lines before and after block elements"`
- `"Output ONLY the final synthesized answer. Do NOT include: Meta-commentary about what you're doing, Explanations of your reasoning process, Descriptions of your approach. Just provide the direct answer."`
- `"Respond in the SAME language as the user's question... If a directive specifies a response language, follow the directive — it takes precedence over this default."`

Split synthesis (`build_chunk_claims_prompt()`): `"Output a markdown bulleted list of factual claims relevant to the question. For EVERY claim: state the fact in one sentence; append its provenance in parentheses, exactly: (mentioned_at: <ISO date or unknown>; occurred: <ISO date/range or unknown>; memory_ids: <comma-separated ids>). Do NOT synthesize, conclude, resolve conflicts, or answer the question."`
Reduce: `"When claims about the same fact conflict, the claim with the LATEST mentioned_at date is authoritative... If equally-recent claims disagree and nothing resolves them, say so explicitly rather than picking one."`

Structured output (`agent.py` `_generate_structured_output`): system `"You are a precise data extraction assistant..."`, user = JSON schema + answer text, temperature 0.0.

### 10.4 Mental-model / knowledge-page delta refresh — `engine/reflect/prompts.py` (+ `delta_ops.py`, `retractions.py`)
- Delta system prompt: `"You are integrating *new information* into an existing structured document... Output a JSON object {"operations": [...]}. Applied to CURRENT DOCUMENT, the operations must produce a document that best answers the TOPIC."`
- `"Preserve existing content: Do NOT remove or replace existing sections just because new facts don't reference them. Only remove when new facts explicitly contradict or supersede it. Merge overlapping topics INTO existing sections rather than duplicating. Preserve examples—concrete examples are MORE valuable than abstract rules."`
- `"Absence is not contradiction: an entity, count or detail missing from SUPPORTING FACTS is NOT thereby wrong... the document was built from facts you cannot see."`
- `"Refutation threshold for removal: you may only remove or overwrite when a SUPPORTING FACT explicitly refutes or corrects that exact detail, OR is a later-DATED statement about the same facet."`
- Retraction prompt: `"Remove from CURRENT DOCUMENT anything that rests on the RETRACTED FACTS, and nothing else... When in doubt, keep it... Content that merely looks related must be left exactly as it is."`
- Full-mode refresh uses the ordinary reflect prompts with the model's `source_query`.

`POST /banks/{id}/prompts/preview` renders the exact retain/consolidation/reflect messages for a
bank (useful to obtain the full consolidation text without reading source).

---

## 11. Benchmarks

### Paper (arXiv:2512.12818, "Hindsight is 20/20: Building Agent Memory that Retains, Recalls, and Reflects"; Latimer, Boschi, Neeser, Bartholomew, Srivastava, Wang, Ramakrishnan)
- Setup: retain + reflect LLM = GPT-OSS-20B or GPT-OSS-120B (Gemini-3 Pro for answers only);
  **judge = GPT-OSS-120B, temperature 0.0**; neutral disposition S=L=E=3, β=0.2; RRF k=60;
  reranker `ms-marco-MiniLM-L-6-v2`.
- LongMemEval (500 questions; S ≈115k tokens, M ≈1.5M tokens; categories: information
  extraction, multi-session reasoning, temporal reasoning, knowledge update, abstention):
  overall **83.6%** (GPT-OSS-20B) vs 39.0% full-context baseline (+44.6); multi-session 79.7% vs
  21.1%; temporal 79.7% vs 31.6%; **89.0%** (GPT-OSS-120B); **91.4%** (Gemini-3 Pro). Baselines:
  full-context GPT-4o 60.2%, Zep+GPT-4o 71.2%, Supermemory+GPT-4o 81.6%.
- LoCoMo (50 conversations, avg 304.9 turns / 9,209 tokens / 19.3 sessions; single-hop,
  multi-hop, open-domain, temporal): **83.18%** (OSS-20B), **85.67%** (OSS-120B), **89.61%**
  (Gemini-3; 95.12% open-domain). Baselines: Memobase 75.78%, Zep 75.14%, Mem0 66.88%,
  Backboard 90.00%.
- Ablations: per-arm numbers **UNVERIFIED** (not captured from the HTML fetch).

### Docs / marketing (hindsight.vectorize.io, vectorize.io/benchmarks)
- "94.6%" on LongMemEval-S (Jan 2026; "independently reproduced by Virginia Tech and The
  Washington Post" per README) and "64.1%" on BEAM at the 10M-token tier ("58% ahead of the
  next-best system"). Models/judge for these runs: **UNVERIFIED**.

### Eval harness (`hindsight-system-evals/README.md`)
- Benchmark runners moved to an external repo "AMB" (Agent Memory Benchmark): "AMB is now the only
  copy. The in-repo LoComo and LongMemEval runners are gone". Datasets cached at
  `~/.cache/hindsight/amb`; `uv run amb splits --dataset locomo`.
- Commands: `uv run run-amb --dataset locomo --split locomo10 -- --unit conv-26 --query-limit 20`;
  `uv run run-amb --dataset longmemeval --split s -- --category single-session-user --query-limit 20`;
  `uv run run-amb --dataset beam --split 100k --api-url https://api.dev.example`. Also PersonaMem
  and a coding-agent suite (sde-bench). `AMB_REF` pins the AMB commit.
- In-repo system evals: `uv run pytest evals [--full] [--output system-evals-results.json]`
  with suites test_01…test_07 (knowledge-page convergence, reflect answers, retain language,
  retain fidelity, refresh cost vs frozen 900-fact bank, page-layer integration, source priority).
  Metrics: correct rate, trap count (must be 0), by_kind, USD cost.
- Env: `HINDSIGHT_EVAL_LLM_PROVIDER/MODEL/API_KEY` (example gemini / gemini-3.7-flash),
  `HINDSIGHT_EVAL_JUDGE_MODEL` (example gemini-2.5-flash), `HINDSIGHT_EVAL_REFLECT_BUDGET=low`,
  `HINDSIGHT_EVAL_API_URL`, `HINDSIGHT_EVAL_SET_<X>` → `HINDSIGHT_API_<X>`, `HINDSIGHT_EVAL_PG0_INSTANCE`.
  Runs daily in `.github/workflows/perf-test.yml`; fresh pg0 per run.
- Answering path for benchmarks (reflect vs recall+external LLM) and judge prompt: **UNVERIFIED**
  (in AMB repo).

---

## 12. Configuration defaults and env vars

Prefix `HINDSIGHT_API_` (omitted below). "(hierarchical)" = per-bank overridable through
`PATCH /config` (`config_resolver.py`). Sources: docs `/developer/configuration`, `config.py`.

### LLM
| Var | Default | Notes |
|---|---|---|
| `LLM_PROVIDER` | `openai` | 25–50+ providers incl. anthropic, gemini, vertexai, groq, ollama, llamacpp (built-in GGUF), litellm/litellmrouter, claude_code, codex, cursor, github_copilot, xai |
| `LLM_MODEL` | `gpt-5-mini` | |
| `LLM_API_KEY`, `LLM_BASE_URL` | — | |
| `LLM_MAX_CONCURRENT` | 32 | |
| `LLM_MAX_RETRIES` / `LLM_INITIAL_BACKOFF` / `LLM_MAX_BACKOFF` | 3 / 1.0 s / 60.0 s | |
| `LLM_TIMEOUT` / `LLM_CONNECT_TIMEOUT` | 120 s / 10 s | |
| `LLM_TEMPERATURE` | per-op | retain 0.1, reflect 0.9, consolidation 0.0 |
| `LLM_REASONING_EFFORT`, `LLM_VISION`, `LLM_STRICT_SCHEMA`(false), `LLM_SEND_BANK_AS_USER`(false) | | |
| `LLM_PROMPT_CACHE_ENABLED` / `REFLECT_PROMPT_CACHE_ENABLED` / `LLM_CACHE_AFFINITY` | true / true / auto | |
| `LLM_STRATEGY` + `LLM_<n>_PROVIDER/MODEL/API_KEY/TIMEOUT/MAX_RETRIES` | unset | `failover`, `round-robin`, `metadata` |
| `RETAIN_LLM_PROVIDER/MODEL/MAX_CONCURRENT/TIMEOUT/TEMPERATURE` | global / global / — / global / 0.1 | |
| `REFLECT_LLM_PROVIDER/MODEL/MAX_CONCURRENT/TIMEOUT/TEMPERATURE`, `REFLECT_MAX_COMPLETION_TOKENS` | global / global / — / 30 s / 0.9 / unset | |
| `CONSOLIDATION_LLM_PROVIDER/MODEL/MAX_CONCURRENT/TIMEOUT/TEMPERATURE` | global / global / — / global / 0.0 | |
| `MENTAL_MODEL_REFRESH_LLM_PROVIDER/MODEL/TIMEOUT` | reflect's / reflect's / global | |
| `VLM_PROVIDER/API_KEY/MODEL/BASE_URL` | retain LLM's | images |
| `LLAMACPP_MODEL_PATH/GPU_LAYERS/CONTEXT_SIZE/CHAT_FORMAT/NO_GRAMMAR` | auto / -1 / 8192 / auto / false | |
| `LLM_OUTPUT_LANGUAGE` | unset (hierarchical) | force output language |

### Embeddings / reranker
| Var | Default |
|---|---|
| `EMBEDDINGS_PROVIDER` | `local` |
| `EMBEDDINGS_LOCAL_MODEL` | `BAAI/bge-small-en-v1.5` (384-d) |
| `EMBEDDINGS_ONNX_MODEL_ID` / `_MAX_TOKENS` / `_POOLING` / `_NORMALIZE` / `_BATCH_SIZE` / `_DEVICE` | `intfloat/multilingual-e5-small` / 512 / mean / true / 32 / cpu |
| `EMBEDDINGS_OPENAI_MODEL` / `_BATCH_SIZE` / `_DIMENSIONS` | `text-embedding-3-small` / 100 / unset |
| `EMBEDDINGS_GEMINI_MODEL` / `_OUTPUT_DIMENSIONALITY` | `gemini-embedding-001` / 768 |
| `EMBEDDINGS_COHERE_MODEL` | `embed-english-v3.0` |
| `EMBEDDINGS_TEI_URL` / `_BATCH_SIZE` | — / 32 |
| `EMBEDDINGS_MAX_INPUT_TOKENS` / `_MAX_CONCURRENT_REQUESTS` / `_MAX_RETRIES` / `_INITIAL_BACKOFF` / `_MAX_BACKOFF` / `_RETRY_BUDGET` | 8192 / 8 / 4 / 0.5 / 4.0 / 15.0 |
| `EMBEDDINGS_QUERY_PREFIX` / `EMBEDDINGS_PASSAGE_PREFIX` | unset |
| `RERANKER_PROVIDER` | `local` |
| `RERANKER_LOCAL_MODEL` | `cross-encoder/ms-marco-MiniLM-L-6-v2` |
| `RERANKER_MAX_CANDIDATES` (+`_LOW/_MID/_HIGH`) | 300 (0 = use global) |
| `RERANKER_LOCAL_BATCH_SIZE` / `_TIMEOUT` / `_MAX_CONCURRENT` | 32 / 300 s / 4 |
| `ENABLE_RERANKING` | true (hierarchical) |

### Retain / entities
| Var | Default |
|---|---|
| `RETAIN_CHUNK_SIZE` | 3000 chars (hierarchical) |
| `RETAIN_STRUCTURED_CHUNK_SIZE` | (hierarchical; keeps structured records whole) |
| `RETAIN_CHUNK_BATCH_SIZE` | 100 chunks |
| `RETAIN_EXTRACTION_MODE` | `concise` |
| `RETAIN_BATCH_ENABLED` | false |
| `RETAIN_MAX_CONCURRENT` | 32 |
| `RETAIN_MAX_COMPLETION_TOKENS` | 64000 |
| `RETAIN_EXTRACT_CAUSAL_LINKS` | true |
| `RETAIN_OPTIONAL_FACT_DIMENSIONS` | false |
| `RETAIN_ATTACHMENT_MAX_SIZE_MB` / `_MAX_COUNT` | 20 / 50 |
| `RETAIN_MEMORY_BUDGET_MB` | 128 |
| `RETAIN_ENTITY_LOOKUP` | `trigram` |
| `RETAIN_ENTITY_RESOLUTION_BATCH_SIZE` / `_MAX_CANDIDATES` | 100 / 200 |
| `ENTITY_TRGM_SIMILARITY_THRESHOLD` / `ENTITY_INTRABATCH_MERGE_SIMILARITY` / `ENTITY_MERGE_MIN_SIMILARITY` | 0.15 / 0.5 / 0.3 |
| `SEMANTIC_LINK_MIN_SIMILARITY` | 0.7 |
| `LINK_EXPANSION_PER_ENTITY_LIMIT` | 200 |
| `ADMISSION_RETAIN_MAX_IN_FLIGHT` / `_MAX_WAIT_MS` | derived / 2000 |

### Recall
| Var | Default |
|---|---|
| `RECALL_BUDGET_FUNCTION` | `fixed` |
| `RECALL_BUDGET_FIXED_LOW/MID/HIGH` | 100 / 300 / 1000 |
| `RECALL_BUDGET_ADAPTIVE_LOW/MID/HIGH` | 0.025 / 0.075 / 0.25 |
| `RECALL_BUDGET_MIN/MAX` | 20 / 2000 |
| `RECALL_MAX_TOKENS` | 4096 (hierarchical) |
| `RECALL_CHUNKS_MAX_TOKENS` | 8192 |
| `RECALL_INCLUDE_CHUNKS` | false |
| `RECALL_MAX_QUERY_TOKENS` | 500 |
| `RECALL_MAX_CANDIDATES_PER_SOURCE` | 0 (off) |
| `RECALL_MAX_CONCURRENT` / `RECALL_CONNECTION_BUDGET` | 32 / 4 |
| `RECALL_STRATEGY_BOOSTS` | "" |
| `ENABLE_TEXT_SEARCH` / `ENABLE_TEMPORAL_RETRIEVAL` / `ENABLE_GRAPH_RETRIEVAL` | true (hierarchical) |
| `SEMANTIC_MIN_SIMILARITY` / `GRAPH_SEED_MIN_SIMILARITY` / `TEMPORAL_SEMANTIC_MIN_SIMILARITY` | 0.3 / 0.3 / 0.1 |
| `BM25_MIN_SCORE` / `BM25_MAX_QUERY_TERMS` / `BM25_SELECTIVE_TERMS` | 0.0 / 16 / true |
| RRF k | 60 (constant in `fusion.py`) |
| Recency / temporal / proof boost α | 0.2 / 0.2 / 0.1 (constants in `recall_boost.py`; recency decay 365 d) |
| `ADMISSION_RECALL_MAX_WAIT_MS` | 30000 |

### Consolidation / reflect / mental models
| Var | Default |
|---|---|
| `ENABLE_OBSERVATIONS` | true (bank `enable_observations`) |
| `ENABLE_AUTO_CONSOLIDATION` | true |
| `CONSOLIDATION_LLM_BATCH_SIZE` | 8 |
| `CONSOLIDATION_BATCH_SIZE`, `CONSOLIDATION_MAX_MEMORIES_PER_ROUND` (0=unlimited), `CONSOLIDATION_LLM_PARALLELISM` | hierarchical; defaults **UNVERIFIED** |
| `CONSOLIDATION_DEDUP_THRESHOLD` | 0.97 |
| `CONSOLIDATION_SOURCE_FACTS_MAX_TOKENS` / `..._PER_OBSERVATION` | unlimited / 256 |
| `MAX_OBSERVATIONS_PER_SCOPE` | −1 (unlimited) — **UNVERIFIED default** |
| `ENABLE_OBSERVATION_HISTORY` / `OBSERVATION_HISTORY_MAX_ENTRIES` | hierarchical |
| `REFLECT_MAX_ITERATIONS` | 10 |
| `REFLECT_MAX_CONTEXT_TOKENS` | 100000 |
| `REFLECT_WALL_TIMEOUT` | **UNVERIFIED** |
| `REFLECT_SOURCE_FACTS_MAX_TOKENS` | hierarchical |
| `REFLECT_SEARCH_OBSERVATIONS_MAX_TOKENS` | 5000 |
| `ADMISSION_REFLECT_MAX_WAIT_MS` | 5000 |
| `ENABLE_MENTAL_MODEL_HISTORY` / `MENTAL_MODEL_HISTORY_MAX_ENTRIES` / `MENTAL_MODEL_MIN_REFRESH_INTERVAL_SECONDS` | true / 50 / 0 |

### Database / search backends / worker / misc
| Var | Default |
|---|---|
| `DATABASE_URL` | `pg0` (embedded Postgres on port 5555, data `~/.hindsight/pg0/`; dev only) |
| `READ_DATABASE_URL`, `MIGRATION_DATABASE_URL` | unset / falls back |
| `DATABASE_SCHEMA` | `public` |
| `DATABASE_BACKEND` | `postgresql` (or `oracle`) |
| `RUN_MIGRATIONS_ON_STARTUP` / `MIGRATION_ISOLATION` / `MIGRATION_CONCURRENCY` | true / false / 1 |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | 5 / 100 |
| `DB_COMMAND_TIMEOUT` / `DB_ACQUIRE_TIMEOUT` / `DB_STATEMENT_TIMEOUT` | 60 / 30 / 600 s |
| `VECTOR_EXTENSION` | `pgvector` (`vchord`, `pgvectorscale`, `scann`) |
| `ANN_ITERATIVE_SCAN` / `ANN_MAX_SCAN_TUPLES` | true / 4000 |
| `VECTOR_INDEX_MIN_ROWS` / `VECTOR_INDEX_MAINTENANCE_MIN_INTERVAL_SECONDS` | 0 / 900 |
| `TEXT_SEARCH_EXTENSION` / `_NATIVE_LANGUAGE` | `native` / `english` |
| `WORKER_ENABLED` / `WORKER_MAX_SLOTS` / `WORKER_<TYPE>_MAX_SLOTS` / `WORKER_POLL_INTERVAL_MS` / `WORKER_MAX_RETRIES` / `WORKER_CONSOLIDATION_BANK_PRIORITY` | true / 10 / consolidation 2 / — / 3 / — |
| `OPERATION_RETENTION_DAYS` | unset (keep forever) |
| `MCP_ENABLED`, `MCP_INSTRUCTIONS`, `HINDSIGHT_MCP_BANK_ID` | true / — / `default` |
| `WEBHOOK_URL` / `WEBHOOK_SECRET` | — |
| `AUDIT_LOG_ENABLED` / `AUDIT_LOG_RETENTION_DAYS` / `LLM_TRACE_ENABLED` / `OTEL_TRACES_ENABLED` | hierarchical |
| `EXTERNALLY_OWNED_ROUTINES` | empty |
| `TENANT_EXTENSION`, `TENANT_API_KEY`, `EXTENSION_PASSTHROUGH_HEADERS` | unset (open access) |
| `HINDSIGHT_CP_*` | control-plane UI |

Docs latency guidance: recall 100–600 ms, reflect 800–3000 ms, retain 500–2000 ms per batch;
"architected from the ground up to prioritize read performance over write performance".

---

## 13. Multi-tenancy, auth, isolation extension points

Sources: docs `/developer/extensions`, `/developer/mcp-server`; `extensions/tenant.py`,
`extensions/operation_validator.py`, `extensions/builtin/tenant.py`.

- **URL tenant segment**: all routes are `/v1/{tenant}/banks/...`; `default` when no tenant
  extension. **Isolation unit = Postgres schema per tenant**; banks isolate within a schema by
  `bank_id` column (every table carries `bank_id`; bank ids widened to TEXT in
  `a1d3f5b7c9e2_widen_remaining_bank_id_to_text`). Tests `test_maintenance_multitenant.py`.
- **TenantExtension**: `async def authenticate(self, context: RequestContext) -> TenantContext`;
  `RequestContext{api_key (Authorization header), extra_headers (opt-in via
  HINDSIGHT_API_EXTENSION_PASSTHROUGH_HEADERS)}`; `TenantContext{schema_name}`. Registered with
  `HINDSIGHT_API_TENANT_EXTENSION=mypackage.module:MyClass`; extension config via
  `HINDSIGHT_API_<TYPE>_<KEY>`. Lifecycle `on_startup()/on_shutdown()`; extensions may ship
  migrations via `alembic_version_locations()` (test `test_extension_alembic_tree.py`).
- Built-ins: `hindsight_api.extensions.builtin.tenant:ApiKeyTenantExtension` (single key from env,
  `public` schema); `hindsight_ext_supabase_tenant:SupabaseTenantExtension` (Supabase JWT → schema
  per user; external registry); `StaticKeysTenantExtension` (users + keys in env, one schema per
  user, "provisioned lazily on first access"). Default deployment: no auth ("Authentication: open").
- **OperationValidatorExtension** hooks: `validate_retain(RetainContext)`,
  `validate_recall(RecallContext)`, `validate_reflect(ReflectContext)`,
  `on_retain_complete(RetainResult)`, `validate_memory_update(MemoryUpdateContext)`,
  `on_memory_update_complete(MemoryUpdateResult)` — for rate limiting, permission checks, audit,
  metrics.
- **HttpExtension** (`/ext/` prefix), **MCPExtension** (extra MCP tools), **MemoryDefenseExtension**
  (always loaded, dormant until a bank policy enables it).
- Per-bank access scoping inside a tenant is done with tags (`user:alice`) + `tags_match`, MCP
  single-bank endpoints (`/mcp/{bank_id}/`), and `mcp_enabled_tools` allowlists. Bank export/import
  supports moving banks across deployments.
- Audit log table, LLM request log (with `bank_id` attribution; `LLM_SEND_BANK_AS_USER`), and
  webhooks with HMAC secret exist for observability.

---

## Appendix — gaps / UNVERIFIED summary

1. Per-arm SQL for recall (semantic top-k, BM25 scoring function, graph hop count/neighbours,
   temporal bucket count) — in `engine/memories/pg/recall.py` / `link_expansion.py` (not read).
2. Full consolidation PROCESSING RULES / DECISION GUIDE / OUTPUT FORMAT text (only excerpts).
3. Exact verbalisation of disposition levels in the reflect system prompt.
4. How `ExtractedFact.what/when/where/who/why` are joined into stored `text`; chunk overlap; the
   `chunks` extraction mode; causal-relation instruction text.
5. Reflect wall-timeout default; consolidation `batch_size`/`parallelism`/`max_memories_per_round`
   defaults; `max_observations_per_scope` default.
6. Reflect endpoint path (`POST /v1/{tenant}/banks/{bank_id}/reflect` inferred from SDK/CLI).
7. Current mental-model / knowledge-page / directive / chunk table DDL (only migration names).
8. Benchmark details behind the 94.6% / 64.1% figures and the AMB judge prompt.
9. Whether the paper's T5-small temporal fallback and bias-strength β survive in current code.
