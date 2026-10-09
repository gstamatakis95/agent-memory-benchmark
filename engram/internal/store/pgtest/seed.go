package pgtest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// typed pins the parameter types of a seed statement: $1 is the namespace uuid and $2 the tenant text, and a parameter
// that is used both as a column value and inside md5($1::text ...) would otherwise be deduced inconsistently.
func typed(q string) string {
	q = strings.ReplaceAll(q, "$1::text", "$1::uuid::text")
	q = strings.ReplaceAll(q, "($1, $2", "($1::uuid, $2::text")
	q = strings.ReplaceAll(q, "SELECT $1, $2", "SELECT $1::uuid, $2::text")
	return q
}

func init() {
	for i := range seedStatements {
		seedStatements[i] = typed(seedStatements[i])
	}
}

// Fixture ids of SeedNamespace. Every id is derived from the namespace so two namespaces never share a key.
const (
	// SeedDoc is the active document of the minimal graph; SeedGoneDoc carries a document tombstone.
	SeedDoc     = "d1"
	SeedGoneDoc = "d2"
	// SeedModel is the embedding model of the fixtures.
	SeedModel = "nomic-embed-text-v1.5"
	// SeedFacts is the number of facts of the minimal graph.
	SeedFacts = 3
)

// id returns the SQL expression for a deterministic uuid of the namespace $1 and a label.
func id(label string) string { return "md5($1::text || '" + label + "')::uuid" }

func hash(label string) string {
	return "decode(md5($1::text || '" + label + "') || md5('" + label + "'), 'hex')"
}

// vec is a constant 768-dimension halfvec; the minimal graph does not rank by it.
const vec = "(('[' || array_to_string(array_fill(0.01::real, ARRAY[768]), ',') || ']')::halfvec(768))"

// seedStatements insert a minimal but complete graph into EVERY namespace-scoped table (parameters: $1 the namespace,
// $2 the tenant), as engram_admin: documents with a version, chunks, facts with vectors, a link, an entity with an
// alias and a mention, an observation with two versions and its evidence, a consolidation batch with proposal and
// stamps, a page with a version and its evidence, every marker, the expunge and metering state and an outbox event.
// TestRLSCanary needs a row in every table to prove a query would have returned something.
var seedStatements = []string{
	`INSERT INTO namespace_ownership (namespace_id, tenant_id, shard_id, epoch, state) VALUES ($1, $2, 1, 1, 'active')`,
	`INSERT INTO namespace_stats (namespace_id, tenant_id) VALUES ($1, $2)`,
	`INSERT INTO namespace_models (namespace_id, tenant_id, embedding_model) VALUES ($1, $2, '` + SeedModel + `')`,
	`INSERT INTO vector_indexes (namespace_id, tenant_id, vector_table, embedding_model)
	   VALUES ($1, $2, 'fact_vectors', '` + SeedModel + `')`,
	`INSERT INTO tag_counts (namespace_id, tenant_id, tag, doc_count) VALUES ($1, $2, 'alpha', 1)`,
	`INSERT INTO ingest_ledger (namespace_id, tenant_id, ledger_id, document_id, operation_id, update_mode,
	     item_timestamp, content_hash, content_bytes, body)
	   VALUES ($1, $2, ` + id("ledger") + `, 'd1', ` + id("op1") + `, 'replace', '2026-01-01T00:00:00Z',
	     ` + hash("body") + `, 5, 'hello')`,
	`INSERT INTO documents (namespace_id, tenant_id, document_id, current_version, tags)
	   VALUES ($1, $2, 'd1', 1, '{alpha}'), ($1, $2, 'd2', 1, '{alpha,beta}')`,
	`INSERT INTO document_versions (namespace_id, tenant_id, document_id, version, content_hash, status,
	     update_mode, operation_id, ledger_id, chunk_count, fact_count, fact_count_by_type)
	   VALUES ($1, $2, 'd1', 1, ` + hash("body") + `, 'active', 'replace', ` + id("op1") + `, ` + id("ledger") + `,
	     2, 3, '{"world": 3, "experience": 0}')`,
	`INSERT INTO chunks (namespace_id, tenant_id, chunk_id, document_id, document_version, life_start, content_hash,
	     header_hash, text, mentioned_at)
	   VALUES ($1, $2, ` + id("c1") + `, 'd1', 1, 1, ` + hash("c1") + `, ` + hash("h1") + `, 'chunk one text',
	     '2026-01-01T00:00:00Z'),
	     ($1, $2, ` + id("c2") + `, 'd1', 1, 1, ` + hash("c2") + `, ` + hash("h2") + `, 'chunk two text',
	     '2026-01-01T00:00:00Z')`,
	`INSERT INTO document_version_chunks (namespace_id, tenant_id, document_id, version, content_hash, chunk_id,
	     ordinal)
	   VALUES ($1, $2, 'd1', 1, ` + hash("c1") + `, ` + id("c1") + `, 0),
	     ($1, $2, 'd1', 1, ` + hash("c2") + `, ` + id("c2") + `, 1)`,
	`INSERT INTO facts (namespace_id, tenant_id, memory_id, document_id, document_version, chunk_id, ordinal,
	     content_hash, extraction_key, text, fact_type, occurred_start, mentioned_at, extraction_version,
	     prompt_version, model)
	   SELECT $1, $2, md5($1::text || 'f' || g)::uuid, 'd1', 1, ` + id("c1") + `, g,
	     decode(md5($1::text || 'fh' || g) || md5('fh' || g), 'hex'), ` + hash("k") + `,
	     'fact ' || g || ' about kestrel', 'world', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 'extract/v1',
	     'm'
	     FROM generate_series(1, 3) g`,
	`INSERT INTO fact_vectors (namespace_id, tenant_id, memory_id, embedding_model, document_id, document_version,
	     chunk_id, fact_type, mentioned_at, embedding)
	   SELECT $1, $2, md5($1::text || 'f' || g)::uuid, '` + SeedModel + `', 'd1', 1, ` + id("c1") + `, 'world',
	     '2026-01-01T00:00:00Z', ` + vec + ` FROM generate_series(1, 3) g`,
	`INSERT INTO chunk_vectors (namespace_id, tenant_id, chunk_id, embedding_model, embedding_effective_at,
	     document_id, document_version, mentioned_at, embedding)
	   VALUES ($1, $2, ` + id("c1") + `, '` + SeedModel + `', '2026-01-01T00:00:00Z', 'd1', 1, '2026-01-01T00:00:00Z',
	     ` + vec + `)`,
	`INSERT INTO fact_links (namespace_id, tenant_id, src_memory_id, dst_memory_id, link_type)
	   VALUES ($1, $2, md5($1::text || 'f1')::uuid, md5($1::text || 'f2')::uuid, 'causal')`,
	`INSERT INTO entities (namespace_id, tenant_id, entity_id, canonical_name, canonical_norm, entity_type,
	     mention_count)
	   VALUES ($1, $2, ` + id("e1") + `, 'Kestrel Corp', 'kestrel corp', 'organization', 1)`,
	`INSERT INTO entity_aliases (namespace_id, tenant_id, entity_id, alias, alias_norm, source, document_id,
	     document_version)
	   VALUES ($1, $2, ` + id("e1") + `, 'KC', 'kc', 'extracted', 'd1', 1)`,
	`INSERT INTO entity_mentions (namespace_id, tenant_id, memory_id, entity_id, mentioned_at)
	   VALUES ($1, $2, md5($1::text || 'f1')::uuid, ` + id("e1") + `, '2026-01-01T00:00:00Z')`,
	`INSERT INTO observations (namespace_id, tenant_id, observation_id, current_version, proof_count)
	   VALUES ($1, $2, ` + id("o1") + `, 2, 1)`,
	`INSERT INTO observation_versions (namespace_id, tenant_id, ov_id, observation_id, version, root_version, text,
	     effective_at, source_count, prompt_version, model, commit_key)
	   VALUES ($1, $2, ` + id("ov1") + `, ` + id("o1") + `, 1, 1, 'observation text v1', '2026-01-01T00:00:00Z', 1,
	     'consolidate/v1', 'm', ` + hash("ck1") + `),
	     ($1, $2, ` + id("ov2") + `, ` + id("o1") + `, 2, 1, 'observation text v2', '2026-01-02T00:00:00Z', 1,
	     'consolidate/v1', 'm', ` + hash("ck2") + `)`,
	`INSERT INTO observation_version_vectors (namespace_id, tenant_id, ov_id, embedding_model, observation_id, version,
	     effective_at, embedding)
	   VALUES ($1, $2, ` + id("ov2") + `, '` + SeedModel + `', ` + id("o1") + `, 2, '2026-01-02T00:00:00Z', ` + vec +
		`)`,
	`INSERT INTO observation_version_meta (namespace_id, tenant_id, observation_id, version, superseded_at)
	   VALUES ($1, $2, ` + id("o1") + `, 1, '2026-01-02T00:00:00Z')`,
	`INSERT INTO observation_sources (namespace_id, tenant_id, observation_id, memory_id, added_version)
	   VALUES ($1, $2, ` + id("o1") + `, md5($1::text || 'f1')::uuid, 1)`,
	`INSERT INTO observation_inputs (namespace_id, tenant_id, observation_id, version, fact_id, document_id,
	     document_version)
	   VALUES ($1, $2, ` + id("o1") + `, 2, md5($1::text || 'f1')::uuid, 'd1', 1)`,
	`INSERT INTO observation_version_sources (namespace_id, tenant_id, observation_id, version, memory_id,
	     document_id, document_version, quote)
	   VALUES ($1, $2, ` + id("o1") + `, 2, md5($1::text || 'f1')::uuid, 'd1', 1, 'quote')`,
	`INSERT INTO consolidation_batches (namespace_id, tenant_id, batch_key, round_id, memory_ids, state, model,
	     prompt_version)
	   VALUES ($1, $2, ` + hash("batch") + `, ` + id("round") + `, ARRAY[md5($1::text || 'f1')::uuid], 'applied', 'm',
	     'consolidate/v1')`,
	`INSERT INTO consolidation_proposals (namespace_id, tenant_id, batch_key, attempt, ops, op_count, input_fact_ids,
	     prompt_version, model)
	   VALUES ($1, $2, ` + hash("batch") + `, 1, '[]'::jsonb, 0, ARRAY[md5($1::text || 'f1')::uuid],
	     'consolidate/v1', 'm')`,
	`INSERT INTO consolidation_applied (namespace_id, tenant_id, op_key, batch_key, attempt, op_index, op_kind,
	     observation_id)
	   VALUES ($1, $2, ` + hash("op") + `, ` + hash("batch") + `, 1, 0, 'create', ` + id("o1") + `)`,
	`INSERT INTO fact_consolidation (namespace_id, tenant_id, memory_id, note, batch_key)
	   VALUES ($1, $2, md5($1::text || 'f1')::uuid, 'done', ` + hash("batch") + `)`,
	`INSERT INTO consolidation_state (namespace_id, tenant_id) VALUES ($1, $2)`,
	`INSERT INTO batch_jobs (namespace_id, tenant_id, batch_key, kind, item_count)
	   VALUES ($1, $2, ` + hash("job") + `, 'extract', 1)`,
	`INSERT INTO pages (namespace_id, tenant_id, page_id, name, source_query, current_version)
	   VALUES ($1, $2, ` + id("p1") + `, 'page one', 'what happened', 1)`,
	`INSERT INTO page_versions (namespace_id, tenant_id, page_id, version, pv_id, root_version, text,
	     markdown_blob_key, effective_at, evidence_hash, commit_key)
	   VALUES ($1, $2, ` + id("p1") + `, 1, ` + id("pv1") + `, 1, 'page text', 'pages/p1/v1-abc.md',
	     '2026-01-01T00:00:00Z', ` + hash("ev") + `, ` + hash("pck") + `)`,
	`INSERT INTO page_version_vectors (namespace_id, tenant_id, pv_id, embedding_model, page_id, version,
	     effective_at, embedding)
	   VALUES ($1, $2, ` + id("pv1") + `, '` + SeedModel + `', ` + id("p1") + `, 1, '2026-01-01T00:00:00Z', ` + vec +
		`)`,
	`INSERT INTO page_version_meta (namespace_id, tenant_id, page_id, version, superseded_at)
	   VALUES ($1, $2, ` + id("p1") + `, 1, '2026-01-03T00:00:00Z')`,
	`INSERT INTO page_version_inputs (namespace_id, tenant_id, page_id, version, kind, source_id, source_version,
	     document_id, document_version)
	   VALUES ($1, $2, ` + id("p1") + `, 1, 'fact', md5($1::text || 'f1')::uuid, 0, 'd1', 1)`,
	`INSERT INTO page_sources (namespace_id, tenant_id, page_id, source_kind, source_id, version_added)
	   VALUES ($1, $2, ` + id("p1") + `, 'fact', md5($1::text || 'f1')::uuid, 1)`,
	`INSERT INTO document_tombstones (namespace_id, tenant_id, document_id, up_to_version, deleted_at, operation_id,
	     intent_key, event_seq)
	   VALUES ($1, $2, 'd2', 1, now(), ` + id("opdel") + `, 'intent-d2', 1)`,
	`INSERT INTO chunk_tombstones (namespace_id, tenant_id, chunk_id, reason)
	   VALUES ($1, $2, ` + id("c2") + `, 'replace')`,
	`INSERT INTO fact_hidden (namespace_id, tenant_id, memory_id, cause, invalidation_op)
	   VALUES ($1, $2, md5($1::text || 'f3')::uuid, 'invalidate', ` + id("opinv") + `)`,
	`INSERT INTO curation_log (namespace_id, tenant_id, operation_id, memory_id, content_hash, document_id,
	     document_version, action)
	   VALUES ($1, $2, ` + id("opinv") + `, md5($1::text || 'f3')::uuid, ` + hash("c") + `, 'd1', 1, 'invalidate')`,
	`INSERT INTO derived_hidden (namespace_id, tenant_id, kind, id, root_version, from_version, cause_kind, cause_id)
	   VALUES ($1, $2, 'observation', ` + id("o1") + `, 1, 1, 'document', 'd2')`,
	`INSERT INTO expunge_progress (namespace_id, tenant_id, unit, table_name) VALUES ($1, $2, 'd2', 'facts')`,
	`INSERT INTO deletion_log (namespace_id, tenant_id, intent_key, kind, subject_class, subject_id, epoch, effect)
	   VALUES ($1, $2, 'intent-d2', 'document', 'document', 'document:d2', 1, '{"up_to_version": 1}')`,
	`INSERT INTO operations (namespace_id, tenant_id, operation_id, kind, workflow_id, task_queue, submitted_epoch)
	   VALUES ($1, $2, ` + id("op1") + `, 'retain_document', 'ns/x/op/y', 'shard-1', 1)`,
	`INSERT INTO idempotency_keys (namespace_id, tenant_id, method, request_id, request_hash)
	   VALUES ($1, $2, '/memory.v1.MemoryService/Retain', 'req-1', ` + hash("req") + `)`,
	`INSERT INTO token_usage_events (namespace_id, tenant_id, usage_key, day, op, model, price_version)
	   VALUES ($1, $2, ` + hash("usage") + `, '2026-01-01', 'extract', 'm', 'p1')`,
	`INSERT INTO token_usage (namespace_id, tenant_id, day, op, model, price_version, calls)
	   VALUES ($1, $2, '2026-01-01', 'extract', 'm', 'p1', 1)`,
	`INSERT INTO quota_counters (namespace_id, tenant_id, quota_key, window_start, used)
	   VALUES ($1, $2, 'recalls', '2026-01-01T00:00:00Z', 1)`,
	`INSERT INTO blob_tombstones (namespace_id, tenant_id, tombstone_id, blob_key, reason)
	   VALUES ($1, $2, ` + id("bt") + `, 'ver/d2/v1', 'document_delete')`,
	`INSERT INTO export_snapshots (namespace_id, tenant_id, version, manifest_key)
	   VALUES ($1, $2, 1, 'export/v1/manifest.json')`,
	`INSERT INTO outbox (namespace_id, tenant_id, epoch, event_type, payload)
	   VALUES ($1, $2, 1, 'ChunkCommitted', '\x00')`,
	`INSERT INTO outbox_skipped (namespace_id, tenant_id, consumer, seq, reason, skipped_by)
	   VALUES ($1, $2, 'index', 1, 'poison', 'test')`,
}

// SeedNamespace inserts the minimal graph described at seedStatements for one namespace, as engram_admin. The
// namespace_ownership row is inserted through the real edge `create` of the state machine.
func SeedNamespace(ctx context.Context, tb testing.TB, ns, tenant string) {
	tb.Helper()
	conn, err := Connect(ctx, Admin)
	if err != nil {
		tb.Fatalf("pgtest: seed connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	tx, err := conn.Begin(ctx)
	if err != nil {
		tb.Fatalf("pgtest: seed begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for i, q := range seedStatements {
		if _, err := tx.Exec(ctx, q, ns, tenant); err != nil {
			tb.Fatalf("pgtest: seed statement %d: %v\n%s", i, err, q)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatalf("pgtest: seed commit: %v", err)
	}
}

// BulkFacts describes a synthetic corpus for plan and latency tests.
type BulkFacts struct {
	Namespace string
	Tenant    string
	Documents int  // documents named bulk-<n>, tagged {bulk}
	PerDoc    int  // facts per document (one chunk per document)
	Vectors   bool // also insert a random 768-dimension vector per fact
	// DB selects a database created by NewDatabase instead of the package database.
	DB *Database
	// ChunkVectors also inserts a random vector per chunk (one chunk per document).
	ChunkVectors bool
	// Vocabulary is the number of distinct words of the fact texts (Zipf-like skew); default 2000.
	Vocabulary int
}

// wordSQL builds a fact text of 12 words drawn with a skew from a vocabulary of w words named w1..wN; the correlated
// subselect (k > -g) keeps the planner from evaluating it once for all rows.
const wordSQL = `(SELECT string_agg('w' || (1 + floor(power(random(), 2.5) * %d))::int, ' ')
                   FROM generate_series(1, 12) k WHERE k > -g)`

// SeedBulk inserts a synthetic corpus into a namespace that already has its ownership row (SeedNamespace or
// SeedOwnership), as engram_admin, and analyzes the touched tables.
func SeedBulk(ctx context.Context, tb testing.TB, b BulkFacts) {
	tb.Helper()
	if b.Vocabulary == 0 {
		b.Vocabulary = 2000
	}
	connect := func(role Role) (*pgx.Conn, error) {
		if b.DB != nil {
			return pgx.Connect(ctx, b.DB.DSN(role))
		}
		return Connect(ctx, role)
	}
	conn, err := connect(Admin)
	if err != nil {
		tb.Fatalf("pgtest: bulk connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// ledger row all bulk versions point at
	type bulkStmt struct {
		sql string
		n   int
	}
	stmts := []bulkStmt{
		{`INSERT INTO ingest_ledger (namespace_id, tenant_id, ledger_id, document_id, operation_id, update_mode,
		     item_timestamp, content_hash, content_bytes, body)
		   VALUES ($1, $2, md5($1::text || 'bulkledger')::uuid, 'bulk', md5($1::text || 'bulkop')::uuid, 'replace',
		     '2026-01-01T00:00:00Z', decode(repeat('cd', 32), 'hex'), 1, 'x') ON CONFLICT DO NOTHING`, 2},
		{`INSERT INTO documents (namespace_id, tenant_id, document_id, current_version, tags)
		   SELECT $1, $2, 'bulk-' || g, 1, ARRAY['bulk'] FROM generate_series(1, $3::int) g`, 3},
		{`INSERT INTO document_versions (namespace_id, tenant_id, document_id, version, content_hash, status,
		     update_mode, operation_id, ledger_id, chunk_count, fact_count, fact_count_by_type)
		   SELECT $1, $2, 'bulk-' || g, 1, decode(repeat('cd', 32), 'hex'), 'active', 'replace',
		     md5($1::text || 'bulkop')::uuid, md5($1::text || 'bulkledger')::uuid, 1, $4::int,
		     jsonb_build_object('world', $4::int, 'experience', 0)
		     FROM generate_series(1, $3::int) g`, 4},
		{`INSERT INTO chunks (namespace_id, tenant_id, chunk_id, document_id, document_version, life_start,
		     content_hash, header_hash, text, mentioned_at)
		   SELECT $1, $2, md5($1::text || 'bc' || g)::uuid, 'bulk-' || g, 1, 1,
		     decode(md5($1::text || 'bch' || g) || md5('bch' || g), 'hex'), decode(repeat('ee', 32), 'hex'),
		     'bulk chunk ' || g, '2026-01-01T00:00:00Z'::timestamptz + (g || ' minutes')::interval
		     FROM generate_series(1, $3::int) g`, 3},
		{fmt.Sprintf(`INSERT INTO facts (namespace_id, tenant_id, memory_id, document_id, document_version, chunk_id,
		     ordinal, content_hash, extraction_key, text, fact_type, occurred_start, mentioned_at,
		     extraction_version, prompt_version, model)
		   SELECT $1, $2, md5($1::text || 'bf' || d || '-' || o)::uuid, 'bulk-' || d, 1, md5($1::text || 'bc' ||
		     d)::uuid,
		     o, decode(md5($1::text || 'bfh' || d || '-' || o) || md5('bfh' || d || o), 'hex'),
		     decode(repeat('ff', 32), 'hex'), %s, 'world',
		     '2026-01-01T00:00:00Z'::timestamptz + ((d * 7 + o) || ' hours')::interval,
		     '2026-01-01T00:00:00Z'::timestamptz + (d || ' minutes')::interval, 1, 'extract/v1', 'm'
		     FROM generate_series(1, $3::int) d, generate_series(1, $4::int) o,
		     LATERAL (SELECT d * 1000 + o AS g) gg`, fmt.Sprintf(wordSQL, b.Vocabulary)), 4},
	}
	if b.Vectors {
		stmts = append(stmts, bulkStmt{`INSERT INTO fact_vectors (namespace_id, tenant_id, memory_id, embedding_model,
		     document_id, document_version, chunk_id, fact_type, mentioned_at, embedding)
		   SELECT f.namespace_id, f.tenant_id, f.memory_id, '` + SeedModel + `', f.document_id, f.document_version,
		     f.chunk_id, f.fact_type, f.mentioned_at,
		     (SELECT array_agg(random()::real) FROM generate_series(1, 768) k WHERE k >
		       -f.ordinal)::vector(768)::halfvec(768)
		     FROM facts f WHERE f.namespace_id = $1 AND f.document_id LIKE 'bulk-%'`, 1})
	}
	if b.ChunkVectors {
		stmts = append(stmts, bulkStmt{`INSERT INTO chunk_vectors (namespace_id, tenant_id, chunk_id, embedding_model,
		     embedding_effective_at, document_id, document_version, mentioned_at, embedding)
		   SELECT c.namespace_id, c.tenant_id, c.chunk_id, '` + SeedModel + `', c.mentioned_at, c.document_id,
		     c.document_version, c.mentioned_at,
		     (SELECT array_agg(random()::real) FROM generate_series(1, 768) k
		      WHERE k > -c.document_version)::vector(768)::halfvec(768)
		     FROM chunks c WHERE c.namespace_id = $1 AND c.document_id LIKE 'bulk-%'`, 1})
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		tb.Fatalf("pgtest: bulk begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for i, q := range stmts {
		args := []any{b.Namespace, b.Tenant, b.Documents, b.PerDoc}[:q.n]
		if _, err := tx.Exec(ctx, typed(q.sql), args...); err != nil {
			tb.Fatalf("pgtest: bulk statement %d: %v", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatalf("pgtest: bulk commit: %v", err)
	}
	// ANALYZE needs ownership (PostgreSQL 16 has no MAINTAIN privilege): the bootstrap superuser does it
	sup, err := connect(Super)
	if err != nil {
		tb.Fatalf("pgtest: analyze connect: %v", err)
	}
	defer func() { _ = sup.Close(ctx) }()
	for _, t := range []string{"documents", "document_versions", "chunks", "facts", "fact_vectors", "chunk_vectors"} {
		if _, err := sup.Exec(ctx, "ANALYZE "+t); err != nil {
			tb.Fatalf("pgtest: analyze %s: %v", t, err)
		}
	}
}

// SeedOwnership inserts only the namespace_ownership, namespace_stats and namespace_models rows of a namespace (the
// base a bulk corpus needs), as engram_admin.
func SeedOwnership(ctx context.Context, tb testing.TB, ns, tenant string) {
	tb.Helper()
	conn, err := Connect(ctx, Admin)
	if err != nil {
		tb.Fatalf("pgtest: ownership connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, q := range seedStatements[:3] {
		if _, err := conn.Exec(ctx, q, ns, tenant); err != nil {
			tb.Fatalf("pgtest: ownership: %v", err)
		}
	}
}
