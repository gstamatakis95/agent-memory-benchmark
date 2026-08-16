// Command server is the single deployable of Phase 6: the MemoryService gRPC
// server, the embedded Temporal enrichment worker, and the schedule
// bootstrap (docs/02-storage.md E.1: "single binary"; docs/03-temporal.md).
//
// Configuration is environment-only (docker-compose.yml is the contract):
//
//	PG_DSN, S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY,
//	S3_PATH_STYLE, TEMPORAL_HOSTPORT, EMBEDDER_ADDR, ENRICHMENT_VERSION
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	tclient "go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentmemv1 "example.com/agentmem/genproto/agentmem/v1"
	"example.com/agentmem/internal/blob"
	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/enrich"
	"example.com/agentmem/internal/pipeline"
	"example.com/agentmem/internal/store"
)

type config struct {
	pgDSN            string
	s3Endpoint       string
	s3Bucket         string
	s3AccessKey      string
	s3SecretKey      string
	s3PathStyle      bool
	temporalHostPort string
	embedderAddr     string
	version          int16
}

func loadConfig() config {
	get := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	version := int64(1)
	if v := os.Getenv("ENRICHMENT_VERSION"); v != "" {
		n, err := strconv.ParseInt(v, 10, 16)
		if err != nil || n <= 0 {
			log.Fatalf("server: invalid ENRICHMENT_VERSION %q", v)
		}
		version = n
	}
	return config{
		pgDSN:            get("PG_DSN", "postgres://app:app@localhost:5432/app?sslmode=disable"),
		s3Endpoint:       get("S3_ENDPOINT", "http://localhost:9000"),
		s3Bucket:         get("S3_BUCKET", "memories"),
		s3AccessKey:      get("S3_ACCESS_KEY", "app"),
		s3SecretKey:      get("S3_SECRET_KEY", "appsecret"),
		s3PathStyle:      get("S3_PATH_STYLE", "true") == "true",
		temporalHostPort: get("TEMPORAL_HOSTPORT", "localhost:7233"),
		embedderAddr:     get("EMBEDDER_ADDR", "localhost:9100"),
		version:          int16(version),
	}
}

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Postgres (retry: compose healthcheck races are still possible).
	pool, err := dialPG(ctx, cfg.pgDSN, 60*time.Second)
	if err != nil {
		log.Fatalf("server: postgres: %v", err)
	}
	defer pool.Close()
	log.Printf("server: postgres connected")

	// S3 / MinIO: aws-sdk-go-v2 with path-style + static creds
	// (docs/02-storage.md E.8).
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.s3AccessKey, cfg.s3SecretKey, "")),
	)
	if err != nil {
		log.Fatalf("server: aws config: %v", err)
	}
	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.s3Endpoint)
		o.UsePathStyle = cfg.s3PathStyle
	})

	// Temporal (retry: auto-setup registers the default namespace after the
	// frontend port opens).
	tc, err := dialTemporal(ctx, cfg.temporalHostPort, 120*time.Second)
	if err != nil {
		log.Fatalf("server: temporal: %v", err)
	}
	defer tc.Close()
	log.Printf("server: temporal connected (%s)", cfg.temporalHostPort)

	// Embedder: ONE long-lived conn, retry service config + keepalive
	// (docs/02-storage.md E.3).
	econn, err := embed.Dial(cfg.embedderAddr)
	if err != nil {
		log.Fatalf("server: embedder dial: %v", err)
	}
	defer econn.Close()
	transport := &dimsRetryEmbedder{inner: embed.NewGRPCEmbedder(econn, nil), retries: 2}
	embedClient := embed.NewClient(transport, pool)

	// Temporal worker with the real dependencies.
	enrich.DefaultVersion = cfg.version
	// Ledger attempt budget: one failed ProcessBatch activity retry cycle
	// appends up to 5 failure events per memory (attempts 1..5), so the
	// store default of 5 total failures would exhaust a memory after a
	// single outage-spanning sweep (docs/06 Tier 3 runs a 45s hard outage
	// and still requires 100% enrichment). Budget several cycles; truly
	// permanent errors (wrong dims, double prefix) still dead-letter
	// immediately via permanent=true, independent of this cap.
	pgStore := enrich.NewPGStore(pool)
	// High ceiling: with the 64s backoff cap, attempts are cheap, and a long
	// upstream incident (e.g. the DMR 512-token rejection bug) can burn 25+
	// attempts before the root cause is fixed. Poison items still dead-letter
	// via permanent=true, which is attempt-independent.
	pgStore.MaxAttempts = 200
	acts := &enrich.Activities{
		Store:    pgStore,
		Source:   &s3TextSource{pool: pool, s3: s3c},
		Embedder: embedClient,
		Version:  cfg.version,
	}
	w := enrich.NewWorker(tc, acts, enrich.WorkerConfig{})
	if err := w.Start(); err != nil {
		log.Fatalf("server: worker start: %v", err)
	}
	defer w.Stop()
	log.Printf("server: enrichment worker started (task queue %q)", enrich.TaskQueue)

	// Schedule bootstrap AFTER the worker is running (AlreadyExists tolerated).
	if err := enrich.EnsureSchedule(ctx, tc, enrich.ScheduleConfig{Version: cfg.version}); err != nil {
		log.Fatalf("server: ensure schedule: %v", err)
	}
	log.Printf("server: schedule %q ensured (version %d)", enrich.ScheduleID, cfg.version)

	// gRPC MemoryService on :8081.
	lis, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("server: listen: %v", err)
	}
	gs := grpc.NewServer()
	agentmemv1.RegisterMemoryServiceServer(gs, &memoryService{
		pool: pool, s3: s3c, temporal: tc, bucket: cfg.s3Bucket, version: cfg.version,
	})
	go func() {
		<-ctx.Done()
		log.Printf("server: shutting down")
		gs.GracefulStop()
	}()
	log.Printf("server: MemoryService serving on :8081 (enrichment version %d)", cfg.version)
	if err := gs.Serve(lis); err != nil {
		log.Fatalf("server: serve: %v", err)
	}
}

func dialPG(ctx context.Context, dsn string, timeout time.Duration) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(timeout)
	for {
		pool, err := store.NewPool(ctx, dsn)
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, err
		}
		time.Sleep(time.Second)
	}
}

func dialTemporal(ctx context.Context, hostPort string, timeout time.Duration) (tclient.Client, error) {
	deadline := time.Now().Add(timeout)
	for {
		c, err := tclient.Dial(tclient.Options{HostPort: hostPort})
		if err == nil {
			return c, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, err
		}
		time.Sleep(2 * time.Second)
	}
}

// dimsRetryEmbedder wraps the transport with a bounded re-ask on wrong-dims
// responses. Rationale (docs/06-testing.md Tier 3 chaos): MOCK_BAD_DIMS_RATE
// injects wrong-dims responses at RANDOM per call, but run.sh always passes
// --fail-on-dead and e2e-chaos must reach 100% enrichment — a single random
// 512-dim response must therefore not dead-letter a memory forever. A retry
// of the same text succeeds with p = 1 - rate. A DETERMINISTICALLY
// wrong-dims embedder still exhausts the retries and surfaces the typed
// PermanentError, so the validator's dead-letter path stays intact.
type dimsRetryEmbedder struct {
	inner   embed.Embedder
	retries int
}

func (d *dimsRetryEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	var lastErr error
	for i := 0; i <= d.retries; i++ {
		vec, err := d.inner.Embed(ctx, text)
		if err == nil || !embed.IsPermanent(err) {
			return vec, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// s3TextSource resolves memory ids to enrichment-ready texts: the memories
// row gives the content-addressed S3 key, the blob envelope gives the raw
// text + date string, and internal/pipeline derives everything else. The
// migrations' DDL keeps NO text in Postgres for raw memories — S3 is the
// source of truth for bytes (docs/02-storage.md E.2).
type s3TextSource struct {
	pool *pgxpool.Pool
	s3   *s3.Client
}

func (src *s3TextSource) FetchTexts(ctx context.Context, ids []int64) ([]enrich.MemoryText, error) {
	rows, err := src.pool.Query(ctx,
		`SELECT id, s3_bucket, s3_key FROM memories WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("server: fetch memory refs: %w", err)
	}
	defer rows.Close()
	type ref struct{ bucket, key string }
	refs := make(map[int64]ref, len(ids))
	for rows.Next() {
		var id int64
		var r ref
		if err := rows.Scan(&id, &r.bucket, &r.key); err != nil {
			return nil, err
		}
		refs[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]enrich.MemoryText, len(ids))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, id := range ids {
		i, id := i, id
		r, ok := refs[id]
		if !ok {
			return nil, fmt.Errorf("server: memory %d not found", id)
		}
		g.Go(func() error {
			obj, err := src.s3.GetObject(gctx, &s3.GetObjectInput{
				Bucket: aws.String(r.bucket), Key: aws.String(r.key),
			})
			if err != nil {
				return fmt.Errorf("server: get blob %s/%s: %w", r.bucket, r.key, err)
			}
			defer obj.Body.Close()
			raw, err := io.ReadAll(obj.Body)
			if err != nil {
				return fmt.Errorf("server: read blob %s: %w", r.key, err)
			}
			env, err := blob.Parse(raw)
			if err != nil {
				return err
			}
			var ts *time.Time
			if env.DateTime != "" {
				if t, err := pipeline.ParseTimestamp(env.DateTime); err == nil {
					ts = &t
				}
			}
			// docs/01-retrieval.md 4.6 step 3: embed "[date] speaker: text";
			// the same string is stored as normalized_text and feeds Tokenize.
			text := pipeline.Normalize(pipeline.FormatForEmbedding(ts, env.Speaker, env.Text))
			out[i] = enrich.MemoryText{
				MemoryID: id,
				Text:     text,
				Lexemes:  pipeline.Tokenize(text),
				TS:       ts,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// memoryService implements agentmem.v1.MemoryService.
type memoryService struct {
	agentmemv1.UnimplementedMemoryServiceServer
	pool     *pgxpool.Pool
	s3       *s3.Client
	temporal tclient.Client
	bucket   string
	version  int16
}

// UploadMemories: client-stream. Per memory: canonical blob -> S3 put (write
// blob first: S3 strong read-after-write) -> insert-only memories row.
// A duplicate natural key (conversation, session, turn) counts as deduped.
func (m *memoryService) UploadMemories(stream agentmemv1.MemoryService_UploadMemoriesServer) error {
	ctx := stream.Context()
	var accepted, deduped uint64
	put := make(map[string]bool) // s3 keys already written this stream
	for {
		mem, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&agentmemv1.UploadAck{Accepted: accepted, Deduped: deduped})
		}
		if err != nil {
			return err
		}
		env := blob.Envelope{
			ConversationID: mem.GetConversationId(),
			SessionID:      mem.GetSessionId(),
			TurnID:         mem.GetTurnId(),
			Speaker:        mem.GetSpeaker(),
			Text:           mem.GetText(),
			DateTime:       mem.GetMetadata()["date_time"],
		}
		raw, err := env.Marshal()
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "marshal blob: %v", err)
		}
		hash := blob.Hash(raw)
		key := blob.Key(hash)
		if !put[key] {
			if _, err := m.s3.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(m.bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader(raw),
			}); err != nil {
				return status.Errorf(codes.Unavailable, "s3 put %s: %v", key, err)
			}
			put[key] = true
		}
		_, err = store.InsertMemory(ctx, m.pool, store.Memory{
			ConversationID: env.ConversationID,
			SessionID:      env.SessionID,
			TurnID:         env.TurnID,
			S3Bucket:       m.bucket,
			S3Key:          key,
			ByteSize:       int32(len(raw)),
			ContentHash:    hash,
		})
		switch {
		case err == nil:
			accepted++
		case isUniqueViolation(err):
			deduped++ // content-addressed re-ingest: row already exists
		default:
			return status.Errorf(codes.Internal, "insert memory: %v", err)
		}
	}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// FetchAllMemories: server-stream, version-pinned (docs/04-append-only.md §4).
func (m *memoryService) FetchAllMemories(req *agentmemv1.FetchReq, stream agentmemv1.MemoryService_FetchAllMemoriesServer) error {
	if req.GetEnrichmentVersion() <= 0 {
		return status.Error(codes.InvalidArgument, "enrichment_version is required (no defaulting)")
	}
	conv := strconv.FormatInt(req.GetConversationId(), 10)
	ems, err := store.FetchAtVersion(stream.Context(), m.pool, conv, int16(req.GetEnrichmentVersion()))
	if err != nil {
		return status.Errorf(codes.Internal, "fetch at version: %v", err)
	}
	for _, em := range ems {
		var tsUnix int64
		if em.TS != nil {
			tsUnix = em.TS.Unix()
		}
		if err := stream.Send(&agentmemv1.EnrichedMemory{
			MemoryId:          em.MemoryID,
			EnrichmentVersion: req.GetEnrichmentVersion(),
			NormalizedText:    em.NormalizedText,
			Lexemes:           em.Lexemes,
			TsUnix:            tsUnix,
			Embedding:         em.Embedding,
			ContentHash:       em.ContentHash,
			S3Key:             em.S3Key,
		}); err != nil {
			return err
		}
	}
	return nil
}

// globalProgressSQL mirrors the enrichment_progress() SQL function without
// the conversation filter, for the harness' "wait until EVERYTHING is
// enriched" poll (conversation_id 0 = all conversations). Read-only.
const globalProgressSQL = `
SELECT count(*) AS total,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM memory_enrichment_events e
           WHERE e.memory_id = m.id AND e.enrichment_version = $1
             AND e.status = 'done')) AS done,
       count(*) FILTER (WHERE NOT EXISTS (
           SELECT 1 FROM memory_enrichment_events e
           WHERE e.memory_id = m.id AND e.enrichment_version = $1
             AND e.status = 'done')) AS remaining,
       count(*) FILTER (WHERE EXISTS (
           SELECT 1 FROM memory_enrichment_events e
           WHERE e.memory_id = m.id AND e.enrichment_version = $1
             AND e.status = 'failed' AND e.permanent)) AS dead
FROM memories m`

// GetProgress: version-pinned progress. conversation_id 0 means "all
// conversations" (used by the client's wait-enriched, which run.sh invokes
// without a dataset argument).
func (m *memoryService) GetProgress(ctx context.Context, req *agentmemv1.ProgressReq) (*agentmemv1.Progress, error) {
	if req.GetEnrichmentVersion() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "enrichment_version is required (no defaulting)")
	}
	version := int16(req.GetEnrichmentVersion())
	var p store.Progress
	var err error
	if req.GetConversationId() == 0 {
		err = m.pool.QueryRow(ctx, globalProgressSQL, version).
			Scan(&p.Total, &p.Done, &p.Remaining, &p.Dead)
	} else {
		conv := strconv.FormatInt(req.GetConversationId(), 10)
		p, err = store.GetProgress(ctx, m.pool, conv, version)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "progress: %v", err)
	}
	return &agentmemv1.Progress{Total: p.Total, Done: p.Done, Remaining: p.Remaining, Dead: p.Dead}, nil
}

// TriggerSweep fires the enrichment schedule immediately.
func (m *memoryService) TriggerSweep(ctx context.Context, _ *agentmemv1.TriggerSweepReq) (*agentmemv1.TriggerSweepResp, error) {
	if err := enrich.TriggerNow(ctx, m.temporal); err != nil {
		return nil, status.Errorf(codes.Internal, "trigger sweep: %v", err)
	}
	return &agentmemv1.TriggerSweepResp{Triggered: true}, nil
}
