// Command client is the benchmark harness CLI (docs/05-diagrams.md run
// flow). Subcommands, all speaking gRPC to the server at SERVER_ADDR
// (default "server:8081" — run.sh runs this binary inside the compose
// network via `docker compose run --rm server /app/client ...`):
//
//	ingest        --dataset fixtures|locomo|longmemeval_s --version N
//	trigger-sweep
//	wait-enriched --version N --timeout 15m [--fail-on-dead]
//	eval          --dataset X --version N --retrieval bm25|dense|hybrid
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	agentmemv1 "example.com/agentmem/genproto/agentmem/v1"
	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/eval"
	"example.com/agentmem/internal/pipeline"
	"example.com/agentmem/internal/retrieve"
)

func serverAddr() string {
	if a := os.Getenv("SERVER_ADDR"); a != "" {
		return a
	}
	return "server:8081"
}

func dialServer() (*grpc.ClientConn, agentmemv1.MemoryServiceClient, error) {
	// WaitForReady: `docker compose up -d --wait server` only waits for the
	// container to be running; the server may still be dialing Temporal when
	// the first RPC arrives. Let the channel block until it is reachable.
	conn, err := grpc.NewClient(serverAddr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		return nil, nil, err
	}
	return conn, agentmemv1.NewMemoryServiceClient(conn), nil
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "ingest":
		err = cmdIngest(args)
	case "trigger-sweep":
		err = cmdTriggerSweep(args)
	case "wait-enriched":
		err = cmdWaitEnriched(args)
	case "eval":
		err = cmdEval(args)
	default:
		usage()
	}
	if err != nil {
		log.Fatalf("client %s: %v", cmd, err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: client <ingest|trigger-sweep|wait-enriched|eval> [flags]`)
	os.Exit(2)
}

// ---------------------------------------------------------------- ingest --

func cmdIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	dataset := fs.String("dataset", "fixtures", "fixtures|locomo|longmemeval_s")
	version := fs.Int("version", 1, "enrichment version (informational; enrichment version is server-side)")
	_ = fs.Parse(args)

	convs, err := loadDataset(*dataset)
	if err != nil {
		return err
	}
	turns, rounds := 0, 0
	for _, c := range convs {
		turns += len(c.Items)
		ri, _ := roundItemsOf(c)
		rounds += len(ri)
	}
	log.Printf("ingest: dataset=%s conversations=%d memories=%d (turns=%d rounds=%d, target version %d)",
		*dataset, len(convs), turns+rounds, turns, rounds, *version)

	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	stream, err := cli.UploadMemories(ctx)
	if err != nil {
		return err
	}
	send := func(c conversation, it item, granularity string) error {
		env := envelopeOf(c.Num, it)
		return stream.Send(&agentmemv1.Memory{
			ConversationId: env.ConversationID,
			SessionId:      env.SessionID,
			TurnId:         env.TurnID,
			Speaker:        env.Speaker,
			Text:           env.Text,
			Granularity:    granularity,
			Metadata: map[string]string{
				"date_time": env.DateTime,
				"dataset":   *dataset,
			},
		})
	}
	for _, c := range convs {
		for _, it := range c.Items {
			if err := send(c, it, "turn"); err != nil {
				return fmt.Errorf("send: %w", err)
			}
		}
		// Round-granularity rows (docs/01-retrieval.md Finding 1 / section 4.6
		// step 4): index rounds alongside the turn rows kept for turn-level
		// evidence scoring.
		ri, _ := roundItemsOf(c)
		for _, it := range ri {
			if err := send(c, it, "round"); err != nil {
				return fmt.Errorf("send round: %w", err)
			}
		}
	}
	ack, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	log.Printf("ingest: accepted=%d deduped=%d", ack.GetAccepted(), ack.GetDeduped())
	return nil
}

// --------------------------------------------------------- trigger-sweep --

func cmdTriggerSweep(args []string) error {
	fs := flag.NewFlagSet("trigger-sweep", flag.ExitOnError)
	_ = fs.Parse(args)

	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := cli.TriggerSweep(ctx, &agentmemv1.TriggerSweepReq{})
	if err != nil {
		return err
	}
	log.Printf("trigger-sweep: triggered=%v", resp.GetTriggered())
	return nil
}

// --------------------------------------------------------- wait-enriched --

// cmdWaitEnriched polls global (conversation_id 0) version-pinned progress
// until remaining == 0. Semantics settled for the chaos tier:
//   - success:  remaining == 0 (a dead memory can never reach 'done', so
//     remaining == 0 implies dead == 0);
//   - --fail-on-dead: exit nonzero the moment dead > 0 — dead rows are
//     permanent at this version, so waiting longer cannot help;
//   - timeout: exit nonzero.
func cmdWaitEnriched(args []string) error {
	fs := flag.NewFlagSet("wait-enriched", flag.ExitOnError)
	version := fs.Int("version", 0, "enrichment version (required)")
	timeout := fs.Duration("timeout", 15*time.Minute, "give up after this long")
	failOnDead := fs.Bool("fail-on-dead", false, "exit nonzero if any memory is dead-lettered")
	_ = fs.Parse(args)
	if *version <= 0 {
		return fmt.Errorf("--version is required (no defaulting)")
	}

	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var last string
	for {
		p, err := cli.GetProgress(ctx, &agentmemv1.ProgressReq{
			ConversationId: 0, EnrichmentVersion: int32(*version),
		})
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("timeout after %s (last: %s)", *timeout, last)
			}
			return err
		}
		line := fmt.Sprintf("total=%d done=%d remaining=%d dead=%d",
			p.GetTotal(), p.GetDone(), p.GetRemaining(), p.GetDead())
		if line != last {
			log.Printf("wait-enriched: %s", line)
			last = line
		}
		if *failOnDead && p.GetDead() > 0 {
			return fmt.Errorf("%d memories dead-lettered at version %d (--fail-on-dead)", p.GetDead(), *version)
		}
		if p.GetTotal() > 0 && p.GetRemaining() == 0 {
			log.Printf("wait-enriched: 100%% enriched at version %d", *version)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout after %s (last: %s)", *timeout, line)
		case <-time.After(2 * time.Second):
		}
	}
}

// ------------------------------------------------------------------ eval --

func cmdEval(args []string) error {
	fs := flag.NewFlagSet("eval", flag.ExitOnError)
	dataset := fs.String("dataset", "fixtures", "fixtures|locomo|longmemeval_s")
	version := fs.Int("version", 0, "enrichment version (required)")
	retrieval := fs.String("retrieval", "hybrid", "bm25|dense|hybrid")
	// docs/01-retrieval.md section 4.3 step 6 prescribes a per-session cap but
	// no numeric value; 4 is a modest default that keeps room for multi-session
	// evidence inside top-10 (0 = uncapped).
	maxPerSession := fs.Int("max-per-session", 4, "MMR per-session result cap (0 = uncapped)")
	_ = fs.Parse(args)
	if *version <= 0 {
		return fmt.Errorf("--version is required (no defaulting)")
	}
	mode, err := retrieve.ParseMode(*retrieval)
	if err != nil {
		return err
	}

	convs, err := loadDataset(*dataset)
	if err != nil {
		return err
	}
	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	// Queries go through the SAME embedder service as the corpus, via the
	// prefix-owning embed.Client (search_query:). Wrapped with the same
	// bounded wrong-dims re-ask as the server so a randomly injected 512-dim
	// response (chaos tier) cannot fail the eval. Query embeds share the
	// persistent embedding_cache when PG_DSN is available (compose always
	// sets it); without it the client gracefully falls back to no cache.
	var qemb retrieve.QueryEmbedder
	if mode != retrieve.ModeBM25 {
		addr := os.Getenv("EMBEDDER_ADDR")
		if addr == "" {
			addr = "embedder-mock:9100"
		}
		econn, err := embed.Dial(addr)
		if err != nil {
			return fmt.Errorf("dial embedder: %w", err)
		}
		defer econn.Close()
		var edb embed.DB
		if dsn := os.Getenv("PG_DSN"); dsn != "" {
			pool, err := newPGPool(ctx, dsn)
			if err != nil {
				log.Printf("eval: WARN query embedding cache disabled (pg unavailable): %v", err)
			} else {
				defer pool.Close()
				edb = pool
			}
		}
		qemb = embed.NewClient(&dimsRetryEmbedder{
			inner: embed.NewGRPCEmbedder(econn, nil), retries: 2,
		}, edb)
	}

	var results []eval.QueryResult
	fetchedRows, mappedRows := 0, 0
	for _, c := range convs {
		// Round rows are part of the corpus; expand maps a retrieved round id
		// back to its member turn ids for turn-level evidence credit.
		roundItems, expand := roundItemsOf(c)
		rows, nFetched, err := fetchCorpus(ctx, cli, c, roundItems, int32(*version))
		if err != nil {
			return fmt.Errorf("conversation %s: %w", c.Name, err)
		}
		fetchedRows += nFetched
		mappedRows += len(rows)
		corpus, err := retrieve.NewCorpus(rows)
		if err != nil {
			return err
		}
		r, err := retrieve.NewRetriever(corpus, qemb, retrieve.Options{
			Mode: mode,
			MMR:  retrieve.MMROptions{MaxPerSession: *maxPerSession},
		})
		if err != nil {
			return err
		}
		for _, q := range c.Questions {
			var qdate time.Time
			if q.QuestionDate != "" {
				if t, err := pipeline.ParseTimestamp(q.QuestionDate); err == nil {
					qdate = t
				}
			}
			scored, err := r.Search(ctx, q.Question, qdate)
			if err != nil {
				return fmt.Errorf("question %s: %w", q.ID, err)
			}
			retrieved := expandRetrieved(scored, expand)
			results = append(results, eval.QueryResult{
				ID: q.ID, Group: q.Group, Retrieved: retrieved, Gold: q.Evidence,
			})
		}
	}

	rep := eval.Evaluate(results, []int{5, 10})
	printReport(*dataset, string(mode), rep, fetchedRows, mappedRows)
	reportCacheRows(ctx)

	if *dataset == "fixtures" {
		if r5 := rep.Overall.Recall[5]; r5 < 0.999999 {
			return fmt.Errorf("FIXTURES GATE FAILED: Recall@5 = %.4f, want 1.0", r5)
		}
		log.Printf("fixtures gate: Recall@5 == 1.0  PASS")
	}
	return nil
}

// fetchCorpus streams the version-pinned enriched rows of one conversation
// and maps them back to dataset turn/round ids via the content hash of the
// recomputed blob (the wire rows carry only content_hash/s3_key identity).
// roundItems are the client-assembled round-granularity twins uploaded
// alongside the turns (see roundItemsOf).
func fetchCorpus(ctx context.Context, cli agentmemv1.MemoryServiceClient, c conversation, roundItems []item, version int32) ([]retrieve.Row, int, error) {
	local := make([]item, 0, len(c.Items)+len(roundItems))
	local = append(local, c.Items...)
	local = append(local, roundItems...)
	byHash := make(map[string]item, len(local))
	for _, it := range local {
		raw, err := envelopeOf(c.Num, it).Marshal()
		if err != nil {
			return nil, 0, err
		}
		byHash[hex.EncodeToString(blobHash(raw))] = it
	}

	stream, err := cli.FetchAllMemories(ctx, &agentmemv1.FetchReq{
		ConversationId: c.Num, EnrichmentVersion: version,
	})
	if err != nil {
		return nil, 0, err
	}
	var rows []retrieve.Row
	fetched := 0
	for {
		em, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A mid-stream failure must fail the eval loudly: silently
			// truncating the corpus would produce wrong metrics with exit 0.
			return nil, fetched, fmt.Errorf("fetch stream (after %d rows): %w", fetched, err)
		}
		fetched++
		it, ok := byHash[hex.EncodeToString(em.GetContentHash())]
		if !ok {
			log.Printf("eval: WARN row memory_id=%d (s3_key %s) has no local twin; skipping",
				em.GetMemoryId(), em.GetS3Key())
			continue
		}
		var ts time.Time
		if em.GetTsUnix() != 0 {
			ts = time.Unix(em.GetTsUnix(), 0).UTC()
		}
		rows = append(rows, retrieve.Row{
			ID:             it.TurnID,
			ConversationID: c.Name,
			SessionID:      it.SessionID,
			TurnID:         it.TurnID,
			Text:           em.GetNormalizedText(),
			Lexemes:        em.GetLexemes(),
			Timestamp:      ts,
			EmbeddingBytes: em.GetEmbedding(),
		})
	}
	if len(rows) < len(local) {
		log.Printf("eval: WARN conversation %s: %d/%d memories enriched at this version",
			c.Name, len(rows), len(local))
	}
	return rows, fetched, nil
}

// expandRetrieved maps the ranked retrieval output to turn-level ids for
// scoring: a retrieved round expands, in place and in member order, to its
// member turn ids (a round containing an evidence turn counts as retrieving
// that turn); turn rows pass through unchanged. The result is deduped while
// preserving rank order, so a turn and the round containing it gain credit
// only once, at the better rank.
func expandRetrieved(scored []retrieve.Scored, expand map[string][]string) []string {
	out := make([]string, 0, len(scored))
	seen := make(map[string]bool, len(scored))
	add := func(id string) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, s := range scored {
		if members, ok := expand[s.ID]; ok {
			for _, id := range members {
				add(id)
			}
			continue
		}
		add(s.ID)
	}
	return out
}

func printReport(dataset, mode string, rep eval.Report, fetched, mapped int) {
	fmt.Println("==================================================")
	fmt.Printf("retrieval eval  dataset=%s  mode=%s\n", dataset, mode)
	fmt.Printf("corpus rows fetched=%d mapped=%d\n", fetched, mapped)
	fmt.Printf("queries scored=%d skipped(no evidence)=%d\n", rep.Overall.N, rep.Skipped)
	fmt.Printf("overall   Recall@5=%.4f  Recall@10=%.4f  NDCG@5=%.4f  NDCG@10=%.4f  MRR=%.4f\n",
		rep.Overall.Recall[5], rep.Overall.Recall[10],
		rep.Overall.NDCG[5], rep.Overall.NDCG[10], rep.Overall.MRR)
	groups := make([]string, 0, len(rep.ByGroup))
	for g := range rep.ByGroup {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	for _, g := range groups {
		s := rep.ByGroup[g]
		fmt.Printf("%-24s n=%-4d Recall@5=%.4f  NDCG@5=%.4f  MRR=%.4f\n",
			g, s.N, s.Recall[5], s.NDCG[5], s.MRR)
	}
	fmt.Println("==================================================")
}

// reportCacheRows prints embedding_cache row counts (docs/06: cache proof via
// "embedding_cache row growth" — the mock itself is frozen and has no
// counter). Best-effort: needs PG_DSN, which compose provides.
func reportCacheRows(ctx context.Context) {
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		return
	}
	pool, err := newPGPool(ctx, dsn)
	if err != nil {
		log.Printf("eval: cache stats unavailable: %v", err)
		return
	}
	defer pool.Close()
	rows, err := pool.Query(ctx,
		`SELECT task_prefix, count(*) FROM embedding_cache GROUP BY task_prefix ORDER BY task_prefix`)
	if err != nil {
		log.Printf("eval: cache stats unavailable: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var prefix string
		var n int64
		if err := rows.Scan(&prefix, &n); err != nil {
			return
		}
		fmt.Printf("embedding_cache rows: task_prefix=%q count=%d\n", prefix, n)
	}
}
