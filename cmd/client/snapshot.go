// Snapshot-serving subcommands (docs/07-snapshot-serving.md): build-snapshot,
// snapshot-info, search, and the --engine snapshot path of `eval`. These
// speak to the same MemoryService the other subcommands use; the snapshot
// itself is built and served entirely by the long-lived server (see the
// compose-run env trap note in AGENTS.md / scripts/run-snapshot.sh — a
// one-off `docker compose run` client needs no extra env for these).
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	agentmemv1 "example.com/agentmem/genproto/agentmem/v1"
	"example.com/agentmem/internal/eval"
	"example.com/agentmem/internal/pipeline"
	"example.com/agentmem/internal/retrieve"
)

// ------------------------------------------------------- build-snapshot --

func cmdBuildSnapshot(args []string) error {
	fs := flag.NewFlagSet("build-snapshot", flag.ExitOnError)
	version := fs.Int("version", 0, "enrichment version to build the snapshot from (required)")
	_ = fs.Parse(args)
	if *version <= 0 {
		return fmt.Errorf("--version is required (no defaulting)")
	}

	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()

	// A full build streams the whole version-pinned ledger and seals a
	// Bleve+vectors artifact; generous but bounded so a stuck build fails
	// loudly instead of hanging the caller forever.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	resp, err := cli.BuildSnapshot(ctx, &agentmemv1.BuildSnapshotReq{EnrichmentVersion: int32(*version)})
	if err != nil {
		return err
	}
	printSnapshotInfo("build-snapshot", resp.GetSnapshot())
	return nil
}

// ---------------------------------------------------------- snapshot-info --

func cmdSnapshotInfo(args []string) error {
	fs := flag.NewFlagSet("snapshot-info", flag.ExitOnError)
	_ = fs.Parse(args)

	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := cli.GetSnapshot(ctx, &agentmemv1.GetSnapshotReq{})
	if err != nil {
		return err
	}
	printSnapshotInfo("snapshot-info", info)
	return nil
}

func printSnapshotInfo(cmd string, info *agentmemv1.SnapshotInfo) {
	if !info.GetLoaded() {
		log.Printf("%s: loaded=false (no snapshot published yet)", cmd)
		return
	}
	log.Printf("%s: loaded=true version=%d enrichment_version=%d doc_count=%d vector_dim=%d embed_model=%s s3_key=%s created_at=%s",
		cmd, info.GetVersion(), info.GetEnrichmentVersion(), info.GetDocCount(), info.GetVectorDim(),
		info.GetEmbedModel(), info.GetS3Key(), info.GetCreatedAt())
}

// ----------------------------------------------------------------- search --

func cmdSearch(args []string) error {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	query := fs.String("query", "", "query text (required)")
	// The wire conversation id is the surrogate id ingest/eval already use —
	// see surrogateConvID(dataset, sampleID) in cmd/client/dataset.go — not
	// a raw dataset sample id.
	conversation := fs.Int64("conversation", 0, "conversation id: surrogateConvID(dataset, sampleID) (cmd/client/dataset.go); 0 = no filter")
	mode := fs.String("mode", "hybrid", "bm25|dense|hybrid")
	topK := fs.Int("topk", 0, "final result depth (0 = server default: 10)")
	depth := fs.Int("depth", 0, "per-arm candidate depth before fusion (0 = server default: 40)")
	rrfK := fs.Int("rrf-k", 0, "RRF fusion constant (0 = server default: 60)")
	recencyBoost := fs.Float64("recency-boost", 0, "post-fusion recency boost strength B (0 = off)")
	halfLifeDays := fs.Float64("half-life-days", 0, "recency half-life in days (0 = server default: 30)")
	abstain := fs.Float64("abstain", 0, "abstention threshold on the top fused score (0 = off)")
	_ = fs.Parse(args)
	if *query == "" {
		return fmt.Errorf("--query is required")
	}

	conn, cli, err := dialServer()
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := cli.Search(ctx, &agentmemv1.SearchReq{
		Query:            *query,
		ConversationId:   *conversation,
		Mode:             *mode,
		TopK:             int32(*topK),
		Depth:            int32(*depth),
		RrfK:             int32(*rrfK),
		RecencyBoost:     *recencyBoost,
		HalfLifeDays:     *halfLifeDays,
		AbstainThreshold: *abstain,
	})
	if err != nil {
		return err
	}
	log.Printf("search: snapshot_version=%d abstained=%v lexical_only=%v hits=%d",
		resp.GetSnapshotVersion(), resp.GetAbstained(), resp.GetLexicalOnly(), len(resp.GetHits()))
	for i, h := range resp.GetHits() {
		fmt.Printf("%3d  memory_id=%-10d score=%.6f session_id=%-16s ts=%-10d s3_key=%s\n",
			i+1, h.GetMemoryId(), h.GetScore(), h.GetSessionId(), h.GetTsUnix(), h.GetS3Key())
	}
	return nil
}

// ---------------------------------------------------------- eval helpers --

// localTwins maps one conversation's local rows (turns + round twins, see
// roundItemsOf) by the content hash of their recomputed blob — the identity
// the server hands back on both FetchAllMemories (fetchCorpus) and Search
// (SearchHit.ContentHash / runSnapshotEval).
func localTwins(c conversation, roundItems []item) (map[string]item, error) {
	local := make([]item, 0, len(c.Items)+len(roundItems))
	local = append(local, c.Items...)
	local = append(local, roundItems...)
	byHash := make(map[string]item, len(local))
	for _, it := range local {
		raw, err := envelopeOf(c.Num, it).Marshal()
		if err != nil {
			return nil, err
		}
		byHash[hex.EncodeToString(blobHash(raw))] = it
	}
	return byHash, nil
}

// coveredTurns returns the set of turn ids covered by some multi-turn round
// in expand (round id -> member turn ids), shared by filterGranularity and
// keepAtGranularity's callers.
func coveredTurns(expand map[string][]string) map[string]bool {
	covered := make(map[string]bool)
	for _, members := range expand {
		for _, id := range members {
			covered[id] = true
		}
	}
	return covered
}

// keepAtGranularity is the per-row predicate behind filterGranularity's
// turn/round/all semantics, factored out so runSnapshotEval can apply the
// identical rule to a ranked hit list instead of a corpus row list.
func keepAtGranularity(id string, expand map[string][]string, covered map[string]bool, granularity string) bool {
	if granularity == "" || granularity == "all" {
		return true
	}
	_, isRound := expand[id]
	switch granularity {
	case "turn":
		return !isRound
	case "round":
		return isRound || !covered[id]
	}
	return true
}

// runSnapshotEval is the --engine snapshot path of cmdEval: instead of
// fetching the whole conversation corpus and ranking client-side, one
// Search RPC per question queries the server's loaded snapshot (BM25 +
// dense fused with RRF, then recency boost + abstention server-side —
// docs/07-snapshot-serving.md section 4/5). MMR / temporal-boost options do
// not apply; the caller is responsible for warning about --max-per-session.
//
// hitsFetched/hitsMapped play the role fetchedRows/mappedRows play for the
// in-process engine: total hits returned by the server vs. hits that mapped
// to a local dataset twin and survived the granularity filter.
func runSnapshotEval(ctx context.Context, cli agentmemv1.MemoryServiceClient, mode retrieve.Mode, tun evalTuning, convs []conversation) (results []eval.QueryResult, hitsFetched, hitsMapped, totalQuestions, skipped int, snapVersion int64, err error) {
	haveVersion := false
	noTwin := 0
	// Over-fetch depth so the client-side granularity filter below cannot
	// starve the final top-k list.
	overfetch := tun.topK
	if overfetch < 40 {
		overfetch = 40
	}

	for _, c := range convs {
		roundItems, expand := roundItemsOf(c)
		byHash, lerr := localTwins(c, roundItems)
		if lerr != nil {
			err = fmt.Errorf("conversation %s: %w", c.Name, lerr)
			return
		}
		covered := coveredTurns(expand)

		for _, q := range c.Questions {
			totalQuestions++
			req := &agentmemv1.SearchReq{
				Query:          q.Question,
				ConversationId: c.Num,
				Mode:           string(mode),
				TopK:           int32(overfetch),
				Depth:          int32(tun.candidates),
				RrfK:           int32(tun.rrfK),
			}
			if q.QuestionDate != "" {
				if t, perr := pipeline.ParseTimestamp(q.QuestionDate); perr == nil {
					req.QuestionDateUnix = t.Unix()
				}
			}

			resp, serr := cli.Search(ctx, req)
			if serr != nil {
				switch status.Code(serr) {
				case codes.FailedPrecondition:
					err = fmt.Errorf("search: no snapshot loaded for conversation %s; run `client build-snapshot --version N` first: %w",
						c.Name, serr)
					return
				case codes.Unavailable, codes.DeadlineExceeded:
					skipped++
					if skipped <= 5 {
						log.Printf("eval: WARN question %s: search unavailable; skipping: %v", q.ID, serr)
					}
					continue
				default:
					err = fmt.Errorf("question %s: search: %w", q.ID, serr)
					return
				}
			}
			if !haveVersion {
				snapVersion = resp.GetSnapshotVersion()
				haveVersion = true
			}

			hits := resp.GetHits()
			hitsFetched += len(hits)
			scored := make([]retrieve.Scored, 0, len(hits))
			for _, h := range hits {
				it, ok := byHash[hex.EncodeToString(h.GetContentHash())]
				if !ok {
					noTwin++
					continue
				}
				if !keepAtGranularity(it.TurnID, expand, covered, tun.granularity) {
					continue
				}
				scored = append(scored, retrieve.Scored{ID: it.TurnID, Score: h.GetScore()})
				if len(scored) >= tun.topK {
					break
				}
			}
			hitsMapped += len(scored)

			retrieved := expandRetrieved(scored, expand)
			results = append(results, eval.QueryResult{
				ID: q.ID, Group: q.Group, Retrieved: retrieved, Gold: q.Evidence,
			})
		}
	}
	if noTwin > 0 {
		log.Printf("eval: snapshot engine: %d/%d hits had no local twin", noTwin, hitsFetched)
	}
	return
}
