// Command nomicbridge is a unary gRPC embed.v1.Embedder server that proxies
// Embed calls to an OpenAI-compatible /embeddings endpoint (Docker Model
// Runner serving ai/nomic-embed-text-v1.5).
//
// The gRPC surface stays strictly unary (one text in, one vector out) but the
// bridge micro-batches internally: concurrent Embed RPCs are collected by a
// single goroutine and sent upstream as ONE array-input POST. This turns the
// client's 32-way unary fan-out into upstream batches (measured: batch-32
// array input embeds ~214 texts/s vs ~1-30/s one at a time).
//
// The caller sends text ALREADY prefixed with "search_document: " /
// "search_query: " — exactly the prefixes the nomic model expects — so the
// input passes through verbatim, no extra prefixing here.
//
// Error mapping keeps the client's retry policy meaningful:
//   - connection errors / HTTP 5xx  -> UNAVAILABLE (transient, client retries)
//   - malformed or wrong-dim reply  -> INTERNAL   (permanent, no retry)
//   - a 500 "too large" batch reply -> retry items individually; an item too
//     large even alone -> INTERNAL  (permanent, dead-letter, no retry)
//
// Env:
//
//	OPENAI_BASE_URL  default http://model-runner.docker.internal/engines/v1
//	EMBED_MODEL      default ai/nomic-embed-text-v1.5
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	embedv1 "example.com/agentmem/genproto/embed/v1"
)

const (
	dims            = 768
	upstreamTimeout = 30 * time.Second // model can queue under load

	// The DMR llama.cpp endpoint hard-rejects inputs beyond its 512-token
	// batch (HTTP 500), so long texts must be truncated here or they fail
	// forever. ~1200 chars keeps even token-dense (non-English, code-heavy,
	// punctuation-heavy) text under 512 tokens — 1500 proved too generous for
	// such content. Callers hash the FULL text for cache keys before the RPC,
	// so a given full text still maps to one deterministic (truncated) vector.
	maxEmbedChars = 1200

	// Micro-batching: flush when batchMax items are queued or batchWindow has
	// elapsed since the first queued item, whichever comes first. batchMax=32
	// matches the client's errgroup fan-out; beyond 32 upstream throughput
	// gains are marginal (batch64 -> ~240/s vs ~214/s). batchWindow=25ms is
	// the worst-case latency tax on a lone call — small next to model time.
	batchMax    = 32
	batchWindow = 25 * time.Millisecond

	// Queue depth: clients keep at most ~32 RPCs in flight, sized generously
	// so enqueue never blocks in practice.
	queueDepth = 512

	statsEvery = 30 * time.Second
)

// pendingEmbed is one waiting Embed RPC: its (already truncated) text and the
// channel its vector-or-error is delivered on. reply is buffered (cap 1) so
// the collector never blocks on a waiter that gave up.
type pendingEmbed struct {
	text  string
	reply chan embedResult
}

type embedResult struct {
	vec []float32
	err error
}

type server struct {
	embedv1.UnimplementedEmbedderServer
	httpc *http.Client
	url   string // full .../embeddings URL
	model string
	queue chan pendingEmbed

	// Batch stats, logged periodically (not per batch).
	statBatches atomic.Int64
	statItems   atomic.Int64
}

type embeddingsRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingsResponse struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

func (s *server) Embed(ctx context.Context, req *embedv1.EmbedRequest) (*embedv1.EmbedResponse, error) {
	text := req.GetText()
	if len(text) > maxEmbedChars {
		cut := maxEmbedChars
		for cut > 0 && !utf8.RuneStart(text[cut]) { // don't split a rune
			cut--
		}
		text = text[:cut]
	}

	p := pendingEmbed{text: text, reply: make(chan embedResult, 1)}
	select {
	case s.queue <- p:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}

	select {
	case res := <-p.reply:
		if res.err != nil {
			return nil, res.err
		}
		return &embedv1.EmbedResponse{Vector: res.vec}, nil
	case <-ctx.Done():
		// The collector still embeds this text as part of its batch; the
		// buffered reply channel means delivery to a gone waiter never blocks.
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// collect is the single collector goroutine: it gathers pending items into
// batches (flush at batchMax items or batchWindow after the first item) and
// flushes them synchronously — new arrivals queue up during the upstream call,
// which naturally forms the next batch.
func (s *server) collect() {
	timer := time.NewTimer(batchWindow)
	if !timer.Stop() {
		<-timer.C
	}
	for {
		first := <-s.queue // block until a batch starts; never flush empty
		batch := []pendingEmbed{first}
		timer.Reset(batchWindow)
	gather:
		for len(batch) < batchMax {
			select {
			case p := <-s.queue:
				batch = append(batch, p)
			case <-timer.C:
				break gather
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		s.flush(batch)
	}
}

// tooLargeError marks an upstream HTTP 500 whose body carries the llama.cpp
// per-input token-overflow message (e.g. `input (545 tokens) is too large to
// process. increase the physical batch size`). It never reaches a waiter:
// flush intercepts it and falls back to per-item embedding.
type tooLargeError struct{ body string }

func (e *tooLargeError) Error() string {
	return "upstream HTTP 500 (input too large): " + e.body
}

// flush embeds the whole batch in one upstream call and delivers a per-item
// result to every waiter. A "too large" 500 means ONE oversize item poisoned
// the whole array-input POST; failing all waiters would make the innocent
// items (and the guilty one) churn through UNAVAILABLE retries forever, so
// instead each item is retried individually to isolate the offender.
func (s *server) flush(batch []pendingEmbed) {
	vecs, err := s.embedBatch(batch)
	var tooLarge *tooLargeError
	switch {
	case err == nil:
		for i, p := range batch {
			p.reply <- embedResult{vec: vecs[i]}
		}
	case errors.As(err, &tooLarge):
		s.flushIndividually(batch)
	default:
		for _, p := range batch {
			p.reply <- embedResult{err: err}
		}
	}
	s.statBatches.Add(1)
	s.statItems.Add(int64(len(batch)))
}

// flushIndividually retries each item of a "too large"-failed batch as its own
// single-item upstream POST (sequential; this is the rare path). An item that
// is too large even on its own gets a permanent codes.Internal error so the
// enrichment path dead-letters it instead of retrying forever; other items
// succeed or fail with the existing error mapping.
func (s *server) flushIndividually(batch []pendingEmbed) {
	for _, p := range batch {
		vecs, err := s.embedBatch([]pendingEmbed{p})
		var tooLarge *tooLargeError
		switch {
		case err == nil:
			p.reply <- embedResult{vec: vecs[0]}
		case errors.As(err, &tooLarge):
			p.reply <- embedResult{err: status.Error(codes.Internal,
				"text exceeds embedder token limit even after truncation")}
		default:
			p.reply <- embedResult{err: err}
		}
	}
}

// embedBatch performs one array-input /embeddings POST. On success it returns
// one 768-dim L2-normalized vector per batch item, index-aligned.
func (s *server) embedBatch(batch []pendingEmbed) ([][]float32, error) {
	texts := make([]string, len(batch))
	for i, p := range batch {
		texts[i] = p.text
	}
	body, err := json.Marshal(embeddingsRequest{Model: s.model, Input: texts})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal request: %v", err)
	}

	// Deliberately not tied to any single waiter's RPC context: the batch
	// serves many callers.
	ctx, cancel := context.WithTimeout(context.Background(), upstreamTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpc.Do(httpReq)
	if err != nil {
		// Connection refused, DNS failure, timeout, ... — all transient.
		return nil, status.Errorf(codes.Unavailable, "upstream request failed: %v", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read upstream response: %v", err)
	}

	if resp.StatusCode >= 500 {
		// llama.cpp rejects a batch containing any input beyond its 512-token
		// cap with a 500 mentioning "too large"; that is a per-input problem,
		// not a transient outage — flag it so flush can isolate the offender.
		if resp.StatusCode == http.StatusInternalServerError && bytes.Contains(bytes.ToLower(raw), []byte("too large")) {
			return nil, &tooLargeError{body: truncate(raw, 200)}
		}
		return nil, status.Errorf(codes.Unavailable, "upstream HTTP %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Internal, "upstream HTTP %d: %s", resp.StatusCode, truncate(raw, 200))
	}

	var parsed embeddingsResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, status.Errorf(codes.Internal, "malformed upstream response: %v", err)
	}
	if len(parsed.Data) != len(batch) {
		return nil, status.Errorf(codes.Internal, "upstream returned %d embeddings, want %d", len(parsed.Data), len(batch))
	}

	vecs := make([][]float32, len(batch))
	for i, d := range parsed.Data {
		idx := d.Index
		if idx < 0 || idx >= len(batch) || vecs[idx] != nil {
			return nil, status.Errorf(codes.Internal, "upstream returned bad/duplicate index %d (item %d of %d)", idx, i, len(batch))
		}
		if len(d.Embedding) != dims {
			return nil, status.Errorf(codes.Internal, "upstream returned %d dims, want %d", len(d.Embedding), dims)
		}
		l2Normalize(d.Embedding) // defensive; idempotent on already-normalized vectors
		vecs[idx] = d.Embedding
	}
	return vecs, nil
}

// logStats periodically logs batch throughput since the previous tick; silent
// when idle.
func (s *server) logStats() {
	tick := time.NewTicker(statsEvery)
	var lastBatches, lastItems int64
	for range tick.C {
		b, it := s.statBatches.Load(), s.statItems.Load()
		db, di := b-lastBatches, it-lastItems
		lastBatches, lastItems = b, it
		if db == 0 {
			continue
		}
		log.Printf("batch stats: %d batches, %d items, avg %.1f items/batch (last %s)",
			db, di, float64(di)/float64(db), statsEvery)
	}
}

// l2Normalize scales v to unit length in place. A zero vector is left as-is.
func l2Normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return
	}
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
}

func truncate(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	baseURL := strings.TrimRight(envStr("OPENAI_BASE_URL", "http://model-runner.docker.internal/engines/v1"), "/")
	model := envStr("EMBED_MODEL", "ai/nomic-embed-text-v1.5")

	s := &server{
		httpc: &http.Client{
			Timeout: upstreamTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		url:   fmt.Sprintf("%s/embeddings", baseURL),
		model: model,
		queue: make(chan pendingEmbed, queueDepth),
	}
	go s.collect()
	go s.logStats()

	lis, err := net.Listen("tcp", ":9100")
	if err != nil {
		log.Fatal(err)
	}
	// Clients ping every 30s (Time:30s, PermitWithoutStream:true); the gRPC
	// server default enforcement MinTime of 5min would reply with GoAway
	// ENHANCE_YOUR_CALM too_many_pings. Allow pings at that cadence.
	g := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             20 * time.Second,
		PermitWithoutStream: true,
	}))
	embedv1.RegisterEmbedderServer(g, s)
	log.Printf("nomic bridge on :9100 (upstream=%s model=%s, batching max=%d window=%s)",
		s.url, s.model, batchMax, batchWindow)
	log.Fatal(g.Serve(lis))
}
