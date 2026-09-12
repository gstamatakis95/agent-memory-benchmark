package snapshot

// Query-path coverage: BM25 sanity, brute-force cosine against a naive
// reference, RRF, the three search modes, recency boost and abstention.

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"example.com/agentmem/internal/embed"
)

func TestLexicalBM25Sanity(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		// Equal length (3 tokens each): doc101 has "cat" tf=2, doc102 tf=1.
		{Doc: Doc{MemoryID: 101}, Lexemes: []string{"cat", "cat", "dog"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 102}, Lexemes: []string{"cat", "dog", "dog"}, Vec: embed.L2Normalize([]float32{0, 1})},
		// df control, all length 3, tf=1: rareterm df=1, commonterm df=3.
		{Doc: Doc{MemoryID: 201}, Lexemes: []string{"rareterm", "x0", "y0"}, Vec: embed.L2Normalize([]float32{1, 1})},
		{Doc: Doc{MemoryID: 202}, Lexemes: []string{"commonterm", "x1", "y1"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 203}, Lexemes: []string{"commonterm", "x2", "y2"}, Vec: embed.L2Normalize([]float32{0, 1})},
		{Doc: Doc{MemoryID: 204}, Lexemes: []string{"commonterm", "x3", "y3"}, Vec: embed.L2Normalize([]float32{1, 1})},
		// Identical content under different ids, for the tiebreak check.
		{Doc: Doc{MemoryID: 5}, Lexemes: []string{"tieterm"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 3}, Lexemes: []string{"tieterm"}, Vec: embed.L2Normalize([]float32{0, 1})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 2}, rows)
	s := mustOpen(t, dir)
	ctx := context.Background()

	// Higher tf outscores at equal document length.
	hits, err := s.LexicalTopN(ctx, []string{"cat"}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 2 || hits[0].MemoryID != 101 {
		t.Fatalf("tf ordering: got %+v", hits)
	}
	if hits[0].Score <= hits[1].Score {
		t.Fatalf("expected doc101 (tf=2) to outscore doc102 (tf=1): %+v", hits)
	}

	// A rarer term outscores a common one at matched tf=1 and doc length.
	rare, err := s.LexicalTopN(ctx, []string{"rareterm"}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	common, err := s.LexicalTopN(ctx, []string{"commonterm"}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rare) != 1 || len(common) != 1 {
		t.Fatalf("expected one hit each: rare=%+v common=%+v", rare, common)
	}
	if rare[0].Score <= common[0].Score {
		t.Fatalf("expected the rare term to outscore the common one: rare=%v common=%v", rare[0].Score, common[0].Score)
	}

	// An unknown term matches nothing.
	none, err := s.LexicalTopN(ctx, []string{"nosuchterm"}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no hits for an unknown term, got %+v", none)
	}

	// n truncates.
	limited, err := s.LexicalTopN(ctx, []string{"commonterm"}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Fatalf("n=2 should truncate to 2 hits, got %d", len(limited))
	}

	// Deterministic tiebreak: a genuine score tie orders ascending by id.
	tie, err := s.LexicalTopN(ctx, []string{"tieterm"}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tie) != 2 || tie[0].Score != tie[1].Score {
		t.Fatalf("expected a genuine score tie: %+v", tie)
	}
	if tie[0].MemoryID != 3 || tie[1].MemoryID != 5 {
		t.Fatalf("tie should break ascending by memory id: got %+v", tie)
	}

	// Empty lexemes is a no-op, not "match everything".
	if got, err := s.LexicalTopN(ctx, nil, 10, nil); err != nil || got != nil {
		t.Fatalf("LexicalTopN(nil) = %+v, %v, want nil, nil", got, err)
	}
}

func hitsEqual(a, b []Hit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Ordinal != b[i].Ordinal || a[i].MemoryID != b[i].MemoryID || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}

func TestVectorTopNMatchesNaive(t *testing.T) {
	const dim = 8
	const n = 500
	const topN = 20
	rnd := rand.New(rand.NewSource(42))
	dir := t.TempDir()

	rows := make([]row, n)
	convOf := make(map[int64]string, n)
	for i := range n {
		id := int64(1000 + i)
		conv := "A"
		if i%2 == 1 {
			conv = "B"
		}
		convOf[id] = conv
		rows[i] = row{
			Doc:     Doc{MemoryID: id, ConversationID: conv, ContentHash: []byte{byte(i)}},
			Lexemes: []string{"x"},
			Vec:     randVec(rnd, dim),
		}
	}
	buildSnapshot(t, dir, Manifest{VectorDim: dim}, rows)
	s := mustOpen(t, dir)
	ctx := context.Background()

	q := randVec(rnd, dim)

	// naive is a single-threaded reference implementation using the exact
	// same accumulation order as vectorScanRange, so scores compare equal,
	// not just approximately equal.
	naive := func(f *Filter) []Hit {
		var out []Hit
		for i := range n {
			if f != nil && !f.Ordinals.Contains(uint32(i)) {
				continue
			}
			vec := rows[i].Vec
			var dot float32
			for j, v := range vec {
				dot += v * q[j]
			}
			out = append(out, Hit{Ordinal: i, MemoryID: rows[i].Doc.MemoryID, Score: float64(dot)})
		}
		sort.Slice(out, func(a, b int) bool { return hitBetter(out[a], out[b]) })
		if len(out) > topN {
			out = out[:topN]
		}
		return out
	}

	want := naive(nil)
	got, err := s.VectorTopN(ctx, q, topN, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hitsEqual(got, want) {
		t.Fatalf("unfiltered VectorTopN mismatch:\ngot  %+v\nwant %+v", got, want)
	}

	f := s.ConversationFilter("A")
	wantF := naive(f)
	gotF, err := s.VectorTopN(ctx, q, topN, f)
	if err != nil {
		t.Fatal(err)
	}
	if !hitsEqual(gotF, wantF) {
		t.Fatalf("filtered VectorTopN mismatch:\ngot  %+v\nwant %+v", gotF, wantF)
	}
	for _, h := range gotF {
		if convOf[h.MemoryID] != "A" {
			t.Fatalf("filtered result returned a row outside the filter: %+v", h)
		}
	}

	empty := &Filter{ConversationID: "nope", Ordinals: roaring.New()}
	noneHits, err := s.VectorTopN(ctx, q, topN, empty)
	if err != nil {
		t.Fatal(err)
	}
	if len(noneHits) != 0 {
		t.Fatalf("expected no hits for an empty-bitmap filter, got %+v", noneHits)
	}

	if _, err := s.VectorTopN(ctx, q[:dim-1], topN, nil); err == nil {
		t.Fatal("VectorTopN with a wrong-dim query vector should error")
	}
}

func TestFilterAppliesToBothArms(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{Doc: Doc{MemoryID: 1, ConversationID: "conv1"}, Lexemes: []string{"alpha"}, Vec: embed.L2Normalize([]float32{1, 0, 0, 0})},
		{Doc: Doc{MemoryID: 2, ConversationID: "conv1"}, Lexemes: []string{"alpha", "beta"}, Vec: embed.L2Normalize([]float32{1, 1, 0, 0})},
		{Doc: Doc{MemoryID: 3, ConversationID: "conv2"}, Lexemes: []string{"alpha"}, Vec: embed.L2Normalize([]float32{1, 0, 0, 0})},
		{Doc: Doc{MemoryID: 4, ConversationID: "conv2"}, Lexemes: []string{"alpha", "beta"}, Vec: embed.L2Normalize([]float32{1, 1, 0, 0})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 4}, rows)
	s := mustOpen(t, dir)
	ctx := context.Background()

	f := s.ConversationFilter("conv1")
	res, err := s.Search(ctx, []string{"alpha", "beta"}, embed.L2Normalize([]float32{1, 1, 0, 0}), f, SearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) == 0 {
		t.Fatal("expected hits")
	}
	for _, h := range res.Hits {
		if h.MemoryID != 1 && h.MemoryID != 2 {
			t.Fatalf("hybrid search with a conv1 filter returned a conv2 row: %+v", h)
		}
	}
}

// TestRRFWorkedExample mirrors the docs/06-testing.md Tier 0 golden example
// (also covered for the string-keyed retrieve.RRF in
// internal/retrieve/rrf_test.go): k=60, a doc at rank 3 in one list and
// rank 7 in the other scores 1/63 + 1/67. This package's RRF is keyed by
// ordinal instead of a string id.
func TestRRFWorkedExample(t *testing.T) {
	bm25 := []Hit{{Ordinal: 0}, {Ordinal: 1}, {Ordinal: 2}, {Ordinal: 3}, {Ordinal: 4}, {Ordinal: 5}, {Ordinal: 6}}        // ordinal 2 is rank 3
	dense := []Hit{{Ordinal: 10}, {Ordinal: 11}, {Ordinal: 12}, {Ordinal: 13}, {Ordinal: 14}, {Ordinal: 15}, {Ordinal: 2}} // ordinal 2 is rank 7

	got := RRF(60, bm25, dense)
	want := 1.0/63.0 + 1.0/67.0
	if math.Abs(got[2]-want) > 1e-9 {
		t.Fatalf("RRF(2) = %v, want %v", got[2], want)
	}
	if got[0] != 1.0/61.0 {
		t.Fatalf("rank-1-only ordinal wrong: %v", got[0])
	}
	if _, ok := got[999]; ok {
		t.Fatal("an ordinal absent from every list must not appear in the fused map")
	}
}

func TestSearchModes(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{Doc: Doc{MemoryID: 1}, Lexemes: []string{"alpha", "alpha", "beta"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 2}, Lexemes: []string{"beta"}, Vec: embed.L2Normalize([]float32{0, 1})},
		{Doc: Doc{MemoryID: 3}, Lexemes: []string{"gamma"}, Vec: embed.L2Normalize([]float32{-1, 0})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 2}, rows)
	s := mustOpen(t, dir)
	ctx := context.Background()
	lexemes := []string{"alpha", "beta"}
	qvec := embed.L2Normalize([]float32{1, 0})

	// bm25 mode never needs qvec.
	res, err := s.Search(ctx, lexemes, nil, nil, SearchOptions{Mode: ModeBM25})
	if err != nil {
		t.Fatalf("bm25 mode with nil qvec: %v", err)
	}
	if len(res.Hits) == 0 {
		t.Fatal("expected bm25 hits")
	}

	// dense mode with a nil qvec errors.
	if _, err := s.Search(ctx, lexemes, nil, nil, SearchOptions{Mode: ModeDense}); err == nil {
		t.Fatal("dense mode with a nil qvec should error")
	}

	// hybrid with a nil qvec degrades to lexical-only.
	res, err = s.Search(ctx, lexemes, nil, nil, SearchOptions{Mode: ModeHybrid})
	if err != nil {
		t.Fatalf("hybrid with nil qvec: %v", err)
	}
	if !res.LexicalOnly {
		t.Fatal("hybrid with a nil qvec should set LexicalOnly")
	}

	// The doc that both arms rank first wins hybrid.
	res, err = s.Search(ctx, lexemes, qvec, nil, SearchOptions{Mode: ModeHybrid})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) == 0 || res.Hits[0].MemoryID != 1 {
		t.Fatalf("expected doc 1 (top of both arms) to win hybrid: %+v", res.Hits)
	}
	if res.LexicalOnly {
		t.Fatal("hybrid with a qvec present should not be LexicalOnly")
	}
}

func TestRecencyBoost(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	oldTS := now.Add(-100 * 24 * time.Hour).Unix()
	rows := []row{
		{Doc: Doc{MemoryID: 1, TSUnix: oldTS}, Lexemes: []string{"term"}, Vec: embed.L2Normalize([]float32{1, 0})},
		{Doc: Doc{MemoryID: 2, TSUnix: now.Unix()}, Lexemes: []string{"term"}, Vec: embed.L2Normalize([]float32{1, 0})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 2}, rows)
	s := mustOpen(t, dir)
	ctx := context.Background()

	base, err := s.LexicalTopN(ctx, []string{"term"}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(base) != 2 || base[0].Score != base[1].Score {
		t.Fatalf("expected identical base scores (same term, tf and length): %+v", base)
	}
	baseScore := base[0].Score

	// RecencyBoost 0 (default off): the ascending-id tiebreak decides.
	res, err := s.Search(ctx, []string{"term"}, nil, nil, SearchOptions{Mode: ModeBM25, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if res.Hits[0].MemoryID != 1 {
		t.Fatalf("with no recency boost, ascending id should win the tie: %+v", res.Hits)
	}

	// RecencyBoost > 0: the recent row ranks first, with the exact boosted
	// score docs/01-retrieval.md's formula predicts.
	res, err = s.Search(ctx, []string{"term"}, nil, nil, SearchOptions{
		Mode: ModeBM25, RecencyBoost: 1, HalfLifeDays: 30, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Hits[0].MemoryID != 2 {
		t.Fatalf("recency boost should rank the recent row first: %+v", res.Hits)
	}

	wantOld := baseScore * (1 + math.Exp(-math.Ln2/30*100))
	wantRecent := baseScore * (1 + math.Exp(-math.Ln2/30*0))
	for _, h := range res.Hits {
		switch h.MemoryID {
		case 1:
			if math.Abs(h.Score-wantOld) > 1e-9 {
				t.Fatalf("old row boosted score = %v, want %v", h.Score, wantOld)
			}
		case 2:
			if math.Abs(h.Score-wantRecent) > 1e-9 {
				t.Fatalf("recent row boosted score = %v, want %v", h.Score, wantRecent)
			}
		}
	}
}

func TestAbstention(t *testing.T) {
	dir := t.TempDir()
	rows := []row{
		{Doc: Doc{MemoryID: 1}, Lexemes: []string{"term"}, Vec: embed.L2Normalize([]float32{1, 0})},
	}
	buildSnapshot(t, dir, Manifest{VectorDim: 2}, rows)
	s := mustOpen(t, dir)
	ctx := context.Background()

	base, err := s.Search(ctx, []string{"term"}, nil, nil, SearchOptions{Mode: ModeBM25})
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Hits) == 0 {
		t.Fatal("expected a hit")
	}
	topScore := base.Hits[0].Score

	res, err := s.Search(ctx, []string{"term"}, nil, nil, SearchOptions{Mode: ModeBM25, AbstainThreshold: topScore + 1})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Abstained {
		t.Fatal("expected Abstained=true when the threshold exceeds the top score")
	}
	if len(res.Hits) == 0 {
		t.Fatal("hits should still be returned when abstaining")
	}

	res, err = s.Search(ctx, []string{"term"}, nil, nil, SearchOptions{Mode: ModeBM25, AbstainThreshold: 0})
	if err != nil {
		t.Fatal(err)
	}
	if res.Abstained {
		t.Fatal("AbstainThreshold 0 should never abstain")
	}

	res, err = s.Search(ctx, []string{"nomatch"}, nil, nil, SearchOptions{Mode: ModeBM25, AbstainThreshold: 0.1})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Abstained {
		t.Fatal("expected Abstained=true with zero hits and a positive threshold")
	}
	if len(res.Hits) != 0 {
		t.Fatalf("expected zero hits for an unmatched term, got %+v", res.Hits)
	}
}
