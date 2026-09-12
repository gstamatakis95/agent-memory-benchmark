package main

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	agentmemv1 "example.com/agentmem/genproto/agentmem/v1"
	"example.com/agentmem/internal/eval"
)

// fakeSearchClient answers Search from a canned hit list per query and
// embeds the (nil) generated interface so only Search is implemented.
type fakeSearchClient struct {
	agentmemv1.MemoryServiceClient
	hits map[string][]*agentmemv1.SearchHit // question text -> hits
	reqs []*agentmemv1.SearchReq
}

func (f *fakeSearchClient) Search(_ context.Context, req *agentmemv1.SearchReq, _ ...grpc.CallOption) (*agentmemv1.SearchResp, error) {
	f.reqs = append(f.reqs, req)
	return &agentmemv1.SearchResp{Hits: f.hits[req.GetQuery()], SnapshotVersion: 42}, nil
}

// hashByTurn inverts localTwins: dataset turn/round id -> content hash bytes.
func hashByTurn(t *testing.T, c conversation, roundItems []item) map[string][]byte {
	t.Helper()
	byHash, err := localTwins(c, roundItems)
	require.NoError(t, err)
	out := make(map[string][]byte, len(byHash))
	for h, it := range byHash {
		b, err := hex.DecodeString(h)
		require.NoError(t, err)
		out[it.TurnID] = b
	}
	return out
}

// The snapshot eval path must map SearchHit.content_hash back to dataset
// turn ids exactly like the in-process path maps FetchAllMemories rows, so
// a server that returns every gold turn (and a round twin containing it)
// scores Recall@5 == 1.0 through the same fixtures gate — and a hit whose
// hash has no local twin is counted, not scored.
func TestRunSnapshotEvalMapsHitsToTurnIDs(t *testing.T) {
	convs, err := loadFixtures("../../testdata/fixtures.json")
	require.NoError(t, err)
	c := convs[0]
	roundItems, expand := roundItemsOf(c)
	hashOf := hashByTurn(t, c, roundItems)

	// For each question: one foreign hit first (no local twin), then a
	// round twin that contains the first gold turn (if any), then the
	// gold turns themselves.
	roundOf := make(map[string]string)
	for rid, members := range expand {
		for _, m := range members {
			roundOf[m] = rid
		}
	}
	hits := make(map[string][]*agentmemv1.SearchHit)
	for _, q := range c.Questions {
		var list []*agentmemv1.SearchHit
		list = append(list, &agentmemv1.SearchHit{MemoryId: 999999, Score: 9, ContentHash: []byte("no-such-blob")})
		if rid, ok := roundOf[q.Evidence[0]]; ok {
			list = append(list, &agentmemv1.SearchHit{MemoryId: 500, Score: 8, ContentHash: hashOf[rid]})
		}
		for i, ev := range q.Evidence {
			list = append(list, &agentmemv1.SearchHit{MemoryId: int64(i + 1), Score: 7 - float64(i), ContentHash: hashOf[ev]})
		}
		hits[q.Question] = list
	}

	for _, gran := range []string{"all", "turn", "round"} {
		fc := &fakeSearchClient{hits: hits}
		tun := evalDefaults("fixtures")
		tun.granularity = gran
		results, fetched, mapped, total, skipped, ver, err := runSnapshotEval(
			context.Background(), fc, "hybrid", tun, convs)
		require.NoError(t, err, gran)
		require.Equal(t, len(c.Questions), total)
		require.Zero(t, skipped)
		require.EqualValues(t, 42, ver)
		require.Greater(t, fetched, mapped, "the foreign hit must be fetched but not mapped")
		rep := eval.Evaluate(results, []int{5, 10})
		require.InDelta(t, 1.0, rep.Overall.Recall[5], 1e-9, "granularity %s", gran)

		// Every request is scoped to the conversation and over-fetches.
		for _, r := range fc.reqs {
			require.Equal(t, c.Num, r.GetConversationId())
			require.GreaterOrEqual(t, r.GetTopK(), int32(40))
			require.Equal(t, "hybrid", r.GetMode())
		}
	}
}

// keepAtGranularity must match filterGranularity's semantics row by row.
func TestKeepAtGranularity(t *testing.T) {
	expand := map[string][]string{"t1+t2": {"t1", "t2"}}
	covered := coveredTurns(expand)
	cases := []struct {
		id, gran string
		keep     bool
	}{
		{"t1", "all", true}, {"t1+t2", "all", true},
		{"t1", "turn", true}, {"t1+t2", "turn", false},
		{"t1", "round", false}, {"t3", "round", true}, {"t1+t2", "round", true},
	}
	for _, tc := range cases {
		require.Equal(t, tc.keep, keepAtGranularity(tc.id, expand, covered, tc.gran), "%s@%s", tc.id, tc.gran)
	}
}
