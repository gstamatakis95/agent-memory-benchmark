package enrich

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"

	"example.com/agentmem/internal/embed"
	"example.com/agentmem/internal/store"
)

// ---- Tier 1 fakes: pure in-memory, no Postgres, no testcontainers ---------

type fakeStore struct {
	mu      sync.Mutex
	pending []int64
	done    []store.DoneEvent
	failed  []store.FailedEvent
}

func (f *fakeStore) CountPending(_ context.Context, _ int16) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.pending)), nil
}

func (f *fakeStore) PendingIDs(_ context.Context, _ int16, limit int) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit > len(f.pending) {
		limit = len(f.pending)
	}
	out := make([]int64, limit)
	copy(out, f.pending[:limit])
	return out, nil
}

func (f *fakeStore) InsertDone(_ context.Context, e store.DoneEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Mirror the uq_enrich_success ON CONFLICT DO NOTHING arbiter.
	for _, d := range f.done {
		if d.MemoryID == e.MemoryID && d.Version == e.Version {
			return nil
		}
	}
	f.done = append(f.done, e)
	return nil
}

func (f *fakeStore) InsertFailed(_ context.Context, e store.FailedEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, e) // failures are never deduped
	return nil
}

func (f *fakeStore) failedFor(id int64) []store.FailedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.FailedEvent
	for _, e := range f.failed {
		if e.MemoryID == id {
			out = append(out, e)
		}
	}
	return out
}

type fakeSource struct{}

func (fakeSource) FetchTexts(_ context.Context, ids []int64) ([]MemoryText, error) {
	out := make([]MemoryText, len(ids))
	for i, id := range ids {
		out[i] = MemoryText{MemoryID: id, Text: textFor(id), Lexemes: []string{"tok"}}
	}
	return out, nil
}

func textFor(id int64) string { return fmt.Sprintf("memory text %d", id) }

// fakeEmbedder records every raw text it is asked to embed and can be
// programmed to fail permanently or transiently for specific texts.
type fakeEmbedder struct {
	mu        sync.Mutex
	calls     []string
	permanent map[string]bool
	transient map[string]bool
}

func (f *fakeEmbedder) EmbedDocument(_ context.Context, raw string) ([]float32, error) {
	f.mu.Lock()
	f.calls = append(f.calls, raw)
	f.mu.Unlock()
	if f.permanent[raw] {
		return nil, embed.Permanentf("embedder returned 512 dims, want %d", embed.Dims)
	}
	if f.transient[raw] {
		return nil, errors.New("UNAVAILABLE: embedder briefly down")
	}
	return []float32{1, 0, 0}, nil
}

func (f *fakeEmbedder) embedded(raw string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == raw {
			return true
		}
	}
	return false
}

// ---- Activity tests (NewTestActivityEnvironment) --------------------------

type ActivitySuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestActivitySuite(t *testing.T) { suite.Run(t, new(ActivitySuite)) }

func (s *ActivitySuite) newEnv(a *Activities) *testsuite.TestActivityEnvironment {
	env := s.NewTestActivityEnvironment()
	env.RegisterActivity(a)
	return env
}

// Heartbeat resume: with heartbeat details = 2, the first two items of the
// batch must be skipped (never re-fetched, never re-embedded) and only the
// remaining tail processed.
func (s *ActivitySuite) TestProcessBatchHeartbeatResume() {
	fs := &fakeStore{}
	fe := &fakeEmbedder{}
	a := &Activities{Store: fs, Source: fakeSource{}, Embedder: fe, Version: 1}
	env := s.newEnv(a)

	env.SetHeartbeatDetails(2) // items 0 and 1 already terminal before the "crash"

	val, err := env.ExecuteActivity(a.ProcessBatch, BatchInput{Version: 1, IDs: []int64{10, 11, 12, 13}})
	s.Require().NoError(err)
	var res BatchResult
	s.Require().NoError(val.Get(&res))

	s.Equal(BatchResult{Done: 2}, res, "only the unprocessed tail is embedded")
	s.False(fe.embedded(textFor(10)), "already-processed item 0 must be skipped")
	s.False(fe.embedded(textFor(11)), "already-processed item 1 must be skipped")
	s.True(fe.embedded(textFor(12)))
	s.True(fe.embedded(textFor(13)))
	s.Len(fs.done, 2)
}

// Permanent embed errors (wrong dims etc.) dead-letter the item: a failed
// event with permanent=true, no activity error, and the rest of the batch
// still succeeds.
func (s *ActivitySuite) TestProcessBatchPermanentDeadLetter() {
	fs := &fakeStore{}
	fe := &fakeEmbedder{permanent: map[string]bool{textFor(21): true}}
	a := &Activities{Store: fs, Source: fakeSource{}, Embedder: fe, Version: 1}
	env := s.newEnv(a)

	val, err := env.ExecuteActivity(a.ProcessBatch, BatchInput{Version: 1, IDs: []int64{20, 21, 22}})
	s.Require().NoError(err, "a permanent item must not fail the batch")
	var res BatchResult
	s.Require().NoError(val.Get(&res))

	s.Equal(BatchResult{Done: 2, Permanent: 1}, res)
	events := fs.failedFor(21)
	s.Require().Len(events, 1, "exactly one dead-letter append, no per-item retry")
	s.True(events[0].Permanent, "wrong-dims must be classified permanent=true")
	s.Contains(events[0].ErrorMessage, "512 dims")
	s.Len(fs.done, 2, "healthy items still complete")
}

// A transient failure among successes appends failed(permanent=false) and
// does NOT fail the batch.
func (s *ActivitySuite) TestProcessBatchTransientDoesNotFailBatch() {
	fs := &fakeStore{}
	fe := &fakeEmbedder{transient: map[string]bool{textFor(31): true}}
	a := &Activities{Store: fs, Source: fakeSource{}, Embedder: fe, Version: 1}
	env := s.newEnv(a)

	val, err := env.ExecuteActivity(a.ProcessBatch, BatchInput{Version: 1, IDs: []int64{30, 31, 32}})
	s.Require().NoError(err)
	var res BatchResult
	s.Require().NoError(val.Get(&res))

	s.Equal(BatchResult{Done: 2, Failed: 1}, res)
	events := fs.failedFor(31)
	s.Require().Len(events, 1)
	s.False(events[0].Permanent, "transient errors must stay retryable (permanent=false)")
}

// If every retryable item fails transiently, the activity itself fails so
// Temporal retries the batch with backoff.
func (s *ActivitySuite) TestProcessBatchAllTransientFailsActivity() {
	fs := &fakeStore{}
	fe := &fakeEmbedder{transient: map[string]bool{textFor(40): true, textFor(41): true}}
	a := &Activities{Store: fs, Source: fakeSource{}, Embedder: fe, Version: 1}
	env := s.newEnv(a)

	_, err := env.ExecuteActivity(a.ProcessBatch, BatchInput{Version: 1, IDs: []int64{40, 41}})
	s.Error(err, "an entirely-failed batch must surface an error for Temporal to retry")
	s.Len(fs.failedFor(40), 1)
	s.Len(fs.failedFor(41), 1)
	s.Empty(fs.done)
}

// CountBacklog and PlanRanges: deterministic paging of the ordered pending
// snapshot into disjoint contiguous pages.
func (s *ActivitySuite) TestCountBacklogAndPlanRanges() {
	fs := &fakeStore{}
	for id := int64(1); id <= 300; id++ {
		fs.pending = append(fs.pending, id)
	}
	a := &Activities{Store: fs, Source: fakeSource{}, Embedder: &fakeEmbedder{}, Version: 1}
	env := s.newEnv(a)

	val, err := env.ExecuteActivity(a.CountBacklog, int16(1))
	s.Require().NoError(err)
	var n int
	s.Require().NoError(val.Get(&n))
	s.Equal(300, n)

	val, err = env.ExecuteActivity(a.PlanRanges, int16(1), 128, 8)
	s.Require().NoError(err)
	var batches []BatchInput
	s.Require().NoError(val.Get(&batches))

	s.Require().Len(batches, 3) // 128 + 128 + 44
	s.Len(batches[0].IDs, 128)
	s.Len(batches[1].IDs, 128)
	s.Len(batches[2].IDs, 44)
	s.Equal(int64(1), batches[0].IDs[0])
	s.Equal(int64(129), batches[1].IDs[0])
	s.Equal(int64(300), batches[2].IDs[43])
	seen := map[int64]bool{}
	for _, b := range batches {
		s.Equal(int16(1), b.Version)
		for _, id := range b.IDs {
			require.False(s.T(), seen[id], "pages must be disjoint")
			seen[id] = true
		}
	}
}
