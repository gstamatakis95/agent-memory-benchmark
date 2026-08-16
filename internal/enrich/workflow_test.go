package enrich

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// Tier 1 (docs/06-testing.md): in-memory Temporal test environment only — no
// server, no Docker.
type SweepSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
	a   *Activities
}

func (s *SweepSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.a = &Activities{}
	s.env.RegisterWorkflow(EnrichmentSweepWorkflow)
	s.env.RegisterActivity(s.a)
}

func (s *SweepSuite) AfterTest(_, _ string) { s.env.AssertExpectations(s.T()) }

func idRange(from, to int64) []int64 {
	ids := make([]int64, 0, to-from+1)
	for id := from; id <= to; id++ {
		ids = append(ids, id)
	}
	return ids
}

// TestSweepDrainsThenExits: one wave drains the backlog; the workflow
// completes without error and ProcessBatch ran exactly once per planned page.
func (s *SweepSuite) TestSweepDrainsThenExits() {
	calls := 0
	s.env.OnActivity(s.a.CountBacklog, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, v int16) (int, error) {
			calls++
			if calls == 1 {
				return 256, nil
			}
			return 0, nil // drained on second poll
		})
	s.env.OnActivity(s.a.PlanRanges, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]BatchInput{
			{Version: 1, IDs: idRange(1, 128)},
			{Version: 1, IDs: idRange(129, 256)},
		}, nil).Once()
	s.env.OnActivity(s.a.ProcessBatch, mock.Anything, mock.Anything).
		Return(BatchResult{Done: 128}, nil).Twice()

	s.env.ExecuteWorkflow(EnrichmentSweepWorkflow, SweepInput{Version: 1})
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	s.Equal(2, calls, "CountBacklog should be polled once per wave plus the drained check")
}

// TestSoftDeadlineExit: the backlog never drains; the workflow must still
// complete via the soft deadline measured in workflow time (the test env
// auto-skips the inter-wave timers, advancing workflow.Now each wave).
func (s *SweepSuite) TestSoftDeadlineExit() {
	s.env.OnActivity(s.a.CountBacklog, mock.Anything, mock.Anything).
		Return(1_000_000, nil) // never drains
	s.env.OnActivity(s.a.PlanRanges, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]BatchInput{{Version: 1, IDs: idRange(1, 128)}}, nil)
	s.env.OnActivity(s.a.ProcessBatch, mock.Anything, mock.Anything).
		Return(BatchResult{Done: 128}, nil)

	s.env.ExecuteWorkflow(EnrichmentSweepWorkflow, SweepInput{Version: 1})
	s.True(s.env.IsWorkflowCompleted(), "must exit on the soft deadline, not hang")
	s.NoError(s.env.GetWorkflowError())
}

// TestSweepExitsWhenNothingClaimable: a nonzero count but an empty plan
// (rows all inside their backoff window) exits cleanly instead of spinning.
func (s *SweepSuite) TestSweepExitsWhenNothingClaimable() {
	s.env.OnActivity(s.a.CountBacklog, mock.Anything, mock.Anything).Return(42, nil).Once()
	s.env.OnActivity(s.a.PlanRanges, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]BatchInput{}, nil).Once()

	s.env.ExecuteWorkflow(EnrichmentSweepWorkflow, SweepInput{Version: 1})
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

// TestSweepToleratesFailedBatch: one ProcessBatch failing (rows stay pending)
// must not fail the sweep.
func (s *SweepSuite) TestSweepToleratesFailedBatch() {
	calls := 0
	s.env.OnActivity(s.a.CountBacklog, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, v int16) (int, error) {
			calls++
			if calls == 1 {
				return 128, nil
			}
			return 0, nil
		})
	s.env.OnActivity(s.a.PlanRanges, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]BatchInput{{Version: 1, IDs: idRange(1, 128)}}, nil).Once()
	s.env.OnActivity(s.a.ProcessBatch, mock.Anything, mock.Anything).
		Return(BatchResult{}, temporal.NewNonRetryableApplicationError(
			"batch exploded", PermanentErrorType, nil)).Once()

	s.env.ExecuteWorkflow(EnrichmentSweepWorkflow, SweepInput{Version: 1})
	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError(), "a failed batch leaves rows pending; the sweep itself succeeds")
}

func TestSweepSuite(t *testing.T) { suite.Run(t, new(SweepSuite)) }
