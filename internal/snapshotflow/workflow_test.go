package snapshotflow

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"example.com/agentmem/internal/snapshot"
)

// Tier 1 (docs/06-testing.md): in-memory Temporal test environment only — no
// server, no Docker. Mirrors internal/enrich/workflow_test.go's pattern.
type BuildSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
	a   *Activities
}

func (s *BuildSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.a = &Activities{}
	s.env.RegisterWorkflow(BuildSnapshotWorkflow)
	s.env.RegisterActivity(s.a)
}

func (s *BuildSuite) AfterTest(_, _ string) { s.env.AssertExpectations(s.T()) }

// TestHappyPathRunsAllThreeActivitiesInOrder: AllocateVersion's output
// version must be threaded into BuildAndSeal, and BuildAndSeal's
// BuildResult into Publish; the workflow's own result is exactly what
// Publish returned.
func (s *BuildSuite) TestHappyPathRunsAllThreeActivitiesInOrder() {
	in := BuildInput{EnrichmentVersion: 3, VectorDim: 768}
	const allocatedVersion = int64(42)

	built := BuildResult{
		Version: allocatedVersion,
		TarPath: "/work/snapshot-v42.tar.zst",
		Manifest: snapshot.Manifest{
			Version:           allocatedVersion,
			EnrichmentVersion: in.EnrichmentVersion,
			DocCount:          1000,
			VectorDim:         768,
		},
	}
	published := PublishResult{
		Version:  allocatedVersion,
		Key:      "snapshots/snapshot-v42.tar.zst",
		Manifest: built.Manifest,
	}

	s.env.OnActivity(s.a.AllocateVersion, mock.Anything, in.EnrichmentVersion).
		Return(allocatedVersion, nil).Once()
	s.env.OnActivity(s.a.BuildAndSeal, mock.Anything, in, allocatedVersion).
		Return(built, nil).Once()
	s.env.OnActivity(s.a.Publish, mock.Anything, built).
		Return(published, nil).Once()

	s.env.ExecuteWorkflow(BuildSnapshotWorkflow, in)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())

	var out PublishResult
	s.NoError(s.env.GetWorkflowResult(&out))
	s.Equal(published, out)
}

// TestBuildAndSealFailureFailsWorkflowWithoutPublishing: a BuildAndSeal
// failure (after its own retries are exhausted) must fail the workflow, and
// Publish must never run — there is nothing to publish.
func (s *BuildSuite) TestBuildAndSealFailureFailsWorkflowWithoutPublishing() {
	in := BuildInput{EnrichmentVersion: 1}
	const allocatedVersion = int64(7)

	s.env.OnActivity(s.a.AllocateVersion, mock.Anything, in.EnrichmentVersion).
		Return(allocatedVersion, nil).Once()
	s.env.OnActivity(s.a.BuildAndSeal, mock.Anything, in, allocatedVersion).
		Return(BuildResult{}, temporal.NewNonRetryableApplicationError(
			"build blew up", "BuildFailed", nil)).Once()
	// No .OnActivity for Publish: AssertExpectations (in AfterTest) would
	// fail the test if it were called without being set up, but to make the
	// intent explicit we also assert it was never invoked below.

	s.env.ExecuteWorkflow(BuildSnapshotWorkflow, in)

	s.True(s.env.IsWorkflowCompleted())
	s.Error(s.env.GetWorkflowError())
	s.env.AssertNotCalled(s.T(), "Publish", mock.Anything, mock.Anything)
}

// TestPublishNonRetryableErrorFailsWorkflow: Publish's non-retryable
// "tarball missing" outcome must fail the workflow (not hang or retry
// forever).
func (s *BuildSuite) TestPublishNonRetryableErrorFailsWorkflow() {
	in := BuildInput{EnrichmentVersion: 2}
	const allocatedVersion = int64(9)
	built := BuildResult{Version: allocatedVersion, TarPath: "/work/snapshot-v9.tar.zst"}

	s.env.OnActivity(s.a.AllocateVersion, mock.Anything, in.EnrichmentVersion).
		Return(allocatedVersion, nil).Once()
	s.env.OnActivity(s.a.BuildAndSeal, mock.Anything, in, allocatedVersion).
		Return(built, nil).Once()
	s.env.OnActivity(s.a.Publish, mock.Anything, built).
		Return(PublishResult{}, temporal.NewNonRetryableApplicationError(
			"snapshotflow: build artifact not found on this worker", SnapshotTarballMissingErrorType, nil)).Once()

	s.env.ExecuteWorkflow(BuildSnapshotWorkflow, in)

	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	s.Error(err)

	var appErr *temporal.ApplicationError
	s.ErrorAs(err, &appErr)
	s.Equal(SnapshotTarballMissingErrorType, appErr.Type())
}

func TestBuildSuite(t *testing.T) { suite.Run(t, new(BuildSuite)) }
