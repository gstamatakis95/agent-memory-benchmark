package enrich

import (
	"context"
	"errors"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ScheduleConfig parameterizes EnsureSchedule; zero values mean the package
// defaults (ID "enrichment-sweep", every 1 minute, overlap Skip, task queue
// "enrichment").
type ScheduleConfig struct {
	ID        string
	TaskQueue string
	Interval  time.Duration
	Version   int16
	Paused    bool // create paused (docs/03-temporal.md Stage 1)
}

// EnsureSchedule idempotently creates the 1-minute sweep schedule on server
// start (docs/03-temporal.md section 3). Call it AFTER the worker is
// registered and started, or the first fire finds no worker. "Already
// exists" outcomes are swallowed in all the shapes the SDK/server can
// produce: the SDK's temporal.ErrScheduleAlreadyRunning sentinel, the
// service's *serviceerror.AlreadyExists, and a bare gRPC ALREADY_EXISTS.
func EnsureSchedule(ctx context.Context, c client.Client, cfg ScheduleConfig) error {
	if cfg.ID == "" {
		cfg.ID = ScheduleID
	}
	if cfg.TaskQueue == "" {
		cfg.TaskQueue = TaskQueue
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.Version == 0 {
		cfg.Version = DefaultVersion
	}

	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: cfg.ID,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: cfg.Interval}},
		},
		// Skip is the default; explicit for clarity: exactly one sweeper run
		// at a time, intervening ticks on a long run are dropped.
		Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP,
		Paused:  cfg.Paused,
		Action: &client.ScheduleWorkflowAction{
			ID:        "enrichment-sweep-wf",
			Workflow:  EnrichmentSweepWorkflow,
			Args:      []any{SweepInput{Version: cfg.Version}},
			TaskQueue: cfg.TaskQueue,
		},
	})
	if err == nil || errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return nil
	}
	var already *serviceerror.AlreadyExists
	if errors.As(err, &already) || status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

// TriggerNow fires an immediate run of the default sweep schedule — the fast
// path after ingest and the client's trigger-sweep RPC. Overlap Skip keeps a
// trigger from piling onto an already-running sweep.
func TriggerNow(ctx context.Context, c client.Client) error {
	return TriggerScheduleNow(ctx, c, ScheduleID)
}

// TriggerScheduleNow triggers an immediate run of the named schedule.
func TriggerScheduleNow(ctx context.Context, c client.Client, scheduleID string) error {
	return c.ScheduleClient().GetHandle(ctx, scheduleID).Trigger(ctx, client.ScheduleTriggerOptions{
		Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
}
