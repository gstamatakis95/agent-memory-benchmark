package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/gstamatakis95/engram/internal/id"
)

// drillRecord is what the restore drill keeps about one workflow so that it can be proven back afterwards.
type drillRecord struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Sentinel   string `json:"sentinel"`
	Events     int    `json:"events"`
}

// cmdSeed runs n workflows to completion (their payloads carry a sentinel) and writes the records.
func cmdSeed(ctx context.Context, args []string) error {
	var c common
	fs := flag.NewFlagSet("drill-seed", flag.ContinueOnError)
	c.flags(fs)
	n := fs.Int("n", 100, "number of workflows")
	out := fs.String("out", "drill-seed.json", "records file; an existing file is appended to")
	tag := fs.String("tag", "pre", "label woven into the sentinel (pre / post backup)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cl, _, err := c.dial()
	if err != nil {
		return err
	}
	defer cl.Close()
	cnt := &counters{}
	ws, err := c.startWorkers(cl, cnt, 8)
	if err != nil {
		return err
	}
	defer func() {
		for _, w := range ws {
			w.Stop()
		}
	}()
	var recs []drillRecord
	if b, err := os.ReadFile(*out); err == nil {
		if err := json.Unmarshal(b, &recs); err != nil {
			return err
		}
	}
	ns := id.NewNamespaceID()
	newRecs := make([]drillRecord, *n)
	var wg sync.WaitGroup
	errs := make(chan error, *n)
	sem := make(chan struct{}, 32)
	for i := 0; i < *n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			sentinel := fmt.Sprintf("DRILL-%s-%d-%d-confidential", *tag, time.Now().UnixNano(), i)
			wfID := fmt.Sprintf("ns/%s/op/drill-%s-%d-%d", ns, *tag, time.Now().UnixNano(), i)
			run, err := cl.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: c.queue(i)},
				"RetainShape", shapeInput{Chunks: 2, Sentinel: sentinel})
			if err != nil {
				errs <- err
				return
			}
			var events int
			if err := run.Get(ctx, &events); err != nil {
				errs <- err
				return
			}
			newRecs[i] = drillRecord{WorkflowID: wfID, RunID: run.GetRunID(), Sentinel: sentinel,
				Events: eventCount(ctx, cl, wfID, run.GetRunID())}
			// the load test counts events as the history length at its last activity + 8; check that against the truth
			if newRecs[i].Events != events+8 {
				fmt.Printf("event count estimate off: history has %d events, estimate %d\n",
					newRecs[i].Events, events+8)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	b, err := json.MarshalIndent(append(recs, newRecs...), "", " ")
	if err != nil {
		return err
	}
	fmt.Printf("seeded %d workflows (%d recorded in total)\n", *n, len(recs)+*n)
	return os.WriteFile(*out, b, 0o644)
}

func eventCount(ctx context.Context, cl client.Client, wfID, runID string) int {
	it := cl.GetWorkflowHistory(ctx, wfID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	n := 0
	for it.HasNext() {
		if _, err := it.Next(); err != nil {
			return -1
		}
		n++
	}
	return n
}

// cmdVerify checks every recorded workflow: closed as completed, the same number of history events, and the sentinel
// decodes from the first event's input through the codec (so the key material is intact too).
func cmdVerify(ctx context.Context, args []string) error {
	var c common
	fs := flag.NewFlagSet("drill-verify", flag.ContinueOnError)
	c.flags(fs)
	in := fs.String("in", "drill-seed.json", "records file written by drill-seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	b, err := os.ReadFile(*in)
	if err != nil {
		return err
	}
	var recs []drillRecord
	if err := json.Unmarshal(b, &recs); err != nil {
		return err
	}
	cl, dc, err := c.dial()
	if err != nil {
		return err
	}
	defer cl.Close()
	bad := 0
	for _, r := range recs {
		d, err := cl.DescribeWorkflowExecution(ctx, r.WorkflowID, r.RunID)
		if err != nil {
			fmt.Printf("MISSING %s: %v\n", r.WorkflowID, err)
			bad++
			continue
		}
		if st := d.GetWorkflowExecutionInfo().GetStatus(); st != enums.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			fmt.Printf("BAD STATUS %s: %v\n", r.WorkflowID, st)
			bad++
			continue
		}
		if n := eventCount(ctx, cl, r.WorkflowID, r.RunID); n != r.Events {
			fmt.Printf("BAD HISTORY %s: %d events, recorded %d\n", r.WorkflowID, n, r.Events)
			bad++
			continue
		}
		it := cl.GetWorkflowHistory(ctx, r.WorkflowID, r.RunID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		ev, err := it.Next()
		if err != nil {
			return err
		}
		var got shapeInput
		if err := dc.FromPayloads(ev.GetWorkflowExecutionStartedEventAttributes().GetInput(), &got); err != nil ||
			!bytes.Equal([]byte(got.Sentinel), []byte(r.Sentinel)) {
			fmt.Printf("BAD PAYLOAD %s: %v (%q)\n", r.WorkflowID, err, got.Sentinel)
			bad++
		}
	}
	fmt.Printf("verified %d workflows, %d problems\n", len(recs), bad)
	if bad > 0 {
		return fmt.Errorf("%d of %d workflows did not come back", bad, len(recs))
	}
	return nil
}
