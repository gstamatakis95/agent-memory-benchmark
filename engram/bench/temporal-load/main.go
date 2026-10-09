// Command temporal-load is the P-9 load test and the restore-drill driver of M0.4 (PLAN.md section 9.1, N71, N139).
//
//	temporal-load run   -ladder 20,40,80 -step 60s     drive RetainDocument-shaped workflows at rising rates
//	temporal-load drill-seed   -n 200 -out seed.json   run workflows to completion and record what must come back
//	temporal-load drill-verify -in seed.json           prove the recorded histories are readable after a restore
//
// The workflows replay the activity graph, payload sizes and fan-out of RetainDocument (shape.go) against the cell's
// Temporal cluster through the real payload codec (internal/workflows/codec), spread over several shard task queues.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gstamatakis95/engram/internal/workflows/codec"
)

type common struct {
	addr, namespace, keysDir, wrappedDir string
	useCodec                             bool
	queues                               int
}

func (c *common) flags(fs *flag.FlagSet) {
	fs.StringVar(&c.addr, "addr", "127.0.0.1:17233", "Temporal frontend address")
	fs.StringVar(&c.namespace, "namespace", "engram", "Temporal namespace")
	fs.StringVar(&c.keysDir, "keys", "deploy/compose/secrets/temporal_codec", "directory of the shard codec keys")
	fs.StringVar(&c.wrappedDir, "wrapped", "", "directory of the wrapped data keys (default: under the user cache dir)")
	fs.BoolVar(&c.useCodec, "codec", true, "encrypt every payload with the Engram codec")
	fs.IntVar(&c.queues, "queues", 4, "number of shard task queues (shard-1 .. shard-N)")
}

func (c *common) dial() (client.Client, converter.DataConverter, error) {
	opts := client.Options{HostPort: c.addr, Namespace: c.namespace,
		Logger: tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelWarn})))}
	dc := converter.GetDefaultDataConverter()
	if c.useCodec {
		dir := c.wrappedDir
		if dir == "" {
			cache, err := os.UserCacheDir()
			if err != nil {
				return nil, nil, err
			}
			dir = filepath.Join(cache, "engram-temporal-load", "wrapped")
		}
		keys := codec.NewFileKeyProvider(c.keysDir, codec.NewDirStore(dir), codec.StaticShard(1))
		cv := codec.New(keys, codec.Options{Worker: true}) // the harness is both starter and worker
		cv.Apply(&opts)
		dc = cv.Data
	}
	cl, err := client.Dial(opts)
	if err != nil {
		return nil, nil, err
	}
	// A namespace created a moment ago takes a few seconds to reach the history service's cache: probe with a real
	// start (frontend to history), the path every later call takes, and terminate the probe.
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		run, perr := cl.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: fmt.Sprintf("probe-%d", time.Now().UnixNano()), TaskQueue: "probe",
			WorkflowExecutionTimeout: time.Minute,
		}, "probe")
		if perr == nil {
			_ = cl.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "probe")
			cancel()
			return cl, dc, nil
		}
		cancel()
		if time.Now().After(deadline) {
			cl.Close()
			return nil, nil, fmt.Errorf("namespace %s: %w", c.namespace, perr)
		}
		time.Sleep(time.Second)
	}
}

func (c *common) queue(i int) string { return fmt.Sprintf("shard-%d", i%c.queues+1) }

// startWorkers polls every shard queue with the shape's workflow and activities; the counters see the activities.
func (c *common) startWorkers(cl client.Client, cnt *counters, pollers int) ([]worker.Worker, error) {
	var ws []worker.Worker
	for i := 0; i < c.queues; i++ {
		w := worker.New(cl, c.queue(i), worker.Options{
			MaxConcurrentActivityExecutionSize:     2000,
			MaxConcurrentWorkflowTaskExecutionSize: 1000,
			MaxConcurrentActivityTaskPollers:       pollers,
			MaxConcurrentWorkflowTaskPollers:       pollers,
			// required by codec.Options.Worker (review m17)
			WorkflowPanicPolicy: worker.BlockWorkflow,
		})
		w.RegisterWorkflowWithOptions(retainShape, workflow.RegisterOptions{Name: "RetainShape"})
		for name, fn := range cnt.activityFns() {
			w.RegisterActivityWithOptions(fn, registerOpts(name))
		}
		if err := w.Start(); err != nil {
			return nil, err
		}
		ws = append(ws, w)
	}
	return ws, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: temporal-load run|drill-seed|drill-verify [flags]")
		os.Exit(2)
	}
	var err error
	ctx := context.Background()
	switch os.Args[1] {
	case "run":
		err = cmdRun(ctx, os.Args[2:])
	case "drill-seed":
		err = cmdSeed(ctx, os.Args[2:])
	case "drill-verify":
		err = cmdVerify(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "temporal-load:", err)
		os.Exit(1)
	}
}
