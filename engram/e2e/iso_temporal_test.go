//go:build integration

// Package e2e holds the tests that need the running dev stack (deploy/compose, scripts/e2e.sh sets the environment).
// PLAN.md section 8.3 homes TestIso_Temporal_PayloadsEncrypted in internal/isolation; that package has no depguard rule
// yet (.golangci.yml belongs to E3), so the test lives here until it does.
package e2e

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/gstamatakis95/engram/internal/id"
	"github.com/gstamatakis95/engram/internal/workflows/codec"
)

// Environment (set by scripts/e2e.sh): the frontend, the shard codec keys and the DSN of the Temporal persistence
// database.
const (
	envAddr = "ENGRAM_TEST_TEMPORAL_ADDR"
	envKeys = "ENGRAM_TEST_CODEC_KEYS"
	envPG   = "ENGRAM_TEST_TEMPORAL_PG_DSN"
)

func need(t *testing.T) (addr, keys, pgDSN string) {
	t.Helper()
	addr, keys, pgDSN = os.Getenv(envAddr), os.Getenv(envKeys), os.Getenv(envPG)
	if addr == "" || keys == "" || pgDSN == "" {
		t.Skipf("needs the dev stack: set %s, %s and %s (scripts/e2e.sh does)", envAddr, envKeys, envPG)
	}
	return addr, keys, pgDSN
}

var activityRuns atomic.Int64

func echoActivity(_ context.Context, text string) (string, error) {
	activityRuns.Add(1)
	return "echo:" + text, nil
}

// failActivity fails with the tenant's text in its message and details, as an extractor quoting a chunk would.
func failActivity(_ context.Context, text string) (string, error) {
	activityRuns.Add(1)
	return "", temporal.NewNonRetryableApplicationError("cannot extract: "+text, "ChunkInvalid", nil, "detail "+text)
}

func echoWorkflow(ctx workflow.Context, text string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
	var out string
	err := workflow.ExecuteActivity(ctx, "echo", text).Get(ctx, &out)
	return out, err
}

func failWorkflow(ctx workflow.Context, text string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
	var out string
	err := workflow.ExecuteActivity(ctx, "fail", text).Get(ctx, &out)
	return out, err
}

func dial(t *testing.T, addr string, cv *codec.Converters) client.Client {
	t.Helper()
	opts := client.Options{HostPort: addr, Namespace: "engram"}
	if cv != nil {
		cv.Apply(&opts)
	}
	c, err := client.Dial(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func startWorker(t *testing.T, c client.Client, queue string) {
	t.Helper()
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflowWithOptions(echoWorkflow, workflow.RegisterOptions{Name: "echoWorkflow"})
	w.RegisterWorkflowWithOptions(failWorkflow, workflow.RegisterOptions{Name: "failWorkflow"})
	w.RegisterActivityWithOptions(echoActivity, registerEcho)
	w.RegisterActivityWithOptions(failActivity, registerFail)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
}

func run(t *testing.T, c client.Client, queue string, ns id.NamespaceID, text string) (string, string) {
	t.Helper()
	wfID, runID, err := start(c, queue, ns, "echoWorkflow", text, "echo:"+text)
	if err != nil {
		t.Fatal(err)
	}
	return wfID, runID
}

// start runs a workflow to completion and checks its result; want == "" expects the workflow to fail.
func start(c client.Client, queue string, ns id.NamespaceID, wf, text, want string) (string, string, error) {
	wfID := fmt.Sprintf("ns/%s/op/%d", ns, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: queue}, wf, text)
	if err != nil {
		return "", "", err
	}
	var out string
	err = r.Get(ctx, &out)
	switch {
	case want == "" && err == nil:
		return "", "", fmt.Errorf("workflow %s should have failed", wfID)
	case want != "" && err != nil:
		return "", "", fmt.Errorf("workflow %s: %w", wfID, err)
	case want != "" && out != want:
		return "", "", fmt.Errorf("workflow %s returned %q", wfID, out)
	}
	return wfID, r.GetRunID(), nil
}

// payloads collects every Payload message reachable from m.
func payloads(m protoreflect.Message, into *[]*commonpb.Payload) {
	if p, ok := m.Interface().(*commonpb.Payload); ok {
		*into = append(*into, p)
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			for i := 0; i < v.List().Len(); i++ {
				payloads(v.List().Get(i).Message(), into)
			}
		case fd.IsMap() && fd.MapValue().Message() != nil:
			v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				payloads(mv.Message(), into)
				return true
			})
		case fd.Message() != nil && !fd.IsMap():
			payloads(v.Message(), into)
		}
		return true
	})
}

// historyPayloads returns every payload of a workflow's history (inputs, results, activity arguments, failures, ...)
// and the number of events whose serialised form contains one of the sentinels.
func historyPayloads(t *testing.T, c client.Client, wfID, runID string, sentinels ...string) (
	[]*commonpb.Payload, int) {
	t.Helper()
	var ps []*commonpb.Payload
	leaks := 0
	it := c.GetWorkflowHistory(context.Background(), wfID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := proto.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sentinels {
			if strings.Contains(string(raw), s) {
				leaks++
			}
		}
		payloads(ev.ProtoReflect(), &ps)
	}
	return ps, leaks
}

func plainCount(ps []*commonpb.Payload) int {
	n := 0
	for _, p := range ps {
		if string(p.GetMetadata()[codec.MetadataEncoding]) != codec.EncodingEncrypted {
			n++
		}
	}
	return n
}

// spellings returns the forms in which a sentinel could sit inside a stored value: as is, lower and upper case hex, and
// base64 at each of its three alignments (the encoding of the sentinel padded on both sides, minus the edge groups).
func spellings(sentinel string) []string {
	h := hex.EncodeToString([]byte(sentinel))
	out := []string{sentinel, h, strings.ToUpper(h)}
	for off := 0; off < 3; off++ {
		enc := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", off) + sentinel + "yyy"))
		out = append(out, enc[4:len(enc)-4])
	}
	return out
}

// persistenceHits scans every row of every table of the Temporal persistence and visibility databases for the
// sentinels, as raw bytes and as text, in each of the spellings above.
func persistenceHits(t *testing.T, dsn string, sentinels ...string) int {
	t.Helper()
	var needles []string
	for _, s := range sentinels {
		needles = append(needles, spellings(s)...)
	}
	hits := 0
	for _, db := range []string{"temporal", "temporal_visibility"} {
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Database = db
		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		tables, err := conn.Query(context.Background(),
			`SELECT table_name FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for tables.Next() {
			var n string
			if err := tables.Scan(&n); err != nil {
				t.Fatal(err)
			}
			names = append(names, n)
		}
		tables.Close()
		for _, n := range names {
			rows, err := conn.Query(context.Background(), "SELECT * FROM "+pgx.Identifier{n}.Sanitize())
			if err != nil {
				t.Fatalf("scan %s.%s: %v", db, n, err)
			}
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range vals {
					var s string
					if b, ok := v.([]byte); ok {
						s = string(b)
					} else {
						s = fmt.Sprint(v)
					}
					for _, needle := range needles {
						if strings.Contains(s, needle) {
							hits++
						}
					}
				}
			}
			rows.Close()
		}
		_ = conn.Close(context.Background())
	}
	return hits
}

// workflowTaskFailed waits for a WorkflowTaskFailed event in a workflow's history.
func workflowTaskFailed(c client.Client, wfID, runID string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		it := c.GetWorkflowHistory(context.Background(), wfID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			ev, err := it.Next()
			if err != nil {
				break
			}
			if ev.GetEventType() == enums.EVENT_TYPE_WORKFLOW_TASK_FAILED {
				return true
			}
		}
		time.Sleep(time.Second)
	}
	return false
}

// refused starts a workflow with starter on a queue that only worker serves and checks that it never completes, that
// its workflow task fails (the worker was reached and refused the payload) and that no activity ran.
func refused(t *testing.T, name string, starter client.Client, queue string, ns id.NamespaceID, text string) {
	t.Helper()
	before := activityRuns.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r, err := starter.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: fmt.Sprintf("ns/%s/op/%s-%d", ns, name, time.Now().UnixNano()), TaskQueue: queue}, "echoWorkflow", text)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = starter.TerminateWorkflow(ctx, r.GetID(), r.GetRunID(), "test done") }()
	if !workflowTaskFailed(starter, r.GetID(), r.GetRunID(), 45*time.Second) {
		t.Errorf("%s: no WorkflowTaskFailed event: the worker never refused the payload", name)
	}
	d, err := starter.DescribeWorkflowExecution(ctx, r.GetID(), r.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	if st := d.GetWorkflowExecutionInfo().GetStatus(); st != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Errorf("%s: workflow is %v, want RUNNING", name, st)
	}
	if after := activityRuns.Load(); after != before {
		t.Errorf("%s: an activity ran %d times on a worker that cannot decode its input", name, after-before)
	}
}

// TestIso_Temporal_PayloadsEncrypted is the N59/N99 row of PLAN.md section 8.3. Workflows of two namespaces run with
// sentinels in their inputs, results and, for one, in the message and details of a failing activity. Then: no sentinel
// appears in any history read through the SDK nor in any row (bytes, text, hex, base64) of the Temporal persistence and
// visibility databases, and every payload carries the `binary/encrypted` marker, failures included. The scan is known
// to bite: the same workflow run without the codec leaves its sentinel in the database. A worker holding another
// shard's codec key, and a strict worker handed a plaintext start, never run the activity (their workflow task fails).
// After `Shred(ns)` NO payload of that namespace decodes - inputs, results, failures - while the other namespace's all
// do. The plan's retain-based form (A1/Z1 retains on shard-1, `temporal workflow show`) waits for M1.1; this one uses
// echo workflows on private queues.
func TestIso_Temporal_PayloadsEncrypted(t *testing.T) {
	addr, keysDir, pgDSN := need(t)
	keys := codec.NewFileKeyProvider(keysDir, codec.NewDirStore(t.TempDir()), codec.StaticShard(1))
	cv := codec.New(keys, codec.Options{Worker: true})
	suffix := fmt.Sprint(time.Now().UnixNano())
	a1, z1, f1, p1 := "SENTINEL-A1-"+suffix, "SENTINEL-Z1-"+suffix, "SENTINEL-FAIL-"+suffix, "SENTINEL-PLAIN-"+suffix
	nsA, nsZ := id.NewNamespaceID(), id.NewNamespaceID()

	encrypted := dial(t, addr, &cv)
	queue := "iso-enc-" + suffix
	startWorker(t, encrypted, queue)
	type wfRef struct{ id, run string }
	var histA, histZ []wfRef
	add := func(l *[]wfRef, id, run string) { *l = append(*l, wfRef{id, run}) }
	id1, run1 := run(t, encrypted, queue, nsA, a1)
	add(&histA, id1, run1)
	id2, run2 := run(t, encrypted, queue, nsZ, z1)
	add(&histZ, id2, run2)
	// a failing activity in namespace A: the failure travels through the failure converter
	idF, runF, err := start(encrypted, queue, nsA, "failWorkflow", f1, "")
	if err != nil {
		t.Fatal(err)
	}
	add(&histA, idF, runF)

	failureSeen := false
	for _, w := range append(append([]wfRef{}, histA...), histZ...) {
		ps, leaks := historyPayloads(t, encrypted, w.id, w.run, a1, z1, f1)
		if len(ps) == 0 || plainCount(ps) != 0 || leaks != 0 {
			t.Errorf("history of %s: %d payloads, %d plain, %d events leak a sentinel", w.id, len(ps),
				plainCount(ps), leaks)
		}
		if w.id == idF {
			failureSeen = len(ps) > 3
		}
	}
	if !failureSeen {
		t.Error("the failing workflow's history carries too few payloads: its failure was not recorded")
	}
	if n := persistenceHits(t, pgDSN, a1, z1, f1); n != 0 {
		t.Errorf("%d persistence cells contain a sentinel of the encrypted runs", n)
	}

	// the scan bites: without the codec the sentinel is in the database
	plainClient := dial(t, addr, nil)
	plainQueue := "iso-plain-" + suffix
	startWorker(t, plainClient, plainQueue)
	wfP, runP := run(t, plainClient, plainQueue, nsA, p1)
	if ps, leaks := historyPayloads(t, plainClient, wfP, runP, p1); plainCount(ps) == 0 || leaks == 0 {
		t.Errorf("control: the unencrypted history shows %d plain payloads and %d leaks, want both > 0",
			plainCount(ps), leaks)
	}
	if n := persistenceHits(t, pgDSN, p1); n == 0 {
		t.Error("control: the persistence scan did not find the sentinel of an unencrypted run, so it proves nothing")
	}

	// a worker that holds another shard's key (shard 2's, not shard 1's) cannot decode shard 1's payloads
	otherDir := t.TempDir()
	key2, err := os.ReadFile(filepath.Join(keysDir, "s2-k1.key"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherDir, "s2-k1.key"), key2, 0o600); err != nil {
		t.Fatal(err)
	}
	otherKeys := codec.NewFileKeyProvider(otherDir, codec.NewDirStore(t.TempDir()), codec.StaticShard(2))
	other := codec.New(otherKeys, codec.Options{Worker: true})
	otherClient := dial(t, addr, &other)
	otherQueue := "iso-othershard-" + suffix
	startWorker(t, otherClient, otherQueue)
	refused(t, "othershard", encrypted, otherQueue, nsA, a1)

	// a strict worker never runs an activity for a workflow that was started without the codec
	strictQueue := "iso-strict-" + suffix
	startWorker(t, encrypted, strictQueue)
	refused(t, "plainstart", plainClient, strictQueue, nsA, p1)

	// after Shred(nsA) no payload of namespace A decodes; every payload of namespace Z still does
	if err := keys.Shred(context.Background(), nsA); err != nil {
		t.Fatal(err)
	}
	decodes := func(p *commonpb.Payload) error {
		var v any
		return cv.Data.FromPayload(p, &v)
	}
	for _, w := range histA {
		ps, _ := historyPayloads(t, encrypted, w.id, w.run)
		for i, p := range ps {
			if err := decodes(p); !errors.Is(err, codec.ErrKeyShredded) {
				t.Errorf("after Shred, payload %d of %s decodes or fails differently: %v", i, w.id, err)
			}
		}
	}
	for _, w := range histZ {
		ps, _ := historyPayloads(t, encrypted, w.id, w.run)
		for i, p := range ps {
			if err := decodes(p); err != nil {
				t.Errorf("namespace Z payload %d of %s must stay readable: %v", i, w.id, err)
			}
		}
	}
	// and the SDK's own view of A's failing workflow no longer quotes the activity error
	var out string
	if err := encrypted.GetWorkflow(context.Background(), idF, runF).Get(context.Background(), &out); err != nil &&
		strings.Contains(err.Error(), f1) {
		t.Errorf("after Shred the failure of namespace A still reads: %v", err)
	}
}
