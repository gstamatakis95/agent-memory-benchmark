package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/gstamatakis95/engram/internal/id"
)

// component is a container whose CPU the sampler reads from its cgroup.
type component struct {
	Name      string
	Container string
	pid       int
}

func (c *component) seconds() (float64, error) {
	if c.pid == 0 {
		out, err := exec.Command("docker", "inspect", "-f", "{{.State.Pid}}", c.Container).Output()
		if err != nil {
			return 0, err
		}
		if c.pid, err = strconv.Atoi(strings.TrimSpace(string(out))); err != nil || c.pid == 0 {
			return 0, fmt.Errorf("container %s is not running", c.Container)
		}
	}
	return cgroupSeconds(c.pid)
}

// cgroupSeconds returns the CPU time consumed by the whole cgroup of pid (every process of the container), in seconds:
// cpuacct.usage on cgroup v1, cpu.stat usage_usec on v2.
func cgroupSeconds(pid int) (float64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			st, err := os.ReadFile("/sys/fs/cgroup" + parts[2] + "/cpu.stat")
			if err != nil {
				return 0, err
			}
			for _, l := range strings.Split(string(st), "\n") {
				if v, ok := strings.CutPrefix(l, "usage_usec "); ok {
					us, _ := strconv.ParseFloat(v, 64)
					return us / 1e6, nil
				}
			}
		case strings.Contains(parts[1], "cpuacct"):
			u, err := os.ReadFile("/sys/fs/cgroup/cpuacct" + parts[2] + "/cpuacct.usage")
			if err != nil {
				return 0, err
			}
			ns, _ := strconv.ParseFloat(strings.TrimSpace(string(u)), 64)
			return ns / 1e9, nil
		}
	}
	return 0, fmt.Errorf("no cpu cgroup for pid %d", pid)
}

// procTicks returns utime+stime of a process (and nothing of its children) in clock ticks (100 Hz on Linux).
func procTicks(pid int) (float64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndex(s, ")")+2:])
	ut, _ := strconv.ParseFloat(f[11], 64)
	st, _ := strconv.ParseFloat(f[12], 64)
	return ut + st, nil
}

// hist is the cumulative bucket counts of one Prometheus histogram, summed over its label sets.
type hist map[float64]float64

// persistence reads persistence_latency_bucket from a Temporal service's /metrics: the sum over all operations and
// the series per operation.
func persistence(url string) (all hist, perOp map[string]hist, err error) {
	resp, err := http.Get(url) //nolint:gosec // a local metrics endpoint
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	all, perOp = hist{}, map[string]hist{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "persistence_latency_bucket{") {
			continue
		}
		lb := line[strings.Index(line, "{")+1 : strings.LastIndex(line, "}")]
		val, _ := strconv.ParseFloat(strings.Fields(line[strings.LastIndex(line, "}")+1:])[0], 64)
		var le float64
		var op string
		for _, kv := range strings.Split(lb, ",") {
			k, v, _ := strings.Cut(kv, "=")
			v = strings.Trim(v, `"`)
			switch k {
			case "le":
				if v == "+Inf" {
					le = 1e18
				} else {
					le, _ = strconv.ParseFloat(v, 64)
				}
			case "operation":
				op = v
			}
		}
		all[le] += val
		if perOp[op] == nil {
			perOp[op] = hist{}
		}
		perOp[op][le] += val
	}
	return all, perOp, sc.Err()
}

func (h hist) sub(o hist) hist {
	d := hist{}
	for le, v := range h {
		d[le] = v - o[le]
	}
	return d
}

func (h hist) count() float64 { return h[1e18] }

// quantile estimates the q-quantile in seconds by linear interpolation inside the bucket (Prometheus' method).
func (h hist) quantile(q float64) float64 {
	n := h.count()
	if n <= 0 {
		return 0
	}
	les := make([]float64, 0, len(h))
	for le := range h {
		les = append(les, le)
	}
	sort.Float64s(les)
	rank := q * n
	prevLe, prevCnt := 0.0, 0.0
	for _, le := range les {
		if h[le] >= rank {
			if le >= 1e18 {
				return prevLe
			}
			if h[le] == prevCnt {
				return le
			}
			return prevLe + (le-prevLe)*(rank-prevCnt)/(h[le]-prevCnt)
		}
		prevLe, prevCnt = le, h[le]
	}
	return prevLe
}

// StepResult is one rung of the ladder.
type StepResult struct {
	OfferedWfPerSec   float64            `json:"offered_wf_per_s"`
	StartedWf         int64              `json:"started_wf"`
	StartErrors       int64              `json:"start_errors"`
	CompletedWf       int64              `json:"completed_wf_in_window"`
	InFlightAtEnd     int64              `json:"in_flight_at_end"`
	EventsPerSec      float64            `json:"events_per_s"`
	EventsPerWf       float64            `json:"events_per_wf"`
	ActivitiesPerSec  float64            `json:"activities_per_s"`
	LatencyP50Ms      float64            `json:"wf_latency_p50_ms"`
	LatencyP99Ms      float64            `json:"wf_latency_p99_ms"`
	CPUCores          map[string]float64 `json:"cpu_cores"`
	PersistP99Ms      float64            `json:"history_persistence_p99_ms"`
	PersistP50Ms      float64            `json:"history_persistence_p50_ms"`
	PersistReqPerSec  float64            `json:"history_persistence_req_per_s"`
	WorstOp           string             `json:"worst_persistence_operation"`
	WorstOpP99Ms      float64            `json:"worst_persistence_operation_p99_ms"`
	WindowSeconds     float64            `json:"window_s"`
	HistoryCPUPercent float64            `json:"history_cpu_percent_of_limit"`
	// HostCPUPressure is the share of the window in which some task of the host waited for a CPU (PSI some, from
	// /proc/pressure/cpu): above about 10 % the machine, not Temporal, is limiting the numbers (review m14).
	HostCPUPressure float64 `json:"host_cpu_pressure_some_percent"`
	// HistorySQLConns are the history service's SQL pool gauges (persistence_sql_*) at the end of the rung.
	HistorySQLConns map[string]float64 `json:"history_sql_conn_gauges"`
}

// cpuPressureMicros returns the cumulative "some" stall time of the host in microseconds (-1 without PSI).
func cpuPressureMicros() float64 {
	b, err := os.ReadFile("/proc/pressure/cpu")
	if err != nil {
		return -1
	}
	for _, f := range strings.Fields(strings.SplitN(string(b), "\n", 2)[0]) {
		if v, ok := strings.CutPrefix(f, "total="); ok {
			us, _ := strconv.ParseFloat(v, 64)
			return us
		}
	}
	return -1
}

// sqlConnGauges reads the SQL pool gauges (persistence_sql_*, open and in-use connections included) of a Temporal
// service's /metrics.
func sqlConnGauges(url string) map[string]float64 {
	out := map[string]float64{}
	resp, err := http.Get(url) //nolint:gosec // a local metrics endpoint
	if err != nil {
		return out
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "persistence_sql_") || strings.Contains(line, "_bucket") ||
			strings.Contains(line, "_sum") || strings.Contains(line, "_count") {
			continue
		}
		name := line[:strings.IndexAny(line, "{ ")]
		val, _ := strconv.ParseFloat(strings.Fields(line)[len(strings.Fields(line))-1], 64)
		out[name] += val
	}
	return out
}

type loadCfg struct {
	common
	ladder        []float64
	step          time.Duration
	chunks        int
	pollers       int
	historyCPUs   float64
	containerBase string
	metricsURL    string
	out           string
}

func cmdRun(ctx context.Context, args []string) error {
	var cfg loadCfg
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	cfg.flags(fs)
	ladder := fs.String("ladder", "10,20,40", "offered workflow starts per second, one step each")
	fs.DurationVar(&cfg.step, "step", 60*time.Second, "duration of each step")
	fs.IntVar(&cfg.chunks, "chunks", 8, "chunks per document")
	fs.IntVar(&cfg.pollers, "pollers", 16, "workflow and activity pollers per queue")
	fs.Float64Var(&cfg.historyCPUs, "history-cpus", 2, "CPU limit of the history container (for the percentage)")
	fs.StringVar(&cfg.containerBase, "containers", "engram-dev", "compose project prefix of the container names")
	fs.StringVar(&cfg.metricsURL, "history-metrics", "http://127.0.0.1:18002/metrics", "history service /metrics")
	fs.StringVar(&cfg.out, "out", "", "write the step results as JSON lines to this file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, s := range strings.Split(*ladder, ",") {
		r, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return err
		}
		cfg.ladder = append(cfg.ladder, r)
	}
	cl, _, err := cfg.dial()
	if err != nil {
		return err
	}
	defer cl.Close()
	cnt := &counters{}
	ws, err := cfg.startWorkers(cl, cnt, cfg.pollers)
	if err != nil {
		return err
	}
	defer func() {
		for _, w := range ws {
			w.Stop()
		}
	}()
	comps := []*component{}
	for _, n := range []string{"history", "matching", "frontend", "worker", "postgres"} {
		comps = append(comps, &component{Name: n, Container: fmt.Sprintf("%s-temporal-%s-1", cfg.containerBase, n)})
	}
	outF := io.Discard
	if cfg.out != "" {
		f, err := os.Create(cfg.out)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		outF = f
	}
	namespaces := make([]id.NamespaceID, 8)
	for i := range namespaces {
		namespaces[i] = id.NewNamespaceID()
	}
	var seq, started, startErrs atomic.Int64
	fmt.Printf("| offered wf/s | events/s | wf done/s | activities/s | in flight | wf p50 / p99 ms |"+
		" history cores (%% of %.0f) | persistence p50 / p99 ms | worst op p99 ms |"+
		" matching / frontend / postgres / worker cores | self cores | host CPU pressure %% | history SQL conns |\n"+
		"|---|---|---|---|---|---|---|---|---|---|---|---|---|\n", cfg.historyCPUs)
	for _, rate := range cfg.ladder {
		res, err := cfg.runStep(ctx, cl, cnt, comps, namespaces, &seq, &started, &startErrs, rate)
		if err != nil {
			return err
		}
		b, _ := json.Marshal(res)
		_, _ = fmt.Fprintln(outF, string(b))
		fmt.Printf("| %.4g | %.0f | %.2f | %.0f | %d | %.0f / %.0f | %.2f (%.0f%%) | %.1f / %.1f | %s %.0f |"+
			" %.2f / %.2f / %.2f / %.2f | %.2f | %.0f | %s |\n",
			res.OfferedWfPerSec, res.EventsPerSec, float64(res.CompletedWf)/res.WindowSeconds, res.ActivitiesPerSec,
			res.InFlightAtEnd, res.LatencyP50Ms, res.LatencyP99Ms, res.CPUCores["history"], res.HistoryCPUPercent,
			res.PersistP50Ms, res.PersistP99Ms, res.WorstOp, res.WorstOpP99Ms, res.CPUCores["matching"],
			res.CPUCores["frontend"], res.CPUCores["postgres"], res.CPUCores["worker"], res.CPUCores["self"],
			res.HostCPUPressure, fmtConns(res.HistorySQLConns))
	}
	return nil
}

func (cfg *loadCfg) runStep(ctx context.Context, cl client.Client, cnt *counters, comps []*component,
	namespaces []id.NamespaceID, seq, started, startErrs *atomic.Int64, rate float64) (StepResult, error) {
	cnt.mu.Lock()
	cnt.latencies = nil
	cnt.mu.Unlock()
	startedBefore, errsBefore := started.Load(), startErrs.Load()
	doneBefore, evBefore, actBefore := cnt.completed.Load(), cnt.events.Load(), cnt.activities.Load()
	ticksBefore := map[string]float64{}
	for _, c := range comps {
		t, err := c.seconds()
		if err != nil {
			return StepResult{}, err
		}
		ticksBefore[c.Name] = t
	}
	selfBefore, _ := procTicks(os.Getpid())
	psiBefore := cpuPressureMicros()
	allBefore, opsBefore, err := persistence(cfg.metricsURL)
	if err != nil {
		return StepResult{}, err
	}
	t0 := time.Now()
	stepCtx, cancel := context.WithTimeout(ctx, cfg.step)
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 1024)
	tick := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer tick.Stop()
loop:
	for {
		select {
		case <-stepCtx.Done():
			break loop
		case <-tick.C:
			n := seq.Add(1)
			select {
			case sem <- struct{}{}:
			default:
				startErrs.Add(1) // the starter itself is saturated: count it as a missed start
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				ns := namespaces[int(n)%len(namespaces)]
				_, err := cl.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
					ID:        fmt.Sprintf("ns/%s/op/load-%d-%d", ns, t0.UnixNano(), n),
					TaskQueue: cfg.queue(int(n)),
				}, "RetainShape", shapeInput{Chunks: cfg.chunks, StartNanos: time.Now().UnixNano()})
				if err != nil {
					startErrs.Add(1)
					return
				}
				started.Add(1)
			}()
		}
	}
	window := time.Since(t0).Seconds()
	doneAfter, evAfter, actAfter := cnt.completed.Load(), cnt.events.Load(), cnt.activities.Load()
	ticksAfter := map[string]float64{}
	for _, c := range comps {
		t, _ := c.seconds()
		ticksAfter[c.Name] = t
	}
	selfAfter, _ := procTicks(os.Getpid())
	psiAfter := cpuPressureMicros()
	allAfter, opsAfter, err := persistence(cfg.metricsURL)
	if err != nil {
		return StepResult{}, err
	}
	wg.Wait()
	res := StepResult{
		OfferedWfPerSec: rate, StartedWf: started.Load() - startedBefore, StartErrors: startErrs.Load() - errsBefore,
		CompletedWf: doneAfter - doneBefore, WindowSeconds: window, CPUCores: map[string]float64{},
		EventsPerSec: float64(evAfter-evBefore) / window, ActivitiesPerSec: float64(actAfter-actBefore) / window,
		InFlightAtEnd: started.Load() - cnt.completed.Load(),
	}
	if res.CompletedWf > 0 {
		res.EventsPerWf = float64(evAfter-evBefore) / float64(res.CompletedWf)
	}
	for n, t := range ticksAfter {
		res.CPUCores[n] = (t - ticksBefore[n]) / window
	}
	res.CPUCores["self"] = (selfAfter - selfBefore) / 100 / window
	res.HistoryCPUPercent = 100 * res.CPUCores["history"] / cfg.historyCPUs
	if psiBefore >= 0 && psiAfter >= 0 {
		res.HostCPUPressure = 100 * (psiAfter - psiBefore) / 1e6 / window
	}
	res.HistorySQLConns = sqlConnGauges(cfg.metricsURL)
	d := allAfter.sub(allBefore)
	res.PersistP99Ms, res.PersistP50Ms = d.quantile(0.99)*1000, d.quantile(0.5)*1000
	res.PersistReqPerSec = d.count() / window
	for op, h := range opsAfter {
		od := h.sub(opsBefore[op])
		if od.count() >= 100 && od.quantile(0.99)*1000 > res.WorstOpP99Ms {
			res.WorstOp, res.WorstOpP99Ms = op, od.quantile(0.99)*1000
		}
	}
	cnt.mu.Lock()
	lat := append([]time.Duration(nil), cnt.latencies...)
	cnt.mu.Unlock()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	if len(lat) > 0 {
		res.LatencyP50Ms = float64(lat[len(lat)/2].Milliseconds())
		res.LatencyP99Ms = float64(lat[len(lat)*99/100].Milliseconds())
	}
	return res, nil
}

func fmtConns(m map[string]float64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, strings.TrimPrefix(k, "persistence_sql_"))
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%.0f", k, m["persistence_sql_"+k]))
	}
	if len(parts) == 0 {
		return "n/a"
	}
	return strings.Join(parts, " ")
}
