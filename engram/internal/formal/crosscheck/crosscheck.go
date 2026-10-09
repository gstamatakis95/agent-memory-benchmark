// Package crosscheck feeds the counterexamples TLC printed for the must-fail configurations (the logs committed in
// formal/tla/results) to the invariant code of internal/formal/<spec>. The specification is the oracle (PLAN.md section
// 7, GUARDRAILS): in every state of a counterexample but the last, the invariants the configuration checks hold; in the
// last state the invariant that formal/tla/EXPECT names does not. A test in each invariant package decodes the TLC
// state into its Go value model and calls Verify.
package crosscheck

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/gstamatakis95/engram/internal/formal/trace"
)

// Trace is the error trace of one must-fail configuration.
type Trace struct {
	Config     string            // e.g. Outbox_NoWatch
	Violated   string            // the invariant EXPECT names
	Invariants []string          // the invariants the configuration checks
	Constants  map[string]string // the configuration's CONSTANTS, right-hand sides as written
	States     []trace.State
}

// FormalDir is the formal/ directory of the repository.
func FormalDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("crosscheck: no caller information")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "formal")
}

var assign = regexp.MustCompile(`^\s+([A-Za-z][A-Za-z0-9_]*)\s*(=|<-)\s*(.*?)\s*$`)

// parseCfg reads the CONSTANTS (name -> right-hand side; replacements keep their `<- Name` text) and INVARIANTS.
func parseCfg(path string) (map[string]string, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	consts := map[string]string{}
	var invs []string
	section := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), `\*`) || strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") {
			section = strings.Fields(line)[0]
			continue
		}
		switch section {
		case "CONSTANTS", "CONSTANT":
			if m := assign.FindStringSubmatch(line); m != nil {
				if m[2] == "<-" {
					consts[m[1]] = "<- " + m[3]
				} else {
					consts[m[1]] = m[3]
				}
			}
		case "INVARIANTS", "INVARIANT":
			invs = append(invs, strings.Fields(line)...)
		}
	}
	return consts, invs, nil
}

// MustFail returns the number of must-fail configurations of the specification in EXPECT.
func MustFail(spec string) (int, error) {
	expect, err := os.ReadFile(filepath.Join(FormalDir(), "tla", "EXPECT"))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, line := range strings.Split(string(expect), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 2 || !strings.HasPrefix(f[1], "violates ") {
			continue
		}
		if cfg := strings.TrimSuffix(f[0], ".cfg"); cfg == spec || strings.HasPrefix(cfg, spec+"_") {
			n++
		}
	}
	return n, nil
}

// Load returns the error traces of the must-fail configurations of one specification whose logs are committed. Logs
// without a trace (a design configuration, or a log not yet produced) are skipped.
func Load(spec string) ([]Trace, error) {
	dir := FormalDir()
	expect, err := os.ReadFile(filepath.Join(dir, "tla", "EXPECT"))
	if err != nil {
		return nil, err
	}
	var out []Trace
	for _, line := range strings.Split(string(expect), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 2 || strings.HasPrefix(line, "#") || !strings.HasPrefix(f[1], "violates ") {
			continue
		}
		cfg := strings.TrimSuffix(f[0], ".cfg")
		if cfg != spec && !strings.HasPrefix(cfg, spec+"_") {
			continue
		}
		log, err := os.Open(filepath.Join(dir, "tla", "results", cfg+".log"))
		if err != nil {
			continue
		}
		states, err := trace.ParseStates(log)
		_ = log.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", cfg, err)
		}
		if len(states) < 2 {
			continue
		}
		consts, invs, err := parseCfg(filepath.Join(dir, "tla", cfg+".cfg"))
		if err != nil {
			return nil, err
		}
		out = append(out, Trace{Config: cfg, Violated: strings.TrimPrefix(f[1], "violates "), Invariants: invs,
			Constants: consts, States: states})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Config < out[j].Config })
	return out, nil
}

// Verify decodes every state (and fails unless every must-fail configuration of the specification has a trace) of every
// trace of the specification with check (which returns the names of the Go invariants that fail in the state) and
// compares with the oracle: in a state but the last, none of the invariants the configuration lists may fail; in the
// last state the invariant EXPECT names must. known lists the invariant names the Go package implements; a
// configuration invariant that is not in known (TypeOK, liveness) is not compared.
func Verify(t *testing.T, spec string, known []string, check func(tr Trace, s trace.State) []string) {
	t.Helper()
	traces, err := Load(spec)
	if err != nil {
		t.Fatal(err)
	}
	want, err := MustFail(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) != want {
		t.Fatalf("%s: %d counterexamples compared but EXPECT lists %d must-fail configurations: a log is missing or "+
			"holds no error trace", spec, len(traces), want)
	}
	isKnown := map[string]bool{}
	for _, k := range known {
		isKnown[k] = true
	}
	states := 0
	defer func() {
		t.Logf("%s: %d counterexamples, %d states compared with the oracle", spec, len(traces), states)
	}()
	for _, tr := range traces {
		last := len(tr.States) - 1
		states += len(tr.States)
		for i, st := range tr.States {
			failing := check(tr, st)
			set := map[string]bool{}
			for _, f := range failing {
				set[f] = true
			}
			if i < last {
				for _, inv := range tr.Invariants {
					if isKnown[inv] && set[inv] {
						t.Errorf("%s state %d: the Go invariant %s fails but TLC held it", tr.Config, st.N, inv)
					}
				}
				continue
			}
			if !isKnown[tr.Violated] {
				t.Errorf("%s: the Go package has no invariant %s", tr.Config, tr.Violated)
			} else if !set[tr.Violated] {
				t.Errorf("%s state %d: TLC violated %s but the Go invariant holds (Go reports %v)", tr.Config, st.N,
					tr.Violated, failing)
			}
		}
	}
}
