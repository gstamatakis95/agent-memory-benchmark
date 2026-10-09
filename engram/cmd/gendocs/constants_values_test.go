package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/gstamatakis95/engram/internal/gen/constants"
)

// goValue converts a figure read from the plan to the Go value the generated constant of that unit must have.
func goValue(n float64, unit string) any {
	switch unit {
	case "ms":
		return time.Duration(n * float64(time.Millisecond))
	case "s":
		return time.Duration(n * float64(time.Second))
	case "min":
		return time.Duration(n * float64(time.Minute))
	case "h":
		return time.Duration(n * float64(time.Hour))
	case "d":
		return time.Duration(n * 24 * float64(time.Hour))
	case "KiB":
		return int64(n) << 10
	case "MiB":
		return int64(n) << 20
	case "MB":
		return int64(n * 1e6)
	case "ratio":
		return n
	}
	return int(n)
}

// planDuration reads "500 ms", "10 s" or "—" (none) as the plan writes a retry policy.
func planDuration(t *testing.T, s string) time.Duration {
	t.Helper()
	if s == "—" {
		return 0
	}
	f := strings.Fields(s)
	if len(f) != 2 {
		t.Fatalf("duration %q", s)
	}
	n, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		t.Fatal(err)
	}
	switch f[1] {
	case "ms":
		return time.Duration(n) * time.Millisecond
	case "s":
		return time.Duration(n) * time.Second
	}
	t.Fatalf("unit of %q", s)
	return 0
}

// Every constant and retry policy of the generated Go table equals the figure the plan prints. The expectation is read
// from the plan through the witness of the table (a pattern with the number left open, which gendocs requires to match
// exactly one figure), never from the value column of constants.yaml, so a value edited in the table alone, or a figure
// the plan changed, fails here as well as in gendocs.
func TestConstants_RegisterValues(t *testing.T) {
	root := repoRoot(t)
	plan, err := LoadPlan(filepath.Join(root, "docs", "plan", "PLAN.md"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "cmd", "gendocs", "constants.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var src constantsFile
	if err := yaml.Unmarshal(raw, &src); err != nil {
		t.Fatal(err)
	}
	have := map[string]constants.Entry{}
	for _, e := range constants.All {
		have[e.ID] = e
	}
	if len(src.Constants) != len(have) {
		t.Fatalf("the source table has %d constants, the Go table %d", len(src.Constants), len(have))
	}
	for _, c := range src.Constants {
		e, ok := have[c.ID]
		if !ok {
			t.Errorf("%s is not in the Go table", c.ID)
			continue
		}
		if s, isString := e.Value.(string); isString {
			text, _ := citedText(plan, c.Rows)
			if want := strings.ReplaceAll(c.Witness, "{v}", s); !strings.Contains(squashWS(text), squashWS(want)) {
				t.Errorf("%s = %q: the text %q is in none of %v", c.ID, s, want, c.Rows)
			}
			continue
		}
		n, err := planValue(plan, c)
		if err != nil {
			t.Errorf("%v", err)
			continue
		}
		if want := goValue(n, e.Unit); e.Value != want {
			t.Errorf("%s = %v (%T), the plan says %v (%T) in %v", c.ID, e.Value, e.Value, want, want, c.Rows)
		}
	}
	policies := []constants.RetryPolicy{constants.PPure, constants.PDB, constants.PFrozen, constants.PLLM,
		constants.PEmbed, constants.PBlob, constants.PPoll, constants.PCatalog, constants.PCutover,
		constants.PTemporal}
	sec, _ := plan.Source("§5.0")
	for _, p := range policies {
		row := regexp.MustCompile("(?m)^\\| `" + regexp.QuoteMeta(p.Name) + "` \\| ([^|]*) \\|").FindStringSubmatch(sec)
		if row == nil {
			t.Errorf("section 5.0 has no row for %s", p.Name)
			continue
		}
		f := strings.Split(row[1], " / ")
		if len(f) != 4 {
			t.Errorf("%s: row %q", p.Name, row[1])
			continue
		}
		coeff, _ := strconv.ParseFloat(f[1], 64)
		attempts, _ := strconv.Atoi(strings.Fields(f[3])[0]) // "unlimited within ..." reads as 0
		got := fmt.Sprint(p.Initial, p.Coefficient, p.MaxInterval, p.MaxAttempts)
		want := fmt.Sprint(planDuration(t, f[0]), coeff, planDuration(t, f[2]), attempts)
		if got != want {
			t.Errorf("%s = %s, the plan says %s", p.Name, got, want)
		}
	}
}
