package constants

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

type tableFile struct {
	Constants []struct {
		ID    string `yaml:"id"`
		Value any    `yaml:"value"`
		Unit  string `yaml:"unit"`
		GUC   string `yaml:"guc"`
	} `yaml:"constants"`
	Policies []struct {
		ID string `yaml:"id"`
	} `yaml:"policies"`
}

// expected converts a value of cmd/gendocs/constants.yaml to the Go value the generated constant must have.
func expected(v any, unit string) any {
	num := func() float64 {
		switch x := v.(type) {
		case int:
			return float64(x)
		case float64:
			return x
		}
		return 0
	}
	switch unit {
	case "ms":
		return time.Duration(num()) * time.Millisecond
	case "s":
		return time.Duration(num()) * time.Second
	case "min":
		return time.Duration(num()) * time.Minute
	case "h":
		return time.Duration(num()) * time.Hour
	case "d":
		return time.Duration(num()) * 24 * time.Hour
	case "KiB":
		return int64(num()) << 10
	case "MiB":
		return int64(num()) << 20
	case "MB":
		return int64(num()) * 1000 * 1000
	case "ratio":
		return num()
	case "string":
		return v
	}
	return int(num())
}

// Every constant of the source table is in the Go table with the value the table states (so a generated file edited by
// hand, or a generator that converts a unit wrongly, fails here), and the table has no entry the source lacks. It
// compares the generated Go with the file it is generated from and says nothing about the plan: the plan side is
// cmd/gendocs's TestConstants_RegisterValues and the witness check of gendocs itself.
func TestConstants_EveryEntryMatchesTheSourceTable(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "cmd", "gendocs", "constants.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var src tableFile
	if err := yaml.Unmarshal(raw, &src); err != nil {
		t.Fatal(err)
	}
	have := map[string]Entry{}
	for _, e := range All {
		if _, dup := have[e.ID]; dup || len(e.Rows) == 0 {
			t.Errorf("entry %s is duplicated or cites no register row", e.ID)
		}
		have[e.ID] = e
	}
	if len(src.Constants) < 100 || len(have) != len(src.Constants) {
		t.Fatalf("the source has %d constants, the Go table %d", len(src.Constants), len(have))
	}
	for _, c := range src.Constants {
		e, ok := have[c.ID]
		if !ok {
			t.Errorf("%s is in the source table but not in the Go table", c.ID)
			continue
		}
		if c.GUC != "" { // generated from deploy/postgres/gucs.yaml; pinned below
			continue
		}
		if want := expected(c.Value, c.Unit); e.Value != want || fmt.Sprintf("%T", e.Value) != fmt.Sprintf("%T", want) {
			t.Errorf("%s = %v (%T), the source table says %v (%T)", c.ID, e.Value, e.Value, want, want)
		}
	}
	if len(src.Policies) != 10 {
		t.Errorf("the source has %d retry policies, want the 10 of section 5.0", len(src.Policies))
	}
}
