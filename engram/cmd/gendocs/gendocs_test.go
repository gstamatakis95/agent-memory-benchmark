package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller information")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func TestSplitSQL_SkipsCommentsAndDollarQuotedBodies(t *testing.T) {
	src := "-- header; not a statement\nCREATE TYPE a AS ENUM ('x;y', 'it''s'); /* block; */\n" +
		"CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; SELECT 2; $$ LANGUAGE sql;\n" +
		"CREATE FUNCTION g() RETURNS int AS $body$ BEGIN; END $body$;\nSELECT 1"
	got := splitSQL(src)
	if len(got) != 4 || !strings.Contains(got[0], "'x;y'") || strings.Contains(got[1], "SELECT 2") {
		t.Fatalf("statements = %q", got)
	}
	m := reEnum.FindStringSubmatch(got[0])
	if m == nil || strings.Join(literals(m[2]), "|") != "x;y|it's" {
		t.Fatalf("enum members = %v", literals(m[2]))
	}
}

func TestParseTuples_JoinsAdjacentLiteralsAndNulls(t *testing.T) {
	rows, err := parseTuples("('a', NULL, 'b ' 'c', 3), ('d', 'e', 'f', 4)", []string{"w", "x", "y", "z"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || !rows[0]["x"].Null || rows[0]["y"].S != "b c" || rows[1]["z"].S != "4" {
		t.Fatalf("rows = %+v", rows)
	}
	if _, err := parseTuples("('a', 'b')", []string{"w"}); err == nil {
		t.Error("a tuple with the wrong number of values was accepted")
	}
}

func TestParseDDL_ReadsTheRealMigrations(t *testing.T) {
	root := repoRoot(t)
	shard, err := ParseDDL(filepath.Join(root, "migrations", "shard"))
	if err != nil {
		t.Fatal(err)
	}
	cat, err := ParseDDL(filepath.Join(root, "migrations", "catalog"))
	if err != nil {
		t.Fatal(err)
	}
	enum := func(d *DDL, name string) []string {
		for _, e := range d.Enums {
			if e.Name == name {
				return e.Members
			}
		}
		return nil
	}
	const ops = "PENDING,RUNNING,DEFERRED,SUCCEEDED,FAILED,CANCELLED"
	if got := strings.Join(enum(shard, "operation_state"), ","); got != ops {
		t.Errorf("operation_state = %s", got)
	}
	if got := len(enum(cat, "move_state")); got != 9 {
		t.Errorf("move_state has %d members, want 9", got)
	}
	found := false
	for _, c := range shard.Checks {
		if c.Table == "namespace_ownership" && c.Column == "freeze_reason" {
			found = strings.Join(c.Members, ",") == "move,delete,restore"
		}
	}
	if !found {
		t.Error("namespace_ownership.freeze_reason CHECK list not found")
	}
	ts, err := readTransitions(shard)
	if err != nil || len(ts) < 25 {
		t.Fatalf("ownership_transitions: %d rows, %v", len(ts), err)
	}
	if got := strings.Join(transitionSQL(ts[3]), " "); !strings.Contains(got, "move_id = $move_id") {
		t.Errorf("start_move statement = %s", got)
	}
}

// The enum lint fails on a planted mismatch: one member removed from the SQL enum of the operation states.
func TestLintEnums_FailsOnAPlantedMismatch(t *testing.T) {
	root := repoRoot(t)
	shard, _ := ParseDDL(filepath.Join(root, "migrations", "shard"))
	cat, _ := ParseDDL(filepath.Join(root, "migrations", "catalog"))
	protos, err := LoadProtos(filepath.Join(root, "proto"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := LoadPlan(filepath.Join(root, "docs", "plan", "PLAN.md"))
	if err != nil {
		t.Fatal(err)
	}
	in := enumInputs{root: root, shard: shard, cat: cat, protos: protos}
	_, problems, err := lintEnums(filepath.Join(root, "cmd", "gendocs", "enums.yaml"), in, plan)
	if err != nil || len(problems) != 0 {
		t.Fatalf("the repository's enums disagree: %v %v", err, problems)
	}
	for i := range shard.Enums {
		if shard.Enums[i].Name == "operation_state" {
			shard.Enums[i].Members = shard.Enums[i].Members[:5] // drop CANCELLED
		}
	}
	_, problems, err = lintEnums(filepath.Join(root, "cmd", "gendocs", "enums.yaml"), in, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems) == 0 || !strings.Contains(problems[0], "Operation states") ||
		!strings.Contains(problems[0], "cancelled") {
		t.Fatalf("a missing member was not reported: %v", problems)
	}
}

const miniPlan = `# Plan

## 4. Api

### 4.1 Conventions

#### 4.1.6 Error model

| Typed detail | gRPC code | Raised when |
|---|---|---|
| ` + "`Boom{x}`" + ` | ` + "`ABORTED`" + ` | never |

## 5. Pipelines

### 5.0 Conventions

The fence never waits: retry after 200 ms. Pool of 32 and a   pool of 8.

## 8. Testing

### 8.1 Tiers

Defined here: ` + "`TestDefined_One`" + ` and a family ` + "`TestFamily_`" + `.

## 10. Roadmap

### 10.2 Milestones

| M0.1 | cites ` + "`TestDefined_One`" + ` and ` + "`TestMissing_Two`" + ` |

## Appendix A. Decision register

### D3. Sizing

The recall pool is 32.

| N82 | **Fence.** retry_after 200 ms; a 35 s single attempt |
`

func miniPlanFile(t *testing.T) *Plan {
	t.Helper()
	path := filepath.Join(t.TempDir(), "PLAN.md")
	if err := os.WriteFile(path, []byte(miniPlan), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := LoadPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPlan_IndexesSectionsAndRegisterRows(t *testing.T) {
	p := miniPlanFile(t)
	if s, ok := p.Source("§5.0"); !ok || !strings.Contains(s, "retry after 200 ms") {
		t.Errorf("section 5.0 = %q", s)
	}
	if s, ok := p.Source("N82"); !ok || !strings.Contains(s, "200 ms") {
		t.Errorf("row N82 = %q", s)
	}
	if s, ok := p.Source("D3"); !ok || !strings.Contains(s, "pool is 32") || strings.Contains(s, "N82") {
		t.Errorf("row D3 = %q", s)
	}
	if _, ok := p.Source("N999"); ok {
		t.Error("an unknown row was found")
	}
}

func TestLoadConstants_ChecksTheValueAgainstThePlan(t *testing.T) {
	plan := miniPlanFile(t)
	gucs, err := loadGUCs(filepath.Join(repoRoot(t), "cmd", "gendocs", "testdata", "gucs.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	load := func(body string, g *gucFile) (*constantsFile, string) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "c.yaml")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		f, problems, err := loadConstants(path, plan, g)
		if err != nil {
			t.Fatal(err)
		}
		return f, strings.Join(problems, "\n")
	}
	const row = "  - { id: FenceRetryAfter, group: locks, value: %s, unit: ms, rows: [N82], " +
		"witness: \"retry_after {v} ms\", what: x }\n"
	if _, probs := load("constants:\n"+strings.Replace(row, "%s", "200", 1), gucs); probs != "" {
		t.Errorf("a value the plan states was refused: %s", probs)
	}
	// only the value is edited: the witness is derived from it, so the plan no longer contains it
	if _, probs := load("constants:\n"+strings.Replace(row, "%s", "250", 1), gucs); !strings.Contains(probs,
		`"retry_after 250 ms" occurs in none of N82`) {
		t.Errorf("an edited value passed: %q", probs)
	}
	noPlaceholder := "constants:\n  - { id: A, group: g, value: 1, unit: count, rows: [N82], " +
		"witness: \"200 ms\", what: x }\n"
	if _, probs := load(noPlaceholder, gucs); !strings.Contains(probs, "has no {v} placeholder") {
		t.Errorf("a witness that does not test the value passed: %q", probs)
	}
	ghost := "constants:\n  - { id: Ghost, group: g, value: 1, unit: count, rows: [N999], witness: \"{v}\", what: y }\n"
	if _, probs := load(ghost, gucs); !strings.Contains(probs, "N999, which the plan does not have") {
		t.Errorf("a missing row passed: %q", probs)
	}
	// N139: the lock timeout is generated from the GUC table; a drifting table fails against the plan, and a value
	// stated next to the guc is refused
	gen := "constants:\n  - { id: Lock, group: g, guc: lock_timeout_exclusive, unit: s, rows: [N82], " +
		"witness: \"a {v} s single attempt\", what: x }\n"
	f, probs := load(gen, gucs)
	if probs != "" || f.Constants[0].Value != 35 {
		t.Errorf("guc-derived constant: %v %q", f.Constants[0].Value, probs)
	}
	drift := *gucs
	drift.Settings = append([]gucSetting(nil), gucs.Settings...)
	thirty := "30s"
	for i := range drift.Settings {
		if drift.Settings[i].Name == "lock_timeout_exclusive" {
			drift.Settings[i].Prod = &thirty
		}
	}
	if _, probs := load(gen, &drift); !strings.Contains(probs, `"a 30 s single attempt" occurs in none of N82`) {
		t.Errorf("a drifting GUC table passed: %q", probs)
	}
	both := strings.Replace(gen, "unit: s,", "value: 35, unit: s,", 1)
	if _, probs := load(both, gucs); !strings.Contains(probs, "must not state a value") {
		t.Errorf("a value next to guc passed: %q", probs)
	}
}

// A Test name in a Go test file that section 8 does not define fails gen-docs, a name cited in section 10.2 that
// section 8 does not define fails it, and so does a stale entry of the local list.
func TestTestIndex_FailsOnUndefinedNames(t *testing.T) {
	plan := miniPlanFile(t)
	root := t.TempDir()
	src := "package a\n\nfunc TestDefined_One(t *testing.T) {}\n\nfunc TestNotInPlan(t *testing.T) {}\n"
	if err := os.WriteFile(filepath.Join(root, "a_test.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(root, "local.txt")
	if err := os.WriteFile(local, []byte("# comment\nTestGone\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	doc, problems, err := genTestIndex(plan, root, local, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(problems, "\n")
	for _, want := range []string{"TestNotInPlan (a_test.go:5) is not defined in section 8",
		"TestMissing_Two is cited in section 10.2 (M0.1) but section 8 does not define it",
		"TestGone is in cmd/gendocs/local-tests.txt but no Go test file uses it"} {
		if !strings.Contains(joined, want) {
			t.Errorf("problems lack %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "TestDefined_One") || strings.Contains(joined, "TestFamily_") {
		t.Errorf("a defined name or a family prefix was reported:\n%s", joined)
	}
	if !strings.Contains(doc, "| `TestDefined_One` | §8.1 | M0.1 | `a_test.go` |") {
		t.Errorf("index row missing:\n%s", doc)
	}
	// listing the name on the local list lets the file pass
	if err := os.WriteFile(local, []byte("TestNotInPlan\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, problems, _ = genTestIndex(plan, root, local, nil, "")
	if strings.Contains(strings.Join(problems, "\n"), "TestNotInPlan") {
		t.Errorf("a listed name still fails:\n%v", problems)
	}
}

// The generator is deterministic, writes nothing when asked to check, and a hand edit of a generated file is a stale
// file.
func TestGenerate_IsDeterministicAndSeesAHandEdit(t *testing.T) {
	root := repoRoot(t)
	out := t.TempDir()
	o := options{root: root, plan: "docs/plan/PLAN.md", out: out, goOut: filepath.Join(out, "constants_gen.go"),
		gucs: filepath.Join(root, defaultGUCs), localTests: "cmd/gendocs/local-tests.txt"}
	if err := generate(o); err != nil {
		t.Fatal(err)
	}
	o.check = true
	if err := generate(o); err != nil {
		t.Fatalf("a second run is not a no-op: %v", err)
	}
	victim := filepath.Join(out, "constants.md")
	b, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(victim, append(b, []byte("hand edit\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := generate(o); err == nil || !strings.Contains(err.Error(), "constants.md") {
		t.Fatalf("a hand edit was not detected: %v", err)
	}
	o.check = false
	if err := generate(o); err != nil {
		t.Fatal(err)
	}
	if now, _ := os.ReadFile(victim); string(now) != string(b) {
		t.Error("regeneration did not restore the file")
	}
}

// The class lists of the DDL cover every table once (N113, N137) and a table without a class is a problem.
func TestGenClasses_CoversEveryTable(t *testing.T) {
	root := repoRoot(t)
	shard, err := ParseDDL(filepath.Join(root, "migrations", "shard"))
	if err != nil {
		t.Fatal(err)
	}
	doc, problems, err := genClasses(shard)
	if err != nil || len(problems) != 0 {
		t.Fatalf("classes: %v %v", err, problems)
	}
	if !strings.Contains(doc, "## insert-only (22)") || !strings.Contains(doc, "`facts`") {
		t.Errorf("classes document:\n%s", doc)
	}
	shard.Tables = append(shard.Tables, "unclassified_table")
	if _, problems, _ = genClasses(shard); len(problems) != 1 || !strings.Contains(problems[0], "unclassified_table") {
		t.Errorf("a table without a class was not reported: %v", problems)
	}
}

// A CHECK replaced by a later migration (DROP CONSTRAINT, ADD CONSTRAINT) is the list in force, and two live lists for
// one column are a problem of the DDL.
func TestParseDDL_ResolvesReplacedChecks(t *testing.T) {
	dir := t.TempDir()
	write := func(name, sql string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("0001.sql", "CREATE TABLE ents (id int, entity_type text CHECK (entity_type IN ('person', 'other')), "+
		"kind text, CONSTRAINT ents_kind_ck CHECK (kind IN ('a', 'b')));")
	write("0002.sql", "ALTER TABLE ents DROP CONSTRAINT ents_entity_type_check, "+
		"ADD CONSTRAINT ents_entity_type_check2 CHECK (entity_type IN ('person'));")
	d, err := ParseDDL(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, c := range d.Checks {
		got[c.Column] = strings.Join(c.Members, ",")
	}
	if len(d.Checks) != 2 || got["entity_type"] != "person" || got["kind"] != "a,b" || len(d.Problems) != 0 {
		t.Errorf("checks = %+v, problems = %v", d.Checks, d.Problems)
	}
	write("0003.sql", "ALTER TABLE ents ADD CONSTRAINT other CHECK (kind IN ('a'));")
	d, err = ParseDDL(dir)
	if err != nil || len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "ents.kind has two live CHECK lists") {
		t.Errorf("two live lists: %v %v", err, d.Problems)
	}
}

// The local list ratchets against the merge base: a name that is not on the base list fails unless a ruling is cited.
func TestTestIndex_LocalListOnlyShrinks(t *testing.T) {
	plan := miniPlanFile(t)
	root := t.TempDir()
	src := "package a\n\nfunc TestOld(t *testing.T) {}\n\nfunc TestNew(t *testing.T) {}\n"
	if err := os.WriteFile(filepath.Join(root, "a_test.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(root, "local.txt")
	if err := os.WriteFile(local, []byte("TestOld\nTestNew\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	newEntries := func(base []string, ruling string) []string {
		_, problems, err := genTestIndex(plan, root, local, base, ruling)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, p := range problems {
			if strings.Contains(p, "new entry") {
				out = append(out, p)
			}
		}
		return out
	}
	got := newEntries([]string{"TestOld"}, "")
	if len(got) != 1 || !strings.Contains(got[0], "TestNew is a new entry") {
		t.Fatalf("an added entry passed: %v", got)
	}
	if got = newEntries([]string{"TestOld"}, "CONFLICTS #30"); len(got) != 0 {
		t.Errorf("a cited ruling did not allow the entry: %v", got)
	}
	if got = newEntries(nil, ""); len(got) != 0 {
		t.Errorf("no base list must skip the check: %v", got)
	}
}

// The per-edge column rules the trigger enforces appear in the generated statements.
func TestTransitionSQL_CarriesThePerEdgeRules(t *testing.T) {
	shard, err := ParseDDL(filepath.Join(repoRoot(t), "migrations", "shard"))
	if err != nil {
		t.Fatal(err)
	}
	ts, err := readTransitions(shard)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"ready_target":  {"floor_lsn = $copy_end_lsn", "w_final = $w_final"},
		"return_move":   {"floor_lsn = NULL"},
		"freeze_delete": {"move_id IS NULL"},
	}
	seen := map[string]bool{}
	for _, tr := range ts {
		for _, frag := range want[tr.Edge] {
			seen[tr.Edge] = true
			if got := strings.Join(transitionSQL(tr), " "); !strings.Contains(got, frag) {
				t.Errorf("%s statement lacks %q: %s", tr.Edge, frag, got)
			}
		}
	}
	for edge := range want {
		if !seen[edge] {
			t.Errorf("edge %s is not in ownership_transitions", edge)
		}
	}
}

// A witness reads every number its placeholder can match, scaled to the unit, so a short witness shows the other
// figures of the row and the digits of a longer number are not taken for the value.
func TestWitnessNumbers_ReadsEveryMatchingFigure(t *testing.T) {
	cases := []struct {
		witness, text string
		want          []float64
	}{
		{"{v} ms p95", "of 8 ms p95 per arm, 20 ms p95 in all", []float64{8, 20}},
		{"{v:,} ids", "above 4,096 ids and 256 ids", []float64{4096, 256}},
		{"θ = {v:k} k", "θ = 10 k, not θ = 50 k", []float64{10000, 50000}},
		{"{v:M} M", "5.5 M tokens", []float64{5.5e6}},
		{"{v} ms", "was 1,106 ms, now 106 ms", []float64{1106, 106}},
		{"seed {v}\n  of", "seed 2 of ns", []float64{2}},
	}
	for _, c := range cases {
		got, err := witnessNumbers(c.witness, c.text)
		if err != nil || len(got) != len(c.want) {
			t.Errorf("%q in %q: %v, %v; want %v", c.witness, c.text, got, err, c.want)
			continue
		}
		for i := range got {
			if !sameNumber(got[i], c.want[i]) {
				t.Errorf("%q in %q: %v, want %v", c.witness, c.text, got, c.want)
			}
		}
	}
	if _, err := witnessNumbers("no placeholder", "x"); err == nil {
		t.Error("a witness without a placeholder must be refused")
	}
}

// With ENGRAM_REQUIRE_BASE set (CI) a checkout without the merge base is a problem, not a skipped check.
func TestBaseRequired_MissingBaseIsAProblemInCI(t *testing.T) {
	empty := t.TempDir()
	if out, err := exec.Command("git", "-C", empty, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	t.Setenv("ENGRAM_REQUIRE_BASE", "")
	if got := baseRequired(empty); len(got) != 0 {
		t.Errorf("without the switch a missing base is no problem: %v", got)
	}
	t.Setenv("ENGRAM_REQUIRE_BASE", "1")
	if got := baseRequired(empty); len(got) != 1 || !strings.Contains(got[0], "git fetch origin main") {
		t.Errorf("a required, missing base must be a problem: %v", got)
	}
}
