// Command gendocs generates the documents that PLAN.md says are generated and the checks that go with them:
//
//	docs/generated/ownership-transitions.md   the ownership edges and their statements, from the shard DDL
//	docs/generated/events.md                  the outbox events, from events.proto
//	docs/generated/table-classes.md           the class tag of every shard table (insert-only, mutable, ...)
//	docs/generated/enum-checks.md             the enum types and CHECK lists of both DDLs
//	docs/generated/errors.md                  the error details, from errors.proto, internal/errs and section 4.1.6
//	docs/generated/gucs.md                    the Postgres settings, from deploy/postgres/gucs.yaml
//	docs/generated/constants.md               the register constants, from cmd/gendocs/constants.yaml
//	docs/generated/enum-lint.md               the concepts of the enum lint and their members
//	docs/generated/prompts.md                 the prompt registry and its pins
//	docs/generated/test-index.md              the test-name index of section 8
//	internal/gen/constants/constants_gen.go   the Go table of the register constants
//
// It also fails (non-zero exit, every problem listed) when the sources disagree: an enum concept spelled differently in
// two places, a register constant the plan does not state, a prompt file edited under its pin, an error detail whose
// gRPC code differs between the proto comment, internal/errs and section 4.1.6, a Test name used outside section 8 that
// section 8 does not define. The plan under docs/plan is read, never written.
//
//	gendocs [flags]            generate everything
//	gendocs -check [flags]     generate in memory and fail when any output file on disk differs (no write)
//	gendocs pin <name>/v<N>    write the HASH pin of a new prompt version
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type options struct {
	root, plan, out, goOut, gucs, localTests string
	check                                    bool
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gendocs:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "pin" {
		return runPin(".", args[1:])
	}
	var o options
	fs := flag.NewFlagSet("gendocs", flag.ContinueOnError)
	fs.StringVar(&o.root, "root", ".", "repository root")
	fs.StringVar(&o.plan, "plan", "docs/plan/PLAN.md", "the plan (read only), relative to -root")
	fs.StringVar(&o.out, "out", "docs/generated", "output directory, relative to -root")
	fs.StringVar(&o.goOut, "go-out", "internal/gen/constants/constants_gen.go", "the generated Go constants table")
	fs.StringVar(&o.gucs, "gucs", defaultGUCs, "the Postgres settings table, relative to -root or absolute")
	fs.StringVar(&o.localTests, "local-tests", "cmd/gendocs/local-tests.txt", "Go tests kept outside section 8")
	fs.BoolVar(&o.check, "check", false, "write nothing; fail when an output file differs from the generated text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return generate(o)
}

const defaultGUCs = "deploy/postgres/gucs.yaml"

// outputs maps a path relative to the root to its content.
type outputs map[string][]byte

func generate(o options) error {
	at := func(p string) string {
		if filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(o.root, p)
	}
	plan, err := LoadPlan(at(o.plan))
	if err != nil {
		return err
	}
	protos, err := LoadProtos(at("proto"))
	if err != nil {
		return err
	}
	shard, err := ParseDDL(at("migrations/shard"))
	if err != nil {
		return err
	}
	catalog, err := ParseDDL(at("migrations/catalog"))
	if err != nil {
		return err
	}
	outs := outputs{}
	var problems []string
	problems = append(problems, shard.Problems...)
	problems = append(problems, catalog.Problems...)
	add := func(name, content string) { outs[filepath.ToSlash(filepath.Join(o.out, name))] = []byte(content) }
	outDir := filepath.ToSlash(o.out)

	own, err := genOwnership(shard)
	if err != nil {
		return err
	}
	add("ownership-transitions.md", own)

	ev, err := genEvents(protos)
	if err != nil {
		return err
	}
	add("events.md", ev)

	cls, probs, err := genClasses(shard)
	if err != nil {
		return err
	}
	add("table-classes.md", cls)
	problems = append(problems, probs...)

	chk, err := genChecks(shard, catalog)
	if err != nil {
		return err
	}
	add("enum-checks.md", chk)

	er, probs, err := genErrors(protos, plan)
	if err != nil {
		return err
	}
	add("errors.md", er)
	problems = append(problems, probs...)

	if err := addGUCs(o, at, add); err != nil {
		return err
	}

	gucs, err := loadGUCs(at(o.gucs))
	if err != nil {
		return err
	}
	cf, probs, err := loadConstants(at("cmd/gendocs/constants.yaml"), plan, gucs)
	if err != nil {
		return err
	}
	problems = append(problems, probs...)
	add("constants.md", genConstantsDoc(cf))
	if len(probs) == 0 { // a table with problems may be incomplete; the problems are listed below
		goSrc, err := genConstantsGo(cf)
		if err != nil {
			return err
		}
		outs[o.goOut] = goSrc
	}

	lint, probs, err := lintEnums(at("cmd/gendocs/enums.yaml"),
		enumInputs{root: o.root, shard: shard, cat: catalog, protos: protos}, plan)
	if err != nil {
		return err
	}
	add("enum-lint.md", lint)
	problems = append(problems, probs...)

	ps, probs, err := genPrompts()
	if err != nil {
		return err
	}
	add("prompts.md", ps)
	problems = append(problems, probs...)

	ti, probs, err := genTestIndex(plan, o.root, at(o.localTests), baseLocalTests(o.root),
		os.Getenv("GENDOCS_LOCAL_TESTS_RULING"))
	if err != nil {
		return err
	}
	add("test-index.md", ti)
	problems = append(problems, probs...)
	problems = append(problems, baseRequired(o.root)...)

	add("README.md", readme(outs, outDir))

	if len(problems) > 0 {
		sort.Strings(problems)
		fmt.Fprintf(os.Stderr, "gendocs: %d problem(s), nothing was written:\n", len(problems))
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "  "+p)
		}
		return fmt.Errorf("the sources disagree (see above)")
	}
	stale, err := write(o, at, outs)
	if err != nil {
		return err
	}
	if o.check && len(stale) > 0 {
		return fmt.Errorf("generated files are out of date (run `make gen-docs`): %s", strings.Join(stale, ", "))
	}
	return nil
}

// addGUCs generates gucs.md from the settings table (deploy/postgres/gucs.yaml, M0.4). A missing table is an error:
// skipping it would take gucs.md out of the hand-edit check.
func addGUCs(o options, at func(string) string, add func(name, content string)) error {
	s, err := genGUCs(at(o.gucs))
	if err != nil {
		return err
	}
	add("gucs.md", s)
	return nil
}

func readme(outs outputs, outDir string) string {
	d := NewDoc("Generated documents")
	d.Para("`make gen-docs` (cmd/gendocs) writes these files from the migrations, the protos, the plan, the prompt " +
		"files and the tables under cmd/gendocs. They are never edited by hand: CI regenerates them and fails on a " +
		"diff.")
	var names []string
	for p := range outs {
		if strings.HasPrefix(p, outDir+"/") && p != outDir+"/README.md" {
			names = append(names, strings.TrimPrefix(p, outDir+"/"))
		}
	}
	sort.Strings(names)
	for _, n := range names {
		d.Bullet("`" + n + "`")
	}
	d.Bullet("`internal/gen/constants/constants_gen.go` (Go table of the register constants)")
	d.EndList()
	return d.String()
}

// write stores the outputs (or, with -check, compares them) and returns the files that differ from the disk. Files of
// the output directory that gendocs no longer produces are removed.
func write(o options, at func(string) string, outs outputs) ([]string, error) {
	var stale []string
	for _, rel := range sortedKeysB(outs) {
		path := at(rel)
		old, err := os.ReadFile(path)
		if err == nil && string(old) == string(outs[rel]) {
			continue
		}
		stale = append(stale, rel)
		if o.check {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, outs[rel], 0o644); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(at(o.out))
	if err == nil {
		for _, e := range entries {
			rel := filepath.ToSlash(filepath.Join(o.out, e.Name()))
			if _, ok := outs[rel]; !ok && !e.IsDir() {
				stale = append(stale, rel+" (no longer generated)")
				if !o.check {
					if err := os.Remove(at(rel)); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return stale, nil
}

func sortedKeysB(m outputs) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
