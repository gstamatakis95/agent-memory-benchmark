package main

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The test-name index (register N177): "Every test name is defined once, here" (PLAN.md section 8). A name is defined
// when section 8 contains it (the plan writes every test name as code); a token that ends in an underscore is a family
// prefix such as TestFence_, not a name. gendocs fails
//
//   - on a Test[A-Z]\w+ token outside section 8 (the register, sections 1 to 7 and 9 to 12, appendices) that section 8
//     does not define, and on one that section 10.2 cites;
//   - on a test function (func TestX in a Go test file) that section 8 does not define and that the local list does not
//     carry.
//
// The local list (cmd/gendocs/local-tests.txt) holds the names of the Go tests that existed before section 8 covered
// them (the unit tests of M0.1 to M0.8). It is a ratchet against the merge base: an entry that is not on main (or
// origin/main) fails unless GENDOCS_LOCAL_TESTS_RULING names the ruling that allows it (CONFLICTS.md #30), so a new
// test must be named in section 8; an entry that section 8 now defines or that no test file uses any more fails too.
// Without a copy of the list on the merge base (before this file is merged) the growth check is skipped.

// testSite is where a name is used in a Go test file.
type testSite struct {
	File string
	Line int
	Decl bool // always true: only declarations are scanned
}

var declRe = regexp.MustCompile(`^func (Test[A-Z]\w*)\(`)

// scanGoTests finds every test function (func TestX) of the *_test.go files under root. A name that appears only in a
// comment or a string, as a fixture of a test about this very check does, is not a test.
func scanGoTests(root string) (map[string][]testSite, error) {
	out := map[string][]testSite{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		for i, line := range strings.Split(string(b), "\n") {
			if decl := declRe.FindStringSubmatch(line); decl != nil {
				out[decl[1]] = append(out[decl[1]], testSite{File: rel, Line: i + 1, Decl: true})
			}
		}
		return nil
	})
	return out, err
}

func readLocalTests(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return out, nil
}

// planTests holds what the plan says about test names.
type planTests struct {
	defined  map[string]string   // name -> the §8 section key of its first occurrence
	outside  map[string][]string // names in the rest of the plan -> section keys (for the message)
	cited    map[string][]string // names in section 10.2 -> milestone ids
	citedAll []string
}

var milestoneRe = regexp.MustCompile(`\bM\d+\.\d+\b`)

func readPlanTests(plan *Plan) planTests {
	pt := planTests{defined: map[string]string{}, outside: map[string][]string{}, cited: map[string][]string{}}
	// walk the plan line by line to know the section of each line
	for i, l := range plan.lines {
		sec := plan.secOf[i]
		top := strings.SplitN(sec, ".", 2)[0]
		in8 := top == "8"
		in102 := sec == "10.2"
		for _, tok := range testTok.FindAllString(l, -1) {
			if strings.HasSuffix(tok, "_") {
				continue
			}
			if in8 {
				if _, ok := pt.defined[tok]; !ok {
					pt.defined[tok] = sec
				}
				continue
			}
			pt.outside[tok] = append(pt.outside[tok], sec)
			if in102 {
				ms := milestoneRe.FindString(l)
				if strings.HasPrefix(strings.TrimSpace(l), "|") {
					ms = milestoneRe.FindString(strings.SplitN(l, "|", 3)[1])
				}
				pt.cited[tok] = append(pt.cited[tok], ms)
			}
		}
	}
	for t := range pt.cited {
		pt.citedAll = append(pt.citedAll, t)
	}
	sort.Strings(pt.citedAll)
	return pt
}

// genTestIndex renders the index and returns the problems of the three rules.
func genTestIndex(plan *Plan, root, localPath string, base []string, ruling string) (string, []string, error) {
	pt := readPlanTests(plan)
	sites, err := scanGoTests(root)
	if err != nil {
		return "", nil, err
	}
	local, err := readLocalTests(localPath)
	if err != nil {
		return "", nil, err
	}
	var problems []string

	for _, t := range sortedKeys(pt.outside) {
		if _, ok := pt.defined[t]; !ok {
			addf(&problems, "tests: %s is cited in section(s) %s of the plan but section 8 does not "+
				"define it (N177)", t, strings.Join(uniq(pt.outside[t]), ", "))
		}
	}
	for _, t := range pt.citedAll {
		if _, ok := pt.defined[t]; !ok {
			addf(&problems, "tests: %s is cited in section 10.2 (%s) but section 8 does not define it",
				t, strings.Join(uniq(pt.cited[t]), ", "))
		}
	}
	if base != nil && ruling == "" {
		onBase := map[string]bool{}
		for _, t := range base {
			onBase[t] = true
		}
		for _, t := range local {
			if !onBase[t] {
				addf(&problems, "tests: %s is a new entry of cmd/gendocs/local-tests.txt (not on the merge base): "+
					"define the test in section 8, or set GENDOCS_LOCAL_TESTS_RULING to the ruling that allows it "+
					"(CONFLICTS.md #30)", t)
			}
		}
	}
	isLocal := map[string]bool{}
	for _, t := range local {
		isLocal[t] = true
		if _, ok := pt.defined[t]; ok {
			addf(&problems, "tests: %s is in cmd/gendocs/local-tests.txt but section 8 now defines it: "+
				"remove it from the list", t)
		} else if _, ok := sites[t]; !ok {
			addf(&problems, "tests: %s is in cmd/gendocs/local-tests.txt but no Go test file uses it: "+
				"remove it from the list", t)
		}
	}
	for _, t := range sortedKeys(sites) {
		if _, ok := pt.defined[t]; ok || isLocal[t] {
			continue
		}
		s := sites[t][0]
		addf(&problems, "tests: %s (%s:%d) is not defined in section 8 of the plan (N177); define it "+
			"there or, with the orchestrator's approval, list it in cmd/gendocs/local-tests.txt", t, s.File, s.Line)
	}

	d := NewDoc("Test-name index", "`docs/plan/PLAN.md` section 8 (read-only), section 10.2, the Go test files",
		"register N177")
	d.Para("Every test name is defined once, in section 8 of the plan. This index lists each name that section 8 "+
		"defines, the section of its first occurrence, the milestones whose row in section 10.2 cites it, and the Go "+
		"test file that declares it (`-` when no file does yet). `make gen-docs` fails on a name used outside "+
		"section 8 or in a Go test file that section 8 does not define, and on a name that section 10.2 cites and "+
		"section 8 does not define. %d names are defined, %d of them are declared in Go test files today; %d further "+
		"Go test names are kept on the local list `cmd/gendocs/local-tests.txt` until section 8 covers them.",
		len(pt.defined), countDeclared(pt, sites), len(local))
	var rows [][]string
	for _, t := range sortedKeys(pt.defined) {
		file := "-"
		for _, s := range sites[t] {
			if s.Decl {
				file = "`" + s.File + "`"
				break
			}
		}
		ms := "-"
		if cs := uniq(pt.cited[t]); len(cs) > 0 {
			ms = strings.Join(cs, ", ")
			if len(ms) > 24 {
				ms = fmt.Sprintf("%s, +%d", strings.Join(cs[:2], ", "), len(cs)-2)
			}
		}
		rows = append(rows, []string{"`" + t + "`", "§" + pt.defined[t], ms, file})
	}
	d.Table([]string{"Test", "Defined", "Cited by", "Go file"}, rows)
	return d.String(), problems, nil
}

// mergeBaseRef is the ref the ratchet and the released-golden guard compare with: origin/main, else main, "" when the
// checkout has neither (a shallow CI checkout that did not fetch the base). With ENGRAM_REQUIRE_BASE set (CI does) that
// is an error and not a skip, so the checks cannot turn themselves off.
func mergeBaseRef(root string) string {
	for _, ref := range []string{"origin/main", "main"} {
		if exec.Command("git", "-C", root, "rev-parse", "--verify", "--quiet", ref+"^{commit}").Run() == nil {
			return ref
		}
	}
	return ""
}

// baseRequired returns the problem to report when the base is required and absent.
func baseRequired(root string) []string {
	if os.Getenv("ENGRAM_REQUIRE_BASE") == "" || mergeBaseRef(root) != "" {
		return nil
	}
	return []string{"tests: ENGRAM_REQUIRE_BASE is set but neither origin/main nor main exists, so the N177 ratchet " +
		"cannot compare the local list with the merge base: fetch it (git fetch origin main:refs/remotes/origin/main)"}
}

// baseLocalTests reads cmd/gendocs/local-tests.txt as it is on the merge base (origin/main, else main). It returns nil
// when no ref has the file.
func baseLocalTests(root string) []string {
	for _, ref := range []string{"origin/main", "main"} {
		out, err := exec.Command("git", "-C", root, "show", ref+":cmd/gendocs/local-tests.txt").Output()
		if err != nil {
			continue
		}
		names := []string{}
		for _, l := range strings.Split(string(out), "\n") {
			if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
				names = append(names, l)
			}
		}
		return names
	}
	return nil
}

func countDeclared(pt planTests, sites map[string][]testSite) int {
	n := 0
	for t := range pt.defined {
		for _, s := range sites[t] {
			if s.Decl {
				n++
				break
			}
		}
	}
	return n
}

func uniq(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
