package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Plan is a read-only view of docs/plan/PLAN.md: the sections by number, the register rows of Appendix A and the test
// names. gendocs never writes to it (docs/plan/ is frozen; the code follows the plan, not the other way round).
type Plan struct {
	lines    []string
	sections map[string]string // "5.0" -> the text of the section and its subsections
	rows     map[string]string // "N106", "D3" -> the text of the register row
	secOf    []string          // the numbered section each line belongs to ("" before the first)
	rowOrder []string
}

var (
	headingRe = regexp.MustCompile(`^(#{2,4}) (\d+(?:\.\d+)*)\.? `)
	nRowRe    = regexp.MustCompile(`^\| (N\d+) \|`)
	dRowRe    = regexp.MustCompile(`^### (D\d+)\.`)
	testTok   = regexp.MustCompile(`Test[A-Z]\w+`)
)

// LoadPlan reads and indexes the plan.
func LoadPlan(path string) (*Plan, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	p := &Plan{lines: strings.Split(string(b), "\n"), sections: map[string]string{}, rows: map[string]string{}}
	p.indexSections()
	p.indexRegister()
	if len(p.sections) == 0 || len(p.rows) == 0 {
		return nil, fmt.Errorf("plan: %s has no numbered sections or no register rows", path)
	}
	return p, nil
}

// indexSections stores, for each numbered heading, the lines up to the next heading of the same or a higher level, and
// records for every line the section it belongs to. Fenced code blocks are skipped: the prompt texts of section 6 hold
// lines such as "## MISSION" that are not headings.
func (p *Plan) indexSections() {
	type open struct {
		key   string
		level int
		start int
	}
	var stack []open
	p.secOf = make([]string, len(p.lines))
	closeTo := func(level, end int) {
		for len(stack) > 0 && stack[len(stack)-1].level >= level {
			o := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, dup := p.sections[o.key]; !dup {
				p.sections[o.key] = strings.Join(p.lines[o.start:end], "\n")
			}
		}
	}
	fenced := false
	cur := ""
	for i, l := range p.lines {
		if strings.HasPrefix(l, "```") {
			fenced = !fenced
		}
		if !fenced {
			if m := headingRe.FindStringSubmatch(l); m != nil {
				closeTo(len(m[1]), i)
				stack = append(stack, open{key: m[2], level: len(m[1]), start: i})
				cur = m[2]
			} else if strings.HasPrefix(l, "## ") {
				closeTo(2, i)
				cur = ""
			}
		}
		p.secOf[i] = cur
	}
	closeTo(2, len(p.lines))
}

// indexRegister reads Appendix A: the D rows are the "### Dn." sections, the N rows are table lines.
func (p *Plan) indexRegister() {
	start := -1
	for i, l := range p.lines {
		if strings.HasPrefix(l, "## Appendix A") {
			start = i
			break
		}
	}
	if start < 0 {
		return
	}
	cur := ""
	for _, l := range p.lines[start:] {
		if m := nRowRe.FindStringSubmatch(l); m != nil {
			p.rows[m[1]] = l
			p.rowOrder = append(p.rowOrder, m[1])
			continue
		}
		if m := dRowRe.FindStringSubmatch(l); m != nil {
			cur = m[1]
			p.rows[cur] = l
			p.rowOrder = append(p.rowOrder, cur)
			continue
		}
		if strings.HasPrefix(l, "## ") || strings.HasPrefix(l, "### ") {
			cur = ""
			continue
		}
		if cur != "" {
			p.rows[cur] += "\n" + l
		}
	}
}

// Source returns the text of a register row ("N106", "D3") or a section ("§5.0"), and whether it exists.
func (p *Plan) Source(ref string) (string, bool) {
	if strings.HasPrefix(ref, "§") {
		s, ok := p.sections[strings.TrimPrefix(ref, "§")]
		return s, ok
	}
	s, ok := p.rows[ref]
	return s, ok
}

// section returns the text of a numbered section.
func (p *Plan) section(num string) string { return p.sections[num] }

// TestTokens returns the sorted distinct Test[A-Z]\w+ tokens of text.
func TestTokens(text string) []string {
	seen := map[string]bool{}
	for _, t := range testTok.FindAllString(text, -1) {
		seen[t] = true
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
