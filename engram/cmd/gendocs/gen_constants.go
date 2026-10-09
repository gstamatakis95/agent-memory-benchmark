package main

import (
	"bytes"
	"fmt"
	"go/format"
	"math"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// constantsFile is cmd/gendocs/constants.yaml.
type constantsFile struct {
	Constants []constant `yaml:"constants"`
	Policies  []policy   `yaml:"policies"`
}

type constant struct {
	ID      string   `yaml:"id"`
	Group   string   `yaml:"group"`
	Value   any      `yaml:"value"`
	Unit    string   `yaml:"unit"`
	Rows    []string `yaml:"rows"`
	Witness string   `yaml:"witness"`
	What    string   `yaml:"what"`
	Budget  string   `yaml:"budget"`
	// GUC names the setting of deploy/postgres/gucs.yaml ("lock_timeout_exclusive" or "roles.engram_app.lock_timeout")
	// the value is generated from (N139: one table). Such an entry has no value of its own; GUCAlso lists further
	// settings that must be equal to it.
	GUC     string   `yaml:"guc"`
	GUCAlso []string `yaml:"guc_also"`
}

type policy struct {
	ID           string   `yaml:"id"`
	Name         string   `yaml:"name"`
	Initial      string   `yaml:"initial"`
	Coefficient  float64  `yaml:"coefficient"`
	MaxInterval  string   `yaml:"max_interval"`
	Attempts     int      `yaml:"attempts"`
	NonRetryable []string `yaml:"non_retryable"`
	UsedFor      string   `yaml:"used_for"`
}

// goType and goExpr give the Go type and the constant expression of an entry.
func (c constant) goType() string {
	switch c.Unit {
	case "ms", "s", "min", "h", "d":
		return "time.Duration"
	case "count", "chars", "tokens":
		return "int"
	case "KiB", "MiB", "MB":
		return "int64"
	case "ratio":
		return "float64"
	case "string":
		return "string"
	}
	return ""
}

func (c constant) goExpr() (string, error) {
	num := func() (string, error) {
		switch v := c.Value.(type) {
		case int:
			return strconv.Itoa(v), nil
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		}
		return "", fmt.Errorf("constants: %s: value %v is not a number", c.ID, c.Value)
	}
	n, err := num()
	if c.Unit == "string" {
		s, ok := c.Value.(string)
		if !ok {
			return "", fmt.Errorf("constants: %s: value %v is not a string", c.ID, c.Value)
		}
		return strconv.Quote(s), nil
	}
	if err != nil {
		return "", err
	}
	switch c.Unit {
	case "ms":
		return n + " * time.Millisecond", nil
	case "s":
		return n + " * time.Second", nil
	case "min":
		return n + " * time.Minute", nil
	case "h":
		return n + " * time.Hour", nil
	case "d":
		return n + " * 24 * time.Hour", nil
	case "MB":
		return n + " * 1000 * 1000", nil
	case "KiB":
		return n + " << 10", nil
	case "MiB":
		return n + " << 20", nil
	}
	return n, nil
}

// display renders the value for the document.
func (c constant) display() string {
	switch v := c.Value.(type) {
	case string:
		return "`" + v + "`"
	case float64:
		return "`" + strconv.FormatFloat(v, 'g', -1, 64) + " " + c.Unit + "`"
	case int:
		if c.Unit == "count" || c.Unit == "chars" || c.Unit == "tokens" {
			return "`" + groupDigits(v) + "`" + map[string]string{"chars": " chars", "tokens": " tokens"}[c.Unit]
		}
		return "`" + groupDigits(v) + " " + c.Unit + "`"
	}
	return fmt.Sprint(c.Value)
}

func groupDigits(n int) string {
	if n < 10000 {
		return strconv.Itoa(n)
	}
	return commaInt(n)
}

// commaInt writes n with a comma every three digits, from 1,000 up.
func commaInt(n int) string {
	s := strconv.Itoa(n)
	var out []string
	for len(s) > 3 {
		out = append([]string{s[len(s)-3:]}, out...)
		s = s[:len(s)-3]
	}
	return strings.Join(append([]string{s}, out...), ",")
}

// numText renders a numeric value the way the plan writes it: the shortest decimal.
func numText(v any) (string, bool) {
	switch x := v.(type) {
	case int:
		return strconv.Itoa(x), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	}
	return "", false
}

// expandWitness fills the placeholders of the witness text from the value: {v} the number, {v:,} with thousands
// commas ("2,000"), {v:k} divided by 1,000 ("10" for "10 k") and {v:M} divided by 1,000,000 ("5.5" for "5.5 M"). The
// text that must occur in the cited rows is therefore derived from the value, so a value edited alone fails.
func expandWitness(c constant) (string, error) {
	if !strings.Contains(c.Witness, "{v") {
		return "", fmt.Errorf("%s: the witness %q has no {v} placeholder, so it would not test the value", c.ID,
			c.Witness)
	}
	if s, ok := c.Value.(string); ok {
		return strings.ReplaceAll(c.Witness, "{v}", s), nil
	}
	n, ok := numText(c.Value)
	if !ok {
		return "", fmt.Errorf("%s: value %v is not a number or a string", c.ID, c.Value)
	}
	f, _ := strconv.ParseFloat(n, 64)
	repl := strings.NewReplacer(
		"{v}", n,
		"{v:,}", commaInt(int(f)),
		"{v:k}", strconv.FormatFloat(f/1e3, 'g', -1, 64),
		"{v:M}", strconv.FormatFloat(f/1e6, 'g', -1, 64),
	)
	return repl.Replace(c.Witness), nil
}

var (
	valuePlaceholderRe = regexp.MustCompile(`\{v(?::([,kM]))?\}`)
	// numberPat is one number as the plan writes it: digits with thousands commas and an optional fraction.
	numberPat = `(\d[\d,]*(?:\.\d+)?)`
	scales    = map[string]float64{"": 1, ",": 1, "k": 1e3, "M": 1e6}
)

// witnessNumbers finds the witness in text with every placeholder read as "any number" and returns the values the
// placeholders captured, scaled to the unit of the constant ({v:k} and {v:M} multiply). It is the plan-side reading of
// a constant: a witness that matches two different numbers is ambiguous, one that matches none is not in the text.
func witnessNumbers(witness, text string) ([]float64, error) {
	var re strings.Builder
	var scale []float64
	rest := squashWS(witness)
	for {
		loc := valuePlaceholderRe.FindStringSubmatchIndex(rest)
		if loc == nil {
			re.WriteString(regexp.QuoteMeta(rest))
			break
		}
		re.WriteString(regexp.QuoteMeta(rest[:loc[0]]) + numberPat)
		kind := ""
		if loc[2] >= 0 {
			kind = rest[loc[2]:loc[3]]
		}
		scale = append(scale, scales[kind])
		rest = rest[loc[1]:]
	}
	if len(scale) == 0 {
		return nil, fmt.Errorf("the witness %q has no {v} placeholder", witness)
	}
	rx, err := regexp.Compile(re.String())
	if err != nil {
		return nil, err
	}
	var out []float64
	for _, m := range rx.FindAllStringSubmatch(squashWS(text), -1) {
		for i, g := range m[1:] {
			f, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimRight(g, ","), ",", ""), 64)
			if err != nil {
				return nil, fmt.Errorf("witness %q captured %q: %w", witness, g, err)
			}
			out = append(out, f*scale[i])
		}
	}
	return out, nil
}

// sameNumber compares two figures of the register: equal up to the rounding of the scaling.
func sameNumber(a, b float64) bool { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }

// citedText joins the register rows or sections a constant cites; ok is false when the plan lacks one.
func citedText(plan *Plan, rows []string) (string, []string) {
	var union, missing []string
	for _, r := range rows {
		text, ok := plan.Source(r)
		if !ok {
			missing = append(missing, r)
			continue
		}
		union = append(union, text)
	}
	return strings.Join(union, "\n"), missing
}

// planValue is the one number the cited rows give for a numeric constant, read through its witness. It does not look at
// the value of the table, so a test can hold the generated constants against the plan.
func planValue(plan *Plan, c constant) (float64, error) {
	text, missing := citedText(plan, c.Rows)
	if len(missing) > 0 {
		return 0, fmt.Errorf("%s cites %s, which the plan does not have", c.ID, strings.Join(missing, ", "))
	}
	nums, err := witnessNumbers(c.Witness, text)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", c.ID, err)
	}
	if len(nums) == 0 {
		return 0, fmt.Errorf("%s: the witness %q occurs in none of %s", c.ID, c.Witness, strings.Join(c.Rows, ", "))
	}
	for _, n := range nums[1:] {
		if !sameNumber(n, nums[0]) {
			return 0, fmt.Errorf("%s: the witness %q matches more than one number in %s (%v and %v): lengthen it",
				c.ID, c.Witness, strings.Join(c.Rows, ", "), nums[0], n)
		}
	}
	return nums[0], nil
}

// witnessText is the text of a retry-policy row the plan must contain, built from the policy's own fields in the
// notation of section 5.0: "| `P-db` | 500 ms / 2.0 / 10 s / 10" (the closing bar is left off: "unlimited" is
// followed by words).
func (p policy) witnessText() string {
	dur := func(s string) string {
		d, err := time.ParseDuration(s)
		switch {
		case err != nil:
			return s
		case d == 0:
			return "—"
		case d%time.Second == 0:
			return fmt.Sprintf("%d s", d/time.Second)
		}
		return fmt.Sprintf("%d ms", d/time.Millisecond)
	}
	coeff, att := strconv.FormatFloat(p.Coefficient, 'f', 1, 64), strconv.Itoa(p.Attempts)
	if p.Coefficient == 0 {
		coeff = "—"
	}
	if p.Attempts == 0 {
		att = "unlimited"
	}
	return fmt.Sprintf("| `%s` | %s / %s / %s / %s", p.Name, dur(p.Initial), coeff, dur(p.MaxInterval), att)
}

// gucConstant converts the production value of a GUC ("35s", "30min", "100") to the unit of a constant.
func gucConstant(g *gucFile, ref, unit string) (int, error) {
	raw, ok := g.lookup(ref)
	if !ok {
		return 0, fmt.Errorf("gucs.yaml has no setting %s", ref)
	}
	unitDur := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "min": time.Minute, "h": time.Hour}
	num := strings.TrimRightFunc(raw, func(r rune) bool { return r >= 'a' && r <= 'z' })
	suffix := strings.TrimPrefix(raw, num)
	n, err := strconv.Atoi(num)
	if err != nil {
		return 0, fmt.Errorf("%s = %q is not a number with an optional unit", ref, raw)
	}
	target, isTime := unitDur[unit]
	if !isTime {
		if suffix != "" {
			return 0, fmt.Errorf("%s = %q has a unit but the constant is %s", ref, raw, unit)
		}
		return n, nil
	}
	have := target // a plain number is in the constant's unit
	if suffix != "" {
		if have, ok = unitDur[suffix]; !ok {
			return 0, fmt.Errorf("%s = %q has the unknown unit %q", ref, raw, suffix)
		}
	}
	return int(time.Duration(n) * have / target), nil
}

func squashWS(s string) string { return strings.Join(strings.Fields(s), " ") }

// loadConstants reads and validates the table against the plan; the returned problems are drift between the table and
// the register (a missing row or section, a witness the cited texts do not contain, a duplicate id, a connection budget
// that does not add up).
func loadConstants(path string, plan *Plan, gucs *gucFile) (*constantsFile, []string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var f constantsFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, nil, fmt.Errorf("constants: %s: %w", path, err)
	}
	var problems []string
	for i := range f.Constants {
		c := &f.Constants[i]
		if c.GUC == "" {
			continue
		}
		if c.Value != nil {
			addf(&problems, "constants: %s is generated from the GUC table (%s) and must not state a value", c.ID,
				c.GUC)
		}
		v, err := gucConstant(gucs, c.GUC, c.Unit)
		if err != nil {
			addf(&problems, "constants: %s: %v", c.ID, err)
			continue
		}
		c.Value = v
		for _, also := range c.GUCAlso {
			w, err := gucConstant(gucs, also, c.Unit)
			switch {
			case err != nil:
				addf(&problems, "constants: %s: %s: %v", c.ID, also, err)
			case w != v:
				addf(&problems, "constants: %s: %s is %v and %s is %v, they must be equal", c.ID, c.GUC, v, also, w)
			}
		}
	}
	ids := map[string]bool{}
	check := func(id string, rows []string, witness string) {
		if ids[id] {
			problems = append(problems, "constants: duplicate id "+id)
		}
		ids[id] = true
		if len(rows) == 0 || witness == "" {
			addf(&problems, "constants: %s needs rows and a witness", id)
			return
		}
		union, missing := citedText(plan, rows)
		for _, r := range missing {
			addf(&problems, "constants: %s cites %s, which the plan does not have", id, r)
		}
		if len(missing) == 0 && !strings.Contains(squashWS(union), squashWS(witness)) {
			addf(&problems, "constants: %s: the text %q occurs in none of %s", id, witness, strings.Join(rows, ", "))
		}
	}
	// checkNumber holds a numeric constant to the plan: the witness, with the placeholder read as any number, must
	// match in the cited rows and every match must carry the value of the table (not another figure of the row).
	checkNumber := func(c constant, want float64) {
		if ids[c.ID] {
			problems = append(problems, "constants: duplicate id "+c.ID)
		}
		ids[c.ID] = true
		union, missing := citedText(plan, c.Rows)
		for _, r := range missing {
			addf(&problems, "constants: %s cites %s, which the plan does not have", c.ID, r)
		}
		if len(missing) > 0 {
			return
		}
		nums, err := witnessNumbers(c.Witness, union)
		if err != nil {
			addf(&problems, "constants: %s: %v", c.ID, err)
			return
		}
		var found, other []string
		for _, n := range nums {
			txt := strconv.FormatFloat(n, 'g', -1, 64)
			switch {
			case sameNumber(n, want):
				found = append(found, txt)
			case !slices.Contains(other, txt):
				other = append(other, txt)
			}
		}
		switch {
		case len(found) == 0:
			expanded, _ := expandWitness(c)
			seen := ""
			if len(other) > 0 {
				seen = " (the witness matches " + strings.Join(other, ", ") + " there)"
			}
			addf(&problems, "constants: %s: the text %q occurs in none of %s%s", c.ID, expanded,
				strings.Join(c.Rows, ", "), seen)
		case len(other) > 0:
			addf(&problems, "constants: %s: the witness %q matches %s as well as %s in %s: it is ambiguous, "+
				"lengthen it until the value is the only number it can match", c.ID, c.Witness,
				strings.Join(other, ", "), found[0], strings.Join(c.Rows, ", "))
		}
	}
	units := map[string]bool{"ms": true, "s": true, "min": true, "h": true, "d": true, "count": true, "chars": true,
		"tokens": true, "KiB": true, "MiB": true, "MB": true, "ratio": true, "string": true}
	sum, budget := 0, 0
	for _, c := range f.Constants {
		w, err := expandWitness(c)
		if err != nil {
			addf(&problems, "constants: %v", err)
			w = c.Witness
		}
		if _, isString := c.Value.(string); isString {
			check(c.ID, c.Rows, w)
		} else if n, ok := numText(c.Value); ok {
			f, _ := strconv.ParseFloat(n, 64)
			checkNumber(c, f)
		} else {
			check(c.ID, c.Rows, w)
		}
		if !units[c.Unit] || c.Group == "" || c.What == "" {
			addf(&problems, "constants: %s needs a known unit, a group and a description", c.ID)
		}
		if c.Budget == "connections" {
			if v, ok := c.Value.(int); ok {
				sum += v
			}
		}
		if c.ID == "ConnectionBudgetAtP4" {
			budget, _ = c.Value.(int)
		}
	}
	// the connection budget of N164: 55 + 2P connections at P = 4 are the pools, the subject leases and the singles
	byID := map[string]int{}
	for _, c := range f.Constants {
		if v, ok := c.Value.(int); ok {
			byID[c.ID] = v
		}
	}
	total := sum + byID["SubjectConnsPerProcess"]*byID["APIProcessesPerShard"]
	if _, has := byID["ConnectionBudgetAtP4"]; has && total != budget {
		addf(&problems, "constants: the connection components sum to %d (pools and singles %d + "+
			"subject %d x P %d), ConnectionBudgetAtP4 is %d", total, sum, byID["SubjectConnsPerProcess"],
			byID["APIProcessesPerShard"], budget)
	}
	if _, has := byID["MaxConnections"]; has && budget >= byID["MaxConnections"] {
		addf(&problems, "constants: the budget %d does not fit max_connections %d", budget,
			byID["MaxConnections"])
	}
	lo, mid, hi := byID["ArmCapLow"], byID["ArmCapMid"], byID["ArmCapHigh"]
	if _, has := byID["ArmCapHigh"]; has && (lo >= mid || mid >= hi) {
		problems = append(problems, "constants: the arm caps must increase from LOW to HIGH")
	}
	for _, p := range f.Policies {
		check(p.ID, []string{"§5.0"}, p.witnessText())
		for _, d := range []string{p.Initial, p.MaxInterval} {
			if _, err := time.ParseDuration(d); err != nil {
				addf(&problems, "constants: policy %s: %v", p.ID, err)
			}
		}
	}
	return &f, problems, nil
}

// genConstantsDoc renders docs/generated/constants.md.
func genConstantsDoc(f *constantsFile) string {
	d := NewDoc("Register constants",
		"`cmd/gendocs/constants.yaml`, checked against the register (Appendix A) of PLAN.md",
		"`internal/gen/constants` is generated from the same table")
	d.Para("Code reads these values only through the generated Go package `internal/gen/constants`. Every entry " +
		"cites its register rows (N and D) or the section of the plan that states it, and the generator fails when " +
		"the cited text does not contain the number, so a change of the register that is not followed here (or " +
		"the reverse) breaks `make gen-docs`. The settings that `deploy/postgres/gucs.yaml` also states (the " +
		"exclusive lock timeout, `max_connections`, the TCP keepalives, the role timeouts) have no value here: they " +
		"are generated from that table (N139) and the plan check runs on the generated value.")
	groups := map[string][]constant{}
	for _, c := range f.Constants {
		groups[c.Group] = append(groups[c.Group], c)
	}
	titles := map[string]string{"recall": "Recall path", "vectors": "Vectors and storage", "sizing": "Sizing",
		"pools": "Connections", "locks": "Fence, locks and timeouts", "pipeline": "Workflows, outbox and payloads",
		"retain": "Retain", "prompts": "Prompts", "gateway": "Gateway and throughput"}
	order := []string{"recall", "vectors", "sizing", "pools", "locks", "pipeline", "retain", "prompts", "gateway"}
	for _, g := range order {
		cs := groups[g]
		if len(cs) == 0 {
			continue
		}
		d.H2(titles[g])
		var rows [][]string
		for _, c := range cs {
			rows = append(rows, []string{"`" + c.ID + "`", c.display(), strings.Join(c.Rows, ", ")})
		}
		d.Table([]string{"Constant", "Value", "Register"}, rows)
		for _, c := range cs {
			what := c.What
			if c.GUC != "" {
				what += " (generated from deploy/postgres/gucs.yaml: " + c.GUC + ")"
			}
			d.Bullet("`" + c.ID + "`: " + what)
		}
		d.EndList()
	}
	d.H2("Retry policies")
	d.Para("Notation `initial / coefficient / max interval / max attempts / non-retryable error types` " +
		"(PLAN.md 5.0); 0 attempts means unlimited within the activity's `ScheduleToClose`.")
	var rows [][]string
	for _, p := range f.Policies {
		rows = append(rows, []string{"`" + p.Name + "`", p.Initial, strconv.FormatFloat(p.Coefficient, 'f', 1, 64),
			p.MaxInterval, strconv.Itoa(p.Attempts)})
	}
	d.Table([]string{"Policy", "Initial", "Coefficient", "Max interval", "Attempts"}, rows)
	for _, p := range f.Policies {
		d.Bullet("`" + p.Name + "`: " + p.UsedFor + ". Non-retryable: " + strings.Join(p.NonRetryable, ", ") + ".")
	}
	d.EndList()
	return d.String()
}

// genConstantsGo renders internal/gen/constants/constants_gen.go.
func genConstantsGo(f *constantsFile) ([]byte, error) {
	var b strings.Builder
	b.WriteString("// Code generated by cmd/gendocs from cmd/gendocs/constants.yaml. DO NOT EDIT.\n\n")
	pkgDoc := "Package constants is the one table of register constants (PLAN.md Appendix A): the numbers that " +
		"the plan fixes in the decision register, such as the rerank-skip reserve, the pool sizes and the retry " +
		"policies. Code reads them here and nowhere else; cmd/gendocs checks every entry against the register " +
		"text it cites."
	for _, l := range wrap(pkgDoc, 118, "// ", "// ") {
		b.WriteString(l + "\n")
	}
	b.WriteString("package constants\n\nimport \"time\"\n\n")
	groups := map[string][]constant{}
	var order []string
	for _, c := range f.Constants {
		if _, ok := groups[c.Group]; !ok {
			order = append(order, c.Group)
		}
		groups[c.Group] = append(groups[c.Group], c)
	}
	for _, g := range order {
		b.WriteString("// " + strings.ToUpper(g[:1]) + g[1:] + " constants.\nconst (\n")
		for _, c := range groups[g] {
			expr, err := c.goExpr()
			if err != nil {
				return nil, err
			}
			for _, l := range wrap(c.ID+" is "+lowerFirst(c.What)+". Register: "+strings.Join(c.Rows, ", ")+".", 116,
				"\t// ", "\t// ") {
				b.WriteString(l + "\n")
			}
			b.WriteString("\t" + c.ID + " " + c.goType() + " = " + expr + "\n")
		}
		b.WriteString(")\n\n")
	}
	b.WriteString("// RetryPolicy is a named retry policy of PLAN.md 5.0. MaxAttempts 0 means unlimited within the " +
		"activity's\n// ScheduleToClose; a zero Initial and Coefficient mean the policy has no backoff.\n" +
		"type RetryPolicy struct {\n\tName         string\n\tInitial      time.Duration\n" +
		"\tCoefficient  float64\n\tMaxInterval  time.Duration\n\tMaxAttempts  int\n\tNonRetryable []string\n}\n\n")
	b.WriteString("// The named retry policies.\nvar (\n")
	for _, p := range f.Policies {
		ini, _ := time.ParseDuration(p.Initial)
		mx, _ := time.ParseDuration(p.MaxInterval)
		nr := make([]string, len(p.NonRetryable))
		for i, s := range p.NonRetryable {
			nr[i] = strconv.Quote(s)
		}
		for _, l := range wrap(p.ID+" is "+p.Name+": "+p.UsedFor+".", 116, "\t// ", "\t// ") {
			b.WriteString(l + "\n")
		}
		fmt.Fprintf(&b, "\t%s = RetryPolicy{\n\t\tName: %q, Initial: %s, Coefficient: %s,\n"+
			"\t\tMaxInterval: %s, MaxAttempts: %d,\n\t\tNonRetryable: []string{%s},\n\t}\n", p.ID, p.Name, durExpr(ini),
			strconv.FormatFloat(p.Coefficient, 'f', 1, 64), durExpr(mx), p.Attempts, strings.Join(nr, ", "))
	}
	b.WriteString(")\n\n")
	b.WriteString("// Entry describes one constant for tests and tools: the constant itself, its unit and its " +
		"register rows.\ntype Entry struct {\n\tID, Group, Unit string\n\tValue           any\n" +
		"\tRows            []string\n}\n\n")
	b.WriteString("// All lists every constant in the order of the table.\nvar All = []Entry{\n")
	for _, c := range f.Constants {
		rows := make([]string, len(c.Rows))
		for i, r := range c.Rows {
			rows[i] = strconv.Quote(r)
		}
		fmt.Fprintf(&b, "\t{ID: %q, Group: %q, Value: %s, Unit: %q, Rows: []string{%s}},\n", c.ID, c.Group, c.ID,
			c.Unit, strings.Join(rows, ", "))
	}
	b.WriteString("}\n")
	out, err := format.Source([]byte(b.String()))
	if err != nil {
		return nil, fmt.Errorf("constants: generated Go does not parse: %w\n%s", err, b.String())
	}
	return out, nil
}

// durExpr renders a duration as a Go expression with the largest exact unit.
func durExpr(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d%time.Hour == 0:
		return fmt.Sprintf("%d * time.Hour", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%d * time.Minute", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%d * time.Second", d/time.Second)
	case d%time.Millisecond == 0:
		return fmt.Sprintf("%d * time.Millisecond", d/time.Millisecond)
	}
	return fmt.Sprintf("%d * time.Nanosecond", d)
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	// keep identifiers and acronyms as written
	if len(s) > 1 && s[1] >= 'A' && s[1] <= 'Z' {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}
