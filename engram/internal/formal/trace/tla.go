package trace

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

var opDef = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_]*)(?:\(([^)]*)\))?\s*==`)

// OperatorArities lists the top-level operators a TLA+ module source defines and their number of parameters. It is how
// the converter checks that a logged action is an operator of the specification and has as many parameters as the event
// supplies.
func OperatorArities(src io.Reader) (map[string]int, error) {
	out := map[string]int{}
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		m := opDef.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		n := 0
		if strings.TrimSpace(m[2]) != "" {
			n = strings.Count(m[2], ",") + 1
		}
		out[m[1]] = n
	}
	return out, sc.Err()
}

// Options configure ToTLA.
type Options struct {
	// Operators are the operators of the specification module (OperatorArities); when non-nil every event must name one
	// of them with the matching number of parameters, and no model value may collide with one.
	Operators map[string]int
	// Domains are the parameter domains of the specification's parameterised actions (ActionDomains). A log that
	// records states (state mode) is replayed through `Next /\ A` for the logged action A of each step, with A's
	// parameters existentially quantified over these domains, so Domains is required for every parameterised action
	// such a log names.
	Domains map[string][]string
	// BaseConfig is the text of the configuration the logged behaviour belongs to; its CONSTANTS and INVARIANTS carry
	// over to the generated configuration. When empty only the module is produced.
	BaseConfig string
}

// Result is the generated module and, with a base configuration, the TLC configuration that checks it.
type Result struct {
	Module string // the module name, <Spec>Trace
	TLA    string
	Config string
}

// ToTLA turns a logged behaviour into the TraceNext module of its specification (PLAN.md section 7.4(b)): a module that
// EXTENDS the specification, holds the events as a constant sequence TraceEvents and replays them through TraceNext,
// one action per step, with the specification's Init and its action operators. The log names the actions and their
// parameters, not the choices inside an action, so the replay is nondeterministic and TLC explores every way the events
// can be executed. The invariants of the configuration are checked on every state of every replay. The generated
// invariant TraceNotDone fails exactly when some execution consumed the whole log, which is how acceptance is observed:
// TLC ends with "Invariant TraceNotDone is violated" when the log is a behaviour of the specification and no invariant
// of the configuration failed on the way; it ends with a violated specification invariant when one failed; and it ends
// with "No error has been found" when no execution consumes the log. The invariant fails one step after the last event
// (len(events)+2 states) so that the invariants of every state of the replay are checked first.
func ToTLA(events []Event, opt Options) (Result, error) {
	if len(events) == 0 {
		return Result{}, fmt.Errorf("trace: no events")
	}
	spec := events[0].Spec
	if !validIdent(spec) {
		return Result{}, fmt.Errorf("trace: spec %q is not a module name", spec)
	}
	mod := spec + "Trace"
	// state mode: every event carries a state and none has args; replay through Next, each step pinned to its state
	stateMode := true
	for _, ev := range events {
		stateMode = stateMode && ev.State != nil && len(ev.Args) == 0
	}
	arity := map[string]int{}
	var order []string
	values := map[string]bool{}
	rows := make([]string, len(events))
	for i, ev := range events {
		if !validIdent(ev.Action) {
			return Result{}, fmt.Errorf("trace: event %d: %q is not an operator name", ev.Seq, ev.Action)
		}
		if stateMode {
			if opt.Operators == nil {
				return Result{}, fmt.Errorf("trace: event %d: a log of states is replayed through the named action, "+
					"which needs the operator table of %s (Options.Operators)", ev.Seq, spec)
			}
			if _, ok := opt.Operators[ev.Action]; !ok {
				return Result{}, fmt.Errorf("trace: event %d: %s is not an operator of %s", ev.Seq, ev.Action, spec)
			}
			if _, ok := arity[ev.Action]; !ok {
				arity[ev.Action] = opt.Operators[ev.Action]
				order = append(order, ev.Action)
			}
			for _, v := range ev.State {
				modelValues(v, values)
			}
			rows[i] = fmt.Sprintf("  [act |-> %q, par |-> << >>]", ev.Action)
			continue
		}
		if want, ok := arity[ev.Action]; ok && want != len(ev.Args) {
			return Result{}, fmt.Errorf("trace: event %d: %s takes %d parameters in event %d and %d here",
				ev.Seq, ev.Action, want, firstSeq(events, ev.Action), len(ev.Args))
		} else if !ok {
			arity[ev.Action] = len(ev.Args)
			order = append(order, ev.Action)
		}
		if opt.Operators != nil {
			n, ok := opt.Operators[ev.Action]
			if !ok {
				return Result{}, fmt.Errorf("trace: event %d: %s is not an operator of %s", ev.Seq, ev.Action, spec)
			}
			if n != len(ev.Args) {
				return Result{}, fmt.Errorf("trace: event %d: %s takes %d parameters, the event has %d",
					ev.Seq, ev.Action, n, len(ev.Args))
			}
		}
		parts, err := renderAll(ev.Args)
		if err != nil {
			return Result{}, fmt.Errorf("trace: event %d: %w", ev.Seq, err)
		}
		for _, a := range ev.Args {
			modelValues(a, values)
		}
		par := "<< >>"
		if len(parts) > 0 {
			par = "<<" + strings.Join(parts, ", ") + ">>"
		}
		rows[i] = fmt.Sprintf("  [act |-> %q, par |-> %s]", ev.Action, par)
	}
	specConsts := map[string]bool{}
	if opt.BaseConfig != "" {
		for _, n := range configConstantNames(opt.BaseConfig) {
			specConsts[n] = true
		}
	}
	var newValues []string
	for v := range values {
		if specConsts[v] {
			continue
		}
		if _, clash := opt.Operators[v]; clash && opt.Operators != nil {
			return Result{}, fmt.Errorf("trace: model value %s collides with an operator of %s", v, spec)
		}
		newValues = append(newValues, v)
	}
	sort.Strings(newValues)

	var stateRows []string
	var vars []string
	if stateMode {
		for k := range events[0].State {
			vars = append(vars, k)
		}
		sort.Strings(vars)
		for _, ev := range events {
			if len(ev.State) != len(vars) {
				return Result{}, fmt.Errorf("trace: event %d: %d variables, event 1 has %d",
					ev.Seq, len(ev.State), len(vars))
			}
			fields := make([]string, len(vars))
			for j, k := range vars {
				v, ok := ev.State[k]
				if !ok || !validIdent(k) {
					return Result{}, fmt.Errorf("trace: event %d: variable %q is missing or not an identifier",
						ev.Seq, k)
				}
				r, err := RenderTLA(v)
				if err != nil {
					return Result{}, fmt.Errorf("trace: event %d, variable %s: %w", ev.Seq, k, err)
				}
				fields[j] = k + " |-> " + r
			}
			stateRows = append(stateRows, "  ["+strings.Join(fields, ", ")+"]")
		}
	}
	var b strings.Builder
	title := " MODULE " + mod + " "
	b.WriteString(strings.Repeat("-", (120-len(title))/2) + title)
	b.WriteString(strings.Repeat("-", 120-(120-len(title))/2-len(title)) + "\n")
	through := "through Next, each step pinned to its logged state"
	if stateMode {
		through = "through Next conjoined with the logged action, each step pinned to its logged state"
	}
	b.WriteString(fillComment(fmt.Sprintf("Generated by `engramctl formal trace-to-tla` from a schema-v%d log of %d "+
		"events of %s. Do not edit. TraceSpec replays the events one per step through the operators of the "+
		"specification (in a log that records states: %s); the invariants of the configuration are checked on every "+
		"state of every replay. TraceNotDone is violated exactly when some execution has consumed the whole log, i.e. "+
		"when the log is a behaviour of the specification.", SchemaVersion, len(events), spec, through)))
	fmt.Fprintf(&b, "EXTENDS %s, Naturals, Sequences, TLC\n\n", spec)
	if len(newValues) > 0 {
		fmt.Fprintf(&b, "CONSTANTS %s\n\n", strings.Join(newValues, ", "))
	}
	b.WriteString("VARIABLE l\n\nTraceEvents == <<\n")
	b.WriteString(strings.Join(rows, ",\n"))
	b.WriteString("\n>>\n\n")
	if stateMode {
		b.WriteString("TraceStates == <<\n" + strings.Join(stateRows, ",\n") + "\n>>\n\n")
	}
	b.WriteString("tvars == <<vars, l>>\n\nTraceInit == Init /\\ l = 1\n\nTraceReplay ==\n")
	b.WriteString("  /\\ l <= Len(TraceEvents)\n  /\\ l' = l + 1\n")
	if stateMode {
		b.WriteString("  /\\ Next\n")
		for _, k := range vars {
			fmt.Fprintf(&b, "  /\\ %s' = TraceStates[l].%s\n", k, k)
		}
		// the step must also be a step of the action the log names (the name is TLC's: the innermost action operator
		// of the step, which may be called from a disjunct of Next, hence the conjunction with Next, not a dispatch)
		b.WriteString("  /\\ \\/ FALSE\n")
		for _, a := range order {
			call, err := quantifyCall(a, opt.Domains[a], arity[a])
			if err != nil {
				return Result{}, err
			}
			fmt.Fprintf(&b, "     \\/ TraceEvents[l].act = %q /\\ %s\n", a, call)
		}
	} else {
		b.WriteString("  /\\ \\/ FALSE\n")
		for _, a := range order {
			call := a
			if n := arity[a]; n > 0 {
				args := make([]string, n)
				for i := range args {
					args[i] = fmt.Sprintf("TraceEvents[l].par[%d]", i+1)
				}
				call += "(" + strings.Join(args, ", ") + ")"
			}
			fmt.Fprintf(&b, "     \\/ TraceEvents[l].act = %q /\\ %s\n", a, call)
		}
	}
	b.WriteString("\n" + fillComment("One step after the last event, so that every state of the replay is checked "+
		"against the invariants before TraceNotDone can fail (TLC checks a state when it generates it)."))
	b.WriteString("TraceFinish == l = Len(TraceEvents) + 1 /\\ l' = l + 1 /\\ UNCHANGED vars\n\n")
	b.WriteString("TraceNext == TraceReplay \\/ TraceFinish\n\nTraceSpec == TraceInit /\\ [][TraceNext]_tvars\n\n")
	b.WriteString("TraceNotDone == l <= Len(TraceEvents) + 1\n\n")
	b.WriteString(strings.Repeat("=", 120) + "\n")
	res := Result{Module: mod, TLA: b.String()}
	if opt.BaseConfig != "" {
		res.Config = traceConfig(opt.BaseConfig, newValues)
	}
	return res, nil
}

// fillComment renders text as a TLA+ line comment filled to 120 columns.
func fillComment(text string) string {
	var b strings.Builder
	line := ""
	for _, w := range strings.Fields(text) {
		if line != "" && len(line)+1+len(w) > 117 {
			b.WriteString("\\* " + line + "\n")
			line = w
			continue
		}
		if line != "" {
			line += " "
		}
		line += w
	}
	b.WriteString("\\* " + line + "\n")
	return b.String()
}

func firstSeq(events []Event, action string) int {
	for _, e := range events {
		if e.Action == action {
			return e.Seq
		}
	}
	return 0
}

var cfgKeyword = regexp.MustCompile(`^(SPECIFICATION|INIT|NEXT|CONSTANTS?|INVARIANTS?|PROPERT(?:Y|IES)|SYMMETRY|` +
	`CONSTRAINTS?|ACTION_CONSTRAINTS?|VIEW|ALIAS|POSTCONDITION|CHECK_DEADLOCK)\b`)

// cfgSections splits a configuration into (keyword, text lines including the keyword line) groups, dropping comments.
func cfgSections(cfg string) []struct {
	Key   string
	Lines []string
} {
	var out []struct {
		Key   string
		Lines []string
	}
	for _, line := range strings.Split(cfg, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), `\*`) || strings.TrimSpace(line) == "" {
			continue
		}
		if m := cfgKeyword.FindStringSubmatch(line); m != nil && !strings.HasPrefix(line, " ") {
			key := strings.TrimSuffix(m[1], "S")
			out = append(out, struct {
				Key   string
				Lines []string
			}{Key: key, Lines: []string{line}})
			continue
		}
		if len(out) > 0 {
			out[len(out)-1].Lines = append(out[len(out)-1].Lines, line)
		}
	}
	return out
}

var cfgAssign = regexp.MustCompile(`^\s*(?:CONSTANTS?\s+)?([A-Za-z][A-Za-z0-9_]*)\s*(?:=|<-)`)

// configConstantNames lists the constants the configuration assigns.
func configConstantNames(cfg string) []string {
	var out []string
	for _, sec := range cfgSections(cfg) {
		if sec.Key != "CONSTANT" {
			continue
		}
		for _, l := range sec.Lines {
			if m := cfgAssign.FindStringSubmatch(l); m != nil {
				out = append(out, m[1])
			}
		}
	}
	return out
}

// traceConfig keeps the CONSTANTS and INVARIANTS of the base configuration, adds TraceNotDone as the last invariant and
// checks TraceSpec instead of Spec.
func traceConfig(base string, newValues []string) string {
	var b strings.Builder
	var invariants []string
	b.WriteString("\\* Generated by `engramctl formal trace-to-tla`. Do not edit.\n")
	b.WriteString("SPECIFICATION TraceSpec\n")
	for _, sec := range cfgSections(base) {
		switch sec.Key {
		case "CONSTANT":
			b.WriteString(strings.Join(sec.Lines, "\n") + "\n")
		case "INVARIANT":
			for i, l := range sec.Lines {
				fields := strings.Fields(l)
				if i == 0 {
					fields = fields[1:]
				}
				invariants = append(invariants, fields...)
			}
		}
	}
	if len(newValues) > 0 {
		b.WriteString("CONSTANTS\n")
		for _, v := range newValues {
			fmt.Fprintf(&b, "  %s = %s\n", v, v)
		}
	}
	b.WriteString("INVARIANTS\n")
	for _, inv := range invariants {
		fmt.Fprintf(&b, "  %s\n", inv)
	}
	b.WriteString("  TraceNotDone\nCHECK_DEADLOCK FALSE\n")
	return b.String()
}
