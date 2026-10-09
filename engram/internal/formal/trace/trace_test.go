package trace

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// formalDir returns <repo>/formal, found from this file's location.
func formalDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller information")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "formal")
}

// specOptions reads the operator table and the parameter domains of a committed specification.
func specOptions(t *testing.T, spec string) Options {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(formalDir(t), "tla", spec+".tla"))
	if err != nil {
		t.Fatal(err)
	}
	ops, err := OperatorArities(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	doms, err := ActionDomains(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return Options{Operators: ops, Domains: doms}
}

func TestLogger_WritesVersionedSequencedLines(t *testing.T) {
	var buf bytes.Buffer
	l := NewLogger(&buf, "Outbox")
	if err := l.Log("Draw", MV("w1"), MV("a")); err != nil {
		t.Fatal(err)
	}
	if err := l.Log("Tick"); err != nil {
		t.Fatal(err)
	}
	if err := l.Log("Odd", "@not-a-constant", 3, true, Set{MV("x"), 2}, []any{1, "s"},
		Fn{{MV("k"), 1}}, map[string]any{"f": MV("g")}); err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"spec":"Outbox","seq":1,"action":"Draw","args":["@w1","@a"]}` + "\n" +
		`{"v":1,"spec":"Outbox","seq":2,"action":"Tick"}` + "\n" +
		`{"v":1,"spec":"Outbox","seq":3,"action":"Odd","args":[{"$str":"@not-a-constant"},3,true,` +
		`{"$set":["@x",2]},[1,"s"],{"$fn":[["@k",1]]},{"f":"@g"}]}` + "\n"
	if buf.String() != want {
		t.Fatalf("log =\n%s\nwant\n%s", buf.String(), want)
	}
	if l.Count() != 3 {
		t.Fatalf("Count = %d", l.Count())
	}
	if err := l.Log(""); err == nil {
		t.Fatal("an empty action must be refused")
	}
}

func TestRead_ValidatesTheLog(t *testing.T) {
	good := `{"v":1,"spec":"X","seq":1,"action":"A"}` + "\n" +
		`{"v":1,"spec":"X","seq":2,"action":"B","args":[1]}` + "\n"
	evs, err := Read(strings.NewReader(good))
	if err != nil || len(evs) != 2 || evs[1].Action != "B" {
		t.Fatalf("Read = %v, %v", evs, err)
	}
	for name, bad := range map[string]string{
		"version": `{"v":2,"spec":"X","seq":1,"action":"A"}`,
		"gap":     `{"v":1,"spec":"X","seq":2,"action":"A"}`,
		"mixed":   good + `{"v":1,"spec":"Y","seq":3,"action":"A"}`,
		"unknown": `{"v":1,"spec":"X","seq":1,"action":"A","extra":1}`,
		"empty":   "\n\n",
		"action":  `{"v":1,"spec":"X","seq":1,"action":""}`,
		"junk":    `not json`,
	} {
		if _, err := Read(strings.NewReader(bad)); err == nil {
			t.Errorf("%s: Read accepted a bad log", name)
		}
	}
}

func TestParseTLA_RoundTripsTLCValues(t *testing.T) {
	cases := []string{
		`w1`,
		`7`,
		`-3`,
		`TRUE`,
		`"create"`,
		`<< >>`,
		`<<1, 2>>`,
		`{1, 2, 3, 4}`,
		`{}`,
		`[kind |-> "create", o |-> o1]`,
		`<<[kind |-> "create", o |-> o1]>>`,
		`(a :> 1 @@ b :> {2})`,
		`<<1, 1>>`,
		`{{1, 2}, {3}}`,
	}
	for _, c := range cases {
		v, err := ParseTLA(c)
		if err != nil {
			t.Fatalf("ParseTLA(%q): %v", c, err)
		}
		// through JSON and back, as a log file does
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var back any
		if err := dec.Decode(&back); err != nil {
			t.Fatal(err)
		}
		got, err := RenderTLA(back)
		if err != nil {
			t.Fatalf("RenderTLA(%q): %v", c, err)
		}
		again, err := ParseTLA(got)
		if err != nil || !reflect.DeepEqual(again, v) {
			t.Errorf("%q renders as %q which parses to %v (%v), want %v", c, got, again, err, v)
		}
	}
	for _, bad := range []string{`<<1, 2`, `[a |-> ]`, `(a :> 1`, `"open`, `1 2`, ``} {
		if _, err := ParseTLA(bad); err == nil {
			t.Errorf("ParseTLA(%q) accepted a malformed value", bad)
		}
	}
}

func TestFromTLCLog_ReadsTheErrorTrace(t *testing.T) {
	log := `Error: Invariant NoLossSafety is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
/\ x = 1

State 2: <Draw(w1,a) line 86, col 3 to line 91, col 76 of module Outbox>
/\ x = 2

State 3: <ProposeOk({1, 2},<<[kind |-> "create", o |-> o1]>>) line 110, col 3 to line 113, col 118 of module C>
/\ x = 3

State 4: <WatchGap line 154, col 3 to line 157, col 88 of module Outbox>
`
	evs, err := FromTLCLog("Outbox", strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Write(&buf, evs); err != nil {
		t.Fatal(err)
	}
	want := `{"v":1,"spec":"Outbox","seq":1,"action":"Draw","args":["@w1","@a"]}` + "\n" +
		`{"v":1,"spec":"Outbox","seq":2,"action":"ProposeOk",` +
		`"args":[{"$set":[1,2]},[{"kind":"create","o":"@o1"}]]}` + "\n" +
		`{"v":1,"spec":"Outbox","seq":3,"action":"WatchGap"}` + "\n"
	if buf.String() != want {
		t.Fatalf("events =\n%s\nwant\n%s", buf.String(), want)
	}
	if _, err := FromTLCLog("Outbox", strings.NewReader("no trace here\n")); err == nil {
		t.Fatal("a log without a trace must be refused")
	}
}

func TestOperatorArities(t *testing.T) {
	src := "Foo == 1\nBar(a, b) == a\nBaz(x) ==\n  x\n  Indented(a) == a\nRECURSIVE Rec(_)\n"
	got, err := OperatorArities(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"Foo": 0, "Bar": 2, "Baz": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("arities = %v, want %v", got, want)
	}
}

const outboxCfg = `\* Counterexample configuration.
SPECIFICATION Spec
CONSTANTS
  Writers = {w1, w2, w3}
  Namespaces = {a, b}
  Timeout = 2
  NoNs = NoNs
SYMMETRY Symm
INVARIANTS
  NoLossSafety
PROPERTIES
  NoLossLive
CHECK_DEADLOCK FALSE
`

func TestToTLA_BuildsTheTraceModule(t *testing.T) {
	events := []Event{
		{V: 1, Spec: "Outbox", Seq: 1, Action: "Draw", Args: []any{"@w1", "@a"}},
		{V: 1, Spec: "Outbox", Seq: 2, Action: "WatchGap"},
		{V: 1, Spec: "Outbox", Seq: 3, Action: "Draw", Args: []any{"@w2", "@NoNs"}},
	}
	res, err := ToTLA(events, Options{
		Operators:  map[string]int{"Draw": 2, "WatchGap": 0},
		BaseConfig: outboxCfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Module != "OutboxTrace" {
		t.Fatalf("module = %s", res.Module)
	}
	for _, want := range []string{
		"MODULE OutboxTrace", "EXTENDS Outbox, Naturals, Sequences, TLC", "CONSTANTS a, w1, w2",
		`[act |-> "Draw", par |-> <<w1, a>>]`, `[act |-> "WatchGap", par |-> << >>]`,
		`TraceEvents[l].act = "Draw" /\ Draw(TraceEvents[l].par[1], TraceEvents[l].par[2])`,
		`TraceEvents[l].act = "WatchGap" /\ WatchGap`, "TraceNotDone == l <= Len(TraceEvents) + 1",
	} {
		if !strings.Contains(res.TLA, want) {
			t.Errorf("module lacks %q:\n%s", want, res.TLA)
		}
	}
	if strings.Contains(res.TLA, "CONSTANTS a, NoNs") || strings.Contains(res.TLA, "NoNs,") {
		t.Errorf("a constant of the specification was redeclared:\n%s", res.TLA)
	}
	for _, line := range strings.Split(res.TLA, "\n") {
		if len(line) > 120 {
			t.Errorf("line of %d characters: %s", len(line), line)
		}
	}
	for _, want := range []string{"SPECIFICATION TraceSpec", "Writers = {w1, w2, w3}", "NoLossSafety", "w1 = w1",
		"TraceNotDone\nCHECK_DEADLOCK FALSE"} {
		if !strings.Contains(res.Config, want) {
			t.Errorf("config lacks %q:\n%s", want, res.Config)
		}
	}
	for _, unwanted := range []string{"SYMMETRY", "NoLossLive", "SPECIFICATION Spec\n"} {
		if strings.Contains(res.Config, unwanted) {
			t.Errorf("config keeps %q:\n%s", unwanted, res.Config)
		}
	}
}

func TestToTLA_RejectsWhatIsNotTheSpec(t *testing.T) {
	ops := map[string]int{"Draw": 2}
	for name, evs := range map[string][]Event{
		"unknown action": {{V: 1, Spec: "Outbox", Seq: 1, Action: "Nope"}},
		"wrong arity":    {{V: 1, Spec: "Outbox", Seq: 1, Action: "Draw", Args: []any{"@w1"}}},
		"two arities": {{V: 1, Spec: "Outbox", Seq: 1, Action: "Draw", Args: []any{"@w1", "@a"}},
			{V: 1, Spec: "Outbox", Seq: 2, Action: "Draw", Args: []any{"@w1"}}},
		"bad value": {{V: 1, Spec: "Outbox", Seq: 1, Action: "Draw", Args: []any{"@w-1", "@a"}}},
		"clash":     {{V: 1, Spec: "Outbox", Seq: 1, Action: "Draw", Args: []any{"@Draw", "@a"}}},
	} {
		if _, err := ToTLA(evs, Options{Operators: ops}); err == nil {
			t.Errorf("%s: ToTLA accepted the trace", name)
		}
	}
	if _, err := ToTLA(nil, Options{}); err == nil {
		t.Error("an empty trace must be refused")
	}
}

// Every TLC log in formal/tla/results that holds an error trace converts to events whose actions are operators of the
// specification with the parameter counts the headers carry. Skipped before the logs exist.
func TestFromTLCLog_AgainstTheCommittedLogs(t *testing.T) {
	logs, _ := filepath.Glob(filepath.Join(formalDir(t), "tla", "results", "*.log"))
	checked := 0
	for _, path := range logs {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(raw, []byte("\nState 2: <")) {
			continue
		}
		name := strings.TrimSuffix(filepath.Base(path), ".log")
		spec := strings.SplitN(name, "_", 2)[0]
		opt := specOptions(t, spec)
		evs, err := FromTLCLog(spec, bytes.NewReader(raw))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if _, err := ToTLA(evs, opt); err != nil {
			t.Errorf("%s: %v", name, err)
		}
		checked++
	}
	if checked == 0 {
		t.Skip("no TLC logs with an error trace under formal/tla/results yet")
	}
}

func TestParseStates_ReadsVariablesAcrossLines(t *testing.T) {
	log := `Error: Invariant X is violated.
State 1: <Initial predicate>
/\ a = {}
/\ wr = ( w1 :> [st |-> "idle", seq |-> 0] @@
  w2 :> [st |-> "holding", seq |-> 1] )
/\ n = 3

State 2: <Draw(w1,a) line 86, col 3 to line 91, col 76 of module Outbox>
/\ a = {1}
/\ wr = <<>>
/\ n = 4

2 states generated, 2 distinct states found, 0 states left on queue.
`
	sts, err := ParseStates(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	if len(sts) != 2 || sts[0].Action != "" || sts[1].Action != "Draw(w1,a)" {
		t.Fatalf("states = %+v", sts)
	}
	fn := sts[0].Vars["wr"].(map[string]any)["$fn"].([]any)
	if len(fn) != 2 || fn[1].([]any)[0] != "@w2" {
		t.Fatalf("wr = %v", sts[0].Vars["wr"])
	}
	if sts[1].Vars["n"] != json.Number("4") {
		t.Fatalf("n = %v", sts[1].Vars["n"])
	}
	none, err := ParseStates(strings.NewReader("no trace\n"))
	if err != nil || len(none) != 0 {
		t.Fatalf("states of a log without a trace = %v, %v", none, err)
	}
}

func TestParseTLA_ExpandsIntervals(t *testing.T) {
	v, err := ParseTLA(`{1..3}`)
	if err != nil {
		t.Fatal(err)
	}
	inner := map[string]any{"$set": []any{json.Number("1"), json.Number("2"), json.Number("3")}}
	want := map[string]any{"$set": []any{inner}}
	if !reflect.DeepEqual(v, want) {
		t.Fatalf("ParseTLA({1..3}) = %v", v)
	}
}

// The converter-proof fixtures under formal/tla/trace (written by scripts/formal-trace-check.sh proof) are what the
// generator produces today: a change of the generator must come with regenerated fixtures.
func TestFixtures_AreCurrent(t *testing.T) {
	fixtures := map[string]string{
		"Outbox": "Outbox_NoWatch", "Consolidation": "Consolidation_NonAtomicKey", "Storage": "Storage_AutoRepair",
		"Derivation": "Derivation_RestoreByVisibleTwin", "Durability": "Durability_AckBeforeIntent",
		"ShardMove": "ShardMove_ReadyBeforeIndex",
	}
	dir := filepath.Join(formalDir(t), "tla")
	for spec, cfg := range fixtures {
		raw, err := os.ReadFile(filepath.Join(dir, "trace", spec+".trace.jsonl"))
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		events, err := Read(bytes.NewReader(raw))
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		src, err := os.Open(filepath.Join(dir, spec+".tla"))
		if err != nil {
			t.Fatal(err)
		}
		ops, err := OperatorArities(src)
		_ = src.Close()
		if err != nil {
			t.Fatal(err)
		}
		base, err := os.ReadFile(filepath.Join(dir, cfg+".cfg"))
		if err != nil {
			t.Fatal(err)
		}
		res, err := ToTLA(events, Options{Operators: ops, BaseConfig: string(base)})
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		for ext, want := range map[string]string{".tla": res.TLA, ".cfg": res.Config} {
			got, err := os.ReadFile(filepath.Join(dir, "trace", spec+"Trace"+ext))
			if err != nil {
				t.Errorf("%s: %v", spec, err)
			} else if string(got) != want {
				t.Errorf("formal/tla/trace/%sTrace%s is stale: run scripts/formal-trace-check.sh proof", spec, ext)
			}
		}
	}
}

// A TLC 2.18 error trace names the action of a step but not its parameters. FromTLCLog then records the states, and the
// module replays the log through Next with each step pinned to its state.
func TestStateMode_FromTLC218Log(t *testing.T) {
	log := `Error: Invariant NoLossSafety is violated.
State 1: <Initial predicate>
/\ cursor = 0
/\ log = (index :> <<>> @@ kafka :> <<>>)

State 2: <Draw line 86, col 3 to line 91, col 76 of module Outbox>
/\ cursor = 0
/\ log = (index :> <<>> @@ kafka :> <<1>>)

State 3: <Tick line 1, col 1 to line 2, col 2 of module Outbox>
/\ cursor = 1
/\ log = (index :> <<>> @@ kafka :> <<1>>)

`
	evs, err := FromTLCLog("Outbox", strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 || evs[0].State == nil || len(evs[0].Args) != 0 {
		t.Fatalf("events = %+v", evs)
	}
	opt := Options{
		Operators:  map[string]int{"Draw": 2, "Tick": 0},
		Domains:    map[string][]string{"Draw": {"Writers", "Namespaces"}},
		BaseConfig: outboxCfg,
	}
	res, err := ToTLA(evs, opt)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"TraceStates == <<", `cursor |-> 0, log |-> (index :> << >> @@ kafka :> <<1>>)`,
		"  /\\ Next\n", "cursor' = TraceStates[l].cursor", "log' = TraceStates[l].log", "CONSTANTS index, kafka",
		// the named action is conjoined: zero-arity directly, the others over the domains of their parameters
		`\/ TraceEvents[l].act = "Tick" /\ Tick`,
		`\/ TraceEvents[l].act = "Draw" /\ \E trArg1 \in Writers, trArg2 \in Namespaces : Draw(trArg1, trArg2)`} {
		if !strings.Contains(res.TLA, want) {
			t.Errorf("module lacks %q:\n%s", want, res.TLA)
		}
	}
	// the state-mode log round-trips through the JSON-lines format
	var buf bytes.Buffer
	if err := Write(&buf, evs); err != nil {
		t.Fatal(err)
	}
	back, err := Read(&buf)
	if err != nil || len(back) != 2 || back[1].State["cursor"] != json.Number("1") {
		t.Fatalf("round trip = %+v, %v", back, err)
	}
}

// The state-level fixtures (a TLC 2.18 trace per specification) are valid logs that convert in state mode.
func TestFixtures_StateLevelConvert(t *testing.T) {
	dir := filepath.Join(formalDir(t), "tla")
	for spec, cfg := range map[string]string{
		"Outbox": "Outbox_NoWatch", "Consolidation": "Consolidation_NonAtomicKey", "Storage": "Storage_AutoRepair",
		"Derivation": "Derivation_RestoreByVisibleTwin", "Durability": "Durability_AckBeforeIntent",
		"ShardMove": "ShardMove_ReadyBeforeIndex",
	} {
		raw, err := os.ReadFile(filepath.Join(dir, "trace", spec+".states.jsonl"))
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		events, err := Read(bytes.NewReader(raw))
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		base, err := os.ReadFile(filepath.Join(dir, cfg+".cfg"))
		if err != nil {
			t.Fatal(err)
		}
		opt := specOptions(t, spec)
		opt.BaseConfig = string(base)
		res, err := ToTLA(events, opt)
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		if !strings.Contains(res.TLA, "TraceStates == <<") || !strings.Contains(res.TLA, "  /\\ Next\n") {
			t.Errorf("%s: the module is not in state mode", spec)
		}
	}
}

func TestActionDomains_ReadsTheQuantifiersOfNext(t *testing.T) {
	src := `Next ==
  \/ \E w \in Writers : \E n \in Namespaces : Draw(w, n)
  \/ \E w \in Writers : Commit(w) \/ Abort(w)
  \/ (\E x \in Rows \X Gens : IndexAdd(x)) \/ (\E a, b \in Gens : Pair(a, b))
  \/ \E p \in 0..MaxT : RestoreTo(p) \/ Tick
Draw(w, n) == w # n
Commit(w) == TRUE
Abort(w) == Commit(w)
IndexAdd(x) == TRUE
Pair(a, b) == TRUE
RestoreTo(p) == TRUE
Tick == TRUE
Lone(z) == Commit(z)
`
	got, err := ActionDomains(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"Draw": {"Writers", "Namespaces"}, "Commit": {"Writers"}, "Abort": {"Writers"}, "IndexAdd": {`Rows \X Gens`},
		"Pair": {"Gens", "Gens"}, "RestoreTo": {"0..MaxT"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("domains = %v, want %v", got, want)
	}
}

// The committed specifications give a domain to every parameterised action their state-level fixtures name.
func TestActionDomains_CommittedSpecifications(t *testing.T) {
	for spec, want := range map[string]map[string][]string{
		"Outbox":        {"Draw": {"Writers", "Namespaces"}, "Commit": {"Writers"}},
		"Consolidation": {"ProposeOk": {"Batches", "Proposals"}, "ApplyEffectOnly": {"Batches"}},
		"Storage":       {"Insert": {"Rows"}, "EmbedRow": {"Rows", "Gens"}, "IndexAdd": {`Rows \X Gens`}},
		"Derivation":    {"Ingest": {"Facts"}, "Verify": {"Writers"}},
		"Durability":    {"Commit": {"OpIds"}, "RestoreTo": {"0..MaxT"}},
		"ShardMove":     {"Begin": {"Clients", "Rows"}, "Backup": {"Shards"}},
	} {
		got := specOptions(t, spec).Domains
		for op, doms := range want {
			if !reflect.DeepEqual(got[op], doms) {
				t.Errorf("%s.%s: domains = %v, want %v", spec, op, got[op], doms)
			}
		}
	}
}

// A log of states is replayed through Next conjoined with the action each event names (N2 of the M0.7 review), so a
// log whose action names were changed is a different module, and a log naming an action the specification does not
// have, or a parameterised one without a domain, is refused.
func TestStateMode_ConjoinsTheNamedAction(t *testing.T) {
	ev := func(seq int, act string) Event {
		return Event{V: 1, Spec: "Outbox", Seq: seq, Action: act, State: map[string]any{"cursor": json.Number("0")}}
	}
	opt := Options{Operators: map[string]int{"Draw": 2, "Tick": 0, "Relay": 0}, Domains: map[string][]string{
		"Draw": {"Writers", "Namespaces"}}}
	good, err := ToTLA([]Event{ev(1, "Draw"), ev(2, "Tick")}, opt)
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := ToTLA([]Event{ev(1, "Draw"), ev(2, "Relay")}, opt)
	if err != nil {
		t.Fatal(err)
	}
	if good.TLA == renamed.TLA {
		t.Fatal("the action names do not reach the generated module")
	}
	for _, want := range []string{`TraceEvents[l].act = "Tick" /\ Tick`, `TraceEvents[l].act = "Draw" /\ \E trArg1`} {
		if !strings.Contains(good.TLA, want) {
			t.Errorf("module lacks %q:\n%s", want, good.TLA)
		}
	}
	relay := `TraceEvents[l].act = "Relay" /\ Relay`
	if strings.Contains(good.TLA, `act = "Relay"`) || !strings.Contains(renamed.TLA, relay) {
		t.Errorf("only the actions of the log are dispatched:\n%s\n%s", good.TLA, renamed.TLA)
	}
	for name, c := range map[string]struct {
		evs []Event
		opt Options
	}{
		"unknown action": {[]Event{ev(1, "Nope")}, opt},
		"no domain":      {[]Event{ev(1, "Draw")}, Options{Operators: opt.Operators}},
		"no operators":   {[]Event{ev(1, "Tick")}, Options{}},
	} {
		if _, err := ToTLA(c.evs, c.opt); err == nil {
			t.Errorf("%s: ToTLA accepted the log", name)
		}
	}
}
