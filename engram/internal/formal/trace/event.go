// Package trace is the trace-validation side of PLAN.md section 7.4(b). The Go code paths of a test (store, relay,
// expunge, intent, move) log the spec-level actions they execute as JSON lines through a Logger, written after the
// transaction commits; `engramctl formal trace-to-tla` turns such a log into a TLA+ module that EXTENDS the
// specification and replays the actions through a TraceNext relation, so TLC checks that the logged behaviour is a
// behaviour of the specification and that every invariant held along it. The package also converts a TLC error trace
// back into the same JSON-lines format, which is how the converter is proved on the counterexamples of the must-fail
// configurations.
//
// Schema (version 1), one JSON object per line:
//
//	{"v":1,"spec":"Outbox","seq":1,"action":"Draw","args":["@w1","@a"]}
//
// An event may also carry "state": {"var": value, ...}, the variables of the specification after the event. A log in
// which every event has a state and none has args is replayed through the specification's Next with each step pinned to
// its state (the form TLC 2.18 error traces convert to); otherwise the named operators are called with the args.
//
// seq counts the events of one log from 1 without gaps. The action is the name of an operator of the specification and
// args are its actual parameters, each encoded as follows: a JSON string beginning with "@" is a TLA+ constant or model
// value (the bare identifier after the "@"), any other JSON string is a TLA+ string, a number is a number, a boolean is
// TRUE or FALSE, an array is a tuple, {"$set":[...]} a set, {"$fn":[[k,v],...]} a function, {"$str":"..."} a TLA+
// string that itself begins with "@", and any other object a record. A log without an action that has parameters needs
// no args field.
package trace

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// SchemaVersion is the version of the JSON-lines schema written by Logger and accepted by Read.
const SchemaVersion = 1

// Event is one spec-level action of a logged behaviour.
type Event struct {
	V      int    `json:"v"`
	Spec   string `json:"spec"`
	Seq    int    `json:"seq"`
	Action string `json:"action"`
	Args   []any  `json:"args,omitempty"`
	// State, when present on every event of a log, is the value of every variable of the specification after the event
	// (JSON encoding of the package comment). A TLC 2.18 error trace names the action of each step but not its
	// parameters, so FromTLCLog records the states instead and the replay pins each step to its logged state.
	State map[string]any `json:"state,omitempty"`
}

// MV is a TLA+ model value or constant referred to by name (encoded "@name").
type MV string

// MarshalJSON encodes the model value as "@name".
func (m MV) MarshalJSON() ([]byte, error) { return json.Marshal("@" + string(m)) }

// Set is a TLA+ set value.
type Set []any

// MarshalJSON encodes the set as {"$set":[...]}.
func (s Set) MarshalJSON() ([]byte, error) {
	items := []any(s)
	if items == nil {
		items = []any{}
	}
	return json.Marshal(map[string]any{"$set": items})
}

// Pair is one key and value of a Fn.
type Pair struct{ K, V any }

// Fn is a TLA+ function value given by its pairs, in domain order.
type Fn []Pair

// MarshalJSON encodes the function as {"$fn":[[k,v],...]}.
func (f Fn) MarshalJSON() ([]byte, error) {
	pairs := make([]any, len(f))
	for i, p := range f {
		pairs[i] = []any{p.K, p.V}
	}
	return json.Marshal(map[string]any{"$fn": pairs})
}

// normalize rewrites Go strings that begin with "@" so they stay TLA+ strings after encoding.
func normalize(v any) any {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "@") {
			return map[string]any{"$str": x}
		}
		return x
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = normalize(e)
		}
		return out
	case Set:
		out := make(Set, len(x))
		for i, e := range x {
			out[i] = normalize(e)
		}
		return out
	case Fn:
		out := make(Fn, len(x))
		for i, p := range x {
			out[i] = Pair{normalize(p.K), normalize(p.V)}
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalize(e)
		}
		return out
	}
	return v
}

// Logger writes the events of one behaviour of one specification as JSON lines. It is safe for concurrent use; the
// order of the lines is the order of the calls to Log, so callers log after the transaction commits and under the same
// serialisation the specification's action has.
type Logger struct {
	mu   sync.Mutex
	w    io.Writer
	spec string
	seq  int
}

// NewLogger returns a Logger for the named specification (Outbox, Consolidation, Storage, Derivation, Durability or
// ShardMove) that writes to w.
func NewLogger(w io.Writer, spec string) *Logger { return &Logger{w: w, spec: spec} }

// Log appends one event.
func (l *Logger) Log(action string, args ...any) error {
	if action == "" {
		return errors.New("trace: empty action")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ev := Event{V: SchemaVersion, Spec: l.spec, Seq: l.seq + 1, Action: action}
	for _, a := range args {
		ev.Args = append(ev.Args, normalize(a))
	}
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("trace: encode %s: %w", action, err)
	}
	if _, err := l.w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("trace: write %s: %w", action, err)
	}
	l.seq++
	return nil
}

// Count is the number of events logged so far.
func (l *Logger) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Read parses a JSON-lines log. It rejects an unknown schema version, a log that mixes specifications, a gap in seq and
// an empty log; args decode with json.Number so integers survive.
func Read(r io.Reader) ([]Event, error) {
	var out []Event
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		text := bytes.TrimSpace(sc.Bytes())
		if len(text) == 0 {
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(text))
		dec.UseNumber()
		dec.DisallowUnknownFields()
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			return nil, fmt.Errorf("trace: line %d: %w", line, err)
		}
		if ev.V != SchemaVersion {
			return nil, fmt.Errorf("trace: line %d: schema version %d, want %d", line, ev.V, SchemaVersion)
		}
		if ev.Action == "" {
			return nil, fmt.Errorf("trace: line %d: empty action", line)
		}
		if len(out) > 0 && ev.Spec != out[0].Spec {
			return nil, fmt.Errorf("trace: line %d: spec %q in a log of %q", line, ev.Spec, out[0].Spec)
		}
		if ev.Seq != len(out)+1 {
			return nil, fmt.Errorf("trace: line %d: seq %d, want %d", line, ev.Seq, len(out)+1)
		}
		out = append(out, ev)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("trace: read: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("trace: empty log")
	}
	return out, nil
}

// Write encodes events (already decoded or built) as JSON lines.
func Write(w io.Writer, events []Event) error {
	for _, ev := range events {
		b, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("trace: encode event %d: %w", ev.Seq, err)
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			return fmt.Errorf("trace: write event %d: %w", ev.Seq, err)
		}
	}
	return nil
}
