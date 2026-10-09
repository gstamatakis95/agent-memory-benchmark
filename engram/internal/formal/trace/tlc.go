package trace

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var stateHeader = regexp.MustCompile(
	`^State (\d+): <(.+) line \d+, col \d+ to line \d+, col \d+ of module [A-Za-z0-9_]+>$`)

// FromTLCLog extracts the error trace of a TLC run (the "State n: <Action(args) ...>" headers of its output) as a
// JSON-lines event list for the named specification. State 1 (the initial predicate) is not an event. A header that
// names an action without parameters yields an event without args. It is the inverse of the converter: the events
// replay the same behaviour through the TraceNext module of `engramctl formal trace-to-tla`.
func FromTLCLog(spec string, r io.Reader) ([]Event, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("trace: read TLC log: %w", err)
	}
	var out []Event
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), " \r")
		if !strings.HasPrefix(line, "State ") {
			continue
		}
		if strings.HasPrefix(line, "State "+strconv.Itoa(len(out)+1)+": <Initial predicate>") {
			continue
		}
		m := stateHeader.FindStringSubmatch(line)
		if m == nil {
			// a stuttering or back-to-state header carries no action
			if strings.Contains(line, "Stuttering") || strings.Contains(line, "Back to state") {
				continue
			}
			return nil, fmt.Errorf("trace: cannot read TLC state header %q", line)
		}
		name, args, err := splitHeader(m[2])
		if err != nil {
			return nil, fmt.Errorf("trace: state %s: %w", m[1], err)
		}
		out = append(out, Event{V: SchemaVersion, Spec: spec, Seq: len(out) + 1, Action: name, Args: args})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("trace: read TLC log: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("trace: the TLC log holds no error trace")
	}
	// TLC 2.18 prints the name of the action but not its parameters. When no header has any, record the states: event i
	// leads to state i+2 of the trace.
	named := false
	for _, ev := range out {
		named = named || len(ev.Args) > 0
	}
	if !named {
		states, err := ParseStates(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		if len(states) != len(out)+1 {
			return nil, fmt.Errorf("trace: %d state headers but %d states", len(out), len(states))
		}
		for i := range out {
			out[i].State = states[i+1].Vars
		}
	}
	return out, nil
}

// splitHeader splits "Name(a,b)" into the action name and its decoded actual parameters.
func splitHeader(h string) (string, []any, error) {
	open := strings.IndexByte(h, '(')
	if open < 0 {
		return h, nil, nil
	}
	if !strings.HasSuffix(h, ")") {
		return "", nil, fmt.Errorf("unbalanced action header %q", h)
	}
	args, err := splitArgs(h[open+1 : len(h)-1])
	if err != nil {
		return "", nil, fmt.Errorf("action header %q: %w", h, err)
	}
	return h[:open], args, nil
}
