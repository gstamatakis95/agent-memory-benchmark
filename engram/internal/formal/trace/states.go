package trace

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// State is one state of a TLC error trace: its number, the action that led to it (empty for the initial state) and the
// value of every variable in the JSON encoding of the package comment (ParseTLA).
type State struct {
	N      int
	Action string
	Vars   map[string]any
}

var stateStart = regexp.MustCompile(`^State (\d+): <(.*)>$`)

// ParseStates reads the states of the error trace in a TLC log, in order. A log without a trace yields no states.
func ParseStates(r io.Reader) ([]State, error) {
	var out []State
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	var cur *State
	var name string
	var val strings.Builder
	flushVar := func() error {
		if cur == nil || name == "" {
			return nil
		}
		v, err := ParseTLA(strings.TrimSpace(val.String()))
		if err != nil {
			return fmt.Errorf("trace: state %d, variable %s: %w", cur.N, name, err)
		}
		cur.Vars[name] = v
		name = ""
		val.Reset()
		return nil
	}
	finish := func() error {
		if err := flushVar(); err != nil {
			return err
		}
		if cur != nil {
			out = append(out, *cur)
			cur = nil
		}
		return nil
	}
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), " \r")
		if m := stateStart.FindStringSubmatch(line); m != nil {
			if err := finish(); err != nil {
				return nil, err
			}
			n, _ := strconv.Atoi(m[1])
			cur = &State{N: n, Vars: map[string]any{}}
			if h := stateHeader.FindStringSubmatch(line); h != nil {
				cur.Action = h[2]
			}
			continue
		}
		if cur == nil {
			continue
		}
		switch {
		case strings.HasPrefix(line, "/\\ "):
			if err := flushVar(); err != nil {
				return nil, err
			}
			rest := line[3:]
			eq := strings.Index(rest, " = ")
			if eq < 0 {
				return nil, fmt.Errorf("trace: state %d: cannot read %q", cur.N, line)
			}
			name = rest[:eq]
			val.WriteString(rest[eq+3:])
		case line == "":
			if err := finish(); err != nil {
				return nil, err
			}
		case name != "":
			val.WriteString("\n" + line)
		default:
			if err := finish(); err != nil {
				return nil, err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("trace: read TLC log: %w", err)
	}
	if err := finish(); err != nil {
		return nil, err
	}
	return out, nil
}
