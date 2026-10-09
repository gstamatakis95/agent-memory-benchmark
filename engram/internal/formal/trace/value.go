package trace

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// ParseTLA parses one TLC-printed value (an identifier, model value, number, string, boolean, tuple <<...>>, set {...},
// record [k |-> v, ...] or function (k :> v @@ ...)) into the JSON encoding of the schema.
func ParseTLA(s string) (any, error) {
	p := &tlaParser{s: s}
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	p.skip()
	if p.i != len(p.s) {
		return nil, fmt.Errorf("trace: trailing text %q after value", p.s[p.i:])
	}
	return v, nil
}

// splitArgs parses the comma-separated actual parameters of an action header.
func splitArgs(s string) ([]any, error) {
	p := &tlaParser{s: s}
	var out []any
	p.skip()
	if p.i == len(p.s) {
		return nil, nil
	}
	for {
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		p.skip()
		if p.i == len(p.s) {
			return out, nil
		}
		if p.s[p.i] != ',' {
			return nil, fmt.Errorf("trace: expected ',' at %q", p.s[p.i:])
		}
		p.i++
	}
}

type tlaParser struct {
	s string
	i int
}

func (p *tlaParser) skip() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n') {
		p.i++
	}
}

func (p *tlaParser) has(tok string) bool { return strings.HasPrefix(p.s[p.i:], tok) }

func (p *tlaParser) expect(tok string) error {
	p.skip()
	if !p.has(tok) {
		rest := p.s[p.i:]
		if len(rest) > 20 {
			rest = rest[:20]
		}
		return fmt.Errorf("trace: expected %q at %q", tok, rest)
	}
	p.i += len(tok)
	return nil
}

func (p *tlaParser) value() (any, error) {
	p.skip()
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("trace: unexpected end of value")
	}
	switch {
	case p.has("<<"):
		p.i += 2
		items, err := p.list(">>")
		if err != nil {
			return nil, err
		}
		if items == nil {
			items = []any{}
		}
		return items, nil
	case p.s[p.i] == '{':
		p.i++
		items, err := p.list("}")
		if err != nil {
			return nil, err
		}
		if items == nil {
			items = []any{}
		}
		return map[string]any{"$set": items}, nil
	case p.s[p.i] == '[':
		return p.record()
	case p.s[p.i] == '(':
		return p.parenthesised()
	case p.s[p.i] == '"':
		return p.str()
	case p.s[p.i] == '-' || (p.s[p.i] >= '0' && p.s[p.i] <= '9'):
		j := p.i + 1
		for j < len(p.s) && p.s[j] >= '0' && p.s[j] <= '9' {
			j++
		}
		n := json.Number(p.s[p.i:j])
		p.i = j
		if p.has("..") { // TLC prints a set that is an integer interval as lo..hi
			return p.interval(n)
		}
		return n, nil
	}
	return p.ident()
}

// interval expands lo..hi (TLC's rendering of an integer-interval set) into a set value.
func (p *tlaParser) interval(lo json.Number) (any, error) {
	p.i += 2
	hiv, err := p.value()
	if err != nil {
		return nil, err
	}
	hi, ok := hiv.(json.Number)
	if !ok {
		return nil, fmt.Errorf("trace: interval bound %v is not an integer", hiv)
	}
	a, errA := strconv.Atoi(lo.String())
	b, errB := strconv.Atoi(hi.String())
	if errA != nil || errB != nil || b-a > 100000 {
		return nil, fmt.Errorf("trace: unsupported interval %s..%s", lo, hi)
	}
	items := make([]any, 0, b-a+1)
	for i := a; i <= b; i++ {
		items = append(items, json.Number(strconv.Itoa(i)))
	}
	return map[string]any{"$set": items}, nil
}

func (p *tlaParser) ident() (any, error) {
	j := p.i
	for j < len(p.s) && (p.s[j] == '_' || unicode.IsLetter(rune(p.s[j])) || unicode.IsDigit(rune(p.s[j]))) {
		j++
	}
	if j == p.i {
		return nil, fmt.Errorf("trace: unexpected %q", p.s[p.i:])
	}
	name := p.s[p.i:j]
	p.i = j
	switch name {
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	}
	return "@" + name, nil
}

func (p *tlaParser) str() (any, error) {
	j := p.i + 1
	var b strings.Builder
	for j < len(p.s) {
		c := p.s[j]
		switch {
		case c == '"':
			p.i = j + 1
			out := b.String()
			if strings.HasPrefix(out, "@") {
				return map[string]any{"$str": out}, nil
			}
			return out, nil
		case c == '\\' && j+1 < len(p.s):
			j++
			switch p.s[j] {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case 'f':
				b.WriteByte('\f')
			default:
				b.WriteByte(p.s[j])
			}
		default:
			b.WriteByte(c)
		}
		j++
	}
	return nil, fmt.Errorf("trace: unterminated string")
}

// list parses values separated by commas up to the closing token (consumed).
func (p *tlaParser) list(closing string) ([]any, error) {
	var items []any
	p.skip()
	if p.has(closing) {
		p.i += len(closing)
		return items, nil
	}
	for {
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		items = append(items, v)
		p.skip()
		switch {
		case p.has(closing):
			p.i += len(closing)
			return items, nil
		case p.has(","):
			p.i++
		default:
			return nil, fmt.Errorf("trace: expected ',' or %q at %q", closing, p.s[p.i:])
		}
	}
}

func (p *tlaParser) record() (any, error) {
	p.i++ // [
	rec := map[string]any{}
	for {
		p.skip()
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		key, _ := name.(string)
		if err := p.expect("|->"); err != nil {
			return nil, err
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		rec[strings.TrimPrefix(key, "@")] = v
		p.skip()
		switch {
		case p.has("]"):
			p.i++
			return rec, nil
		case p.has(","):
			p.i++
		default:
			return nil, fmt.Errorf("trace: expected ',' or ']' at %q", p.s[p.i:])
		}
	}
}

// parenthesised is a function (k :> v @@ k2 :> v2) or a parenthesised value.
func (p *tlaParser) parenthesised() (any, error) {
	p.i++ // (
	var pairs []any
	for {
		k, err := p.value()
		if err != nil {
			return nil, err
		}
		p.skip()
		if len(pairs) == 0 && p.has(")") {
			p.i++
			return k, nil
		}
		if err := p.expect(":>"); err != nil {
			return nil, err
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, []any{k, v})
		p.skip()
		switch {
		case p.has(")"):
			p.i++
			return map[string]any{"$fn": pairs}, nil
		case p.has("@@"):
			p.i += 2
		default:
			return nil, fmt.Errorf("trace: expected '@@' or ')' at %q", p.s[p.i:])
		}
	}
}

// RenderTLA renders a decoded JSON argument (see the package comment) as TLA+ source text.
func RenderTLA(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", fmt.Errorf("trace: null argument")
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case json.Number:
		if _, err := strconv.ParseInt(x.String(), 10, 64); err != nil {
			return "", fmt.Errorf("trace: number %q is not an integer", x)
		}
		return x.String(), nil
	case float64:
		if x != float64(int64(x)) {
			return "", fmt.Errorf("trace: number %v is not an integer", x)
		}
		return strconv.FormatInt(int64(x), 10), nil
	case string:
		if strings.HasPrefix(x, "@") {
			name := x[1:]
			if !validIdent(name) {
				return "", fmt.Errorf("trace: %q is not a TLA+ identifier", x)
			}
			return name, nil
		}
		return quote(x), nil
	case []any:
		parts, err := renderAll(x)
		if err != nil {
			return "", err
		}
		if len(parts) == 0 {
			return "<< >>", nil
		}
		return "<<" + strings.Join(parts, ", ") + ">>", nil
	case map[string]any:
		return renderObject(x)
	}
	return "", fmt.Errorf("trace: unsupported argument %T", v)
}

func renderAll(xs []any) ([]string, error) {
	parts := make([]string, len(xs))
	for i, e := range xs {
		s, err := RenderTLA(e)
		if err != nil {
			return nil, err
		}
		parts[i] = s
	}
	return parts, nil
}

func renderObject(m map[string]any) (string, error) {
	if len(m) == 1 {
		for k, v := range m {
			switch k {
			case "$str":
				s, ok := v.(string)
				if !ok {
					return "", fmt.Errorf("trace: $str needs a string")
				}
				return quote(s), nil
			case "$set":
				items, ok := v.([]any)
				if !ok {
					return "", fmt.Errorf("trace: $set needs an array")
				}
				parts, err := renderAll(items)
				if err != nil {
					return "", err
				}
				return "{" + strings.Join(parts, ", ") + "}", nil
			case "$fn":
				pairs, ok := v.([]any)
				if !ok {
					return "", fmt.Errorf("trace: $fn needs an array of pairs")
				}
				if len(pairs) == 0 {
					return "<< >>", nil
				}
				parts := make([]string, len(pairs))
				for i, pr := range pairs {
					kv, ok := pr.([]any)
					if !ok || len(kv) != 2 {
						return "", fmt.Errorf("trace: $fn pair %d is not [k, v]", i)
					}
					ks, err := RenderTLA(kv[0])
					if err != nil {
						return "", err
					}
					vs, err := RenderTLA(kv[1])
					if err != nil {
						return "", err
					}
					parts[i] = ks + " :> " + vs
				}
				return "(" + strings.Join(parts, " @@ ") + ")", nil
			}
		}
	}
	if len(m) == 0 {
		return "", fmt.Errorf("trace: empty record")
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		if !validIdent(k) {
			return "", fmt.Errorf("trace: record field %q is not a TLA+ identifier", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		s, err := RenderTLA(m[k])
		if err != nil {
			return "", err
		}
		parts[i] = k + " |-> " + s
	}
	return "[" + strings.Join(parts, ", ") + "]", nil
}

func validIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		letter := r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		digit := i > 0 && r >= '0' && r <= '9'
		if !letter && !digit {
			return false
		}
	}
	return true
}

func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		case '\f':
			b.WriteString(`\f`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// modelValues collects the "@name" identifiers an argument mentions.
func modelValues(v any, into map[string]bool) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "@") {
			into[x[1:]] = true
		}
	case []any:
		for _, e := range x {
			modelValues(e, into)
		}
	case map[string]any:
		for k, e := range x {
			if k == "$str" {
				continue
			}
			modelValues(e, into)
		}
	}
}
