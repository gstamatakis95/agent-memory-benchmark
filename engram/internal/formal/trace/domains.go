package trace

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// existsBinding matches the head of an existential quantifier up to the colon that ends its bound-variable list. The
// colon must be followed by white space (or the end of the line) so that the TLA+ function constructor ":>" does not
// end a list.
var existsBinding = regexp.MustCompile(`\\E\s+(.+?)\s:(?:\s|$)`)

// definitionStart matches the first line of a top-level definition: an identifier at column 0, optional parameters and
// "==".
var definitionStart = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*(?:\([^)]*\))?\s*==`)

// ActionDomains derives, for every operator of the specification module that takes parameters, the domain each
// parameter is drawn from. The specifications reach their parameterised actions through existential quantifiers over
// the sets the configuration defines, for example `\E w \in Writers : \E n \in Namespaces : Draw(w, n)`; the domain of
// parameter i of Draw is then the set bound to the i-th argument at the call site (`Writers`, `Namespaces`). A call
// site whose arguments are not all variables bound by a quantifier of the same definition is not usable and is
// ignored; several usable call sites with different domains give their union. An operator without a usable call site
// is absent from the result.
//
// The TraceNext generator uses the domains to conjoin a logged action in state mode (ToTLA): the log of a TLC 2.18
// error trace names the action of a step but not its parameters, so the step must satisfy the named action for some
// value of each parameter in its domain.
func ActionDomains(src io.Reader) (map[string][]string, error) {
	var lines []string
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	// arities of the operators of the module, and the text of every definition
	var bodies []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			bodies = append(bodies, strings.Join(cur, "\n"))
		}
		cur = nil
	}
	for _, line := range lines {
		if definitionStart.MatchString(line) {
			flush()
		} else if strings.HasPrefix(line, "====") || strings.HasPrefix(line, "----") {
			flush()
			continue
		}
		if len(cur) > 0 || definitionStart.MatchString(line) {
			cur = append(cur, line)
		}
	}
	flush()
	arities, err := OperatorArities(strings.NewReader(strings.Join(lines, "\n")))
	if err != nil {
		return nil, err
	}
	found := map[string][][]string{} // operator -> one entry per usable call site, each the domains of its parameters
	for _, body := range bodies {
		// the header of the definition is not a call site: drop it
		if loc := definitionStart.FindStringIndex(body); loc != nil {
			body = strings.Repeat(" ", loc[1]) + body[loc[1]:]
		}
		bound := bindingsOf(body)
		for name, n := range arities {
			if n == 0 {
				continue
			}
			call := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\(([^()]*)\)`)
			for _, m := range call.FindAllStringSubmatchIndex(body, -1) {
				args := strings.Split(body[m[2]:m[3]], ",")
				if len(args) != n {
					continue
				}
				doms := make([]string, n)
				ok := true
				for i, a := range args {
					d, has := domainAt(bound, strings.TrimSpace(a), m[0])
					if !has {
						ok = false
						break
					}
					doms[i] = d
				}
				if ok {
					found[name] = append(found[name], doms)
				}
			}
		}
	}
	out := map[string][]string{}
	for name, sites := range found {
		n := arities[name]
		doms := make([]string, n)
		for i := 0; i < n; i++ {
			var uniq []string
			for _, s := range sites {
				if !contains(uniq, s[i]) {
					uniq = append(uniq, s[i])
				}
			}
			if len(uniq) == 1 {
				doms[i] = uniq[0]
			} else {
				for j := range uniq {
					uniq[j] = "(" + uniq[j] + ")"
				}
				doms[i] = strings.Join(uniq, " \\cup ")
			}
		}
		out[name] = doms
	}
	return out, nil
}

// binding is one `var \in Domain` of an existential quantifier and the offset of the quantifier in the definition.
type binding struct {
	v, domain string
	at        int
}

// bindingsOf lists the variables bound by the existential quantifiers of a definition, in text order.
func bindingsOf(body string) []binding {
	var out []binding
	for _, m := range existsBinding.FindAllStringSubmatchIndex(body, -1) {
		list := body[m[2]:m[3]]
		var pending []string // variables waiting for the domain of a "v1, v2 \in D" group
		for _, item := range splitTop(list, ',') {
			item = strings.TrimSpace(item)
			if v, d, ok := strings.Cut(item, `\in`); ok {
				vars := make([]string, 0, len(pending)+1)
				vars = append(vars, pending...)
				vars = append(vars, strings.TrimSpace(v))
				pending = nil
				for _, name := range vars {
					if validIdent(name) {
						out = append(out, binding{v: name, domain: strings.TrimSpace(d), at: m[0]})
					}
				}
			} else if validIdent(item) {
				pending = append(pending, item)
			}
		}
	}
	return out
}

// domainAt returns the domain of the variable v bound by the nearest quantifier before the call at offset at.
func domainAt(bound []binding, v string, at int) (string, bool) {
	for i := len(bound) - 1; i >= 0; i-- {
		if bound[i].v == v && bound[i].at < at {
			return bound[i].domain, true
		}
	}
	return "", false
}

// splitTop splits s at sep outside of parentheses, braces, angle brackets and brackets.
func splitTop(s string, sep byte) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
		case '<':
			if i+1 < len(s) && s[i+1] == '<' {
				depth++
				i++
			}
		case '>':
			if i+1 < len(s) && s[i+1] == '>' {
				depth--
				i++
			}
		case sep:
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// quantifyCall renders the conjunct that executes the operator name with every parameter drawn from its domain: the
// operator itself when it takes none, else `\E a1 \in D1, a2 \in D2 : name(a1, a2)`.
func quantifyCall(name string, domains []string, arity int) (string, error) {
	if arity == 0 {
		return name, nil
	}
	if len(domains) != arity {
		return "", fmt.Errorf("trace: %s takes %d parameters and the specification gives no domain for them "+
			"(Options.Domains from ActionDomains)", name, arity)
	}
	vars := make([]string, arity)
	bind := make([]string, arity)
	for i, d := range domains {
		vars[i] = fmt.Sprintf("trArg%d", i+1)
		bind[i] = vars[i] + " \\in " + d
	}
	return `\E ` + strings.Join(bind, ", ") + " : " + name + "(" + strings.Join(vars, ", ") + ")", nil
}
