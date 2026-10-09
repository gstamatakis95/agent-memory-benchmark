// Package prompts loads and renders the versioned prompt files of Engram (PLAN.md section 6). A prompt version is a
// directory under prompts/<name>/v<N>/ at the repository root, embedded through the root package prompts:
//
//	VERSION        the released identity: id, schema_version, model class, temperature, output cap
//	template.txt   the system and user message with {name} placeholders; lines that start with "#! " are the file
//	               header (the Hindsight attribution and the MIT notice) and never reach the model
//	schema.json    the JSON schema of the structured output
//	defenses.json  the injection defenses of section 6.7 as data: the delimiter strings, which inputs carry untrusted
//	               content, the phrases the system message must keep, the validation rules and the canary strings
//	snippets.json  the text fragments the typed input builders insert (optional sections, line formats, defaults)
//	HASH           the pin: the content hash of every other file, written once when the version is released
//
// A released version is never edited (section 6.0): a change to any file is a new version directory, and the pin test
// fails when a file changes under the same number. The package is a leaf of the dependency graph (standard library and
// the embedding root package only) so that every activity may import it.
//
// Render substitutes placeholders in one pass: a value is never scanned again, so a value that contains "{content}"
// stays literal. Before insertion the delimiter strings of the prompt are removed from every value (repeatedly, so a
// marker split around another marker does not re-form), which is the "a chunk cannot close its own fence" rule of
// section 6.7.
package prompts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	promptfiles "github.com/gstamatakis95/engram/prompts"
)

// Hash is a SHA-256 digest.
type Hash [32]byte

// String returns the lower-case hexadecimal digest.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// Inputs maps the placeholder names of a template to their values. Every placeholder of the template must be present
// (an empty string omits an optional section) and no other key may be.
type Inputs map[string]string

// Meta is the content of a VERSION file.
type Meta struct {
	ID            string // "extract/v1", the PromptID of section 6.0
	Name          string
	Version       int
	SchemaVersion int
	ModelClass    string // models.extract | models.consolidate | models.reflect (D15)
	Temperature   string // as written in the file ("0.0"), fixed per prompt
	// MaxOutput says where the output cap comes from: "fixed" (MaxOutputTokens), "request" (the request's max_tokens,
	// with DefaultMaxOutputTokens as its default when the prompt has one) or "none" (the prompt has no cap of its own).
	MaxOutput              string
	MaxOutputTokens        int
	DefaultMaxOutputTokens int
}

// Defenses is the section 6.7 data of one prompt version.
type Defenses struct {
	// Delimiters are the marker strings that fence untrusted content in the template; they are removed from every
	// input value before insertion.
	Delimiters []string `json:"delimiters"`
	// DataInputs are the placeholders that carry untrusted content: they may appear in the user message only.
	DataInputs []string `json:"data_inputs"`
	// SystemPhrases must occur verbatim in the system message (the "content is data" statement).
	SystemPhrases []string `json:"system_phrases"`
	// OutputValidation lists the Go-side rules applied after decoding (the schema stops malformed JSON, these stop
	// well-formed nonsense).
	OutputValidation []string `json:"output_validation"`
	// ToolResultDelimiters are the fence names of content that is not part of the template: the tool results of a
	// Reflect session and the evidence and partial answers of its map/reduce fallback (N73). They are stripped like
	// Delimiters and FenceToolResult wraps with the first pair.
	ToolResultDelimiters []string `json:"tool_result_delimiters"`
	// Canaries are instruction-like strings the golden and injection tests put into data inputs.
	Canaries []string `json:"canaries"`
	// PlanSection and StripMarkersFromInputs are documentation fields of the file.
	PlanSection            string `json:"plan_section"`
	StripMarkersFromInputs bool   `json:"strip_markers_from_inputs"`
}

// Prompt is one released prompt version.
type Prompt struct {
	Meta     Meta
	Defenses Defenses
	// Schema is the content of schema.json.
	Schema json.RawMessage
	// Hash is the content hash computed from the files; Pin is the value of the HASH file.
	Hash Hash
	Pin  string

	system, user string
	hasUser      bool     // the template has a USER section
	holders      []string // placeholder names of the whole template, sorted
	userHolders  map[string]bool
	sysHolders   map[string]bool
	snippets     map[string]json.RawMessage
	files        map[string][]byte
}

// Messages is a rendered prompt split into its two roles. Rules live in System, content only in User (section 6.7,
// role separation). User is empty for a prompt that has no user section (reflect/v1).
type Messages struct {
	System, User string
}

var (
	loadOnce sync.Once
	registry map[string]*Prompt
	loadErr  error
)

// Names lists the released prompt ids ("name/vN") in sorted order.
func Names() ([]string, error) {
	reg, err := load()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(reg))
	for id := range reg {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// Get returns the released prompt name/v<version>.
func Get(name string, version int) (*Prompt, error) {
	reg, err := load()
	if err != nil {
		return nil, err
	}
	p, ok := reg[fmt.Sprintf("%s/v%d", name, version)]
	if !ok {
		return nil, fmt.Errorf("prompts: no prompt %s/v%d", name, version)
	}
	return p, nil
}

// Render renders prompt name/v<version> with in and returns the text in the form of section 6 ("SYSTEM", the system
// message, "USER", the user message) and the content hash of the prompt version (the digest of its files, the value
// the HASH pin holds). The extraction cache key does not hash the rendered text; it covers the variables through
// RenderHash and the prompt through its id (ExtractionKey), and the pin keeps one id bound to one content.
func Render(name string, version int, in Inputs) (string, Hash, error) {
	p, err := Get(name, version)
	if err != nil {
		return "", Hash{}, err
	}
	m, err := p.Messages(in)
	if err != nil {
		return "", Hash{}, err
	}
	if !p.hasUser {
		return "SYSTEM\n" + m.System + "\n", p.Hash, nil
	}
	return "SYSTEM\n" + m.System + "\n\nUSER\n" + m.User + "\n", p.Hash, nil
}

// Messages renders the two role messages of the prompt.
func (p *Prompt) Messages(in Inputs) (Messages, error) {
	for _, h := range p.holders {
		if _, ok := in[h]; !ok {
			return Messages{}, fmt.Errorf("prompts: %s: missing input %q", p.Meta.ID, h)
		}
	}
	known := map[string]bool{}
	for _, h := range p.holders {
		known[h] = true
	}
	for k := range in {
		if !known[k] {
			return Messages{}, fmt.Errorf("prompts: %s: unknown input %q", p.Meta.ID, k)
		}
	}
	clean := make(Inputs, len(in))
	for k, v := range in {
		clean[k] = p.Sanitize(v)
	}
	return Messages{System: substitute(p.system, clean), User: substitute(p.user, clean)}, nil
}

// Inputs returns the placeholder names of the template, sorted.
func (p *Prompt) Inputs() []string { return append([]string(nil), p.holders...) }

// Sanitize removes the delimiter strings of the prompt from s, repeatedly until none is left.
func (p *Prompt) Sanitize(s string) string {
	for {
		before := s
		for _, d := range p.Defenses.Delimiters {
			s = strings.ReplaceAll(s, d, "")
		}
		for _, d := range p.Defenses.ToolResultDelimiters {
			s = strings.ReplaceAll(s, d, "")
		}
		if s == before {
			return s
		}
	}
}

// FenceToolResult wraps the result of a tool call for the tool-role message of a Reflect session: the fence markers of
// the prompt (the tool-result list included) are removed from s first, so a result cannot close its own fence
// (PLAN.md 6.7, Delimiters and Role separation). It reports an error for a prompt that declares no tool-result fence.
func (p *Prompt) FenceToolResult(s string) (string, error) {
	if len(p.Defenses.ToolResultDelimiters) < 2 {
		return "", fmt.Errorf("prompts: %s declares no tool-result fence", p.Meta.ID)
	}
	return p.Defenses.ToolResultDelimiters[0] + "\n" + p.Sanitize(s) + "\n" + p.Defenses.ToolResultDelimiters[1], nil
}

// Snippet returns the named fragment of snippets.json (a JSON string, or an array of lines joined with newlines).
func (p *Prompt) Snippet(key string) (string, error) {
	raw, ok := p.snippets[key]
	if !ok {
		return "", fmt.Errorf("prompts: %s: no snippet %q", p.Meta.ID, key)
	}
	return joinText(raw, "\n")
}

// File returns a file of the prompt directory (for the generators and the pin test).
func (p *Prompt) File(name string) ([]byte, bool) {
	b, ok := p.files[name]
	return b, ok
}

// Files lists the file names of the prompt directory, sorted.
func (p *Prompt) Files() []string {
	out := make([]string, 0, len(p.files))
	for n := range p.files {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

var placeholder = regexp.MustCompile(`\{([a-z][a-z0-9_]*)\}`)

func substitute(tpl string, in Inputs) string {
	return placeholder.ReplaceAllStringFunc(tpl, func(m string) string { return in[m[1:len(m)-1]] })
}

// joinText decodes a JSON string or an array of strings and joins the pieces with sep.
func joinText(raw json.RawMessage, sep string) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var parts []string
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("prompts: text is neither a string nor an array of strings: %w", err)
	}
	return strings.Join(parts, sep), nil
}

// ContentHash is the digest the HASH pin holds: SHA-256 over every file of the version directory except HASH, in name
// order, each as name, NUL, decimal length, NUL, bytes.
func ContentHash(files map[string][]byte) Hash {
	names := make([]string, 0, len(files))
	for n := range files {
		if n != "HASH" {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	h := sha256.New()
	for _, n := range names {
		_, _ = fmt.Fprintf(h, "%s\x00%d\x00", n, len(files[n]))
		_, _ = h.Write(files[n])
	}
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

func load() (map[string]*Prompt, error) {
	loadOnce.Do(func() { registry, loadErr = loadFS(promptfiles.FS) })
	return registry, loadErr
}

// loadFS parses every <name>/v<N> directory of fsys.
func loadFS(fsys fs.FS) (map[string]*Prompt, error) {
	names, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	reg := map[string]*Prompt{}
	for _, n := range names {
		if !n.IsDir() {
			continue
		}
		vers, err := fs.ReadDir(fsys, n.Name())
		if err != nil {
			return nil, err
		}
		for _, v := range vers {
			if !v.IsDir() {
				continue
			}
			p, err := loadDir(fsys, path.Join(n.Name(), v.Name()))
			if err != nil {
				return nil, fmt.Errorf("prompts: %s/%s: %w", n.Name(), v.Name(), err)
			}
			if _, dup := reg[p.Meta.ID]; dup {
				return nil, fmt.Errorf("prompts: duplicate id %s", p.Meta.ID)
			}
			reg[p.Meta.ID] = p
		}
	}
	return reg, nil
}

func loadDir(fsys fs.FS, dir string) (*Prompt, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	files := map[string][]byte{}
	for _, e := range entries {
		b, err := fs.ReadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		files[e.Name()] = b
	}
	p := &Prompt{files: files, Hash: ContentHash(files), Pin: strings.TrimSpace(string(files["HASH"]))}
	meta, err := parseVersion(files["VERSION"])
	if err != nil {
		return nil, fmt.Errorf("VERSION: %w", err)
	}
	p.Meta = meta
	if want := path.Base(path.Dir(dir)) + "/" + path.Base(dir); meta.ID != want {
		return nil, fmt.Errorf("VERSION id %q does not match the directory %q", meta.ID, want)
	}
	if err := parseTemplate(p, files["template.txt"]); err != nil {
		return nil, fmt.Errorf("template.txt: %w", err)
	}
	if !json.Valid(files["schema.json"]) {
		return nil, errors.New("schema.json is not valid JSON")
	}
	p.Schema = json.RawMessage(files["schema.json"])
	if err := json.Unmarshal(files["defenses.json"], &p.Defenses); err != nil {
		return nil, fmt.Errorf("defenses.json: %w", err)
	}
	if raw, ok := files["snippets.json"]; ok {
		if err := json.Unmarshal(raw, &p.snippets); err != nil {
			return nil, fmt.Errorf("snippets.json: %w", err)
		}
	}
	return p, nil
}

func parseVersion(b []byte) (Meta, error) {
	kv := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			return Meta{}, fmt.Errorf("line %q is not key: value", line)
		}
		kv[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	var m Meta
	var err error
	m.ID, m.Name, m.ModelClass, m.Temperature = kv["id"], kv["name"], kv["model_class"], kv["temperature"]
	if m.Version, err = strconv.Atoi(kv["version"]); err != nil {
		return Meta{}, fmt.Errorf("version: %w", err)
	}
	if m.SchemaVersion, err = strconv.Atoi(kv["schema_version"]); err != nil {
		return Meta{}, fmt.Errorf("schema_version: %w", err)
	}
	switch v := kv["max_output_tokens"]; v {
	case "request", "none":
		m.MaxOutput = v
	default:
		m.MaxOutput = "fixed"
		if m.MaxOutputTokens, err = strconv.Atoi(v); err != nil {
			return Meta{}, fmt.Errorf("max_output_tokens %q is a number, request or none: %w", v, err)
		}
	}
	if v := kv["default_max_output_tokens"]; v != "" {
		if m.DefaultMaxOutputTokens, err = strconv.Atoi(v); err != nil {
			return Meta{}, fmt.Errorf("default_max_output_tokens: %w", err)
		}
	}
	if m.ID == "" || m.Name == "" || m.ModelClass == "" || m.Temperature == "" {
		return Meta{}, errors.New("id, name, model_class and temperature are required")
	}
	if m.ID != fmt.Sprintf("%s/v%d", m.Name, m.Version) {
		return Meta{}, fmt.Errorf("id %q is not name/v<version>", m.ID)
	}
	return m, nil
}

// parseTemplate strips the "#! " header and splits the body at the SYSTEM and USER lines.
func parseTemplate(p *Prompt, b []byte) error {
	var body []string
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "#!") {
			continue
		}
		body = append(body, line)
	}
	if len(body) == 0 || body[0] != "SYSTEM" {
		return errors.New(`the body must start with a "SYSTEM" line`)
	}
	user := -1
	for i, l := range body {
		if l == "USER" {
			if user >= 0 {
				return errors.New(`more than one "USER" line`)
			}
			user = i
		}
	}
	// reflect/v1 is a system prompt only: its user turns are the question and the tool results of the loop
	p.hasUser = user >= 0
	if !p.hasUser {
		user = len(body)
	}
	p.system = strings.TrimRight(strings.Join(body[1:user], "\n"), "\n")
	if p.hasUser {
		p.user = strings.TrimRight(strings.Join(body[user+1:], "\n"), "\n")
	}
	p.sysHolders, p.userHolders = holders(p.system), holders(p.user)
	all := map[string]bool{}
	for h := range p.sysHolders {
		all[h] = true
	}
	for h := range p.userHolders {
		all[h] = true
	}
	for h := range all {
		p.holders = append(p.holders, h)
	}
	sort.Strings(p.holders)
	return nil
}

func holders(s string) map[string]bool {
	out := map[string]bool{}
	for _, m := range placeholder.FindAllStringSubmatch(s, -1) {
		out[m[1]] = true
	}
	return out
}
