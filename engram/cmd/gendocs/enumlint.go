package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// The enum lint (register N139, A-8): one concept, one member list, in every Go, proto and SQL place that spells it.
// enums.yaml names the concepts and where each is spelled; the lint reads every place and fails on a difference.

type enumsFile struct {
	Concepts []concept `yaml:"concepts"`
}

type concept struct {
	Name    string       `yaml:"name"`
	Rows    []string     `yaml:"rows"`
	Sources []enumSource `yaml:"sources"`
	Note    string       `yaml:"note"`
}

// enumSource is one place that spells the concept.
type enumSource struct {
	Kind   string   `yaml:"kind"`   // sql-enum | sql-check | proto-enum | go-consts | go-const-names | json-schema
	Name   string   `yaml:"name"`   // sql-enum, proto-enum: the type name
	DB     string   `yaml:"db"`     // sql-*: shard | catalog
	Table  string   `yaml:"table"`  // sql-check
	Column string   `yaml:"column"` // sql-check
	Strip  string   `yaml:"strip"`  // proto-enum, go-const-names: prefix removed from each name
	Skip   []string `yaml:"skip"`   // proto-enum, go-consts: members left out (UNSPECIFIED, the empty state)
	Dir    string   `yaml:"dir"`    // go-*: package directory
	Type   string   `yaml:"type"`   // go-*: the named type
	Part   string   `yaml:"part"`   // go-consts: before | after the "/" of a value such as frozen/move
	File   string   `yaml:"file"`   // json-schema
	Path   []string `yaml:"path"`   // json-schema: keys down to the enum array
}

func (s enumSource) String() string {
	switch s.Kind {
	case "sql-enum":
		return "SQL enum `" + s.Name + "` (" + s.DB + ")"
	case "sql-check":
		return "SQL CHECK `" + s.Table + "." + s.Column + "` (" + s.DB + ")"
	case "proto-enum":
		return "proto enum `" + s.Name + "`"
	case "go-consts", "go-const-names":
		return "Go constants `" + s.Dir + "." + s.Type + "`"
	case "json-schema":
		return "JSON schema `" + s.File + "` `" + strings.Join(s.Path, ".") + "`"
	}
	return s.Kind
}

// enumInputs is everything the sources are read from.
type enumInputs struct {
	root   string
	shard  *DDL
	cat    *DDL
	protos *Protos
}

func normalizeMembers(ms []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range ms {
		m = strings.ToLower(m)
		if m != "" && !seen[m] {
			seen[m] = true
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

func (in enumInputs) members(s enumSource) ([]string, error) {
	ddl := in.shard
	if s.DB == "catalog" {
		ddl = in.cat
	}
	switch s.Kind {
	case "sql-enum":
		for _, e := range ddl.Enums {
			if e.Name == s.Name {
				return e.Members, nil
			}
		}
		return nil, fmt.Errorf("no SQL enum %q in the %s DDL", s.Name, s.DB)
	case "sql-check":
		for _, c := range ddl.Checks {
			if c.Table == s.Table && c.Column == s.Column {
				return c.Members, nil
			}
		}
		return nil, fmt.Errorf("no CHECK list on %s.%s in the %s DDL", s.Table, s.Column, s.DB)
	case "proto-enum":
		e, _, ok := in.protos.FindEnum(s.Name)
		if !ok {
			return nil, fmt.Errorf("no proto enum %q", s.Name)
		}
		var out []string
		for _, v := range EnumValues(e) {
			v = strings.TrimPrefix(v, s.Strip)
			if !contains(s.Skip, v) {
				out = append(out, v)
			}
		}
		return out, nil
	case "go-consts", "go-const-names":
		return goConsts(filepath.Join(in.root, s.Dir), s)
	case "json-schema":
		return jsonEnum(filepath.Join(in.root, s.File), s.Path)
	}
	return nil, fmt.Errorf("unknown source kind %q", s.Kind)
}

// goConsts reads the constants of the named type from the non-test files of a package: their string values
// (go-consts) or their identifiers (go-const-names).
func goConsts(dir string, s enumSource) ([]string, error) {
	fset := token.NewFileSet()
	notTest := func(fi os.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }
	pkgs, err := parser.ParseDir(fset, dir, notTest, 0)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				cur := "" // the type of the spec, inherited by the specs that omit it (iota groups)
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					if id, ok := vs.Type.(*ast.Ident); ok {
						cur = id.Name
					} else if vs.Type != nil {
						cur = ""
					}
					if cur != s.Type {
						continue
					}
					for i, name := range vs.Names {
						if s.Kind == "go-const-names" {
							out = append(out, strings.TrimPrefix(name.Name, s.Strip))
							continue
						}
						if i >= len(vs.Values) {
							continue
						}
						lit, ok := vs.Values[i].(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							continue
						}
						v, err := strconv.Unquote(lit.Value)
						if err != nil {
							return nil, err
						}
						before, after, hasSlash := strings.Cut(v, "/")
						switch s.Part {
						case "before":
							v = before
						case "after":
							if !hasSlash {
								continue
							}
							v = after
						}
						if !contains(s.Skip, v) {
							out = append(out, v)
						}
					}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no constants of type %s in %s", s.Type, dir)
	}
	return out, nil
}

func jsonEnum(path string, keys []string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s: no object at %q", path, k)
		}
		if v, ok = m[k]; !ok {
			return nil, fmt.Errorf("%s: no key %q", path, k)
		}
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s: %s is not an array", path, strings.Join(keys, "."))
	}
	var out []string
	for _, a := range arr {
		out = append(out, fmt.Sprint(a))
	}
	return out, nil
}

// lintEnums reads every source of every concept and returns the document and the mismatches.
func lintEnums(yamlPath string, in enumInputs, plan *Plan) (string, []string, error) {
	raw, err := os.ReadFile(yamlPath)
	if err != nil {
		return "", nil, err
	}
	var f enumsFile
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return "", nil, fmt.Errorf("enums: %s: %w", yamlPath, err)
	}
	d := NewDoc("Enum lint", "`cmd/gendocs/enums.yaml`", "register N139 (A-8), N113, N137, N187")
	d.Para("One concept has one member list wherever it is spelled. The lint reads each place below (members are " +
		"compared case-insensitively, `UNSPECIFIED` and the empty state are not members) and `make gen-docs` fails " +
		"when two places of the same concept differ.")
	var problems []string
	for _, c := range f.Concepts {
		if len(c.Sources) < 2 {
			addf(&problems, "enums: concept %q needs at least two sources", c.Name)
		}
		d.H2(c.Name)
		for _, r := range c.Rows {
			if _, ok := plan.Source(r); !ok {
				addf(&problems, "enums: %s cites %s, which the plan does not have", c.Name, r)
			}
		}
		if len(c.Rows) > 0 {
			d.Para("Register: %s.", strings.Join(c.Rows, ", "))
		}
		var ref []string
		var refSrc string
		for i, s := range c.Sources {
			ms, err := in.members(s)
			if err != nil {
				addf(&problems, "enums: %s: %s: %v", c.Name, s, err)
				continue
			}
			ms = normalizeMembers(ms)
			d.Bullet(s.String() + ": " + quoteList(ms))
			if i == 0 || ref == nil {
				ref, refSrc = ms, s.String()
				continue
			}
			if missing, extra := diff(ref, ms); len(missing)+len(extra) > 0 {
				msg := fmt.Sprintf("enums: %s: %s differs from %s", c.Name, s, refSrc)
				if len(missing) > 0 {
					msg += "; missing " + strings.Join(missing, ", ")
				}
				if len(extra) > 0 {
					msg += "; extra " + strings.Join(extra, ", ")
				}
				problems = append(problems, msg)
			}
		}
		d.EndList()
		if c.Note != "" {
			d.Para("%s", c.Note)
		}
	}
	return d.String(), problems, nil
}

// diff returns the members of ref that ms lacks and the members of ms that ref lacks.
func diff(ref, ms []string) (missing, extra []string) {
	for _, r := range ref {
		if !contains(ms, r) {
			missing = append(missing, r)
		}
	}
	for _, m := range ms {
		if !contains(ref, m) {
			extra = append(extra, m)
		}
	}
	return missing, extra
}
