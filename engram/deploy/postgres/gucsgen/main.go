// Command gucsgen renders deploy/postgres/gucs.yaml (the Postgres settings table of PLAN.md section 9.1, N131) into
// the settings file the shard image reads and, with -roles, into the ALTER ROLE statements the migrations carry.
// deploy/postgres/engram-postgres.sh turns the file into `postgres -c name=value` arguments (the section 9.1 excerpt's
// `-c include_dir=` is not a valid postgres argument). The table is the only place a setting's value is written down,
// per server kind (shard, catalog, standby, temporal): the compose files mount the generated file, the container limits
// come from the same table (-limits), and the only values compose passes itself are ports and the load test's
// durability overrides.
//
//	go run ./deploy/postgres/gucsgen -profile dev -kind shard -stanza shard-1 > deploy/compose/gucs/shard-1.conf
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Setting is one row of the table.
type Setting struct {
	Name string  `yaml:"name"`
	Prod *string `yaml:"prod"`
	Dev  *string `yaml:"dev"`
	// Plain replaces the value on the plain-PostgreSQL servers (catalog, standby, temporal-postgres: no pg_search).
	Plain *string `yaml:"plain"`
	// Kinds replaces the value for one server kind (shard, catalog, standby, temporal); it wins over everything.
	Kinds map[string]string `yaml:"kinds"`
	// Only restricts the setting to the listed server kinds (all kinds when empty).
	Only []string `yaml:"only"`
	// unsized is filled from Table.Unsized: the kinds that get no prod value for this setting.
	unsized []string
	Scope   string `yaml:"scope"`
	Ref     string `yaml:"ref"`
}

// Kinds are the server kinds the table renders for.
var Kinds = []string{"shard", "catalog", "standby", "temporal"}

// Container holds the container limits of a shard in a profile (rendered into a compose file by -limits).
type Container struct {
	CPUs    string `yaml:"cpus"`
	Memory  string `yaml:"memory"`
	ShmSize string `yaml:"shm_size"`
}

// Table is the parsed gucs.yaml.
type Table struct {
	Container map[string]Container         `yaml:"container"`
	Settings  []Setting                    `yaml:"settings"`
	Roles     map[string]map[string]string `yaml:"roles"`
	// Unsized names the settings the plan sizes only for the shard and the kinds that therefore get the Postgres
	// default in the prod profile (review M0.4 m18).
	Unsized struct {
		Kinds    []string `yaml:"kinds"`
		Settings []string `yaml:"settings"`
	} `yaml:"unsized"`
}

// Load parses a gucs.yaml document and rejects unknown keys, duplicate names and unknown scopes.
func Load(r io.Reader) (*Table, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var t Table
	if err := dec.Decode(&t); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, s := range t.Settings {
		switch s.Scope {
		case "server", "storage", "txn":
		default:
			return nil, fmt.Errorf("setting %s: unknown scope %q", s.Name, s.Scope)
		}
		if s.Name == "" || s.Prod == nil {
			return nil, fmt.Errorf("setting %q: name and prod are required", s.Name)
		}
		for _, k := range append(append([]string{}, s.Only...), keysOf(s.Kinds)...) {
			if !slices.Contains(Kinds, k) {
				return nil, fmt.Errorf("setting %s: unknown server kind %q", s.Name, k)
			}
		}
		if seen[s.Name] {
			return nil, fmt.Errorf("setting %s is listed twice", s.Name)
		}
		seen[s.Name] = true
	}
	for _, k := range t.Unsized.Kinds {
		if !slices.Contains(Kinds, k) {
			return nil, fmt.Errorf("unsized: unknown server kind %q", k)
		}
	}
	for _, name := range t.Unsized.Settings {
		if !seen[name] {
			return nil, fmt.Errorf("unsized: unknown setting %q", name)
		}
	}
	for i := range t.Settings {
		if slices.Contains(t.Unsized.Settings, t.Settings[i].Name) {
			t.Settings[i].unsized = t.Unsized.Kinds
		}
	}
	for _, p := range []string{"prod", "dev"} {
		if _, ok := t.Container[p]; !ok {
			return nil, fmt.Errorf("container.%s is missing", p)
		}
	}
	return &t, nil
}

func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Value returns the value of s for a server kind in the profile, and false when the setting does not apply to the kind.
// Precedence, highest first: the per-kind value, the plain-server value (every kind but the shard), the dev value in
// the dev profile, the prod value.
func (s Setting) Value(profile, kind string) (string, bool) {
	if len(s.Only) > 0 && !slices.Contains(s.Only, kind) {
		return "", false
	}
	if v, ok := s.Kinds[kind]; ok {
		return v, true
	}
	if slices.Contains(s.unsized, kind) && (profile != "dev" || s.Dev == nil) {
		return "", false
	}
	if kind != "shard" && s.Plain != nil {
		return *s.Plain, true
	}
	if profile == "dev" && s.Dev != nil {
		return *s.Dev, true
	}
	return *s.Prod, true
}

func quote(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// RenderConf renders the server-scope settings of a server kind as a settings file.
func (t *Table) RenderConf(w io.Writer, profile, kind, stanza string) error {
	if profile != "dev" && profile != "prod" {
		return fmt.Errorf("profile must be dev or prod, got %q", profile)
	}
	if !slices.Contains(Kinds, kind) {
		return fmt.Errorf("kind must be one of %v, got %q", Kinds, kind)
	}
	_, _ = fmt.Fprintf(w, "# generated by deploy/postgres/gucsgen from gucs.yaml (profile %s, kind %s, stanza %s);"+
		" do not edit\n", profile, kind, stanza)
	for _, s := range t.Settings {
		val, ok := s.Value(profile, kind)
		if s.Scope != "server" || !ok {
			continue
		}
		v := strings.ReplaceAll(val, "${STANZA}", stanza)
		if _, err := fmt.Fprintf(w, "%s = %s\n", s.Name, quote(v)); err != nil {
			return err
		}
	}
	return nil
}

// RenderRoles renders the role settings as ALTER ROLE statements in a stable order.
func (t *Table) RenderRoles(w io.Writer) error {
	roles := make([]string, 0, len(t.Roles))
	for r := range t.Roles {
		roles = append(roles, r)
	}
	sort.Strings(roles)
	for _, r := range roles {
		keys := make([]string, 0, len(t.Roles[r]))
		for k := range t.Roles[r] {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if _, err := fmt.Fprintf(w, "ALTER ROLE %s SET %s = %s;\n", r, k, quote(t.Roles[r][k])); err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	in := flag.String("in", "deploy/postgres/gucs.yaml", "the settings table")
	profile := flag.String("profile", "dev", "dev | prod")
	stanza := flag.String("stanza", "shard-1", "pgBackRest stanza substituted for ${STANZA}")
	kind := flag.String("kind", "shard", "server kind: shard | catalog | standby | temporal")
	roles := flag.Bool("roles", false, "render the ALTER ROLE statements instead of the include file")
	limits := flag.Bool("limits", false, "render the shard container limits as a compose file for `extends`")
	flag.Parse()
	f, err := os.Open(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gucsgen:", err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()
	t, err := Load(f)
	if err == nil {
		switch {
		case *roles:
			err = t.RenderRoles(os.Stdout)
		case *limits:
			c := t.Container[*profile]
			_, err = fmt.Printf("# generated by deploy/postgres/gucsgen from gucs.yaml (profile %s); do not edit\n"+
				"services:\n  shard-1-postgres:\n    cpus: %s\n    mem_limit: %s\n    shm_size: %s\n",
				*profile, c.CPUs, c.Memory, c.ShmSize)
		default:
			err = t.RenderConf(os.Stdout, *profile, *kind, *stanza)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "gucsgen:", err)
		os.Exit(1)
	}
}
