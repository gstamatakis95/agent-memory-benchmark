package main

import (
	"fmt"
	"os"
	"strings"

	"go.yaml.in/yaml/v3"
)

// gucFile is deploy/postgres/gucs.yaml (PLAN.md 9.1, N131): the one table behind the `postgres -c` include file, the
// role GUCs and `engramctl config lint`.
type gucFile struct {
	Container map[string]map[string]string `yaml:"container"`
	Settings  []gucSetting                 `yaml:"settings"`
	Roles     map[string]map[string]string `yaml:"roles"`
	// Unsized lists the settings that stay at PostgreSQL defaults on the servers of some kinds until they are sized.
	Unsized struct {
		Kinds    []string `yaml:"kinds"`
		Settings []string `yaml:"settings"`
	} `yaml:"unsized"`
}

type gucSetting struct {
	Name  string            `yaml:"name"`
	Prod  *string           `yaml:"prod"`
	Dev   *string           `yaml:"dev"`
	Plain *string           `yaml:"plain"`
	Kinds map[string]string `yaml:"kinds"`
	Only  []string          `yaml:"only"`
	Scope string            `yaml:"scope"`
	Ref   string            `yaml:"ref"`
}

func orDash(s *string) string {
	if s == nil {
		return "-"
	}
	if *s == "" {
		return "``"
	}
	return "`" + *s + "`"
}

// loadGUCs reads and checks the settings table at path.
func loadGUCs(path string) (*gucFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("gucs: %w (deploy/postgres/gucs.yaml is required; -gucs PATH reads another table)", err)
	}
	var f gucFile
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	// not strict: the table belongs to M0.4 and may grow fields this generator does not render
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("gucs: %s: %w", path, err)
	}
	if len(f.Settings) == 0 {
		return nil, fmt.Errorf("gucs: %s has no settings", path)
	}
	seen := map[string]bool{}
	for _, s := range f.Settings {
		if s.Name == "" || seen[s.Name] {
			return nil, fmt.Errorf("gucs: setting %q is empty or listed twice", s.Name)
		}
		seen[s.Name] = true
		switch s.Scope {
		case "server", "role", "storage", "txn":
		default:
			return nil, fmt.Errorf("gucs: %s: scope %q is not server, role, storage or txn", s.Name, s.Scope)
		}
	}
	return &f, nil
}

// lookup returns the production value of a setting ("lock_timeout_exclusive") or of a role setting
// ("roles.engram_app.lock_timeout").
func (f *gucFile) lookup(ref string) (string, bool) {
	if rest, ok := strings.CutPrefix(ref, "roles."); ok {
		role, key, ok := strings.Cut(rest, ".")
		v, found := f.Roles[role][key]
		return v, ok && found
	}
	for _, s := range f.Settings {
		if s.Name == ref && s.Prod != nil {
			return *s.Prod, true
		}
	}
	return "", false
}

// genGUCs renders the GUC table from the YAML at path.
func genGUCs(path string) (string, error) {
	f, err := loadGUCs(path)
	if err != nil {
		return "", err
	}
	d := NewDoc("Postgres settings", "`deploy/postgres/gucs.yaml`",
		"PLAN.md 9.1; register N114, N122, N131, N139, N166, N167")
	d.Para("One table feeds the `postgres -c` include file, the `ALTER ROLE` statements and `engramctl config lint`. " +
		"`prod` is the value on a 128 GB, 8 vCPU shard host and `dev` the same setting scaled for the single-host " +
		"compose. Scopes: `server` = postgresql.conf, `role` = ALTER ROLE, `storage` = per-table storage parameter " +
		"set by the migrations, `txn` = `SET LOCAL` inside a transaction by `engramctl`.")
	d.H2("Container")
	var rows [][]string
	for _, p := range sortedKeys(f.Container) {
		v := f.Container[p]
		rows = append(rows, []string{p, "`" + v["cpus"] + "`", "`" + v["memory"] + "`", "`" + v["shm_size"] + "`"})
	}
	d.Table([]string{"Profile", "cpus", "memory", "shm_size"}, rows)
	d.H2("Settings")
	rows = nil
	for _, s := range f.Settings {
		rows = append(rows, []string{"`" + s.Name + "`", orDash(s.Prod), orDash(s.Dev), s.Scope})
	}
	d.Table([]string{"Setting", "prod", "dev", "Scope"}, rows)
	d.H3("Server kinds and notes")
	for _, s := range f.Settings {
		var extra []string
		if s.Plain != nil {
			extra = append(extra, "plain servers `"+*s.Plain+"`")
		}
		for _, k := range sortedKeys(s.Kinds) {
			extra = append(extra, k+" `"+s.Kinds[k]+"`")
		}
		if len(s.Only) > 0 {
			extra = append(extra, "only on "+strings.Join(s.Only, ", "))
		}
		line := "`" + s.Name + "`"
		if len(extra) > 0 {
			line += " (" + strings.Join(extra, "; ") + ")"
		}
		d.Bullet(line + ": " + s.Ref)
	}
	d.EndList()
	if len(f.Unsized.Settings) > 0 {
		d.H3("Unsized servers")
		d.Para("On the servers of kind %s these settings stay at the PostgreSQL defaults until someone sizes them: %s.",
			strings.Join(f.Unsized.Kinds, ", "), strings.Join(f.Unsized.Settings, ", "))
	}
	d.H2("Role settings")
	for _, r := range sortedKeys(f.Roles) {
		var kv []string
		for _, k := range sortedKeys(f.Roles[r]) {
			kv = append(kv, "`"+k+" = "+f.Roles[r][k]+"`")
		}
		d.Bullet("`" + r + "`: " + strings.Join(kv, ", "))
	}
	d.EndList()
	return d.String(), nil
}
