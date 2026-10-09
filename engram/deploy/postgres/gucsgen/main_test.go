package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func load(t *testing.T) *Table {
	t.Helper()
	f, err := os.Open("../gucs.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	tab, err := Load(f)
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func render(t *testing.T, tab *Table, profile, kind string) string {
	t.Helper()
	var b strings.Builder
	if err := tab.RenderConf(&b, profile, kind, "stanza-x"); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestRenderConf(t *testing.T) {
	tab := load(t)
	prod, dev := render(t, tab, "prod", "shard"), render(t, tab, "dev", "catalog")
	for _, want := range []string{"shared_buffers = '32GB'", "checkpoint_timeout = '30min'", "wal_compression = 'zstd'",
		"archive_command = 'pgbackrest --stanza=stanza-x archive-push %p'", "tcp_keepalives_idle = '10'",
		"synchronous_commit = 'local'", "autovacuum_freeze_max_age = '1000000000'",
		"shared_preload_libraries = 'pg_search,pg_cron,pg_stat_statements'"} {
		if !strings.Contains(prod, want) {
			t.Errorf("prod shard conf lacks %q", want)
		}
	}
	for _, want := range []string{"shared_buffers = '256MB'", "synchronous_commit = 'on'",
		"synchronous_standby_names = ''", "max_replication_slots = '5'", "max_slot_wal_keep_size = '2GB'",
		"hot_standby = 'on'",
		"shared_preload_libraries = 'pg_stat_statements'"} {
		if !strings.Contains(dev, want) {
			t.Errorf("dev catalog conf lacks %q", want)
		}
	}
	for _, not := range []string{"lock_timeout_exclusive", "autovacuum_freeze_min_age", "${STANZA}",
		"synchronous_standby_names", "max_slot_wal_keep_size"} {
		if strings.Contains(prod, not) {
			t.Errorf("a non-shard or non-server setting leaked into the shard conf: %s", not)
		}
	}
	// the plan sizes only the shard: in prod the other kinds keep the Postgres defaults of the sized settings
	for _, kind := range []string{"catalog", "standby", "temporal"} {
		p := render(t, tab, "prod", kind)
		sized := []string{"shared_buffers", "effective_cache_size", "max_wal_size", "autovacuum_freeze_max_age"}
		for _, not := range sized {
			if strings.Contains(p, not) {
				t.Errorf("prod %s conf carries the shard-sized %s", kind, not)
			}
		}
		if !strings.Contains(p, "archive_command") || !strings.Contains(p, "tcp_keepalives_idle") {
			t.Errorf("prod %s conf lost its unsized-independent settings", kind)
		}
	}
	if strings.Contains(dev, "autovacuum_freeze_max_age") {
		t.Error("dev catalog conf must not raise the freeze age (no dev value, unsized)")
	}
	standby, temporal := render(t, tab, "dev", "standby"), render(t, tab, "dev", "temporal")
	if strings.Contains(standby, "synchronous_standby_names") || !strings.Contains(standby, "hot_standby = 'on'") ||
		!strings.Contains(standby, "archive_command = 'pgbackrest --stanza=stanza-x") {
		t.Error("the standby runs the catalog's table (hot standby, archive command, no synchronous_standby_names)")
	}
	if !strings.Contains(temporal, "max_connections = '400'") || strings.Contains(temporal, "hot_standby") {
		t.Error("temporal-postgres: 400 connections for the Temporal pools, no replication settings")
	}
}

func TestLimits(t *testing.T) {
	tab := load(t)
	if tab.Container["dev"].ShmSize == "" || tab.Container["prod"].Memory != "112g" {
		t.Errorf("container limits: %+v", tab.Container)
	}
}

func TestLoadRejectsBadTables(t *testing.T) {
	for name, doc := range map[string]string{
		"unknown key": "container: {prod: {}, dev: {}}\nsettings: [{name: a, prod: b, scope: server, bogus: 1}]\n",
		"duplicate": "container: {prod: {}, dev: {}}\n" +
			"settings: [{name: a, prod: b, scope: server}, {name: a, prod: c, scope: server}]\n",
		"unknown kind": "container: {prod: {}, dev: {}}\n" +
			"settings: [{name: a, prod: b, scope: server, only: [pgbouncer]}]\n",
		"unknown scope": "container: {prod: {}, dev: {}}\nsettings: [{name: a, prod: b, scope: pgbouncer}]\n",
		"no dev limits": "container: {prod: {}}\nsettings: []\n",
	} {
		if _, err := Load(strings.NewReader(doc)); err == nil {
			t.Errorf("%s: Load accepted the table", name)
		}
	}
}

// knownMissing are role settings of the table that migration 0001 does not carry yet (PLAN.md section 9.1 lists
// plan_cache_mode for engram_app; migrations/shard/0001_init.sql, owned by M0.2, omits it - see the M0.4 report). The
// test fails when the gap closes, so the entry is removed with it.
var knownMissing = map[string]bool{"engram_app.plan_cache_mode": true}

// TestRolesMatchMigration is the drift check between the table and the role defaults the migration applies.
func TestRolesMatchMigration(t *testing.T) {
	tab := load(t)
	sql, err := os.ReadFile("../../../migrations/shard/0001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?m)^ALTER ROLE (\w+)\s+SET (\w+) = '([^']*)';`)
	inMigration := map[string]string{}
	for _, m := range re.FindAllStringSubmatch(string(sql), -1) {
		inMigration[m[1]+"."+m[2]] = m[3]
	}
	if len(inMigration) == 0 {
		t.Fatal("found no ALTER ROLE ... SET statements in migration 0001")
	}
	for key, v := range inMigration {
		role, name, _ := strings.Cut(key, ".")
		if got, ok := tab.Roles[role][name]; !ok || got != v {
			t.Errorf("migration sets %s = %q, gucs.yaml has %q (present %v)", key, v, got, ok)
		}
	}
	for role, kv := range tab.Roles {
		for name, v := range kv {
			key := role + "." + name
			if _, ok := inMigration[key]; !ok {
				if !knownMissing[key] {
					t.Errorf("gucs.yaml sets %s = %q but migration 0001 does not", key, v)
				}
				continue
			}
			if knownMissing[key] {
				t.Errorf("%s is now in migration 0001: remove it from knownMissing", key)
			}
		}
	}
}
