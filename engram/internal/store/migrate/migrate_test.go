package migrate

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

func shardFiles(t *testing.T) map[string]string {
	t.Helper()
	fsys, err := FS(Shard)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	err = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := fs.ReadFile(fsys, p)
		files[p] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// TestMigrations_Layout pins the section 9.2 layout: four numbered files, each with an Up and a Down.
func TestMigrations_Layout(t *testing.T) {
	files := shardFiles(t)
	for _, name := range []string{"0001_init.sql", "0002_partitions.sql", "0003_indexes.sql", "0004_markers.sql"} {
		body, ok := files[name]
		if !ok {
			t.Fatalf("migration %s is missing", name)
		}
		up, down := strings.Index(body, "-- +goose Up"), strings.Index(body, "-- +goose Down")
		if up < 0 || down < up {
			t.Errorf("%s needs '-- +goose Up' followed by '-- +goose Down'", name)
		}
		if strings.Contains(body, "NO TRANSACTION") {
			t.Errorf("%s: the shard migrations are transactional (SET LOCAL ROLE needs it)", name)
		}
	}
	if len(files) != 4 {
		t.Errorf("expected exactly four shard migrations, found %d", len(files))
	}
}

// TestMigrations_GooseAnnotations: PLAN.md 9.2 wraps every plpgsql body in StatementBegin/End; an unbalanced pair or
// a CREATE FUNCTION / DO outside one is split by goose mid-statement.
func TestMigrations_GooseAnnotations(t *testing.T) {
	for name, body := range shardFiles(t) {
		if b, e := strings.Count(body, "-- +goose StatementBegin"), strings.Count(body,
			"-- +goose StatementEnd"); b != e {
			t.Errorf("%s: %d StatementBegin but %d StatementEnd", name, b, e)
		}
		inside := false
		for i, ln := range strings.Split(body, "\n") {
			switch {
			case strings.HasPrefix(ln, "-- +goose StatementBegin"):
				inside = true
			case strings.HasPrefix(ln, "-- +goose StatementEnd"):
				inside = false
			case !inside && (strings.HasPrefix(ln, "CREATE FUNCTION") || strings.HasPrefix(ln,
				"CREATE OR REPLACE FUNCTION") ||
				strings.HasPrefix(ln, "DO $$")):
				t.Errorf("%s:%d: %q is not wrapped in StatementBegin/StatementEnd", name, i+1, ln)
			}
		}
	}
}

// TestMigrations_Rules checks the repository-wide rules the migrations must keep: 120 columns, no generated column
// (N113), no CREATE INDEX CONCURRENTLY statement (it cannot run in a goose transaction nor on a partitioned parent;
// index changes go through engramctl index) and no column rename in place (PLAN.md 9.2).
func TestMigrations_Rules(t *testing.T) {
	cic := regexp.MustCompile(`(?im)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+CONCURRENTLY`)
	generated := regexp.MustCompile(`(?i)\bGENERATED\s+ALWAYS\b|\bGENERATED\s+\w+\s+AS\s*\(`)
	rename := regexp.MustCompile(`(?i)RENAME\s+COLUMN`)
	for name, body := range shardFiles(t) {
		for i, ln := range strings.Split(body, "\n") {
			if n := utf8.RuneCountInString(ln); n > 120 {
				t.Errorf("%s:%d: %d characters (limit 120)", name, i+1, n)
			}
		}
		if cic.MatchString(body) {
			t.Errorf("%s: CREATE INDEX CONCURRENTLY at statement level", name)
		}
		if generated.MatchString(body) {
			t.Errorf("%s: generated column (N113)", name)
		}
		if rename.MatchString(body) {
			t.Errorf("%s: column rename in place (PLAN.md 9.2)", name)
		}
	}
}

func TestFS_UnknownSet(t *testing.T) {
	if _, err := FS("nope"); err == nil {
		t.Fatal("expected an error for an unknown set")
	}
	if _, err := FS(Catalog); err == nil {
		t.Fatal("the catalog set must not exist before M0.3")
	}
}
