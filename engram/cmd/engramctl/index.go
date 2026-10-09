package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gstamatakis95/engram/internal/store/pgindex"
)

func init() {
	Register(Command{
		Name:  "index",
		Short: "partitioned-index procedure (build|drop|status) and per-namespace HNSW statements (hnsw)",
		Run:   runIndex,
	})
}

const indexUsage = `usage: engramctl index build  --dsn DSN --parent TABLE --name INDEX --columns "a, b"
                             [--using METHOD] [--where PREDICATE]
       engramctl index drop   --dsn DSN --name INDEX
       engramctl index status --dsn DSN
       engramctl index hnsw   --dsn DSN --table VECTOR_TABLE --namespace UUID --model MODEL
                             [--action create|drop|rebuild]

build   adds an index to a partitioned table (PLAN.md section 9.2): CREATE INDEX ... ON ONLY the parent, then
        CREATE INDEX CONCURRENTLY on each of the partitions (two at a time, lock_timeout 5 s, an INVALID leftover is
        dropped first), then ATTACH PARTITION; re-running resumes. CREATE INDEX CONCURRENTLY on the parent itself is
        refused by PostgreSQL and by engramlint migrate, which is why this command exists.
hnsw    runs the engram_hnsw_ddl statements of one namespace: the per-namespace partial HNSW (N112). An INVALID index
        is dropped before every build (never IF NOT EXISTS).

The DSN must be that of role engram_migrate: it owns every index (N138); engram_admin cannot create or drop one.`

func runIndex(ctx context.Context, args []string) error { return indexMain(ctx, args, os.Stdout) }

func indexMain(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(strings.TrimSpace(indexUsage))
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("index "+sub, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dsn := fs.String("dsn", os.Getenv("ENGRAM_MIGRATE_DSN"), "DSN of role engram_migrate (default $ENGRAM_MIGRATE_DSN)")
	parent := fs.String("parent", "", "partitioned table")
	name := fs.String("name", "", "index name on the parent")
	cols := fs.String("columns", "", "index definition inside the parentheses")
	using := fs.String("using", "btree", "access method")
	where := fs.String("where", "", "partial index predicate")
	table := fs.String("table", "", "vector table (hnsw)")
	ns := fs.String("namespace", "", "namespace id (hnsw)")
	model := fs.String("model", "", "embedding model (hnsw)")
	action := fs.String("action", "create", "hnsw action: create, drop or rebuild")
	if err := fs.Parse(rest); err != nil {
		return fmt.Errorf("%w\n%s", err, indexUsage)
	}
	if *dsn == "" {
		return fmt.Errorf("--dsn is required\n%s", indexUsage)
	}
	r := &pgindex.Runner{DSN: *dsn}
	switch sub {
	case "build":
		if *parent == "" || *name == "" || *cols == "" {
			return fmt.Errorf("build needs --parent, --name and --columns\n%s", indexUsage)
		}
		spec := pgindex.Spec{Parent: *parent, Name: *name, Columns: *cols, Using: *using, Where: *where}
		if err := r.BuildPartitioned(ctx, spec); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(out, "index %s on %s built and attached\n", *name, *parent)
		return nil
	case "drop":
		if *name == "" {
			return fmt.Errorf("drop needs --name\n%s", indexUsage)
		}
		return r.DropPartitioned(ctx, *name)
	case "status":
		inv, err := r.Status(ctx)
		if err != nil {
			return err
		}
		for _, v := range inv {
			_, _ = fmt.Fprintf(out, "INVALID\t%s\t%s\n", v.Index, v.Table)
		}
		return nil
	case "hnsw":
		if *table == "" || *ns == "" || *model == "" {
			return fmt.Errorf("hnsw needs --table, --namespace and --model\n%s", indexUsage)
		}
		ran, err := r.HNSW(ctx, *table, *ns, *model, *action)
		for _, s := range ran {
			_, _ = fmt.Fprintln(out, s)
		}
		return err
	default:
		return fmt.Errorf("unknown index subcommand %q\n%s", sub, indexUsage)
	}
}
