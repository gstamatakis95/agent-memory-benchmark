// Package pgindex executes the index procedures of PLAN.md section 3.3.4 and 9.2 against a shard: the generated
// statements of engram_partitioned_index_ddl (an ordinary index added to a partitioned table later, P-10) and of
// engram_hnsw_ddl (the per-namespace partial HNSW, N112). CREATE INDEX CONCURRENTLY cannot run inside a transaction or
// on a partitioned parent, so every statement runs as its own top-level statement on a connection of the index owner
// (role engram_migrate, N138); `engramctl index` is the CLI around this package and TestIndex_PartitionedProcedure
// exercises it.
package pgindex

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// buildLockTimeout bounds every statement's wait for a conflicting lock (section 9.2: lock_timeout = 5 s); a
// build interrupted by it leaves an INVALID index that the next run drops first.
const buildLockTimeout = "5s"

// maxParallel is the number of partitions built at the same time (section 9.2: "at most two at a time").
const maxParallel = 2

// Spec describes an ordinary index to add to a partitioned table.
type Spec struct {
	Parent  string // the partitioned table
	Name    string // the index name on the parent; partition indexes are <Name>_<partition>
	Columns string // the index definition inside the parentheses, e.g. "namespace_id, mentioned_at"
	Using   string // access method; empty means btree
	Where   string // optional partial-index predicate
}

// Runner holds the connection string of the index owner.
type Runner struct {
	DSN string
	// Parallel overrides the number of simultaneously built partitions (default and maximum two).
	Parallel int
}

func (r *Runner) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(r.DSN)
	if err != nil {
		return nil, fmt.Errorf("pgindex: parse dsn: %w", err)
	}
	cfg.RuntimeParams["lock_timeout"] = buildLockTimeout
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgindex: connect: %w", err)
	}
	return c, nil
}

var (
	reOnly   = regexp.MustCompile(`^CREATE INDEX (\S+) ON ONLY `)
	reCreate = regexp.MustCompile(`^CREATE INDEX CONCURRENTLY (\S+) ON `)
	reAttach = regexp.MustCompile(`^ALTER INDEX (\S+) ATTACH PARTITION (\S+)$`)
)

// indexState reports whether an index of that name exists in public and whether it is valid.
func indexState(ctx context.Context, c *pgx.Conn, name string) (exists, valid bool, err error) {
	err = c.QueryRow(ctx, `SELECT x.indisvalid FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
		 WHERE i.relnamespace = 'public'::regnamespace AND i.relname = $1`, name).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	return err == nil, valid, err
}

func isAttached(ctx context.Context, c *pgx.Conn, name string) (bool, error) {
	var n int
	err := c.QueryRow(ctx, `SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		 WHERE c.relnamespace = 'public'::regnamespace AND c.relname = $1`, name).Scan(&n)
	return n > 0, err
}

// step is one generated statement grouped by the partition index it belongs to.
type step struct {
	kind string // only | create | attach
	name string // the index the statement creates or attaches
	sql  string
}

func classify(stmt string) (step, error) {
	if m := reOnly.FindStringSubmatch(stmt); m != nil {
		return step{kind: "only", name: m[1], sql: stmt}, nil
	}
	if m := reCreate.FindStringSubmatch(stmt); m != nil {
		return step{kind: "create", name: m[1], sql: stmt}, nil
	}
	if m := reAttach.FindStringSubmatch(stmt); m != nil {
		return step{kind: "attach", name: m[2], sql: stmt}, nil
	}
	return step{}, fmt.Errorf("pgindex: unexpected statement from engram_partitioned_index_ddl: %q", stmt)
}

// Statements returns the statements engram_partitioned_index_ddl generates for spec, in execution order.
func (r *Runner) Statements(ctx context.Context, spec Spec) ([]string, error) {
	c, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close(ctx) }()
	return statements(ctx, c, spec)
}

func statements(ctx context.Context, c *pgx.Conn, spec Spec) ([]string, error) {
	using := spec.Using
	if using == "" {
		using = "btree"
	}
	var where *string
	if spec.Where != "" {
		where = &spec.Where
	}
	rows, err := c.Query(ctx, `SELECT statement FROM engram_partitioned_index_ddl($1::regclass, $2, $3, $4, $5)
		 ORDER BY step`, spec.Parent, spec.Name, spec.Columns, using, where)
	if err != nil {
		return nil, fmt.Errorf("pgindex: generate statements: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// BuildPartitioned adds spec to the partitioned table the way section 9.2 prescribes: CREATE INDEX ... ON ONLY the
// parent (created INVALID, builds nothing), CREATE INDEX CONCURRENTLY on every partition (at most two at a time, an
// INVALID leftover of an interrupted build is dropped first), then ALTER INDEX ... ATTACH PARTITION; the parent becomes
// valid when the last partition is attached. A run is resumable: whatever already exists and is valid is skipped.
func (r *Runner) BuildPartitioned(ctx context.Context, spec Spec) error {
	main, err := r.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = main.Close(ctx) }()
	stmts, err := statements(ctx, main, spec)
	if err != nil {
		return err
	}
	var steps []step
	for _, s := range stmts {
		st, err := classify(s)
		if err != nil {
			return err
		}
		steps = append(steps, st)
	}
	for _, st := range steps {
		if st.kind != "only" {
			continue
		}
		if exists, _, err := indexState(ctx, main, st.name); err != nil {
			return err
		} else if !exists {
			if _, err := main.Exec(ctx, st.sql); err != nil {
				return fmt.Errorf("pgindex: %s: %w", st.sql, err)
			}
		}
	}
	par := r.Parallel
	if par < 1 || par > maxParallel {
		par = maxParallel
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(par)
	for _, st := range steps {
		if st.kind != "create" {
			continue
		}
		g.Go(func() error { return r.buildOne(gctx, st) })
	}
	if err := g.Wait(); err != nil {
		return err
	}
	for _, st := range steps {
		if st.kind != "attach" {
			continue
		}
		if ok, err := isAttached(ctx, main, st.name); err != nil {
			return err
		} else if ok {
			continue
		}
		if _, err := main.Exec(ctx, st.sql); err != nil {
			return fmt.Errorf("pgindex: %s: %w", st.sql, err)
		}
	}
	return nil
}

// buildOne builds one partition index on its own connection: drop an INVALID leftover, skip a valid one.
func (r *Runner) buildOne(ctx context.Context, st step) error {
	c, err := r.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	exists, valid, err := indexState(ctx, c, st.name)
	if err != nil {
		return err
	}
	if exists && valid {
		return nil
	}
	if exists {
		if _, err := c.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+pgx.Identifier{st.name}.Sanitize()); err != nil {
			return fmt.Errorf("pgindex: drop invalid %s: %w", st.name, err)
		}
	}
	if _, err := c.Exec(ctx, st.sql); err != nil {
		return fmt.Errorf("pgindex: %s: %w", st.sql, err)
	}
	return nil
}

// DropPartitioned drops the parent index; the attached partition indexes go with it.
func (r *Runner) DropPartitioned(ctx context.Context, name string) error {
	c, err := r.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	_, err = c.Exec(ctx, "DROP INDEX IF EXISTS "+pgx.Identifier{name}.Sanitize())
	return err
}

// Invalid is an INVALID index (a failed or interrupted CREATE INDEX CONCURRENTLY).
type Invalid struct {
	Index string
	Table string
}

// Status lists the INVALID indexes of the schema (view engram_invalid_indexes).
func (r *Runner) Status(ctx context.Context) ([]Invalid, error) {
	c, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, `SELECT index_name, table_name FROM engram_invalid_indexes ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("pgindex: status: %w", err)
	}
	defer rows.Close()
	var out []Invalid
	for rows.Next() {
		var v Invalid
		if err := rows.Scan(&v.Index, &v.Table); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// HNSW runs the statements of engram_hnsw_ddl(table, ns, model, action) for one namespace: action is create, drop or
// rebuild. For create it reads index_state first and acts on it (section 3.3.4): an INVALID index is dropped before the
// build (never CREATE ... IF NOT EXISTS, which would accept an invalid index and leave the namespace on the exact
// path), a valid one is left alone. It returns the statements it ran.
func (r *Runner) HNSW(ctx context.Context, table, namespace, model, action string) ([]string, error) {
	c, err := r.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close(ctx) }()
	type row struct{ part, idx, state, stmt string }
	rows, err := c.Query(ctx, `SELECT partition_name, index_name, index_state, statement
		 FROM engram_hnsw_ddl($1, $2::uuid, $3, $4)`, table, namespace, model, action)
	if err != nil {
		return nil, fmt.Errorf("pgindex: engram_hnsw_ddl: %w", err)
	}
	var plan []row
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.part, &x.idx, &x.state, &x.stmt); err != nil {
			rows.Close()
			return nil, err
		}
		plan = append(plan, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(plan, func(i, j int) bool { return plan[i].part < plan[j].part })
	var ran []string
	for _, x := range plan {
		if action == "create" {
			if x.state == "valid" {
				continue
			}
			if x.state == "invalid" {
				drop := "DROP INDEX CONCURRENTLY IF EXISTS " + pgx.Identifier{x.idx}.Sanitize()
				if _, err := c.Exec(ctx, drop); err != nil {
					return ran, fmt.Errorf("pgindex: %s: %w", drop, err)
				}
				ran = append(ran, drop)
			}
		}
		if _, err := c.Exec(ctx, x.stmt); err != nil {
			return ran, fmt.Errorf("pgindex: %s: %w", x.stmt, err)
		}
		ran = append(ran, x.stmt)
	}
	return ran, nil
}
