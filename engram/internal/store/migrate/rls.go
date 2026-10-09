package migrate

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Violation is one row of the static RLS check: a relation (or role) and what is wrong with it.
type Violation struct {
	Relation string
	Problem  string
}

func (v Violation) String() string { return v.Relation + "\t" + v.Problem }

// nsPolicyQual is the qualifier pg_policies renders for the ns_isolation policy of every namespace-scoped relation
// (PLAN.md section 3.3.8); the check compares against this exact string for USING and WITH CHECK.
const nsPolicyQual = "(namespace_id = (current_setting('engram.namespace_id'::text))::uuid)"

// shardLevelTables are the tables of the shard schema that have no namespace_id column (PLAN.md section 3.3.8:
// "exactly shard_meta, outbox_cursors, ownership_transitions, engram_seq_log and goose_db_version"). PLAN.md section
// 8.3 abbreviates the list to three; the section 3.3.8 list is what the DDL and its self-check carry.
var shardLevelTables = []string{
	"shard_meta", "outbox_cursors", "ownership_transitions", "engram_seq_log", "goose_db_version",
}

// rlsCheckSQL returns one row per violation. A relation is namespace-scoped iff it has a namespace_id column; the
// relations of extensions (PostGIS's spatial_ref_sys in the ParadeDB template database) are not ours and are skipped.
const rlsCheckSQL = `
WITH rel AS (
  SELECT c.oid, c.relname::text AS relname, c.relrowsecurity, c.relforcerowsecurity,
         EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attname = 'namespace_id'
                    AND a.attnum > 0 AND NOT a.attisdropped) AS scoped
    FROM pg_class c
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'p')
     AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
), bad AS (
  SELECT relname, 'row level security is not enabled' AS problem FROM rel WHERE scoped AND NOT relrowsecurity
  UNION ALL
  SELECT relname, 'row level security is not forced' FROM rel WHERE scoped AND NOT relforcerowsecurity
  UNION ALL
  SELECT relname, 'policy ns_isolation is missing or its qualifier differs' FROM rel
   WHERE scoped AND NOT EXISTS (
     SELECT 1 FROM pg_policies p
      WHERE p.schemaname = 'public' AND p.tablename = rel.relname AND p.policyname = 'ns_isolation'
        AND p.permissive = 'PERMISSIVE' AND p.cmd = 'ALL' AND p.roles = '{public}'
        AND p.qual = $1 AND p.with_check = $1)
  UNION ALL
  SELECT relname, 'has no namespace_id column and is not on the shard-level allowlist' FROM rel
   WHERE NOT scoped AND relname <> ALL ($2::text[])
  UNION ALL
  SELECT p.tablename::text, 'unexpected policy ' || p.policyname FROM pg_policies p
   WHERE p.schemaname = 'public'
     AND NOT (p.policyname = 'ns_isolation'
              OR (p.policyname LIKE 'move_target\_%' AND p.roles = '{engram_move}' AND p.permissive = 'RESTRICTIVE')
              OR (p.policyname = 'relay_read_all' AND p.tablename = 'outbox' AND p.roles = '{engram_relay}'
                  AND p.cmd = 'SELECT'))
  UNION ALL
  SELECT r.rolname::text, 'role may bypass row level security' FROM pg_roles r
   WHERE r.rolname IN ('engram_app', 'engram_relay', 'engram_move') AND r.rolbypassrls
  UNION ALL
  SELECT r.rolname::text, 'role is a member (directly or transitively) of the BYPASSRLS role ' || g.rolname
    FROM pg_roles r JOIN pg_roles g ON g.rolbypassrls AND g.oid <> r.oid AND pg_has_role(r.oid, g.oid, 'MEMBER')
   WHERE r.rolname IN ('engram_app', 'engram_relay', 'engram_move')
  UNION ALL
  SELECT c.relname::text, 'view readable by ' || r.rolname || ' without security_invoker (reads past RLS)'
    FROM pg_class c JOIN pg_roles r ON r.rolname IN ('engram_app', 'engram_relay', 'engram_move')
   WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('v', 'm')
     AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
     AND has_table_privilege(r.oid, c.oid, 'SELECT')
     AND NOT coalesce('security_invoker=true' = ANY (c.reloptions), false)
)
SELECT relname, problem FROM bad ORDER BY relname, problem`

// CheckRLS is the static check of PLAN.md section 8.3 (`engramctl migrate --check-rls`, TestEveryTableHasRLS): every
// relation with a namespace_id column has RLS enabled and forced and the ns_isolation policy with the expected
// qualifier, no table without one is off the allowlist, only the relay/move/admin extra policies exist, and the three
// application roles cannot bypass RLS. The expected result is empty. conn must see the migrated database.
func CheckRLS(ctx context.Context, conn *pgx.Conn) ([]Violation, error) {
	rows, err := conn.Query(ctx, rlsCheckSQL, nsPolicyQual, shardLevelTables)
	if err != nil {
		return nil, fmt.Errorf("migrate: rls check: %w", err)
	}
	defer rows.Close()
	var out []Violation
	for rows.Next() {
		var v Violation
		if err := rows.Scan(&v.Relation, &v.Problem); err != nil {
			return nil, fmt.Errorf("migrate: rls check: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
