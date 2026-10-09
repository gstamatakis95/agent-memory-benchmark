package main

import (
	"fmt"
	"sort"
	"strings"
)

// genChecks renders the closed lists of the DDL: every enum type and every CHECK that fixes a column to a list of
// literals, per database. PLAN.md (N139, A-8) generates the `operations` CHECK lists from the proto enums and the
// prose from the DDL; this file is the DDL side of that statement and the input of the enum lint.
func genChecks(shard, catalog *DDL) (string, error) {
	d := NewDoc("Enum types and CHECK lists of the DDL", "`migrations/shard`, `migrations/catalog`",
		"register N113, N137, N139, N187")
	d.Para("Closed sets of the schema: `CREATE TYPE ... AS ENUM` types and `CHECK (column IN (...))` constraints, " +
		"as the migrations define them. A CHECK that also allows NULL is marked `or NULL`.")
	for _, db := range []struct {
		name string
		ddl  *DDL
	}{{"Shard schema", shard}, {"Catalog schema", catalog}} {
		d.H2(db.name)
		d.H3("Enum types")
		enums := append([]EnumType(nil), db.ddl.Enums...)
		sort.Slice(enums, func(i, j int) bool { return enums[i].Name < enums[j].Name })
		for _, e := range enums {
			d.Bullet("`" + e.Name + "`: " + quoteList(e.Members))
		}
		d.EndList()
		d.H3("CHECK lists")
		checks := append([]CheckList(nil), db.ddl.Checks...)
		sort.SliceStable(checks, func(i, j int) bool {
			if checks[i].Table != checks[j].Table {
				return checks[i].Table < checks[j].Table
			}
			return checks[i].Column < checks[j].Column
		})
		for _, c := range checks {
			suffix := ""
			if c.Nullable {
				suffix = " (or NULL)"
			}
			d.Bullet("`" + c.Table + "." + c.Column + "`: " + quoteList(c.Members) + suffix)
		}
		d.EndList()
	}
	if len(shard.Enums) == 0 || len(shard.Checks) == 0 {
		return "", fmt.Errorf("checks: the shard DDL yielded no enum types or CHECK lists")
	}
	return d.String(), nil
}

func quoteList(ms []string) string {
	q := make([]string, len(ms))
	for i, m := range ms {
		q[i] = "`" + m + "`"
	}
	return strings.Join(q, ", ")
}
