package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The DDL is read as text, not executed: gendocs runs without a database. The scanner understands exactly what the
// migrations use: line and block comments, single-quoted strings, dollar-quoted function bodies (dropped), and
// statements ended by a semicolon.

// splitSQL returns the statements of src with comments removed and every dollar-quoted body replaced by "$$ $$".
func splitSQL(src string) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == '-' && strings.HasPrefix(src[i:], "--"):
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '/' && strings.HasPrefix(src[i:], "/*"):
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				i = len(src)
			} else {
				i += end + 4
			}
		case c == '\'':
			j := i + 1
			for j < len(src) {
				if src[j] == '\'' {
					if j+1 < len(src) && src[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j >= len(src) {
				j = len(src) - 1
			}
			cur.WriteString(src[i : j+1])
			i = j + 1
		case c == '$':
			m := dollarTag.FindString(src[i:])
			if m == "" {
				cur.WriteByte(c)
				i++
				continue
			}
			end := strings.Index(src[i+len(m):], m)
			if end < 0 {
				cur.WriteString(src[i:])
				i = len(src)
				continue
			}
			cur.WriteString(m + " " + m)
			i += len(m) + end + len(m)
		case c == ';':
			flush()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	flush()
	return out
}

var dollarTag = regexp.MustCompile(`^\$[A-Za-z_]*\$`)

// EnumType is a CREATE TYPE ... AS ENUM of the DDL.
type EnumType struct {
	Name    string
	Members []string
	File    string
}

// CheckList is a CHECK constraint that fixes a column to a closed list of literals.
type CheckList struct {
	// Name is the constraint name: the one the DDL gives (CONSTRAINT name CHECK ...) or PostgreSQL's default
	// <table>_<column>_check. A later migration refers to it in DROP CONSTRAINT.
	Name          string
	Table, Column string
	Members       []string
	Nullable      bool // CHECK (col IS NULL OR col IN (...))
	File          string
}

// DDL is what the generators read from the migrations of one database.
type DDL struct {
	Enums []EnumType
	// Checks are the CHECK lists in force after every migration: a DROP CONSTRAINT removes its list, a later list for
	// the same constraint replaces the earlier one.
	Checks []CheckList
	// Problems are inconsistencies of the DDL itself: two live CHECK lists for one column.
	Problems []string
	// Tables are the names of the tables CREATEd, partitions excluded.
	Tables []string
	// Classes maps a table class tag (insert-only, mutable, ...) to its tables, read from the FOREACH lists that set
	// COMMENT ON TABLE 'class: <tag>' (N113, N137).
	Classes map[string][]string
	// Inserts are the INSERT ... VALUES statements by table name, as parsed tuples.
	Inserts map[string][]InsertRow
}

// InsertRow is one row of an INSERT: column name to value.
type InsertRow map[string]sqlVal

type sqlVal struct {
	S    string
	Null bool
}

func (v sqlVal) String() string {
	if v.Null {
		return "NULL"
	}
	return v.S
}

// litList matches one or more comma-separated string literals.
const litList = `(?:'(?:[^']|'')*'\s*,?\s*)+`

var (
	reEnum      = regexp.MustCompile(`(?is)^CREATE\s+TYPE\s+(\w+)\s+AS\s+ENUM\s*\((.*)\)$`)
	reAlterEnum = regexp.MustCompile(`(?is)^ALTER\s+TYPE\s+(\w+)\s+ADD\s+VALUE\s+(?:IF\s+NOT\s+EXISTS\s+)?'([^']*)'`)
	reCreateTab = regexp.MustCompile(`(?is)^CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\(`)
	reAlterTab  = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(\w+)\s`)
	reLiteral   = regexp.MustCompile(`'((?:[^']|'')*)'`)
	reCheckIn   = regexp.MustCompile(`(?is)^(\w+)\s+IN\s*\(\s*(` + litList + `)\)$`)
	reCheckNull = regexp.MustCompile(`(?is)^(\w+)\s+IS\s+NULL\s+OR\s+(\w+)\s+IN\s*\(\s*(` + litList + `)\)$`)
	reClassList = regexp.MustCompile(`(?s)FOREACH\s+\w+\s+IN\s+ARRAY\s+ARRAY\[([^\]]*)\]\s*LOOP\s*EXECUTE\s+` +
		`format\('COMMENT ON TABLE %I IS %L',\s*\w+,\s*'class: ([a-z-]+)'\)`)
	reInsert = regexp.MustCompile(`(?is)^INSERT\s+INTO\s+(\w+)\s*\(([^)]*)\)\s*VALUES\s*(.*)$`)
)

func literals(list string) []string {
	var out []string
	for _, m := range reLiteral.FindAllStringSubmatch(list, -1) {
		out = append(out, strings.ReplaceAll(m[1], "''", "'"))
	}
	return out
}

var reConstraintName = regexp.MustCompile(`(?is)CONSTRAINT\s+(\w+)\s*$`)

// checkExprs returns the constraint name ("" when unnamed) and the balanced-parenthesis expression of every CHECK in s.
func checkExprs(s string) [][2]string {
	var out [][2]string
	for _, loc := range regexp.MustCompile(`(?i)\bCHECK\s*\(`).FindAllStringIndex(s, -1) {
		name := ""
		if m := reConstraintName.FindStringSubmatch(s[:loc[0]]); m != nil {
			name = m[1]
		}
		depth, start := 0, loc[1]-1
		inStr := false
		for i := start; i < len(s); i++ {
			switch {
			case s[i] == '\'':
				inStr = !inStr
			case inStr:
			case s[i] == '(':
				depth++
			case s[i] == ')':
				depth--
				if depth == 0 {
					out = append(out, [2]string{name, strings.TrimSpace(s[start+1 : i])})
					i = len(s)
				}
			}
		}
	}
	return out
}

func checkLists(table, stmt, file string) []CheckList {
	var out []CheckList
	for _, ne := range checkExprs(stmt) {
		e := strings.Join(strings.Fields(ne[1]), " ")
		var c CheckList
		if m := reCheckIn.FindStringSubmatch(e); m != nil {
			c = CheckList{Table: table, Column: m[1], Members: literals(m[2]), File: file}
		} else if m := reCheckNull.FindStringSubmatch(e); m != nil && m[1] == m[2] {
			c = CheckList{Table: table, Column: m[1], Members: literals(m[3]), Nullable: true, File: file}
		} else {
			continue
		}
		c.Name = ne[0]
		if c.Name == "" {
			c.Name = table + "_" + c.Column + "_check"
		}
		out = append(out, c)
	}
	return out
}

// ParseDDL reads every *.sql file of dir in name order.
func ParseDDL(dir string) (*DDL, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("ddl: no migrations in %s", dir)
	}
	d := &DDL{Inserts: map[string][]InsertRow{}, Classes: map[string][]string{}}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		name := filepath.ToSlash(f)
		for _, m := range reClassList.FindAllStringSubmatch(string(b), -1) {
			d.Classes[m[2]] = append(d.Classes[m[2]], literals(m[1])...)
		}
		for _, st := range splitSQL(string(b)) {
			switch {
			case reEnum.MatchString(st):
				m := reEnum.FindStringSubmatch(st)
				d.Enums = append(d.Enums, EnumType{Name: m[1], Members: literals(m[2]), File: name})
			case reAlterEnum.MatchString(st):
				m := reAlterEnum.FindStringSubmatch(st)
				for i := range d.Enums {
					if d.Enums[i].Name == m[1] {
						d.Enums[i].Members = append(d.Enums[i].Members, m[2])
					}
				}
			case reCreateTab.MatchString(st):
				tab := reCreateTab.FindStringSubmatch(st)[1]
				if !strings.Contains(strings.ToUpper(st), "PARTITION OF") {
					d.Tables = append(d.Tables, tab)
				}
				d.addChecks(tab, checkLists(tab, st, name))
			case reAlterTab.MatchString(st):
				tab := reAlterTab.FindStringSubmatch(st)[1]
				for _, m := range reDropConstraint.FindAllStringSubmatch(st, -1) {
					d.dropCheck(tab, m[1])
				}
				d.addChecks(tab, checkLists(tab, st, name))
			case reInsert.MatchString(st):
				m := reInsert.FindStringSubmatch(st)
				rows, err := parseTuples(m[3], splitCols(m[2]))
				if err != nil {
					return nil, fmt.Errorf("%s: INSERT INTO %s: %w", name, m[1], err)
				}
				d.Inserts[m[1]] = append(d.Inserts[m[1]], rows...)
			}
		}
	}
	return d, nil
}

var reDropConstraint = regexp.MustCompile(`(?i)DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?(\w+)`)

// dropCheck removes the CHECK list called name from table.
func (d *DDL) dropCheck(table, name string) {
	kept := d.Checks[:0]
	for _, c := range d.Checks {
		if c.Table != table || c.Name != name {
			kept = append(kept, c)
		}
	}
	d.Checks = kept
}

// addChecks adds lists in force: a list with the name of an earlier one replaces it; two lists with different names
// for one column are a problem of the DDL (the narrower one is not the only rule).
func (d *DDL) addChecks(table string, cs []CheckList) {
	for _, c := range cs {
		replaced := false
		for i := range d.Checks {
			old := d.Checks[i]
			if old.Table != table {
				continue
			}
			switch {
			case old.Name == c.Name:
				d.Checks[i], replaced = c, true
			case old.Column == c.Column:
				d.Problems = append(d.Problems, fmt.Sprintf("ddl: %s.%s has two live CHECK lists, %s (%s) and %s (%s)",
					table, c.Column, old.Name, old.File, c.Name, c.File))
			}
		}
		if !replaced {
			d.Checks = append(d.Checks, c)
		}
	}
}

func splitCols(s string) []string {
	var out []string
	for _, c := range strings.Split(s, ",") {
		out = append(out, strings.TrimSpace(c))
	}
	return out
}

// parseTuples reads "(v, ...), (v, ...)" where a value is NULL, a number or one or more adjacent string literals.
func parseTuples(s string, cols []string) ([]InsertRow, error) {
	var rows []InsertRow
	i := 0
	skip := func() {
		for i < len(s) && (s[i] == ' ' || s[i] == '\n' || s[i] == '\t' || s[i] == '\r') {
			i++
		}
	}
	for {
		skip()
		if i >= len(s) {
			return rows, nil
		}
		if s[i] == ',' {
			i++
			continue
		}
		if s[i] != '(' {
			return nil, fmt.Errorf("expected a tuple at %q", s[i:min(i+20, len(s))])
		}
		i++
		var vals []sqlVal
		for {
			skip()
			switch {
			case i < len(s) && s[i] == '\'':
				var lit strings.Builder
				for i < len(s) && s[i] == '\'' {
					j := i + 1
					for j < len(s) {
						if s[j] == '\'' {
							if j+1 < len(s) && s[j+1] == '\'' {
								lit.WriteByte('\'')
								j += 2
								continue
							}
							break
						}
						lit.WriteByte(s[j])
						j++
					}
					i = j + 1
					skip()
				}
				vals = append(vals, sqlVal{S: lit.String()})
			case strings.HasPrefix(strings.ToUpper(s[i:]), "NULL"):
				vals = append(vals, sqlVal{Null: true})
				i += 4
			default:
				j := i
				for j < len(s) && s[j] != ',' && s[j] != ')' {
					j++
				}
				vals = append(vals, sqlVal{S: strings.TrimSpace(s[i:j])})
				i = j
			}
			skip()
			if i < len(s) && s[i] == ',' {
				i++
				continue
			}
			if i < len(s) && s[i] == ')' {
				i++
				break
			}
			return nil, fmt.Errorf("malformed tuple near %q", s[i:min(i+20, len(s))])
		}
		if len(vals) != len(cols) {
			return nil, fmt.Errorf("tuple has %d values for %d columns", len(vals), len(cols))
		}
		row := InsertRow{}
		for k, c := range cols {
			row[c] = vals[k]
		}
		rows = append(rows, row)
	}
}
