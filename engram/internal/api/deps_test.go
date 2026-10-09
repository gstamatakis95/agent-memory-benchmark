package api_test

import (
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "github.com/gstamatakis95/engram/gen/go/memory/admin/v1" // registers memory.admin.v1
	_ "github.com/gstamatakis95/engram/gen/go/memory/v1"       // registers memory.v1
	"github.com/gstamatakis95/engram/internal/api"
	"github.com/gstamatakis95/engram/internal/catalog"
)

// servedRPCs lists every RPC of the served protos (memory.v1 and memory.admin.v1) from the generated descriptors.
func servedRPCs(t *testing.T) []string {
	t.Helper()
	var rpcs []string
	for _, pkg := range []protoreflect.FullName{"memory.v1", "memory.admin.v1"} {
		protoregistry.GlobalFiles.RangeFilesByPackage(pkg, func(fd protoreflect.FileDescriptor) bool {
			for i := 0; i < fd.Services().Len(); i++ {
				svc := fd.Services().Get(i)
				for j := 0; j < svc.Methods().Len(); j++ {
					rpcs = append(rpcs, "/"+string(svc.FullName())+"/"+string(svc.Methods().Get(j).Name()))
				}
			}
			return true
		})
	}
	sort.Strings(rpcs)
	return rpcs
}

// TestDeps_EveryRPCHasPath is the generated (RPC -> interface method) coverage check of N157: every public and admin
// RPC maps to a method path through api.Deps, and every path resolves by reflection, so a new RPC cannot ship without
// the method that implements it. The half of the §8 row that says "move.Orchestrator is in the api allow-list" is
// carried by the depguard rule `layer_api` in .golangci.yml (a missing import fails `make lint`), not by a Go test.
func TestDeps_EveryRPCHasPath(t *testing.T) {
	rpcs := servedRPCs(t)
	if len(rpcs) != 55 {
		t.Fatalf("found %d RPCs in the generated stubs, want 55 (nine services): the walk is broken", len(rpcs))
	}
	for _, rpc := range rpcs {
		if len(api.Coverage[rpc]) == 0 {
			t.Errorf("RPC %s has no path in api.Coverage", rpc)
		}
	}
	known := map[string]bool{}
	for _, r := range rpcs {
		known[r] = true
	}
	for rpc := range api.Coverage {
		if !known[rpc] {
			t.Errorf("api.Coverage names %s, which is not an RPC of the served protos", rpc)
		}
	}
	if err := api.VerifyCoverage(); err != nil {
		t.Error(err)
	}
	// N184(1): the same test asserts that every non-derived namespace_moves column has exactly one section 2 writer.
	for _, problem := range checkMoveColumnWriters(namespaceMovesColumns(t), moveColumnWriters, moveWriterMethods()) {
		t.Error("namespace_moves: " + problem)
	}
}

// TestDeps_PathCheckHasTeeth shows that a missing method, a wrong hop and a field that merely exists are all rejected.
func TestDeps_PathCheckHasTeeth(t *testing.T) {
	deps := reflect.TypeOf(api.Deps{})
	bad := []api.Path{
		{"Stores", "store.Stores.For", "store.Store.NoSuchMethod"},
		// Store is not reachable from Stores without For.
		{"Stores", "store.Store.ReadNamespace"},
		// skips ReadSession.Tx.
		{"Stores", "store.Stores.For", "store.Store.ReadNamespace", "store.ReadTx.Content"},
		{"NoSuchField", "catalog.Namespaces.Create"},
		{"Catalog", "catalog.Namespaces.Create", "catalog.Moves.Plan"},
		{"Catalog"},
	}
	for _, p := range bad {
		if err := api.ResolvePath(deps, p); err == nil {
			t.Errorf("path %v must not resolve", p)
		}
	}
	good := api.Path{"Catalog", "catalog.Namespaces.Create"}
	if err := api.ResolvePath(deps, good); err != nil {
		t.Errorf("path %v: %v", good, err)
	}
}

// TestDeps_FieldsAreInterfaces pins "every field is an interface" (section 2.3) so a service stays swappable.
func TestDeps_FieldsAreInterfaces(t *testing.T) {
	d := reflect.TypeOf(api.Deps{})
	for i := 0; i < d.NumField(); i++ {
		f := d.Field(i)
		if k := f.Type.Kind(); k != reflect.Interface && k != reflect.Func {
			t.Errorf("Deps.%s is a %s; every dependency is an interface (or the Clock func)", f.Name, k)
		}
	}
}

// moveColumnWriters maps every non-derived namespace_moves column to the section 2 methods that write it (N184, A8-2),
// written by hand from the method comments. N184(1): exactly one writer per column. N184(5) itself adds the catalog
// reconcile (Reconciler.FromShards) as a second writer of the columns it re-derives; that is the only permitted second
// writer (reconcileMayAlsoWrite). `state` is the transition column: only the compare-and-set methods Plan, Advance,
// Commit and Rollback move it, and the catalog_check_move_transition trigger validates every edge. `created_at` and
// `updated_at` are derived (a default and a trigger).
var moveColumnWriters = map[string][]string{
	"move_id": {"Moves.Plan"}, "namespace_id": {"Moves.Plan"}, "tenant_id": {"Moves.Plan"},
	"source_shard_id": {"Moves.Plan"}, "target_shard_id": {"Moves.Plan"}, "from_epoch": {"Moves.Plan"},
	"to_epoch": {"Moves.Plan"}, "source_system_id": {"Moves.Plan"}, "source_timeline_id": {"Moves.Plan"},
	"w_est_seconds": {"Moves.Plan"}, "window_seconds": {"Moves.Plan"}, "created_by": {"Moves.Plan"},
	"frozen_at": {"Moves.Advance"}, "freeze_deadline": {"Moves.Advance"}, "w_final": {"Moves.Advance"},
	"committed_at": {"Moves.Commit"}, "error": {"Moves.Rollback"},
	"ready_at": {"MoveStamps.Stamp"}, "moved_out_at": {"MoveStamps.Stamp"}, "activated_at": {"MoveStamps.Stamp"},
	"finished_at":               {"MoveStamps.Stamp"},
	"copy_end_lsn":              {"MoveStamps.RecordFloor", "Reconciler.FromShards"},
	"copy_end_timeline":         {"MoveStamps.RecordFloor", "Reconciler.FromShards"},
	"copy_sealed_at":            {"MoveStamps.RecordFloor", "Reconciler.FromShards"},
	"committed_replicated_at":   {"MoveStamps.RecordReplicated", "Reconciler.FromShards"},
	"rolled_back_replicated_at": {"MoveStamps.RecordReplicated"},
	"reconciled_at":             {"Reconciler.FromShards"},
}

var (
	moveStateMachineColumns = map[string]bool{"state": true}
	moveDerivedColumns      = map[string]bool{"created_at": true, "updated_at": true}
	// reconcileMayAlsoWrite are the columns N184(5) names for the reconcile: it takes copy_end_lsn/copy_end_timeline
	// from the target's floor, stamps copy_sealed_at = committed_replicated_at = reconciled_at.
	reconcileMayAlsoWrite = map[string]bool{
		"copy_end_lsn": true, "copy_end_timeline": true, "copy_sealed_at": true, "committed_replicated_at": true,
		"reconciled_at": true,
	}
	// moveColumnsAwaitingRuling are the columns the prose gives a writer (Drain's jsonb upsert, N97; the restore
	// tooling, N183) but for which section 2 declares no method. CONFLICTS.md #11 (human ruling pending) decides which
	// method writes them. They are the ONLY columns allowed to have no writer, and the test fails as soon as one gains
	// one, so the entry is removed together with the ruling.
	moveColumnsAwaitingRuling = map[string]bool{
		"terminated_workflows": true, "lost_at": true, "lost_restore_id": true, "recovered_from_move_id": true,
	}
)

// checkMoveColumnWriters returns every violation of the N184 rule for the given DDL columns, writer table and methods.
func checkMoveColumnWriters(cols []string, writers map[string][]string, methods map[string]bool) []string {
	var bad []string
	seen := map[string]bool{}
	for _, c := range cols {
		seen[c] = true
		ws := writers[c]
		switch {
		case moveStateMachineColumns[c] || moveDerivedColumns[c]:
			continue
		case moveColumnsAwaitingRuling[c]:
			if len(ws) != 0 {
				bad = append(bad, c+" now has a writer: remove it from moveColumnsAwaitingRuling")
			}
			continue
		case len(ws) == 0:
			bad = append(bad, c+" has no writer in section 2 (N184) and is not awaiting the ruling of CONFLICTS.md #11")
			continue
		}
		primary := 0
		for _, w := range ws {
			switch {
			case !methods[w]:
				bad = append(bad, c+" names writer "+w+", which is not a section 2 method")
			case w == "Reconciler.FromShards":
				if !reconcileMayAlsoWrite[c] {
					bad = append(bad, c+" is written by the reconcile, which N184(5) does not allow for it")
				}
			default:
				primary++
			}
		}
		if primary > 1 || (primary == 0 && c != "reconciled_at") {
			bad = append(bad, c+" must have exactly one writer besides the reconcile (N184(1))")
		}
	}
	for c := range writers {
		if !seen[c] {
			bad = append(bad, "writer table names "+c+", which is not a namespace_moves column")
		}
	}
	return bad
}

// namespaceMovesColumns parses the column list of namespace_moves from the DDL.
func namespaceMovesColumns(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("../../docs/plan/sql/catalog_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(b)
	start := strings.Index(sql, "CREATE TABLE namespace_moves (")
	end := start + strings.Index(sql[start:], "\n);")
	var cols []string
	colRE := regexp.MustCompile(`^  ([a-z_]+)\s+[a-z]`)
	for _, line := range strings.Split(sql[start:end], "\n")[1:] {
		if m := colRE.FindStringSubmatch(line); m != nil && !strings.HasPrefix(strings.TrimSpace(line), "CHECK") {
			cols = append(cols, m[1])
		}
	}
	if len(cols) != 34 {
		t.Fatalf("parsed %d columns of namespace_moves, want 34: %v", len(cols), cols)
	}
	return cols
}

func moveWriterMethods() map[string]bool {
	methods := map[string]bool{}
	for _, typ := range []reflect.Type{reflect.TypeOf((*catalog.Moves)(nil)).Elem(),
		reflect.TypeOf((*catalog.MoveStamps)(nil)).Elem(), reflect.TypeOf((*catalog.Reconciler)(nil)).Elem()} {
		for i := 0; i < typ.NumMethod(); i++ {
			methods[typ.Name()+"."+typ.Method(i).Name] = true
		}
	}
	return methods
}

// TestDeps_MoveColumnCheckHasTeeth: removing a writer, adding a second one, naming a non-method and letting the
// reconcile write a column N184(5) does not name each fail the check.
func TestDeps_MoveColumnCheckHasTeeth(t *testing.T) {
	cols, methods := namespaceMovesColumns(t), moveWriterMethods()
	if bad := checkMoveColumnWriters(cols, moveColumnWriters, methods); len(bad) != 0 {
		t.Fatalf("the real table must pass: %v", bad)
	}
	mutate := func(f func(w map[string][]string)) []string {
		w := map[string][]string{}
		for k, v := range moveColumnWriters {
			w[k] = append([]string(nil), v...)
		}
		f(w)
		return checkMoveColumnWriters(cols, w, methods)
	}
	set := func(col string, ws ...string) func(map[string][]string) {
		return func(w map[string][]string) { w[col] = ws }
	}
	probes := map[string]func(map[string][]string){
		"a removed writer":                  func(w map[string][]string) { delete(w, "frozen_at") },
		"a second primary writer":           set("frozen_at", "Moves.Advance", "Moves.Commit"),
		"a writer that is no method":        set("frozen_at", "Moves.NoSuchMethod"),
		"a reconcile writer not named in 5": set("frozen_at", "Moves.Advance", "Reconciler.FromShards"),
		"a column of no table":              set("no_such_column", "Moves.Plan"),
		"an awaiting column with an owner":  set("lost_at", "MoveStamps.Stamp"),
	}
	for name, f := range probes {
		if len(mutate(f)) == 0 {
			t.Errorf("probe %q passed the check", name)
		}
	}
}
