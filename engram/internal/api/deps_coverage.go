package api

import (
	"fmt"
	"reflect"
	"strings"
)

// Path is one chain from a Deps field to the interface method that implements part of an RPC: element 0 is the Deps
// field, every further element is "package.Interface.Method" and must be reachable from the previous one (an interface
// returned by it, or a parameter of a callback it takes). A path is checked by reflection against Deps, so a dependency
// field that satisfies every RPC trivially cannot hide a missing method (N157, A-6).
type Path []string

// store path prefixes shared by the per-shard RPCs.
var (
	read    = Path{"Stores", "store.Stores.For", "store.Store.ReadNamespace", "store.ReadSession.Tx"}
	content = append(append(Path{}, read...), "store.ReadTx.Content")
	write   = Path{"Stores", "store.Stores.For", "store.Store.InNamespace"}
)

func with(prefix Path, steps ...string) Path { return append(append(Path{}, prefix...), steps...) }

// Coverage maps every RPC of the served protos (memory.v1 and memory.admin.v1) to the paths that implement it. It is
// the hand-written predecessor of the table `make gen-docs` will generate from the proto service list
// (deps_coverage_gen.go); TestDeps_EveryRPCHasPath asserts that its key set equals the RPC set of the generated stubs
// and that every path resolves. NewServer panics at wiring time when a row names a method no non-nil dependency
// provides.
var Coverage = map[string][]Path{
	// MemoryService
	"/memory.v1.MemoryService/Retain":  {{"Retain", "api.Submitter.Submit"}},
	"/memory.v1.MemoryService/Recall":  {{"Recall", "recall.Planner.Recall"}},
	"/memory.v1.MemoryService/Reflect": {{"Reflect", "reflectagent.Agent.Run"}},
	"/memory.v1.MemoryService/GetMemory": {
		with(content, "store.ContentReader.Facts", "store.FactReader.ByIDs"),
		with(read, "store.ReadTx.Derived", "store.DerivedReader.Observations", "store.ObservationReader.Served"),
	},
	"/memory.v1.MemoryService/ListMemories": {
		with(content, "store.ContentReader.Facts", "store.FactReader.Lister", "store.FactLister.List"),
	},
	"/memory.v1.MemoryService/BatchGetMemories": {with(content, "store.ContentReader.Facts", "store.FactReader.ByIDs")},
	"/memory.v1.MemoryService/Invalidate":       {{"Deleter", "api.Deleter.Invalidate"}},
	"/memory.v1.MemoryService/Restore":          {{"Deleter", "api.Deleter.Restore"}},

	// DocumentService
	"/memory.v1.DocumentService/GetDocument": {
		with(content, "store.ContentReader.Documents", "store.DocumentReader.Get"),
	},
	"/memory.v1.DocumentService/ListDocuments": {
		with(content, "store.ContentReader.Documents", "store.DocumentReader.List"),
	},
	"/memory.v1.DocumentService/DeleteDocument": {{"Deleter", "api.Deleter.DeleteDocument"}},
	"/memory.v1.DocumentService/GetDocumentVersion": {
		with(content, "store.ContentReader.Documents", "store.DocumentReader.Version"),
	},
	"/memory.v1.DocumentService/GetDocumentBody": {
		with(content, "store.ContentReader.Documents", "store.DocumentReader.Bodies", "store.DocumentBodies.Ref"),
	},
	"/memory.v1.DocumentService/ListTags": {with(content, "store.ContentReader.Tags", "store.TagLister.List")},
	"/memory.v1.DocumentService/UpdateDocumentTags": {
		with(write, "store.Tx.Write", "store.Writes.Tags", "store.DocumentTags.Update"),
	},

	// NamespaceService
	"/memory.v1.NamespaceService/CreateNamespace":    {{"Catalog", "catalog.Namespaces.Create"}},
	"/memory.v1.NamespaceService/GetNamespace":       {{"Catalog", "catalog.Namespaces.Resolve"}},
	"/memory.v1.NamespaceService/ListNamespaces":     {{"NsAdmin", "catalog.NamespaceAdmin.List"}},
	"/memory.v1.NamespaceService/UpdateNamespace":    {{"NsAdmin", "catalog.NamespaceAdmin.Update"}},
	"/memory.v1.NamespaceService/GetEffectiveConfig": {{"Config", "config.Resolver.Resolve"}},
	"/memory.v1.NamespaceService/DeleteNamespace":    {{"Deleter", "api.Deleter.DeleteNamespace"}},

	// OperationService
	"/memory.v1.OperationService/GetOperation": {with(read, "store.ReadTx.Operations", "store.OperationReader.Get")},
	"/memory.v1.OperationService/ListOperations": {
		with(read, "store.ReadTx.Operations", "store.OperationReader.List"),
	},
	"/memory.v1.OperationService/CancelOperation": {{"Wait", "workflows.Waiter.Cancel"}},
	"/memory.v1.OperationService/WaitOperation":   {{"Ops", "api.OperationWaiter.Wait"}},

	// ExportService (phase 3: answers UNIMPLEMENTED until then, N14)
	"/memory.v1.ExportService/CreateSnapshot": {
		{"Snapshots", "export.Builder.Begin"}, {"Start", "workflows.Starter.StartExport"},
	},
	"/memory.v1.ExportService/GetSnapshotManifest": {
		{"SnapList", "export.SnapshotLister.Manifest"},
		{"Stream", "export.Streamer.Latest"},
		{"Stream", "export.Streamer.Overlay"},
	},
	"/memory.v1.ExportService/ListSnapshots":  {{"SnapList", "export.SnapshotLister.List"}},
	"/memory.v1.ExportService/StreamSnapshot": {{"Stream", "export.Streamer.Stream"}},

	// PageService (phase 3: answers UNIMPLEMENTED until then, N14)
	"/memory.v1.PageService/CreatePage":  {{"PageWriter", "pages.Writer.Create"}},
	"/memory.v1.PageService/GetPage":     {{"Pages", "pages.Reader.Get"}, {"Pages", "pages.Reader.ByName"}},
	"/memory.v1.PageService/ListPages":   {{"Pages", "pages.Reader.List"}},
	"/memory.v1.PageService/SearchPages": {{"Pages", "pages.Reader.Search"}},
	"/memory.v1.PageService/UpdatePage":  {{"PageAdmin", "pages.Admin.Update"}},
	"/memory.v1.PageService/DeletePage":  {{"PageWriter", "pages.Writer.Delete"}},
	"/memory.v1.PageService/RefreshPage": {{"Signal", "workflows.Signaller.SignalPageRefresh"}},

	// TenantService (UpdateTenant and UpdateTenantLimits both go through Tenants.Update, one scope each, N167)
	"/memory.admin.v1.TenantService/CreateTenant":       {{"Tenants", "catalog.Tenants.Create"}},
	"/memory.admin.v1.TenantService/GetTenant":          {{"Tenants", "catalog.Tenants.Get"}},
	"/memory.admin.v1.TenantService/ListTenants":        {{"Tenants", "catalog.Tenants.List"}},
	"/memory.admin.v1.TenantService/UpdateTenant":       {{"Tenants", "catalog.Tenants.Update"}},
	"/memory.admin.v1.TenantService/UpdateTenantLimits": {{"Tenants", "catalog.Tenants.Update"}},
	"/memory.admin.v1.TenantService/DeleteTenant": {
		{"Deleter", "api.Deleter.DeleteTenant"}, {"Tenants", "catalog.Tenants.BeginDelete"},
	},
	"/memory.admin.v1.TenantService/GetTenantOperation": {{"TenantOps", "catalog.TenantOps.Operation"}},

	// ShardService (ReleaseNamespace marks the catalog row deleted after the expunge, D8, N119)
	"/memory.admin.v1.ShardService/RegisterShard":    {{"Registry", "catalog.Registry.RegisterShard"}},
	"/memory.admin.v1.ShardService/GetShard":         {{"Registry", "catalog.Registry.GetShard"}},
	"/memory.admin.v1.ShardService/ListShards":       {{"Registry", "catalog.Registry.ListShards"}},
	"/memory.admin.v1.ShardService/DrainShard":       {{"Registry", "catalog.Registry.SetShardState"}},
	"/memory.admin.v1.ShardService/UpdateShard":      {{"Registry", "catalog.Registry.UpdateShard"}},
	"/memory.admin.v1.ShardService/ResolveNamespace": {{"Catalog", "catalog.Namespaces.Resolve"}},
	"/memory.admin.v1.ShardService/ReleaseNamespace": {{"Catalog", "catalog.Namespaces.SetState"}},

	// MoveService: StartMove/RollbackMove/CleanupMove = Start/Abort/Cleanup (api may import move, N157, N167)
	"/memory.admin.v1.MoveService/StartMove":    {{"Mover", "move.Orchestrator.Start"}},
	"/memory.admin.v1.MoveService/GetMove":      {{"MoveReader", "catalog.MoveReader.Get"}},
	"/memory.admin.v1.MoveService/ListMoves":    {{"MoveReader", "catalog.MoveReader.List"}},
	"/memory.admin.v1.MoveService/RollbackMove": {{"Mover", "move.Orchestrator.Abort"}},
	"/memory.admin.v1.MoveService/CleanupMove":  {{"Mover", "move.Orchestrator.Cleanup"}},
}

// ResolvePath checks one path against the type of Deps (pass reflect.TypeOf(Deps{})).
func ResolvePath(deps reflect.Type, p Path) error {
	if len(p) < 2 {
		return fmt.Errorf("path %v has no method step", p)
	}
	f, ok := deps.FieldByName(p[0])
	if !ok {
		return fmt.Errorf("path %v: Deps has no field %q", p, p[0])
	}
	cur := []reflect.Type{f.Type}
	for _, step := range p[1:] {
		dot := strings.LastIndex(step, ".")
		if dot < 0 {
			return fmt.Errorf("path %v: step %q is not Interface.Method", p, step)
		}
		typeName, method := step[:dot], step[dot+1:]
		var recv reflect.Type
		for _, t := range cur {
			if t.String() == typeName {
				recv = t
			}
		}
		if recv == nil {
			return fmt.Errorf("path %v: %s is not reachable from the previous step", p, typeName)
		}
		m, ok := recv.MethodByName(method)
		if !ok {
			return fmt.Errorf("path %v: %s has no method %s", p, typeName, method)
		}
		cur = reachable(m.Type, nil)
	}
	return nil
}

// reachable lists the types a method hands to its caller: its results and, recursively, the parameters of the callbacks
// it takes (store.Store.InNamespace hands out a store.Tx through its func argument).
func reachable(ft reflect.Type, acc []reflect.Type) []reflect.Type {
	for i := 0; i < ft.NumOut(); i++ {
		acc = append(acc, ft.Out(i))
	}
	for i := 0; i < ft.NumIn(); i++ {
		if in := ft.In(i); in.Kind() == reflect.Func {
			acc = reachable(in, acc)
			for j := 0; j < in.NumIn(); j++ {
				acc = append(acc, in.In(j))
			}
		}
	}
	return acc
}

// VerifyCoverage resolves every path of Coverage against Deps and returns the first failure.
func VerifyCoverage() error {
	t := reflect.TypeOf(Deps{})
	for rpc, paths := range Coverage {
		if len(paths) == 0 {
			return fmt.Errorf("%s has no path", rpc)
		}
		for _, p := range paths {
			if err := ResolvePath(t, p); err != nil {
				return fmt.Errorf("%s: %w", rpc, err)
			}
		}
	}
	return nil
}
