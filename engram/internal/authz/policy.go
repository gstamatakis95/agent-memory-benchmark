package authz

import "github.com/gstamatakis95/engram/internal/quota"

// Service name prefixes of the served protos.
const (
	memorySvc    = "/memory.v1.MemoryService/"
	documentSvc  = "/memory.v1.DocumentService/"
	namespaceSvc = "/memory.v1.NamespaceService/"
	operationSvc = "/memory.v1.OperationService/"
	pageSvc      = "/memory.v1.PageService/"
	exportSvc    = "/memory.v1.ExportService/"
	tenantSvc    = "/memory.admin.v1.TenantService/"
	shardSvc     = "/memory.admin.v1.ShardService/"
	moveSvc      = "/memory.admin.v1.MoveService/"
)

// DefaultPolicy is the policy table of PLAN.md section 4.1.9: one row per RPC of the served protos, which is what lets
// the interceptor stay the only enforcement point. TestPolicy_CoversEveryRPC asserts that the key set equals the RPC
// set of the generated stubs and that each row's Target matches the shape of its request message.
//
// Buckets (D13): Recall is metered against recalls_per_min and Retain against retains_per_min; no other method has a
// rate bucket (Reflect, snapshots and the rest are bounded by their deadlines and by quota.Reserve, N130).
func DefaultPolicy() Policy {
	p := Policy{}
	ns := func(svc, method string, s Scope) MethodPolicy {
		mp := MethodPolicy{Scope: s, Target: TargetNamespace}
		p[svc+method] = mp
		return mp
	}
	for _, m := range []string{"Recall", "Reflect", "GetMemory", "ListMemories", "BatchGetMemories"} {
		ns(memorySvc, m, ScopeMemoryRead)
	}
	for _, m := range []string{"GetDocument", "ListDocuments", "GetDocumentVersion", "GetDocumentBody", "ListTags"} {
		ns(documentSvc, m, ScopeMemoryRead)
	}
	for _, m := range []string{"GetNamespace", "GetEffectiveConfig"} {
		ns(namespaceSvc, m, ScopeMemoryRead)
	}
	ns(operationSvc, "ListOperations", ScopeMemoryRead)
	for _, m := range []string{"GetSnapshotManifest", "ListSnapshots", "StreamSnapshot"} {
		ns(exportSvc, m, ScopeMemoryRead)
	}
	for _, m := range []string{"GetPage", "ListPages", "SearchPages"} {
		ns(pageSvc, m, ScopeMemoryRead)
	}
	for _, m := range []string{"Invalidate", "Restore"} {
		ns(memorySvc, m, ScopeMemoryWrite)
	}
	ns(documentSvc, "DeleteDocument", ScopeMemoryWrite)
	ns(documentSvc, "UpdateDocumentTags", ScopeMemoryWrite)
	ns(exportSvc, "CreateSnapshot", ScopeMemoryWrite)
	for _, m := range []string{"CreatePage", "UpdatePage", "DeletePage", "RefreshPage"} {
		ns(pageSvc, m, ScopeMemoryWrite)
	}
	ns(operationSvc, "CancelOperation", ScopeMemoryWrite)
	for _, m := range []string{"UpdateNamespace", "DeleteNamespace"} {
		ns(namespaceSvc, m, ScopeMemoryAdmin)
	}

	// The two metered methods.
	recall := p[memorySvc+"Recall"]
	recall.Bucket = quota.BucketRecall
	p[memorySvc+"Recall"] = recall
	p[memorySvc+"Retain"] = MethodPolicy{Scope: ScopeMemoryWrite, Target: TargetNamespace, Bucket: quota.BucketRetain}

	// OperationService.GetOperation / WaitOperation are readable while the namespace is DELETING, so that the
	// DELETE_NAMESPACE operation the delete returned can be awaited (N70, N127).
	for _, m := range []string{"GetOperation", "WaitOperation"} {
		p[operationSvc+m] = MethodPolicy{Scope: ScopeMemoryRead, Target: TargetNamespace, AllowDeleting: true}
	}

	// Tenant-level methods of a namespace owner: the token's tenant must be the request's tenant.
	p[namespaceSvc+"CreateNamespace"] = MethodPolicy{Scope: ScopeMemoryAdmin, Target: TargetTenant}
	p[namespaceSvc+"ListNamespaces"] = MethodPolicy{Scope: ScopeMemoryRead, Target: TargetTenant}

	// The tenant's own lifecycle is tenant.admin; engram.operator may also call GetTenant, DeleteTenant and
	// GetTenantOperation on the admin route (the operator token has no tenant, so the tenant check is skipped for it).
	p[tenantSvc+"UpdateTenant"] = MethodPolicy{Scope: ScopeTenantAdmin, Target: TargetTenant}
	for _, m := range []string{"GetTenant", "DeleteTenant"} {
		p[tenantSvc+m] = MethodPolicy{Scope: ScopeTenantAdmin, Target: TargetTenant, AltScope: ScopeOperator}
	}
	p[tenantSvc+"GetTenantOperation"] = MethodPolicy{Scope: ScopeTenantAdmin, Target: TargetTenant,
		AltScope: ScopeOperator, AllowDeleting: true}

	// Fleet-wide methods: engram.operator only.
	for _, m := range []string{"CreateTenant", "ListTenants", "UpdateTenantLimits"} {
		p[tenantSvc+m] = MethodPolicy{Scope: ScopeOperator, Target: TargetOperator}
	}
	for _, m := range []string{"RegisterShard", "GetShard", "ListShards", "DrainShard", "UpdateShard",
		"ResolveNamespace"} {
		p[shardSvc+m] = MethodPolicy{Scope: ScopeOperator, Target: TargetOperator}
	}
	for _, m := range []string{"StartMove", "GetMove", "ListMoves", "RollbackMove"} {
		p[moveSvc+m] = MethodPolicy{Scope: ScopeOperator, Target: TargetOperator}
	}

	// The workers' service identity is valid on exactly these two methods, bound to the cell of the shard (N167).
	p[shardSvc+"ReleaseNamespace"] = MethodPolicy{Scope: ScopeOperator, Target: TargetOperator, AltScope: ScopeWorker,
		CellBound: true}
	p[moveSvc+"CleanupMove"] = MethodPolicy{Scope: ScopeOperator, Target: TargetOperator, AltScope: ScopeWorker,
		CellBound: true}
	return p
}
