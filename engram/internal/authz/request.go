package authz

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// namespaceRefName is the full name of the message that addresses a namespace in every request (common.proto).
const namespaceRefName protoreflect.FullName = "memory.v1.NamespaceRef"

// addressing is what the interceptor needs from a request: the tenant it names and, for a namespace-scoped method, the
// namespace. It is read by reflection, so that no handler and no generated type needs an accessor interface: every
// namespace-scoped request carries a `namespace` field of type NamespaceRef, every tenant-level request a `tenant_id`
// (or, for the Create/Update tenant methods, a `tenant` or `patch` message that holds one).
type addressing struct {
	tenant        id.TenantID // as the request named it (unvalidated text when raw is set)
	rawTenant     string
	namespace     id.NamespaceID
	hasNamespace  bool
	requestID     string
	tenantPresent bool
}

// addressOf reads the addressing of m. wantNamespace says the method's policy target is a Namespace: then the ref is
// required and both its ids must parse; otherwise the tenant id is read from wherever the message keeps it.
func addressOf(m proto.Message, wantNamespace bool) (addressing, error) {
	var a addressing
	if m == nil {
		return a, errs.Validation("request", "missing")
	}
	msg := m.ProtoReflect()
	a.requestID = requestID(msg)
	if wantNamespace {
		fd := msg.Descriptor().Fields().ByName("namespace")
		if fd == nil || fd.Message() == nil || fd.Message().FullName() != namespaceRefName {
			return a, errs.Internal("policy says namespace-scoped but the request has no NamespaceRef", nil)
		}
		if !msg.Has(fd) {
			return a, errs.ValidationReason("namespace", errs.ReasonRequired, "the namespace reference is required")
		}
		ref := msg.Get(fd).Message()
		a.rawTenant = stringField(ref, "tenant_id")
		t, err := id.ParseTenantID(a.rawTenant)
		if err != nil {
			return a, errs.ValidationReason("namespace.tenant_id", errs.ReasonBadFormat, "not a tenant id")
		}
		ns, err := id.ParseNamespaceID(stringField(ref, "namespace_id"))
		if err != nil {
			return a, errs.ValidationReason("namespace.namespace_id", errs.ReasonBadFormat, "not a namespace id")
		}
		a.tenant, a.namespace, a.hasNamespace, a.tenantPresent = t, ns, true, true
		return a, nil
	}
	raw, present := tenantText(msg)
	a.rawTenant, a.tenantPresent = raw, present
	if present {
		t, err := id.ParseTenantID(raw)
		if err != nil {
			return a, errs.ValidationReason("tenant_id", errs.ReasonBadFormat, "not a tenant id")
		}
		a.tenant = t
	}
	return a, nil
}

// tenantText finds the tenant id of a tenant-level request: its own `tenant_id`, else the `tenant_id` of a `patch` or
// `tenant` message (UpdateTenant, UpdateTenantLimits, CreateTenant).
func tenantText(msg protoreflect.Message) (string, bool) {
	fields := msg.Descriptor().Fields()
	if fd := fields.ByName("tenant_id"); fd != nil && fd.Kind() == protoreflect.StringKind {
		return msg.Get(fd).String(), true
	}
	for _, name := range []protoreflect.Name{"patch", "tenant"} {
		fd := fields.ByName(name)
		if fd == nil || fd.Message() == nil || !msg.Has(fd) {
			continue
		}
		sub := msg.Get(fd).Message()
		if tf := sub.Descriptor().Fields().ByName("tenant_id"); tf != nil && tf.Kind() == protoreflect.StringKind {
			return sub.Get(tf).String(), true
		}
	}
	return "", false
}

func stringField(m protoreflect.Message, name protoreflect.Name) string {
	if fd := m.Descriptor().Fields().ByName(name); fd != nil && fd.Kind() == protoreflect.StringKind {
		return m.Get(fd).String()
	}
	return ""
}

// requestID reads meta.request_id when the request has a `meta` message.
func requestID(msg protoreflect.Message) string {
	fd := msg.Descriptor().Fields().ByName("meta")
	if fd == nil || fd.Message() == nil || !msg.Has(fd) {
		return ""
	}
	return stringField(msg.Get(fd).Message(), "request_id")
}
