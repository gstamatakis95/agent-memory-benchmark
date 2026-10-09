package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gstamatakis95/engram/internal/id"
)

// ChannelName is the NOTIFY channel the catalog writes every routing-relevant change to (catalog_notify_event).
const ChannelName = "catalog_changes"

// Event kinds of a notification (catalog_events.kind).
const (
	KindNamespace = "namespace"
	KindTenant    = "tenant"
	KindShard     = "shard"
	KindMove      = "move"
)

// Notification is the payload of a catalog_changes NOTIFY: {event_id, kind, namespace_id, tenant_id, shard_id, epoch,
// state}. A tenant event names no namespace; the Resolver then drops every cached namespace of the tenant.
type Notification struct {
	EventID   int64
	Kind      string
	Namespace id.NamespaceID // zero for a tenant or shard event
	Tenant    id.TenantID
	Shard     *id.ShardID
	Epoch     *id.Epoch
	State     string
}

type rawNotification struct {
	EventID     int64   `json:"event_id"`
	Kind        string  `json:"kind"`
	NamespaceID *string `json:"namespace_id"`
	TenantID    *string `json:"tenant_id"`
	ShardID     *int32  `json:"shard_id"`
	Epoch       *int64  `json:"epoch"`
	State       *string `json:"state"`
}

// ParseNotification decodes and validates one payload. A payload that does not parse, names an unknown kind, or lacks
// the id its kind needs is an error; the Resolver answers an error by flushing the whole cache, because a notification
// it cannot read is a change it cannot apply.
func ParseNotification(payload []byte) (Notification, error) {
	var raw rawNotification
	if err := json.Unmarshal(payload, &raw); err != nil {
		return Notification{}, fmt.Errorf("catalog: notification %q: %w", truncate(payload), err)
	}
	n := Notification{EventID: raw.EventID, Kind: raw.Kind}
	if raw.State != nil {
		n.State = *raw.State
	}
	if raw.ShardID != nil {
		s := id.ShardID(*raw.ShardID)
		n.Shard = &s
	}
	if raw.Epoch != nil {
		e := id.Epoch(*raw.Epoch)
		n.Epoch = &e
	}
	if raw.TenantID != nil {
		t, err := id.ParseTenantID(*raw.TenantID)
		if err != nil {
			return Notification{}, fmt.Errorf("catalog: notification tenant: %w", err)
		}
		n.Tenant = t
	}
	if raw.NamespaceID != nil {
		ns, err := id.ParseNamespaceID(*raw.NamespaceID)
		if err != nil {
			return Notification{}, fmt.Errorf("catalog: notification namespace: %w", err)
		}
		n.Namespace = ns
	}
	switch n.Kind {
	case KindNamespace, KindMove:
		if n.Namespace.IsZero() {
			return Notification{}, fmt.Errorf("catalog: %s notification without namespace_id", n.Kind)
		}
	case KindTenant:
		if n.Tenant == "" {
			return Notification{}, fmt.Errorf("catalog: tenant notification without tenant_id")
		}
	case KindShard:
		if n.Shard == nil {
			return Notification{}, fmt.Errorf("catalog: shard notification without shard_id")
		}
	default:
		return Notification{}, fmt.Errorf("catalog: notification of unknown kind %q", n.Kind)
	}
	return n, nil
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

// Listener is the change feed of the Resolver: a LISTEN connection to the catalog.
type Listener interface {
	// Listen subscribes to the catalog_changes channel and blocks. It calls ready once the subscription is established
	// (the Resolver flushes its cache then: a notification sent while the previous connection was down is lost, and a
	// full flush is the only way to guarantee that none was missed) and fn for every payload, until ctx ends (nil) or
	// the connection is lost (the error). fn must not block.
	Listen(ctx context.Context, ready func(), fn func(payload []byte)) error
}
