package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	memoryv1 "github.com/gstamatakis95/engram/gen/go/memory/v1"
	"github.com/gstamatakis95/engram/internal/config"
	"github.com/gstamatakis95/engram/internal/errs"
	"github.com/gstamatakis95/engram/internal/id"
)

// Postgres is the engram_catalog-backed implementation of Namespaces (PLAN.md section 2.2.3) and the Listener of the
// Resolver. It connects as catalog_app (engram-api) or catalog_admin (engramctl): the grants of migration 0001 decide
// what each may do. Every method is one statement or one short transaction; none holds a connection across a network
// call to a shard.
type Postgres struct {
	pool *pgxpool.Pool
	dsn  string
	boot ShardBootstrapper
	ping pingConfig
}

type pingConfig struct{ every, timeout time.Duration }

var (
	_ Namespaces = (*Postgres)(nil)
	_ Listener   = (*Postgres)(nil)
)

// PostgresOptions configure OpenPostgres.
type PostgresOptions struct {
	// Bootstrapper runs phase 2 of Create on the new namespace's shard; nil skips it.
	Bootstrapper ShardBootstrapper
	// MaxConns is the pool size (default 8: the catalog is off the hot path, D4).
	MaxConns int32
	// ConnectTimeout bounds a connection attempt (default 2 s), so that an unreachable catalog fails a load quickly.
	ConnectTimeout time.Duration
	// ListenPingEvery and ListenPingTimeout: when the LISTEN connection has been silent for ListenPingEvery (default
	// 10 s) it is pinged, and a ping that does not answer within ListenPingTimeout (default 5 s) is a lost connection,
	// so that the Resolver reconnects and flushes within seconds of a failover that left the socket dead instead of
	// waiting for the kernel's keepalive (about 150 s).
	ListenPingEvery, ListenPingTimeout time.Duration
}

// OpenPostgres connects a pool to the catalog database named by dsn. The LISTEN connection is opened separately by
// Listen on a direct (never pooled) connection, because LISTEN needs session state.
func OpenPostgres(ctx context.Context, dsn string, o PostgresOptions) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("catalog: dsn: %w", err)
	}
	if o.MaxConns == 0 {
		o.MaxConns = 8
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 2 * time.Second
	}
	if o.ListenPingEvery == 0 {
		o.ListenPingEvery = 10 * time.Second
	}
	if o.ListenPingTimeout == 0 {
		o.ListenPingTimeout = 5 * time.Second
	}
	cfg.MaxConns = o.MaxConns
	cfg.ConnConfig.ConnectTimeout = o.ConnectTimeout
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec // pgbouncer-safe (CONFLICTS.md #13)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, unavailableOr(err)
	}
	return &Postgres{pool: pool, dsn: dsn, boot: o.Bootstrapper,
		ping: pingConfig{o.ListenPingEvery, o.ListenPingTimeout}}, nil
}

// Close releases the pool.
func (p *Postgres) Close() { p.pool.Close() }

// Pool exposes the pool to the other catalog capabilities (tenants, shards, moves) that share the connection.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// unavailableOr maps a connection-level failure (refused, reset, timed out, closed pool) to UNAVAILABLE{RetryInfo 2 s}
// and anything else to INTERNAL; typed errors pass through.
func unavailableOr(err error) error {
	if err == nil {
		return nil
	}
	var typed *errs.Error
	if errors.As(err, &typed) {
		return err
	}
	var ne net.Error
	var ce *pgconn.ConnectError
	switch {
	case errors.As(err, &ce), errors.As(err, &ne), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, pgx.ErrTxClosed), pgconn.Timeout(err),
		pgconn.SafeToRetry(err):
		u := errs.Unavailable("catalog unavailable", unavailableRetry)
		u.Cause = err
		return u
	}
	return errs.Internal("catalog", err)
}

// classify maps a database error to the errs taxonomy; ns is the namespace the statement was about (may be zero).
func classify(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		// The server's text, constraint and trigger names are internals (N128): the public message is generic and the
		// PostgreSQL error travels as the Cause, for the logs.
		case "23505": // unique_violation: the tenant-unique name, or an id, is taken
			e := nameTaken("")
			e.Msg, e.Cause = "already exists", pe
			return e
		case "23514": // check_violation, including the move-transition and entry triggers
			e := errs.PreconditionFailed("CATALOG_CHECK", "", "the change violates a catalog rule")
			e.Cause = pe
			return e
		case "23503": // foreign_key_violation: tenant or shard unknown
			e := errs.PreconditionFailed("CATALOG_REFERENCE", "", "the change refers to something that does not exist")
			e.Cause = pe
			return e
		case "53400": // pick_shard found no shard with capacity (RESOURCE_EXHAUSTED at the API)
			return errs.QuotaExceeded("namespace_placement", memoryv1.QuotaScope_QUOTA_SCOPE_TENANT, 0, 0, 0)
		}
	}
	return unavailableOr(err)
}

const selectEntry = `SELECT n.namespace_id, n.tenant_id, n.name, n.shard_id, n.epoch, n.state::text, n.embedding_model,
       n.embedding_dims, n.config, t.display_name, t.state::text, t.isolation::text, t.config,
       coalesce(n.profile ->> 'group', '')
  FROM namespaces n JOIN tenants t ON t.tenant_id = n.tenant_id`

func scanEntry(row pgx.Row) (*Entry, error) {
	var (
		nsID                     [16]byte
		tenant, name, state      string
		shard                    int32
		epoch                    int64
		model                    string
		dims                     int
		nsCfg, tCfg              []byte
		tName, tState, tIsolated string
		group                    string
	)
	if err := row.Scan(&nsID, &tenant, &name, &shard, &epoch, &state, &model, &dims, &nsCfg, &tName, &tState,
		&tIsolated, &tCfg, &group); err != nil {
		return nil, err
	}
	nsLayer, err := layerOf(nsCfg)
	if err != nil {
		return nil, err
	}
	tLayer, err := layerOf(tCfg)
	if err != nil {
		return nil, err
	}
	return &Entry{Namespace: id.NamespaceID(nsID), Tenant: id.TenantID(tenant), Name: name, Shard: id.ShardID(shard),
		Epoch: id.Epoch(epoch), State: NamespaceState(state), EmbeddingModel: model, EmbeddingDims: dims,
		Config: nsLayer, Group: group, TenantEntry: &TenantEntry{Tenant: id.TenantID(tenant), DisplayName: tName,
			State: tState, Isolation: tIsolated, Config: tLayer}}, nil
}

func layerOf(raw []byte) (config.Layer, error) {
	var vals map[string]any
	if err := json.Unmarshal(raw, &vals); err != nil {
		return config.Layer{}, fmt.Errorf("catalog: config column: %w", err)
	}
	return config.Layer{Values: vals}, nil
}

// Resolve implements Namespaces: one indexed read of the namespace joined to its tenant.
func (p *Postgres) Resolve(ctx context.Context, ns id.NamespaceID) (*Entry, error) {
	e, err := scanEntry(p.pool.QueryRow(ctx, selectEntry+` WHERE n.namespace_id = $1`, ns.String()))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nsNotFound(ns)
	case err != nil:
		return nil, classify(err)
	}
	return e, nil
}

// ResolveByName implements Namespaces: the live (not deleted) namespace named name of tenant t.
func (p *Postgres) ResolveByName(ctx context.Context, t id.TenantID, name string) (*Entry, error) {
	e, err := scanEntry(p.pool.QueryRow(ctx,
		selectEntry+` WHERE n.tenant_id = $1 AND n.name = $2 AND n.state <> 'deleted'`, string(t), name))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_NAMESPACE, stringer(name))
	case err != nil:
		return nil, classify(err)
	}
	return e, nil
}

// Create implements Namespaces. Phase 1 is one transaction: pick_shard (when p.Shard is AutoShard), then the insert of
// the row in state creating at epoch 1. Phase 2 is the ShardBootstrapper; phase 3 is the CAS creating -> active, which
// the trigger announces. A crash between the phases leaves a creating row for the op-sweeper (PLAN.md section 3.2).
func (p *Postgres) Create(ctx context.Context, cp CreateParams) (*Entry, error) {
	if err := validateCreate(&cp); err != nil {
		return nil, err
	}
	ns := cp.Namespace
	if ns.IsZero() {
		ns = id.NewNamespaceID()
	}
	cfg, err := json.Marshal(cp.Config.Values)
	if err != nil || string(cfg) == "null" {
		cfg = []byte("{}")
	}
	profile := "{}"
	if cp.Group != "" {
		b, _ := json.Marshal(map[string]string{"group": cp.Group})
		profile = string(b)
	}
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		// The tenant row is locked FOR SHARE until this transaction ends (catalog_lock_tenant), so that the
		// DeleteTenant ack serialises with the create: either it committed first and the state read here is
		// `deleting`, or it waits and its enumeration of the tenant's namespaces sees the row inserted below (N122,
		// N182).
		var tenantState *string
		if err := tx.QueryRow(ctx, `SELECT catalog_lock_tenant($1)`, string(cp.Tenant)).Scan(&tenantState); err != nil {
			return err
		}
		switch {
		case tenantState == nil:
			return errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_TENANT, cp.Tenant)
		case *tenantState != "active":
			return tenantNotActive(cp.Tenant, *tenantState)
		}
		shard := cp.Shard
		if shard == AutoShard {
			if err := tx.QueryRow(ctx, `SELECT pick_shard($1)`, string(cp.Tenant)).Scan(&shard); err != nil {
				return p.placementError(ctx, cp.Tenant, err)
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO namespaces (namespace_id, tenant_id, name, shard_id, epoch, state,
			embedding_model, embedding_dims, config, profile)
			VALUES ($1, $2, $3, $4, 1, 'creating', $5, $6, $7, $8)`,
			ns.String(), string(cp.Tenant), cp.Name, int32(shard), cp.EmbeddingModel, cp.EmbeddingDims, string(cfg),
			profile)
		return err
	})
	if err != nil {
		return nil, p.createError(ctx, cp, err)
	}
	if p.boot != nil {
		created, err := p.Resolve(ctx, ns)
		if err != nil {
			return nil, err
		}
		if err := p.boot.Bootstrap(ctx, created); err != nil {
			return nil, err // the row stays in state creating
		}
	}
	if err := p.SetState(ctx, ns, StateCreating, StateActive); err != nil {
		return nil, err
	}
	return p.Resolve(ctx, ns)
}

// placementError maps a failed pick_shard (the tenant was checked just before, so only capacity can refuse).
func (p *Postgres) placementError(_ context.Context, _ id.TenantID, err error) error {
	return classify(err)
}

// createError maps a failed phase 1 to the same error the MemoryCatalog returns.
func (p *Postgres) createError(ctx context.Context, cp CreateParams, err error) error {
	var typed *errs.Error
	if errors.As(err, &typed) {
		return err
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "23503":
			if pe.ConstraintName == "namespaces_tenant_id_fkey" {
				return errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_TENANT, cp.Tenant)
			}
			return errs.NotFound(memoryv1.ResourceKind_RESOURCE_KIND_SHARD, cp.Shard)
		case "23505":
			return nameTaken(cp.Name)
		}
	}
	_ = ctx
	return classify(err)
}

// SetState implements Namespaces: UPDATE ... WHERE namespace_id = $1 AND state = $2, a CAS; zero rows distinguishes an
// unknown namespace (NOT_FOUND) from a state that is not `from` (FAILED_PRECONDITION).
func (p *Postgres) SetState(ctx context.Context, ns id.NamespaceID, from, to NamespaceState) error {
	if err := validateSetState(from, to); err != nil {
		return err
	}
	tag, err := p.pool.Exec(ctx, `UPDATE namespaces SET state = $3::namespace_state,
		deleted_at = CASE WHEN $3 = 'deleted' THEN now() ELSE NULL END
		WHERE namespace_id = $1 AND state = $2::namespace_state`, ns.String(), string(from), string(to))
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var actual string
	err = p.pool.QueryRow(ctx, `SELECT state::text FROM namespaces WHERE namespace_id = $1`, ns.String()).Scan(&actual)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nsNotFound(ns)
	case err != nil:
		return classify(err)
	}
	return stateMismatch(ns, from, NamespaceState(actual))
}

// BumpEpoch implements Namespaces: epoch = epoch + 1 WHERE epoch = expected, returning the new epoch.
func (p *Postgres) BumpEpoch(ctx context.Context, ns id.NamespaceID, expected id.Epoch,
	why EpochReason) (id.Epoch, error) {
	if why == "" {
		return 0, errs.Validation("reason", "required")
	}
	var epoch int64
	err := p.pool.QueryRow(ctx, `UPDATE namespaces SET epoch = epoch + 1
		WHERE namespace_id = $1 AND epoch = $2 RETURNING epoch`, ns.String(), int64(expected)).Scan(&epoch)
	if err == nil {
		return id.Epoch(epoch), nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, classify(err)
	}
	var actual int64
	err = p.pool.QueryRow(ctx, `SELECT epoch FROM namespaces WHERE namespace_id = $1`, ns.String()).Scan(&actual)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return 0, nsNotFound(ns)
	case err != nil:
		return 0, classify(err)
	}
	return 0, epochMismatch(ns, expected, id.Epoch(actual))
}

// TenantState implements TenantStateReader.
func (p *Postgres) TenantState(ctx context.Context, t id.TenantID) (string, error) {
	var st string
	err := p.pool.QueryRow(ctx, `SELECT state::text FROM tenants WHERE tenant_id = $1`, string(t)).Scan(&st)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", tenantMissing(t)
	case err != nil:
		return "", classify(err)
	}
	return st, nil
}

// Listen implements Listener on a dedicated connection that is not taken from the pool: LISTEN is session state, and
// the pool may sit behind pgbouncer in transaction mode (N15), which is why this DSN must be the catalog's direct one.
func (p *Postgres) Listen(ctx context.Context, ready func(), fn func(payload []byte)) error {
	conn, err := pgx.Connect(ctx, p.dsn)
	if err != nil {
		return unavailableOr(err)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = conn.Close(cctx) // a partitioned catalog must not hold the reconnect loop here
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+ChannelName); err != nil {
		return unavailableOr(err)
	}
	ready()
	for {
		wctx, cancel := context.WithTimeout(ctx, p.ping.every)
		n, err := conn.WaitForNotification(wctx)
		cancel()
		switch {
		case err == nil:
			fn([]byte(n.Payload))
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, context.DeadlineExceeded): // silent for a while: is anybody there?
			pctx, pcancel := context.WithTimeout(ctx, p.ping.timeout)
			perr := conn.Ping(pctx)
			pcancel()
			if perr != nil {
				if ctx.Err() != nil {
					return nil
				}
				return unavailableOr(perr)
			}
		default:
			return unavailableOr(err)
		}
	}
}
