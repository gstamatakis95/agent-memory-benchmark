// Package pgtest is the T3 (integration) harness of PLAN.md section 8.1: one PostgreSQL server per test package,
// started in TestMain from the pinned ParadeDB image with testcontainers-go, and a database on it that the SAME code
// path as `engramctl migrate` (internal/store/migrate) has brought to the latest shard schema.
//
// Two developer knobs change no test (CONFLICTS.md #3):
//
//   - ENGRAM_TEST_DOCKER_HOST_NETWORK=1 starts the container with the host network on a free port (a daemon without a
//     bridge network cannot map ports);
//   - ENGRAM_TEST_PG_DSN uses an already running server (a superuser DSN) instead of starting a container; the package
//     gets its own freshly created database on it.
//
// Usage, in a package under build tag integration:
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Main(m)) }
package pgtest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"

	"github.com/gstamatakis95/engram/internal/store/migrate"
)

// Image is the pinned shard image (PostgreSQL 16.15, pg_search 0.26.0, pgvector 0.8.6, pg_trgm, btree_gin, btree_gist;
// shared_preload_libraries already includes pg_search). The digest is never floated (PLAN.md section 9.2 toolchain
// pins).
const Image = "paradedb/paradedb@sha256:7aba4dbac45cfcaaaddce50feb7081b3fc907061834c68b52a23aa7b66442b76"

// Environment knobs.
const (
	EnvHostNetwork = "ENGRAM_TEST_DOCKER_HOST_NETWORK"
	EnvDSN         = "ENGRAM_TEST_PG_DSN"
)

// ShardID is the identity inserted into shard_meta of every harness database (provisioning does it in production).
const ShardID = 1

// Password is the password the harness gives every engram_* role of its server.
const Password = "engram-test"

// Role is a database role the tests connect as.
type Role string

// The roles of the shard schema (PLAN.md section 3.3) plus the bootstrap superuser.
const (
	Super   Role = "postgres"
	Migrate Role = "engram_migrate"
	App     Role = "engram_app"
	Relay   Role = "engram_relay"
	Move    Role = "engram_move"
	Admin   Role = "engram_admin"
)

// AllRoles lists the five engram roles in a stable order.
var AllRoles = []Role{Migrate, App, Relay, Move, Admin}

type server struct {
	ref        string // container name or id, for Pause and Unpause (empty with EnvDSN)
	host, port string
	superUser  string
	superPass  string
	stop       func(context.Context) error
}

func (s *server) dsn(role Role, db string) string {
	user, pass := string(role), Password
	if role == Super {
		user, pass = s.superUser, s.superPass
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     net.JoinHostPort(s.host, s.port),
		Path:     "/" + db,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

var (
	srv     *server
	pkgDB   string
	dbCount atomic.Int64
	poolsMu sync.Mutex
	pools   = map[Role]*pgxpool.Pool{}
)

// Main starts the package's server, builds its database and runs the tests; it returns the exit code for os.Exit.
func Main(m *testing.M) int { return run(m, true) }

// MainServerOnly is Main for a package that builds its own databases (the catalog tests migrate the catalog schema,
// not the shard schema): it starts the server and runs the tests, and creates no package database. Databases come from
// NewDatabase(tb, false).
func MainServerOnly(m *testing.M) int { return run(m, false) }

func run(m *testing.M, packageDB bool) int {
	ctx := context.Background()
	stop, err := start(ctx, packageDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pgtest: %v\n", err)
		return 1
	}
	code := m.Run()
	poolsMu.Lock()
	for _, p := range pools {
		p.Close()
	}
	poolsMu.Unlock()
	stop()
	return code
}

func start(ctx context.Context, packageDB bool) (func(), error) {
	var err error
	if dsn := os.Getenv(EnvDSN); dsn != "" {
		srv, err = fromDSN(dsn)
	} else {
		srv, err = startContainer(ctx)
	}
	if err != nil {
		return nil, err
	}
	stop := func() {
		if srv.stop != nil {
			c, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := srv.stop(c); err != nil {
				fmt.Fprintf(os.Stderr, "pgtest: stop: %v\n", err)
			}
		}
	}
	if packageDB {
		if pkgDB, err = createDatabase(ctx, true); err != nil {
			stop()
			return nil, err
		}
	}
	return func() {
		if os.Getenv(EnvDSN) != "" && pkgDB != "" {
			_ = dropDatabase(context.Background(), pkgDB)
		}
		stop()
	}, nil
}

func fromDSN(dsn string) (*server, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", EnvDSN, err)
	}
	return &server{host: cfg.Host, port: strconv.Itoa(int(cfg.Port)), superUser: cfg.User, superPass: cfg.Password}, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// serverArgs are the postgres settings of the disposable test server: nothing here needs durability, and the data
// directory lives on tmpfs.
var serverArgs = []string{"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off",
	"-c", "max_connections=100", "-c", "shared_buffers=512MB", "-c", "max_wal_size=4GB"}

func startContainer(ctx context.Context) (*server, error) {
	if os.Getenv(EnvHostNetwork) == "1" {
		return startHostNetwork(ctx)
	}
	req := testcontainers.ContainerRequest{
		Image:        Image,
		Env:          map[string]string{"POSTGRES_PASSWORD": Password},
		Labels:       map[string]string{"engram-test": "pgtest"},
		Cmd:          serverArgs,
		ExposedPorts: []string{"5432/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ShmSize = 1 << 30
			hc.Tmpfs = map[string]string{"/var/lib/postgresql/data": "rw,size=4g"}
		},
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req,
		Started: true})
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", Image, err)
	}
	s := &server{superUser: "postgres", superPass: Password, ref: c.GetContainerID()}
	s.stop = func(ctx context.Context) error {
		return testcontainers.TerminateContainer(c, testcontainers.StopContext(ctx))
	}
	h, err := c.Host(ctx)
	if err != nil {
		_ = s.stop(ctx)
		return nil, err
	}
	mp, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		_ = s.stop(ctx)
		return nil, err
	}
	s.host, s.port = h, mp.Port()
	if err := waitReady(ctx, s); err != nil {
		_ = s.stop(ctx)
		return nil, err
	}
	return s, nil
}

// startHostNetwork is the ENGRAM_TEST_DOCKER_HOST_NETWORK=1 path (CONFLICTS.md #3): the container runs with the host
// network on a free port. It drives `docker run` directly because testcontainers-go always publishes the image's
// EXPOSEd port, and a daemon without a bridge network refuses a container that carries port bindings (the docker CLI
// silently drops them in host mode, the API does not). The image, the settings and the readiness probe are the same.
func startHostNetwork(ctx context.Context) (*server, error) {
	p, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("free port: %w", err)
	}
	port := strconv.Itoa(p)
	name := fmt.Sprintf("engram-pgtest-%d-%s", os.Getpid(), port)
	args := []string{"run", "-d", "--rm", "--name", name, "--network", "host", "--label", "engram-test=pgtest",
		"--shm-size", "1g", "--tmpfs", "/var/lib/postgresql/data:rw,size=4g",
		"-e", "POSTGRES_PASSWORD=" + Password, "-e", "PGPORT=" + port, Image}
	args = append(args, serverArgs...)
	args = append(args, "-c", "port="+port)
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("docker run %s: %w\n%s", Image, err, out)
	}
	s := &server{host: "127.0.0.1", port: port, superUser: "postgres", superPass: Password, ref: name}
	s.stop = func(ctx context.Context) error {
		out, err := exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput()
		if err != nil {
			return fmt.Errorf("docker rm -f %s: %w\n%s", name, err, out)
		}
		return nil
	}
	if err := waitReady(ctx, s); err != nil {
		_ = s.stop(ctx)
		return nil, err
	}
	return s, nil
}

// waitReady polls until the server accepts a TCP connection and answers SELECT 1 (the entrypoint's temporary init
// server listens on the socket only, so a TCP answer means the final server is up).
func waitReady(ctx context.Context, s *server) error {
	deadline := time.Now().Add(3 * time.Minute)
	var last error
	for time.Now().Before(deadline) {
		c, err := pgx.Connect(ctx, s.dsn(Super, "postgres"))
		if err == nil {
			var one int
			err = c.QueryRow(ctx, "SELECT 1").Scan(&one)
			_ = c.Close(ctx)
			if err == nil {
				return nil
			}
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("server not ready after 3 minutes: %w", last)
}

// createDatabase creates a database (from template0, so the image's template-database extras do not leak in) and, when
// migrated is set, applies the shard migrations to it through migrate.Up and provisions it the way section 9 does:
// the shard_meta identity row and the role passwords.
func createDatabase(ctx context.Context, migrated bool) (string, error) {
	name := fmt.Sprintf("engram_t3_%d_%d", os.Getpid(), dbCount.Add(1))
	admin, err := pgx.Connect(ctx, srv.dsn(Super, "postgres"))
	if err != nil {
		return "", err
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0"); err != nil {
		return "", fmt.Errorf("create database: %w", err)
	}
	if !migrated {
		return name, nil
	}
	if _, err := migrate.Up(ctx, migrate.Options{DSN: srv.dsn(Super, name)}); err != nil {
		return "", fmt.Errorf("migrate: %w", err)
	}
	return name, provision(ctx, name)
}

// provision is what shard provisioning does after the migrations: the identity row and the role passwords.
func provision(ctx context.Context, db string) error {
	c, err := pgx.Connect(ctx, srv.dsn(Super, db))
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	if _, err := c.Exec(ctx, `INSERT INTO shard_meta (shard_id, schema_version)
		 SELECT $1, max(version_id) FROM goose_db_version WHERE is_applied`, ShardID); err != nil {
		return fmt.Errorf("shard_meta: %w", err)
	}
	for _, r := range AllRoles {
		q := "ALTER ROLE " + pgx.Identifier{string(r)}.Sanitize() + " PASSWORD " + quoteLiteral(Password)
		if _, err := c.Exec(ctx, q); err != nil {
			return fmt.Errorf("password of %s: %w", r, err)
		}
	}
	return nil
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func dropDatabase(ctx context.Context, name string) error {
	admin, err := pgx.Connect(ctx, srv.dsn(Super, "postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close(ctx) }()
	_, err = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}

// DSN returns the connection string of role on the package database.
func DSN(role Role) string { return srv.dsn(role, pkgDB) }

// Addr returns the host and port the package server listens on (for tools that sit in front of it, such as pgbouncer).
func Addr() (host, port string) { return srv.host, srv.port }

// DatabaseName is the name of the package database.
func DatabaseName() string { return pkgDB }

// Connect opens a fresh connection as role on the package database.
func Connect(ctx context.Context, role Role) (*pgx.Conn, error) { return pgx.Connect(ctx, DSN(role)) }

// Pool returns the package-wide pool of role (closed by Main).
func Pool(tb testing.TB, role Role) *pgxpool.Pool {
	tb.Helper()
	poolsMu.Lock()
	defer poolsMu.Unlock()
	if p, ok := pools[role]; ok {
		return p
	}
	cfg, err := pgxpool.ParseConfig(DSN(role))
	if err != nil {
		tb.Fatalf("pgtest: pool config: %v", err)
	}
	cfg.MaxConns = 24
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("pgtest: pool: %v", err)
	}
	pools[role] = p
	return p
}

// Database is a database of its own on the package server, dropped when the test ends.
type Database struct {
	Name string
}

// DSN returns the connection string of role on this database.
func (d Database) DSN(role Role) string { return srv.dsn(role, d.Name) }

// RoleDSN returns the connection string of a login that is not one of the shard roles (the catalog's catalog_app,
// catalog_admin, ...) with the password the test gave it.
func (d Database) RoleDSN(user, password string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(user, password),
		Host: net.JoinHostPort(srv.host, srv.port), Path: "/" + d.Name, RawQuery: "sslmode=disable"}
	return u.String()
}

// Pause freezes the server's container (docker pause): connections hang instead of being refused, which is how a
// catalog outage looks to the Resolver (a partition, not a crash). Unpause resumes it. Neither works with EnvDSN.
func Pause(ctx context.Context) error { return docker(ctx, "pause") }

// Unpause resumes a paused server.
func Unpause(ctx context.Context) error { return docker(ctx, "unpause") }

func docker(ctx context.Context, verb string) error {
	if srv == nil || srv.ref == "" {
		return fmt.Errorf("pgtest: cannot %s a server that was not started by the harness", verb)
	}
	if out, err := exec.CommandContext(ctx, "docker", verb, srv.ref).CombinedOutput(); err != nil {
		return fmt.Errorf("pgtest: docker %s: %w\n%s", verb, err, out)
	}
	return nil
}

// NewDatabase creates an extra database on the package server: empty when migrated is false (for up/down tests),
// otherwise fully migrated and provisioned like the package database.
func NewDatabase(tb testing.TB, migrated bool) Database {
	tb.Helper()
	ctx := context.Background()
	name, err := createDatabase(ctx, migrated)
	if err != nil {
		tb.Fatalf("pgtest: new database: %v", err)
	}
	tb.Cleanup(func() { _ = dropDatabase(context.Background(), name) })
	return Database{Name: name}
}

// Scope is the per-transaction scope of PLAN.md section 3.3 (set_config(..., true) = SET LOCAL).
type Scope struct {
	Namespace string
	Tenant    string
	Epoch     int64
}

// BeginScoped starts a transaction on pool and sets the three scope GUCs the way every store transaction does.
func BeginScoped(ctx context.Context, pool *pgxpool.Pool, sc Scope) (pgx.Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := SetScope(ctx, tx, sc); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

// SetScope sets engram.namespace_id, engram.tenant_id and engram.epoch for the current transaction.
func SetScope(ctx context.Context, tx pgx.Tx, sc Scope) error {
	_, err := tx.Exec(ctx,
		`SELECT set_config('engram.namespace_id', $1, true), set_config('engram.tenant_id', $2, true),
		set_config('engram.epoch', $3, true)`, sc.Namespace, sc.Tenant, strconv.FormatInt(sc.Epoch, 10))
	return err
}
