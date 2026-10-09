package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5"

	"github.com/gstamatakis95/engram/internal/store/migrate"
)

func init() {
	Register(Command{
		Name:  "migrate",
		Short: "apply goose migrations (up|down|status) or run the static RLS check (--check-rls)",
		Run:   runMigrate,
	})
}

const migrateUsage = `usage: engramctl migrate --dsn DSN [--target shard] [--to VERSION] [--dry-run] [up|down|status]
       engramctl migrate --dsn DSN --check-rls

  up          apply every pending migration (or up to --to); the default action
  down        revert down to --to (required; 0 reverts everything)
  status      list the embedded migrations and whether each is applied
  --check-rls print every namespace-scoped relation without a forced RLS policy (PLAN.md section 8.3); the expected
              output is empty, and the exit status is 1 when it is not
  --dry-run   print the plan (the migrations that would run) and change nothing

The DSN must be a superuser connection to the shard database: migration 0001 creates the roles and extensions and then
switches to engram_migrate, which owns every object (PLAN.md section 9.2, N133e).`

func runMigrate(ctx context.Context, args []string) error {
	return migrateMain(ctx, args, os.Stdout)
}

func migrateMain(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dsn := fs.String("dsn", os.Getenv("ENGRAM_DSN"), "superuser DSN of the database (default $ENGRAM_DSN)")
	target := fs.String("target", "shard", "migration set: shard (catalog arrives with M0.3)")
	to := fs.Int64("to", -1, "target version (up: default latest; down: required)")
	dry := fs.Bool("dry-run", false, "print the plan and change nothing")
	checkRLS := fs.Bool("check-rls", false, "print every namespace-scoped relation without a forced RLS policy")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w\n%s", err, migrateUsage)
	}
	if *dsn == "" {
		return fmt.Errorf("--dsn is required\n%s", migrateUsage)
	}
	action := "up"
	if fs.NArg() > 0 {
		action = fs.Arg(0)
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("unexpected arguments %q after the action %q: flags must come before the action (for example"+
			" `migrate --dsn DSN --to 0 down`)\n%s", fs.Args()[1:], action, migrateUsage)
	}
	opts := migrate.Options{DSN: *dsn, Set: migrate.Set(*target)}
	if *checkRLS {
		return checkRLSCommand(ctx, *dsn, out)
	}
	switch action {
	case "status":
		return printStatus(ctx, opts, out)
	case "up":
		if *to >= 0 {
			opts.To = *to
		}
		if *dry {
			return printPlan(ctx, opts, out, true)
		}
		applied, err := migrate.Up(ctx, opts)
		for _, v := range applied {
			_, _ = fmt.Fprintf(out, "applied %04d\n", v)
		}
		return err
	case "down":
		if *to < 0 {
			return fmt.Errorf("down needs --to VERSION (0 reverts everything)\n%s", migrateUsage)
		}
		opts.To = *to
		if *dry {
			return printPlan(ctx, opts, out, false)
		}
		reverted, err := migrate.DownTo(ctx, opts)
		for _, v := range reverted {
			_, _ = fmt.Fprintf(out, "reverted %04d\n", v)
		}
		return err
	default:
		return fmt.Errorf("unknown migrate action %q\n%s", action, migrateUsage)
	}
}

func printStatus(ctx context.Context, opts migrate.Options, out io.Writer) error {
	sts, err := migrate.List(ctx, opts)
	if err != nil {
		return err
	}
	for _, s := range sts {
		state := "pending"
		if s.Applied {
			state = "applied"
		}
		_, _ = fmt.Fprintf(out, "%04d\t%s\t%s\n", s.Version, state, s.Source)
	}
	return nil
}

// printPlan lists what an up (applied=false -> would run) or down (applied=true -> would revert) would touch. Every
// shard migration runs in its own transaction under lock_timeout 5 s; none uses a long exclusive lock on existing data
// (0001 to 0004 create objects), so there are no extra lock requirements to print.
func printPlan(ctx context.Context, opts migrate.Options, out io.Writer, up bool) error {
	sts, err := migrate.List(ctx, opts)
	if err != nil {
		return err
	}
	for i := range sts {
		s := sts[i]
		if up && !s.Applied && (opts.To == 0 || s.Version <= opts.To) {
			_, _ = fmt.Fprintf(out, "would apply %04d %s (one transaction, lock_timeout 5s)\n", s.Version, s.Source)
		}
		if !up && s.Applied && s.Version > opts.To {
			_, _ = fmt.Fprintf(out, "would revert %04d %s (one transaction, lock_timeout 5s)\n", s.Version, s.Source)
		}
	}
	return nil
}

var errRLSViolations = errors.New("rls check: violations found")

func checkRLSCommand(ctx context.Context, dsn string, out io.Writer) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	vs, err := migrate.CheckRLS(ctx, conn)
	if err != nil {
		return err
	}
	for _, v := range vs {
		_, _ = fmt.Fprintln(out, v)
	}
	if len(vs) > 0 {
		return fmt.Errorf("%w (%d rows)", errRLSViolations, len(vs))
	}
	return nil
}
