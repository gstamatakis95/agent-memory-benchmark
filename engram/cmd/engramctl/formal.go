package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gstamatakis95/engram/internal/formal/trace"
)

func init() {
	Register(Command{
		Name:  "formal",
		Short: "trace validation against the TLA+ specifications (trace-to-tla, tlc-to-trace)",
		Run:   runFormal,
	})
}

const formalUsage = `usage:
  engramctl formal trace-to-tla [--tla-dir formal/tla] [--cfg CONFIG.cfg] [--out formal/tla/trace] SPEC LOG.jsonl
      writes <out>/<SPEC>Trace.tla (a module that EXTENDS SPEC and replays the logged actions through TraceNext) and,
      with --cfg, <out>/<SPEC>Trace.cfg that checks the invariants of that configuration along the replay
  engramctl formal tlc-to-trace [-o OUT.jsonl] SPEC TLC.log
      converts the error trace of a TLC run into the JSON-lines log format`

func runFormal(_ context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(formalUsage)
	}
	switch args[0] {
	case "trace-to-tla":
		return runTraceToTLA(args[1:])
	case "tlc-to-trace":
		return runTLCToTrace(args[1:], os.Stdout)
	}
	return fmt.Errorf("unknown formal subcommand %q\n%s", args[0], formalUsage)
}

func runTraceToTLA(args []string) error {
	fs := flag.NewFlagSet("formal trace-to-tla", flag.ContinueOnError)
	tlaDir := fs.String("tla-dir", "formal/tla", "directory of the specifications")
	cfgPath := fs.String("cfg", "", "configuration of the logged behaviour (its CONSTANTS and INVARIANTS carry over)")
	outDir := fs.String("out", filepath.Join("formal", "tla", "trace"), "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New(formalUsage)
	}
	spec, logPath := fs.Arg(0), fs.Arg(1)

	f, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	events, err := trace.Read(f)
	if err != nil {
		return err
	}
	if events[0].Spec != spec {
		return fmt.Errorf("the log is a trace of %s, not of %s", events[0].Spec, spec)
	}
	src, err := os.Open(filepath.Join(*tlaDir, spec+".tla"))
	if err != nil {
		return fmt.Errorf("specification: %w", err)
	}
	defer func() { _ = src.Close() }()
	raw, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	ops, err := trace.OperatorArities(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	doms, err := trace.ActionDomains(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	opt := trace.Options{Operators: ops, Domains: doms}
	if *cfgPath != "" {
		cfg, err := os.ReadFile(*cfgPath)
		if err != nil {
			return err
		}
		opt.BaseConfig = string(cfg)
	}
	res, err := trace.ToTLA(events, opt)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*outDir, res.Module+".tla"), []byte(res.TLA), 0o644); err != nil {
		return err
	}
	if res.Config != "" {
		return os.WriteFile(filepath.Join(*outDir, res.Module+".cfg"), []byte(res.Config), 0o644)
	}
	return nil
}

func runTLCToTrace(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("formal tlc-to-trace", flag.ContinueOnError)
	outPath := fs.String("o", "", "output file (default standard output)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New(formalUsage)
	}
	f, err := os.Open(fs.Arg(1))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	events, err := trace.FromTLCLog(fs.Arg(0), f)
	if err != nil {
		return err
	}
	w := stdout
	if *outPath != "" {
		out, err := os.Create(*outPath)
		if err != nil {
			return err
		}
		defer func() { _ = out.Close() }()
		w = out
	}
	return trace.Write(w, events)
}
