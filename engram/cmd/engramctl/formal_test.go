package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFormalTraceToTLA_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "tla", "Toy.tla"), "---- MODULE Toy ----\nTick == TRUE\nDraw(w, n) == TRUE\n====\n")
	write(t, filepath.Join(dir, "Toy.cfg"), "SPECIFICATION Spec\nCONSTANTS\n  Writers = {w1}\nINVARIANTS\n  Inv\n")
	write(t, filepath.Join(dir, "toy.jsonl"),
		`{"v":1,"spec":"Toy","seq":1,"action":"Draw","args":["@w1","@a"]}`+"\n"+
			`{"v":1,"spec":"Toy","seq":2,"action":"Tick"}`+"\n")
	out := filepath.Join(dir, "out")
	err := runFormal(context.Background(), []string{"trace-to-tla", "--tla-dir", filepath.Join(dir, "tla"),
		"--cfg", filepath.Join(dir, "Toy.cfg"), "--out", out, "Toy", filepath.Join(dir, "toy.jsonl")})
	if err != nil {
		t.Fatal(err)
	}
	mod, err := os.ReadFile(filepath.Join(out, "ToyTrace.tla"))
	if err != nil || !strings.Contains(string(mod), `[act |-> "Draw", par |-> <<w1, a>>]`) {
		t.Fatalf("module: %v\n%s", err, mod)
	}
	cfg, err := os.ReadFile(filepath.Join(out, "ToyTrace.cfg"))
	if err != nil || !strings.Contains(string(cfg), "SPECIFICATION TraceSpec") {
		t.Fatalf("config: %v\n%s", err, cfg)
	}
	// a log of another specification is refused
	err = runFormal(context.Background(), []string{"trace-to-tla", "--tla-dir", filepath.Join(dir, "tla"),
		"--out", out, "Other", filepath.Join(dir, "toy.jsonl")})
	if err == nil {
		t.Fatal("a log of another specification must be refused")
	}
}

func TestFormalTLCToTrace(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "tlc.log"), "State 1: <Initial predicate>\nx\nState 2: <Tick line 1, col 1 to line 2, "+
		"col 2 of module Toy>\n")
	var buf bytes.Buffer
	if err := runTLCToTrace([]string{"Toy", filepath.Join(dir, "tlc.log")}, &buf); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), `{"v":1,"spec":"Toy","seq":1,"action":"Tick"}`+"\n"; got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
}

func TestFormalUsage(t *testing.T) {
	if err := runFormal(context.Background(), nil); err == nil {
		t.Fatal("no subcommand must print the usage")
	}
	if err := runFormal(context.Background(), []string{"nope"}); err == nil {
		t.Fatal("unknown subcommand")
	}
}
