// Command methodcount is the method-count lint of PLAN.md section 2.1 (register N132, N140, N157): every `type X
// interface { ... }` under the given packages has at most five methods in its COMPLETE method set. The count comes from
// go/types, so an embedded interface of another package (io.ReadWriteCloser), an embedded generic instantiation and a
// chain of embeddings all contribute their full method set, and splitting an interface by embedding hides nothing.
// Test files are loaded (most tagged code is _test.go), as are the files behind the build tags integration,
// faultinject and testauth and the interface types declared inside function bodies; generated code is not. Usage:
// go run ./scripts/methodcount [-list] ./internal/... ./adapters/...
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// BuildTags are the build tags every Go file of the repository can sit behind; the lint loads all of them.
const BuildTags = "integration,faultinject,testauth"

// MaxMethods is the limit of section 2: every interface has at most five methods.
const MaxMethods = 5

// Entry is one interface with the size of its complete method set.
type Entry struct {
	Package string
	Name    string
	Pos     token.Position
	Count   int
}

// Violation formats an Entry above the limit.
func (e Entry) Violation() string {
	return fmt.Sprintf("%s: interface %s.%s has %d methods, the limit is %d (split it by capability)", e.Pos, e.Package,
		e.Name, e.Count, MaxMethods)
}

// Inventory type-checks the packages matched by patterns (relative to dir, test files included) and returns every
// declared interface type, sorted by package path and name, plus the ones above the limit.
func Inventory(dir string, patterns []string) (all, over []Entry, err error) {
	cfg := &packages.Config{
		Dir:        dir,
		BuildFlags: []string{"-tags=" + BuildTags},
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedImports | packages.NeedDeps | packages.NeedTypes |
			packages.NeedSyntax | packages.NeedTypesInfo,
		Tests: true, // _test.go files carry most of the tagged code; the variants of one package are deduplicated below
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, nil, err
	}
	var broken int
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			fmt.Fprintln(os.Stderr, "methodcount:", e)
			broken++
		}
	})
	if broken > 0 {
		return nil, nil, fmt.Errorf("%d package errors; fix the build first", broken)
	}
	seen := map[string]bool{} // a file is loaded once per test variant of its package: count each declaration once
	for _, p := range pkgs {
		if strings.HasSuffix(p.ID, ".test") { // the generated test main
			continue
		}
		for _, f := range p.Syntax {
			// ast.Inspect reaches package-level declarations and those nested in function bodies alike.
			ast.Inspect(f, func(n ast.Node) bool {
				spec, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				tn, ok := p.TypesInfo.Defs[spec.Name].(*types.TypeName)
				if !ok || tn.IsAlias() {
					return true
				}
				it, ok := tn.Type().Underlying().(*types.Interface)
				if !ok {
					return true
				}
				e := Entry{Package: p.Name, Name: spec.Name.Name, Pos: p.Fset.Position(tn.Pos()),
					Count: it.NumMethods()}
				if seen[e.Pos.String()] {
					return true
				}
				seen[e.Pos.String()] = true
				all = append(all, e)
				if e.Count > MaxMethods {
					over = append(over, e)
				}
				return true
			})
		}
	}
	less := func(s []Entry) func(i, j int) bool {
		return func(i, j int) bool { return s[i].Pos.String() < s[j].Pos.String() }
	}
	sort.Slice(all, less(all))
	sort.Slice(over, less(over))
	return all, over, nil
}

func main() {
	args := os.Args[1:]
	list := len(args) > 0 && args[0] == "-list"
	if list {
		args = args[1:]
	}
	if len(args) == 0 {
		args = []string{"./internal/...", "./adapters/..."}
	}
	all, over, err := Inventory("", args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "methodcount:", err)
		os.Exit(2)
	}
	if list { // the inventory of the M0.1 report: package, interface, method count
		key := func(e Entry) string { return e.Package + "." + e.Name }
		sort.SliceStable(all, func(i, j int) bool { return key(all[i]) < key(all[j]) })
		for _, e := range all {
			fmt.Printf("%s\t%s\t%d\n", e.Package, e.Name, e.Count)
		}
		return
	}
	for _, e := range over {
		fmt.Fprintln(os.Stderr, e.Violation())
	}
	if len(over) > 0 {
		os.Exit(1)
	}
	fmt.Printf("methodcount: %d interfaces, none above %d methods\n", len(all), MaxMethods)
}
