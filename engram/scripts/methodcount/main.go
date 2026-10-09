// Command methodcount is the method-count lint of PLAN.md section 2.1 (register N132, N140, N157): every `type X
// interface { ... }` under the given packages has at most five methods in its COMPLETE method set. The count comes from
// go/types, so an embedded interface of another package (io.ReadWriteCloser), an embedded generic instantiation and a
// chain of embeddings all contribute their full method set, and splitting an interface by embedding hides nothing. Test
// files and generated code are not loaded. Usage: go run ./scripts/methodcount [-list] ./internal/... ./adapters/...
package main

import (
	"fmt"
	"go/token"
	"go/types"
	"os"
	"sort"

	"golang.org/x/tools/go/packages"
)

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

// Inventory type-checks the packages matched by patterns (relative to dir, test files excluded) and returns every
// declared interface type, sorted by package path and name, plus the ones above the limit.
func Inventory(dir string, patterns []string) (all, over []Entry, err error) {
	cfg := &packages.Config{
		Dir: dir,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedImports | packages.NeedDeps | packages.NeedTypes |
			packages.NeedSyntax | packages.NeedTypesInfo,
		Tests: false,
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
	for _, p := range pkgs {
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			it, ok := tn.Type().Underlying().(*types.Interface)
			if !ok {
				continue
			}
			e := Entry{Package: p.Name, Name: name, Pos: p.Fset.Position(tn.Pos()), Count: it.NumMethods()}
			all = append(all, e)
			if e.Count > MaxMethods {
				over = append(over, e)
			}
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
