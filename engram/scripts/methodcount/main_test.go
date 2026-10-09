package main

import (
	"os"
	"path/filepath"
	"testing"
)

// probe writes src as package p of a throw-away module and returns the module directory.
func probe(t *testing.T, src string) string { return probeFiles(t, map[string]string{"x.go": src}) }

// probeFiles writes the given files (name -> body) next to a go.mod.
func probeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	all := map[string]string{"go.mod": "module probe\n\ngo 1.25\n"}
	for k, v := range files {
		all[k] = v
	}
	for name, body := range all {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want map[string]int // violating interface -> method count
	}{
		{"five is fine", `package p
type A interface { M1(); M2(); M3(); M4(); M5() }`, nil},
		{"six is a violation", `package p
type A interface { M1(); M2(); M3(); M4(); M5(); M6() }`, map[string]int{"A": 6}},
		{"one-line six", `package p
type A interface{ M1(); M2(); M3(); M4(); M5(); M6() }`, map[string]int{"A": 6}},
		{"embedding a same-package interface counts", `package p
type A interface { M1(); M2(); M3() }
type B interface { A; M4(); M5(); M6() }`, map[string]int{"B": 6}},
		// Review F1 probe 1: io.ReadWriteCloser has Read, Write and Close; with A, B, C that is six.
		{"embedded interface of another package counts its methods", `package p
import "io"
type Wide interface { io.ReadWriteCloser; A(); B(); C() }`, map[string]int{"Wide": 6}},
		// Review F1 probe 2: G has three methods, H adds three.
		{"embedded generic instantiation counts its methods", `package p
type G[T any] interface { M1() T; M2() T; M3() T }
type H interface { G[int]; M4(); M5(); M6() }`, map[string]int{"H": 6}},
		{"a generic interface is counted itself", `package p
type G[T any] interface { M1() T; M2() T; M3() T; M4() T; M5() T; M6() T }`, map[string]int{"G": 6}},
		{"a diamond of embeddings counts each method once", `package p
type Base interface { M1(); M2(); M3() }
type L interface { Base; M4() }
type R interface { Base; M5() }
type D interface { L; R }`, nil},
		{"embedding a foreign interface within the limit", `package p
import "fmt"
type A interface { fmt.Stringer; M1(); M2(); M3(); M4() }`, nil},
		// Review-2 finding N1: files behind the repository's build tags and local type declarations are checked too.
		{"a tagged file is loaded", "//go:build integration\n\npackage p\n" +
			`type A interface { M1(); M2(); M3(); M4(); M5(); M6() }`, map[string]int{"A": 6}},
		{"a tag outside the set is not loaded", "//go:build neverset\n\npackage p\n" +
			`type A interface { M1(); M2(); M3(); M4(); M5(); M6() }` + "\ntype B interface { M1() }", nil},
		{"an interface declared in a function body", `package p
func f() {
	type local interface { M1(); M2(); M3(); M4(); M5(); M6() }
	var _ local
}`, map[string]int{"local": 6}},
		{"a local alias is not a declaration", `package p
type A interface { M1(); M2(); M3(); M4(); M5() }
func f() { type L = A; var _ L }`, nil},
		{"anonymous interfaces are not type declarations", `package p
func f(x interface { M1(); M2(); M3(); M4(); M5(); M6() }) {}`, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, over, err := Inventory(probe(t, tc.src), []string{"./..."})
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]int{}
			for _, e := range over {
				got[e.Name] = e.Count
			}
			if len(got) != len(tc.want) {
				t.Fatalf("violations = %v, want %v", got, tc.want)
			}
			for k, n := range tc.want {
				if got[k] != n {
					t.Errorf("violations = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestInventoryReportsBrokenBuilds(t *testing.T) {
	if _, _, err := Inventory(probe(t, "package p\nvar x = undefinedName\n"), []string{"./..."}); err == nil {
		t.Error("a package that does not type-check must fail the lint, not pass it")
	}
}

func TestRepositoryInterfacesAreWithinTheLimit(t *testing.T) {
	all, over, err := Inventory("../..", []string{"./internal/...", "./adapters/..."})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 100 {
		t.Fatalf("only %d interfaces found: the load is broken", len(all))
	}
	for _, e := range over {
		t.Error(e.Violation())
	}
}

// TestCheckTestFiles is review F14: most tagged code is _test.go, so a six-method interface there must fail the lint,
// package-level or local, behind a tag or not, in an external test package too, and be reported once.
func TestCheckTestFiles(t *testing.T) {
	six := "type A interface { M1(); M2(); M3(); M4(); M5(); M6() }"
	tests := []struct {
		name  string
		files map[string]string
		want  int // violations
	}{
		{"package-level in a test file", map[string]string{"x.go": "package p\n", "x_test.go": "package p\n" + six}, 1},
		{"local in a test file", map[string]string{"x.go": "package p\n",
			"x_test.go": "package p\nfunc f() {\n" + six + "\nvar _ A\n}"}, 1},
		{"in a tagged test file", map[string]string{"x.go": "package p\n",
			"x_test.go": "//go:build integration\n\npackage p\n" + six}, 1},
		{"in an external test package", map[string]string{"x.go": "package p\n",
			"x_test.go": "package p_test\n" + six}, 1},
		{"a test file within the limit", map[string]string{"x.go": "package p\n",
			"x_test.go": "package p\ntype A interface { M1(); M2() }"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, over, err := Inventory(probeFiles(t, tc.files), []string{"./..."})
			if err != nil {
				t.Fatal(err)
			}
			if len(over) != tc.want {
				t.Errorf("violations = %v; want %d", over, tc.want)
			}
		})
	}
}
