// Command engram-worker runs the Temporal worker, the outbox relay and the move executor (PLAN.md section 1.1, D1). It
// is a composition root: the only kind of package that may import everything, and it wires dependencies by constructor.
// In M0.1 it wires nothing yet; the milestones that own the wiring (M0.3, M1.1, M1.6) fill it in.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "engram-worker: composition root not wired yet (M0.1 stub)")
	os.Exit(2)
}
