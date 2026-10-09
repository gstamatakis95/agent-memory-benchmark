package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gstamatakis95/engram/internal/prompts"
)

// runPin writes the HASH pin of a prompt version that has none yet: `gendocs pin extract/v2`. A released version is
// never re-pinned (PLAN.md section 6.0: a file, once released, is never edited): an existing HASH that differs from the
// content is an error that tells the author to create the next version directory instead.
func runPin(root string, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: gendocs pin <name>/v<N>")
	}
	name, ver, ok := strings.Cut(args[0], "/v")
	if !ok {
		return fmt.Errorf("gendocs pin: %q is not <name>/v<N>", args[0])
	}
	var n int
	if _, err := fmt.Sscanf(ver, "%d", &n); err != nil {
		return fmt.Errorf("gendocs pin: version %q: %w", ver, err)
	}
	p, err := prompts.Get(name, n)
	if err != nil {
		return err
	}
	path := filepath.Join(root, "prompts", name, fmt.Sprintf("v%d", n), "HASH")
	if p.Pin != "" {
		if p.Pin == p.Hash.String() {
			fmt.Printf("%s is already pinned to %s\n", p.Meta.ID, p.Pin)
			return nil
		}
		return fmt.Errorf("%s is released (pinned to %s) but its files hash to %s: a released prompt is never edited; "+
			"create %s/v%d instead", p.Meta.ID, p.Pin, p.Hash, name, n+1)
	}
	if err := os.WriteFile(path, []byte(p.Hash.String()+"\n"), 0o644); err != nil {
		return err
	}
	fmt.Printf("%s pinned to %s\n", p.Meta.ID, p.Hash)
	return nil
}
