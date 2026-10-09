// Command engramctl is the operator CLI (PLAN.md section 1.1). Every subcommand lives in its own file and registers itself
// with Register from an init function, so the chains that own `migrate`, `index`, `catalog`, `formal`, `backup` and the
// rest never edit a shared file. The dispatcher is deliberately minimal; flags are parsed by each subcommand.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
)

// Command is one engramctl subcommand. Run receives the arguments after the subcommand name.
type Command struct {
	Name  string
	Short string
	Run   func(ctx context.Context, args []string) error
}

var commands = map[string]Command{}

// Register adds a subcommand; a duplicate name is a programming error and panics at init.
func Register(c Command) {
	if _, dup := commands[c.Name]; dup {
		panic("engramctl: duplicate subcommand " + c.Name)
	}
	commands[c.Name] = c
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: engramctl <command> [args]")
	names := make([]string, 0, len(commands))
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-12s %s\n", n, commands[n].Short)
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	c, ok := commands[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "engramctl: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err := c.Run(context.Background(), os.Args[2:]); err != nil {
		fmt.Fprintf(os.Stderr, "engramctl %s: %v\n", c.Name, err)
		os.Exit(1)
	}
}
