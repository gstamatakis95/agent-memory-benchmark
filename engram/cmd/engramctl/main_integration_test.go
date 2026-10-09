//go:build integration

package main

import (
	"os"
	"testing"

	"github.com/gstamatakis95/engram/internal/store/pgtest"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m)) }
