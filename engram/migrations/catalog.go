// Package migrations embeds the goose SQL migrations of the two databases Engram runs (PLAN.md section 9.2): the shard
// schema (migrations/shard, identical on every shard) and the catalog schema (migrations/catalog). This file carries
// the catalog half (M0.3); the shard half (M0.2) sits in embed.go on its own branch, so that the two merge without a
// conflict. engramctl embeds the files, so a migration never depends on files next to the binary.
package migrations

import "embed"

// Catalog holds migrations/catalog/*.sql.
//
//go:embed catalog/*.sql
var Catalog embed.FS
