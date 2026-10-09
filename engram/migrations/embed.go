// Package migrations embeds the goose SQL migrations of the two databases Engram runs (PLAN.md section 9.2): the shard
// schema (migrations/shard, identical on every shard) and, from M0.3 on, the catalog schema (migrations/catalog).
// engramctl embeds them so a migration never depends on files next to the binary, and the T3 test harness applies the
// very same bytes through the very same code path (internal/store/migrate).
package migrations

import "embed"

// Shard holds migrations/shard/*.sql.
//
//go:embed shard/*.sql
var Shard embed.FS
