// Package migrations embeds the goose SQL migrations so that the migrate
// binary and the test suite apply the exact same schema.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
