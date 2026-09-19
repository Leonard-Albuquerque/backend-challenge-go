// Package migrations embeds the versioned SQL migrations.
package migrations

import "embed"

// FS contains the *.up.sql and *.down.sql files.
//
//go:embed *.sql
var FS embed.FS
