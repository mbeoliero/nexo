package migrations

import "embed"

//go:embed postgres/*.sql mysql/*.sql
var FS embed.FS
