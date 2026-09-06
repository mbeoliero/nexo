package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mbeoliero/kit/log"
	"github.com/pressly/goose/v3"

	"github.com/mbeoliero/nexo/migrations"
)

// Apply takes driver and dsn rather than the whole DbConfig: those are the only two fields it
// reads, and the signature then matches gormstore.New / pgstore.New in the sibling packages.
func Apply(ctx context.Context, driver, dsn string) error {
	sqlDriver, dialect := driverFor(driver)
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return fmt.Errorf("migrate: open: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("migrate: ping: %w", err)
	}

	dir, err := fs.Sub(migrations.FS, driver)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	p, err := goose.NewProvider(dialect, db, dir, goose.WithVerbose(false))
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	results, err := p.Up(ctx)
	for _, r := range results {
		log.CtxInfo(ctx, "migrate: applied %s in %s", r.Source.Path, r.Duration)
	}
	if err != nil {
		return fmt.Errorf("migrate: apply: %w", err)
	}
	log.CtxInfo(ctx, "migrate: %d applied, driver=%s", len(results), driver)
	return nil
}

func driverFor(driver string) (sqlDriver string, dialect goose.Dialect) {
	if driver == "mysql" {
		return "mysql", goose.DialectMySQL
	}
	return "pgx", goose.DialectPostgres
}
