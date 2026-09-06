package postgres

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"

	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/bus/bustest"
)

func TestBus(t *testing.T) {
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NEXO_TEST_PG_DSN not set")
	}
	bustest.Run(t, func(t *testing.T) bus.Bus {
		b, err := New(t.Context(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		return bustest.Join(t, b)
	})
}

func TestReconnectAfterBackendKilled(t *testing.T) {
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NEXO_TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	name := "nexo-test-" + uuid.NewV7().String()
	b, err := New(ctx, namedDsn(t, dsn, name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	bustest.RunReconnect(t, b, name, func() {
		rows, err := b.pool.Query(ctx, `SELECT pid FROM pg_stat_activity
			WHERE application_name = $1 AND datname = current_database() AND query = 'LISTEN nexo_events'`, name)
		if err != nil {
			t.Fatal(err)
		}
		pid, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int32])
		if err != nil {
			t.Fatalf("identify owned listener: %v", err)
		}
		var killed bool
		if err := b.pool.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&killed); err != nil {
			t.Fatal(err)
		}
		if !killed {
			t.Fatal("owned listener was not terminated")
		}
	})
}

// ConnString preserves the input, not edits to ConnConfig.RuntimeParams.
func namedDsn(t *testing.T, dsn, name string) string {
	t.Helper()
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("application_name", name)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " application_name=" + name
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.ConnString()
}
