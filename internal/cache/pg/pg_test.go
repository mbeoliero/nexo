package pg

import (
	"context"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/internal/cache"
	"github.com/mbeoliero/nexo/internal/cache/cachetest"
	"github.com/mbeoliero/nexo/internal/cache/pg/gen"
)

func TestSuite(t *testing.T) {
	c := testCache(t)
	cachetest.Run(t, c)
}

func TestCleanupPreservesForeignRows(t *testing.T) {
	c := testCache(t)
	ctx := t.Context()
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := tx.Rollback(ctx); err != nil {
			t.Errorf("rollback cleaner test: %v", err)
		}
	})

	p := cache.KeyPrefix + "test:" + uuid.NewV7().String() + ":"
	_, err = tx.Exec(ctx, `INSERT INTO cache (key, value, expires_at)
		VALUES ($1, 'live', now() + interval '1 hour'), ($2, 'expired', now() - interval '1 hour')`,
		p+"live", p+"expired")
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup is global; a transaction-local shadow table confines it to owned rows.
	if _, err := tx.Exec(ctx, "CREATE TEMP TABLE cache (LIKE cache INCLUDING ALL) ON COMMIT DROP"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO cache (key, value, expires_at)
		VALUES ('expired', 'v', now() - interval '1 hour'), ('live', 'v', now() + interval '1 hour')`)
	if err != nil {
		t.Fatal(err)
	}
	q := gen.New(tx)
	if n, err := q.Cleanup(ctx, 100); err != nil || n != 1 {
		t.Fatalf("cleanup: deleted %d, %v", n, err)
	}
	if v, err := q.Get(ctx, "live"); err != nil || v != "v" {
		t.Fatalf("cleanup changed live temp row: value=%q err=%v", v, err)
	}
	if n, err := q.Cleanup(ctx, 100); err != nil || n != 0 {
		t.Fatalf("repeated cleanup: deleted %d, %v", n, err)
	}
	if _, err := tx.Exec(ctx, "DROP TABLE pg_temp.cache"); err != nil {
		t.Fatal(err)
	}
	var preserved int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM cache
		WHERE (key = $1 AND value = 'live' AND expires_at = now() + interval '1 hour')
		   OR (key = $2 AND value = 'expired' AND expires_at = now() - interval '1 hour')`,
		p+"live", p+"expired").Scan(&preserved)
	if err != nil || preserved != 2 {
		t.Fatalf("foreign rows preserved: got %d, want 2, err=%v", preserved, err)
	}
}

func testCache(t *testing.T) *Cache {
	t.Helper()
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NEXO_TEST_PG_DSN not set")
	}
	c, err := New(t.Context(), dsn, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
