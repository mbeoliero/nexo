package gormstore

import (
	"context"
	"os"
	"testing"

	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func TestUsers(t *testing.T) {
	for driver, env := range map[string]string{"postgres": "NEXO_TEST_PG_DSN", "mysql": "NEXO_TEST_MYSQL_DSN"} {
		t.Run(driver, func(t *testing.T) {
			dsn := os.Getenv(env)
			if dsn == "" {
				t.Skipf("%s not set", env)
			}
			s, err := New(driver, dsn, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			sqlDb, _ := s.db.DB()
			storetest.Reset(t, func(ctx context.Context, q string) error { _, err := sqlDb.ExecContext(ctx, q); return err })
			storetest.RunUsers(t, s)
			storetest.RunGroups(t, s)
			storetest.RunConversations(t, s)
			storetest.RunMessages(t, s)
			storetest.RunOnline(t, s)
			storetest.RunInsertConstraints(t, s)
		})
	}
}
