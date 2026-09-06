package pgstore

import (
	"context"
	"os"
	"testing"

	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func TestUsers(t *testing.T) {
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NEXO_TEST_PG_DSN not set")
	}
	s, err := New(t.Context(), dsn, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	storetest.Reset(t, func(ctx context.Context, q string) error { _, err := s.pool.Exec(ctx, q); return err })
	storetest.RunUsers(t, s)
	storetest.RunGroups(t, s)
	storetest.RunConversations(t, s)
	storetest.RunMessages(t, s)
	storetest.RunOnline(t, s)
	storetest.RunInsertConstraints(t, s)
}
