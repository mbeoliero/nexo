package storetest

import (
	"context"
	"os"
	"testing"
)

var tables = []string{"messages", "user_conversations", "conversations", "group_members", "chat_groups", "online_conns", "users"}

func Reset(t *testing.T, exec func(context.Context, string) error) {
	t.Helper()
	if os.Getenv("NEXO_TEST_DISPOSABLE") != "1" {
		t.Fatal("table reset requires NEXO_TEST_DISPOSABLE=1; use make test-all or a dedicated disposable database")
	}
	for _, table := range tables {
		if err := exec(t.Context(), "DELETE FROM "+table); err != nil {
			t.Fatalf("Reset %s: %v", table, err)
		}
	}
}
