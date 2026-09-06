package storetest

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestResetRequiresDisposable(t *testing.T) {
	if os.Getenv("NEXO_TEST_RESET_PROBE") == "1" {
		Reset(t, func(context.Context, string) error {
			t.Fatal("unsafe reset executed SQL")
			return nil
		})
		return
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "0", "true"} {
		t.Run("refuse-"+value, func(t *testing.T) {
			t.Setenv("NEXO_TEST_DISPOSABLE", value)
			cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestResetRequiresDisposable$")
			cmd.Env = append(os.Environ(), "NEXO_TEST_RESET_PROBE=1")
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "requires NEXO_TEST_DISPOSABLE=1") {
				t.Fatalf("guard: err=%v output=%s", err, out)
			}
			if strings.Contains(string(out), "unsafe reset executed SQL") {
				t.Fatalf("guard ran SQL: %s", out)
			}
		})
	}
	t.Run("allow", func(t *testing.T) {
		t.Setenv("NEXO_TEST_DISPOSABLE", "1")
		queries := []string{}
		Reset(t, func(_ context.Context, query string) error {
			queries = append(queries, query)
			return nil
		})
		want := []string{}
		for _, table := range tables {
			want = append(want, "DELETE FROM "+table)
		}
		if !slices.Equal(queries, want) {
			t.Fatalf("queries=%v, want %v", queries, want)
		}
	})
}
