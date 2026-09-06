package redis

import (
	"os"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/onlinestore/onlinetest"
)

func TestStore(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	s, err := New(t.Context(), addr, "", 0, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	onlinetest.Run(t, s, func(now func() time.Time) { s.now = now }, onlinetest.Opts{Purgeless: true})
}
