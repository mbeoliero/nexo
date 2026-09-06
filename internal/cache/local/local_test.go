package local

import (
	"testing"

	"github.com/mbeoliero/nexo/internal/cache/cachetest"
)

func TestSuite(t *testing.T) {
	c := New()
	t.Cleanup(func() { _ = c.Close() })
	cachetest.Run(t, c)
}
